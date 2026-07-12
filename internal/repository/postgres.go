package repository

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"proxy-server/internal/contracts"
)

type Repository struct{ pool *pgxpool.Pool }
type Operation struct {
	Command  contracts.Command
	Attempts int
}
type Outbox struct {
	ID, Subject string
	Payload     []byte
}
type Stats struct {
	Pending, Retrying, Processing, Failed, OutboxPending int64
	OldestPendingSeconds                                 float64
}

//go:embed migrations/*.sql
var migrations embed.FS

func Open(ctx context.Context, dsn string, maxConns int32) (*Repository, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Repository{pool: pool}, nil
}
func (r *Repository) Close()                         { r.pool.Close() }
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }
func (r *Repository) Migrate(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS proxy_schema_migrations(version bigint PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		version, err := strconv.ParseInt(strings.SplitN(entry.Name(), "_", 2)[0], 10, 64)
		if err != nil {
			return err
		}
		var exists bool
		if err = r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM proxy_schema_migrations WHERE version=$1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		tx, err := r.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, string(sql)); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO proxy_schema_migrations(version) VALUES($1)`, version)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %d: %w", version, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) AcceptCommand(ctx context.Context, c contracts.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `INSERT INTO proxy_operations(id, command_type, command_version, command, status, next_attempt_at) VALUES($1,$2,$3,$4,'pending',now()) ON CONFLICT (id) DO NOTHING`, c.ID, c.Type, c.Version, b)
	return err
}

func (r *Repository) ClaimOperation(ctx context.Context, lease time.Duration) (Operation, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback(ctx)
	var raw []byte
	var attempts int
	err = tx.QueryRow(ctx, `SELECT command, attempts FROM proxy_operations WHERE (status IN ('pending','retrying') AND next_attempt_at<=now()) OR (status='processing' AND lease_until<now()) ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&raw, &attempts)
	if err != nil {
		return Operation{}, err
	}
	var c contracts.Command
	if err = json.Unmarshal(raw, &c); err != nil {
		return Operation{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE proxy_operations SET status='processing', attempts=attempts+1, lease_until=now()+$2::interval, updated_at=now() WHERE id=$1`, c.ID, pgInterval(lease))
	if err != nil {
		return Operation{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Operation{}, err
	}
	return Operation{Command: c, Attempts: attempts + 1}, nil
}

func (r *Repository) Retry(ctx context.Context, id, code, message string, next time.Time) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_operations SET status='retrying', last_error_code=$2, last_error=$3, next_attempt_at=$4, lease_until=NULL, updated_at=now() WHERE id=$1`, id, code, message, next)
	return err
}
func (r *Repository) Complete(ctx context.Context, result contracts.Result, subject string) error {
	return r.finish(ctx, result, subject, "completed")
}
func (r *Repository) Fail(ctx context.Context, result contracts.Result, subject, dlqSubject string) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE proxy_operations SET status='failed', result=$2, lease_until=NULL, updated_at=now() WHERE id=$1 AND status='processing'`, result.CommandID, payload)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("operation is not processing")
	}
	for id, destination := range map[string]string{result.CommandID + ":result": subject, result.CommandID + ":dlq": dlqSubject} {
		if _, err = tx.Exec(ctx, `INSERT INTO proxy_outbox(id, subject, payload) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING`, id, destination, payload); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (r *Repository) finish(ctx context.Context, result contracts.Result, subject, status string) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE proxy_operations SET status=$2, result=$3, lease_until=NULL, updated_at=now() WHERE id=$1 AND status='processing'`, result.CommandID, status, payload)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("operation is not processing")
	}
	_, err = tx.Exec(ctx, `INSERT INTO proxy_outbox(id, subject, payload) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING`, result.CommandID+":result", subject, payload)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) SaveWebhook(ctx context.Context, events []contracts.Event, subjectPrefix string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, e := range events {
		payload, marshalErr := json.Marshal(e)
		if marshalErr != nil {
			return marshalErr
		}
		tag, execErr := tx.Exec(ctx, `INSERT INTO proxy_webhook_events(id, provider, event_type, payload) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING`, e.ID, e.Provider, e.Type, payload)
		if execErr != nil {
			return execErr
		}
		if tag.RowsAffected() == 1 {
			if _, execErr = tx.Exec(ctx, `INSERT INTO proxy_outbox(id, subject, payload) VALUES($1,$2,$3)`, e.ID+":event", subjectPrefix+"."+e.Type, payload); execErr != nil {
				return execErr
			}
		}
	}
	return tx.Commit(ctx)
}

func (r *Repository) ClaimOutbox(ctx context.Context, lease time.Duration) (Outbox, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Outbox{}, err
	}
	defer tx.Rollback(ctx)
	var o Outbox
	err = tx.QueryRow(ctx, `SELECT id, subject, payload FROM proxy_outbox WHERE published_at IS NULL AND (lease_until IS NULL OR lease_until<now()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&o.ID, &o.Subject, &o.Payload)
	if err != nil {
		return o, err
	}
	_, err = tx.Exec(ctx, `UPDATE proxy_outbox SET lease_until=now()+$2::interval, publish_attempts=publish_attempts+1 WHERE id=$1`, o.ID, pgInterval(lease))
	if err != nil {
		return o, err
	}
	if err = tx.Commit(ctx); err != nil {
		return o, err
	}
	return o, nil
}
func (r *Repository) MarkPublished(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_outbox SET published_at=now(), lease_until=NULL WHERE id=$1`, id)
	return err
}
func (r *Repository) Release(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_operations SET status='retrying',next_attempt_at=now(),lease_until=NULL,updated_at=now() WHERE id=$1 AND status='processing'`, id)
	return err
}
func (r *Repository) Cleanup(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.pool.Exec(ctx, `WITH deleted_outbox AS (DELETE FROM proxy_outbox WHERE published_at<$1), deleted_events AS (DELETE FROM proxy_webhook_events WHERE received_at<$1), deleted_limits AS (DELETE FROM proxy_rate_limits WHERE window_start<now()-interval '1 hour') DELETE FROM proxy_operations WHERE status IN ('completed','failed') AND updated_at<$1`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
func (r *Repository) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := r.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='pending'),count(*) FILTER(WHERE status='retrying'),count(*) FILTER(WHERE status='processing'),count(*) FILTER(WHERE status='failed'),COALESCE(EXTRACT(EPOCH FROM now()-min(created_at) FILTER(WHERE status IN ('pending','retrying'))),0),(SELECT count(*) FROM proxy_outbox WHERE published_at IS NULL) FROM proxy_operations`).Scan(&s.Pending, &s.Retrying, &s.Processing, &s.Failed, &s.OldestPendingSeconds, &s.OutboxPending)
	return s, err
}
func (r *Repository) AcquireRateToken(ctx context.Context, provider string, limit int) error {
	for {
		tag, err := r.pool.Exec(ctx, `INSERT INTO proxy_rate_limits(provider,window_start,used) VALUES($1,date_trunc('second',now()),1) ON CONFLICT(provider,window_start) DO UPDATE SET used=proxy_rate_limits.used+1 WHERE proxy_rate_limits.used<$2`, provider, limit)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func pgInterval(d time.Duration) string { return fmt.Sprintf("%f seconds", d.Seconds()) }
