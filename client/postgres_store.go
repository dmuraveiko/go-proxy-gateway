package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultTablePrefix = "natsproxyclient_"

var prefixPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

type PostgresStoreOption func(*postgresStoreOptions) error
type postgresStoreOptions struct{ prefix string }

func WithTablePrefix(prefix string) PostgresStoreOption {
	return func(options *postgresStoreOptions) error {
		if !prefixPattern.MatchString(prefix) {
			return errors.New("table prefix must contain only letters, digits and underscores and cannot start with a digit")
		}
		options.prefix = prefix
		return nil
	}
}

type PostgresStore struct {
	pool       *pgxpool.Pool
	prefix     string
	operations string
	callbacks  string
	owned      bool
}

func NewPostgresStore(ctx context.Context, pool *pgxpool.Pool, options ...PostgresStoreOption) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("PostgreSQL pool is required")
	}
	config := postgresStoreOptions{prefix: DefaultTablePrefix}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	store := &PostgresStore{
		pool: pool, prefix: config.prefix,
		operations: quoteIdentifier(config.prefix + "operations"),
		callbacks:  quoteIdentifier(config.prefix + "callback_events"),
	}
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func OpenPostgresStore(ctx context.Context, dsn string, options ...PostgresStoreOption) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	store, err := NewPostgresStore(ctx, pool, options...)
	if err != nil {
		pool.Close()
		return nil, err
	}
	store.owned = true
	return store, nil
}

func (s *PostgresStore) Close() {
	if s != nil && s.owned {
		s.pool.Close()
	}
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  request_id text PRIMARY KEY,
  request jsonb NOT NULL,
  result jsonb,
  state text NOT NULL CHECK (state IN ('outgoing','accepted','result_saved','complete')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS %s ON %s(updated_at) WHERE state <> 'complete';
CREATE TABLE IF NOT EXISTS %s (
  delivery_id text PRIMARY KEY,
  event jsonb NOT NULL,
  completed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS %s ON %s(updated_at) WHERE completed = false;
`, s.operations, quoteIdentifier(s.prefix+"operations_pending_idx"), s.operations,
		s.callbacks, quoteIdentifier(s.prefix+"callback_pending_idx"), s.callbacks)
	_, err := s.pool.Exec(ctx, query)
	return err
}

func (s *PostgresStore) SaveOutgoing(ctx context.Context, request Request) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO %s(request_id,request,state) VALUES($1,$2,'outgoing') ON CONFLICT(request_id) DO NOTHING`, s.operations)
	tag, err := s.pool.Exec(ctx, query, request.RequestID, payload)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	var existing []byte
	if err = s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT request FROM %s WHERE request_id=$1`, s.operations), request.RequestID).Scan(&existing); err != nil {
		return err
	}
	var saved Request
	if json.Unmarshal(existing, &saved) != nil || !sameRequest(saved, request) {
		return ErrRequestConflict
	}
	return nil
}

func (s *PostgresStore) Load(ctx context.Context, id string) (StoredOperation, error) {
	var operation StoredOperation
	var request, result []byte
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT request,COALESCE(result,'null'::jsonb),state FROM %s WHERE request_id=$1`, s.operations), id).Scan(&request, &result, &operation.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return operation, ErrOperationNotFound
	}
	if err != nil {
		return operation, err
	}
	if err = json.Unmarshal(request, &operation.Request); err != nil {
		return operation, err
	}
	if string(result) != "null" {
		var value Result
		if err = json.Unmarshal(result, &value); err != nil {
			return operation, err
		}
		operation.Result = &value
	}
	return operation, nil
}

func (s *PostgresStore) ListPending(ctx context.Context, limit int) ([]StoredOperation, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT request,COALESCE(result,'null'::jsonb),state FROM %s WHERE state<>'complete' ORDER BY created_at LIMIT $1`, s.operations), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var operations []StoredOperation
	for rows.Next() {
		var operation StoredOperation
		var request, result []byte
		if err = rows.Scan(&request, &result, &operation.State); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(request, &operation.Request); err != nil {
			return nil, err
		}
		if string(result) != "null" {
			var value Result
			if err = json.Unmarshal(result, &value); err != nil {
				return nil, err
			}
			operation.Result = &value
		}
		operations = append(operations, operation)
	}
	return operations, rows.Err()
}

func (s *PostgresStore) MarkAccepted(ctx context.Context, id string) error {
	query := fmt.Sprintf(`UPDATE %s SET state=CASE WHEN state='outgoing' THEN 'accepted' ELSE state END,updated_at=now() WHERE request_id=$1`, s.operations)
	tag, err := s.pool.Exec(ctx, query, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrOperationNotFound
	}
	return err
}

func (s *PostgresStore) SaveResult(ctx context.Context, result Result) (Result, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)
	var current []byte
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(result,'null'::jsonb) FROM %s WHERE request_id=$1 FOR UPDATE`, s.operations), result.RequestID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, ErrOperationNotFound
	}
	if err != nil {
		return Result{}, err
	}
	if string(current) != "null" {
		var existing Result
		if err = json.Unmarshal(current, &existing); err != nil {
			return Result{}, err
		}
		if existing.State != "unknown" && result.State == "unknown" {
			return existing, tx.Commit(ctx)
		}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return Result{}, err
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET result=$2,state='result_saved',updated_at=now() WHERE request_id=$1`, s.operations), result.RequestID, payload); err != nil {
		return Result{}, err
	}
	return result, tx.Commit(ctx)
}

func (s *PostgresStore) MarkComplete(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET state='complete',updated_at=now() WHERE request_id=$1`, s.operations), id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrOperationNotFound
	}
	return err
}

func (s *PostgresStore) SaveCallback(ctx context.Context, event contracts.WebhookEvent) (bool, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf(`INSERT INTO %s(delivery_id,event) VALUES($1,$2) ON CONFLICT(delivery_id) DO UPDATE SET updated_at=now() RETURNING completed`, s.callbacks)
	var complete bool
	err = s.pool.QueryRow(ctx, query, event.DeliveryID, payload).Scan(&complete)
	return complete, err
}

func (s *PostgresStore) MarkCallbackComplete(ctx context.Context, deliveryID string) error {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET completed=true,updated_at=now() WHERE delivery_id=$1`, s.callbacks), deliveryID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrOperationNotFound
	}
	return err
}

// Cleanup removes only fully completed client operations and callbacks.
func (s *PostgresStore) Cleanup(ctx context.Context, before time.Time, batch int) (int64, error) {
	if batch <= 0 {
		batch = 1000
	}
	operations, err := s.pool.Exec(ctx, fmt.Sprintf(`WITH doomed AS (SELECT request_id FROM %s WHERE state='complete' AND updated_at<$1 ORDER BY updated_at LIMIT $2) DELETE FROM %s WHERE request_id IN (SELECT request_id FROM doomed)`, s.operations, s.operations), before, batch)
	if err != nil {
		return 0, err
	}
	callbacks, err := s.pool.Exec(ctx, fmt.Sprintf(`WITH doomed AS (SELECT delivery_id FROM %s WHERE completed=true AND updated_at<$1 ORDER BY updated_at LIMIT $2) DELETE FROM %s WHERE delivery_id IN (SELECT delivery_id FROM doomed)`, s.callbacks, s.callbacks), before, batch)
	if err != nil {
		return operations.RowsAffected(), err
	}
	return operations.RowsAffected() + callbacks.RowsAffected(), nil
}

func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func jsonEqual(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	normalizedA, _ := json.Marshal(a)
	normalizedB, _ := json.Marshal(b)
	return string(normalizedA) == string(normalizedB)
}

func sameRequest(left, right Request) bool {
	left.CreatedAt = right.CreatedAt
	left.Timeout = right.Timeout
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && jsonEqual(a, b)
}

var _ Store = (*PostgresStore)(nil)
var _ CallbackStore = (*PostgresStore)(nil)
