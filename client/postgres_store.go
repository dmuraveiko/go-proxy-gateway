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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, s.prefix+"schema"); err != nil {
		return err
	}
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  proxy_id text NOT NULL,
  request_id text NOT NULL,
  request jsonb NOT NULL,
  result jsonb,
  state text NOT NULL CHECK (state IN ('outgoing','accepted','result_saved','complete')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(proxy_id,request_id)
);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS proxy_id text;
UPDATE %s SET proxy_id=COALESCE(NULLIF(request->>'proxy_id',''),'legacy') WHERE proxy_id IS NULL;
ALTER TABLE %s ALTER COLUMN proxy_id SET NOT NULL;
CREATE TABLE IF NOT EXISTS %s (
  proxy_id text NOT NULL,
  delivery_id text NOT NULL,
  event jsonb NOT NULL,
  response jsonb,
  completed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(proxy_id,delivery_id)
);
ALTER TABLE %s ADD COLUMN IF NOT EXISTS proxy_id text;
ALTER TABLE %s ADD COLUMN IF NOT EXISTS response jsonb;
UPDATE %s SET proxy_id=COALESCE(NULLIF(event->>'proxy_id',''),'legacy') WHERE proxy_id IS NULL;
ALTER TABLE %s ALTER COLUMN proxy_id SET NOT NULL;
`, s.operations, s.operations, s.operations, s.operations,
		s.callbacks, s.callbacks, s.callbacks, s.callbacks, s.callbacks)
	if _, err = tx.Exec(ctx, query); err != nil {
		return err
	}
	if err = ensureCompositePrimaryKey(ctx, tx, s.operations, "request_id"); err != nil {
		return err
	}
	if err = ensureCompositePrimaryKey(ctx, tx, s.callbacks, "delivery_id"); err != nil {
		return err
	}
	indexes := fmt.Sprintf(`
CREATE INDEX IF NOT EXISTS %s ON %s(proxy_id,updated_at) WHERE state <> 'complete';
CREATE INDEX IF NOT EXISTS %s ON %s(proxy_id,updated_at) WHERE completed = false;
`, quoteIdentifier(s.prefix+"operations_proxy_pending_idx"), s.operations,
		quoteIdentifier(s.prefix+"callback_proxy_pending_idx"), s.callbacks)
	if _, err = tx.Exec(ctx, indexes); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func ensureCompositePrimaryKey(ctx context.Context, tx pgx.Tx, table, idColumn string) error {
	var constraint string
	var columns []string
	err := tx.QueryRow(ctx, `SELECT c.conname,array_agg(a.attname ORDER BY k.ordinality) FROM pg_constraint c JOIN unnest(c.conkey) WITH ORDINALITY AS k(attnum,ordinality) ON true JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum WHERE c.conrelid=to_regclass($1) AND c.contype='p' GROUP BY c.conname`, table).Scan(&constraint, &columns)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if len(columns) == 2 && columns[0] == "proxy_id" && columns[1] == idColumn {
		return nil
	}
	if constraint != "" {
		if _, err = tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s`, table, quoteIdentifier(constraint))); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD PRIMARY KEY(proxy_id,%s)`, table, quoteIdentifier(idColumn)))
	return err
}

func (s *PostgresStore) SaveOutgoing(ctx context.Context, request Request) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO %s(proxy_id,request_id,request,state) VALUES($1,$2,$3,'outgoing') ON CONFLICT(proxy_id,request_id) DO NOTHING`, s.operations)
	tag, err := s.pool.Exec(ctx, query, request.ProxyID, request.RequestID, payload)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	var existing []byte
	if err = s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT request FROM %s WHERE proxy_id=$1 AND request_id=$2`, s.operations), request.ProxyID, request.RequestID).Scan(&existing); err != nil {
		return err
	}
	var saved Request
	if json.Unmarshal(existing, &saved) != nil || !sameRequest(saved, request) {
		return ErrRequestConflict
	}
	return nil
}

func (s *PostgresStore) Load(ctx context.Context, proxyID, id string) (StoredOperation, error) {
	var operation StoredOperation
	var request, result []byte
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT request,COALESCE(result,'null'::jsonb),state FROM %s WHERE proxy_id=$1 AND request_id=$2`, s.operations), proxyID, id).Scan(&request, &result, &operation.State)
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

func (s *PostgresStore) ListPending(ctx context.Context, proxyID string, limit int) ([]StoredOperation, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT request,COALESCE(result,'null'::jsonb),state FROM %s WHERE proxy_id=$1 AND state<>'complete' ORDER BY created_at LIMIT $2`, s.operations), proxyID, limit)
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

func (s *PostgresStore) MarkAccepted(ctx context.Context, proxyID, id string) error {
	query := fmt.Sprintf(`UPDATE %s SET state=CASE WHEN state='outgoing' THEN 'accepted' ELSE state END,updated_at=now() WHERE proxy_id=$1 AND request_id=$2`, s.operations)
	tag, err := s.pool.Exec(ctx, query, proxyID, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrOperationNotFound
	}
	return err
}

func (s *PostgresStore) SaveResult(ctx context.Context, proxyID string, result Result) (Result, error) {
	if result.ProxyID != proxyID {
		return Result{}, ErrRequestConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)
	var current []byte
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(result,'null'::jsonb) FROM %s WHERE proxy_id=$1 AND request_id=$2 FOR UPDATE`, s.operations), proxyID, result.RequestID).Scan(&current)
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
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET result=$3,state='result_saved',updated_at=now() WHERE proxy_id=$1 AND request_id=$2`, s.operations), proxyID, result.RequestID, payload); err != nil {
		return Result{}, err
	}
	return result, tx.Commit(ctx)
}

func (s *PostgresStore) MarkComplete(ctx context.Context, proxyID, id string) error {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET state='complete',updated_at=now() WHERE proxy_id=$1 AND request_id=$2`, s.operations), proxyID, id)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrOperationNotFound
	}
	return err
}

func (s *PostgresStore) SaveCallback(ctx context.Context, event contracts.WebhookEvent) (StoredCallback, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return StoredCallback{}, err
	}
	query := fmt.Sprintf(`INSERT INTO %s(proxy_id,delivery_id,event) VALUES($1,$2,$3) ON CONFLICT(proxy_id,delivery_id) DO NOTHING`, s.callbacks)
	if _, err = s.pool.Exec(ctx, query, event.ProxyID, event.DeliveryID, payload); err != nil {
		return StoredCallback{}, err
	}
	var rawEvent, rawResponse []byte
	var stored StoredCallback
	err = s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT event,COALESCE(response,'null'::jsonb),completed FROM %s WHERE proxy_id=$1 AND delivery_id=$2`, s.callbacks), event.ProxyID, event.DeliveryID).Scan(&rawEvent, &rawResponse, &stored.Completed)
	if err != nil {
		return StoredCallback{}, err
	}
	if err = json.Unmarshal(rawEvent, &stored.Event); err != nil {
		return StoredCallback{}, err
	}
	if !sameCallbackEvent(stored.Event, event) {
		return StoredCallback{}, ErrRequestConflict
	}
	if string(rawResponse) != "null" {
		var response contracts.WebhookResponse
		if err = json.Unmarshal(rawResponse, &response); err != nil {
			return StoredCallback{}, err
		}
		stored.Response = &response
	}
	return stored, nil
}

func (s *PostgresStore) SaveCallbackResponse(ctx context.Context, proxyID string, response contracts.WebhookResponse) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var eventPayload, current []byte
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT event,COALESCE(response,'null'::jsonb) FROM %s WHERE proxy_id=$1 AND delivery_id=$2 FOR UPDATE`, s.callbacks), proxyID, response.DeliveryID).Scan(&eventPayload, &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOperationNotFound
	}
	if err != nil {
		return err
	}
	var event contracts.WebhookEvent
	if json.Unmarshal(eventPayload, &event) != nil || event.ProxyID != proxyID || event.EventID != response.EventID {
		return ErrRequestConflict
	}
	if string(current) != "null" {
		var stored contracts.WebhookResponse
		if json.Unmarshal(current, &stored) != nil || !sameCallbackResponse(stored, response) {
			return ErrRequestConflict
		}
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET response=$3,updated_at=now() WHERE proxy_id=$1 AND delivery_id=$2`, s.callbacks), proxyID, response.DeliveryID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListPendingCallbacks(ctx context.Context, proxyID string, limit int) ([]StoredCallback, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT event,COALESCE(response,'null'::jsonb),completed FROM %s WHERE proxy_id=$1 AND completed=false ORDER BY created_at LIMIT $2`, s.callbacks), proxyID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var callbacks []StoredCallback
	for rows.Next() {
		var stored StoredCallback
		var eventPayload, responsePayload []byte
		if err = rows.Scan(&eventPayload, &responsePayload, &stored.Completed); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(eventPayload, &stored.Event); err != nil {
			return nil, err
		}
		if string(responsePayload) != "null" {
			var response contracts.WebhookResponse
			if err = json.Unmarshal(responsePayload, &response); err != nil {
				return nil, err
			}
			stored.Response = &response
		}
		callbacks = append(callbacks, stored)
	}
	return callbacks, rows.Err()
}

func (s *PostgresStore) MarkCallbackComplete(ctx context.Context, proxyID, deliveryID string) error {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET completed=true,updated_at=now() WHERE proxy_id=$1 AND delivery_id=$2`, s.callbacks), proxyID, deliveryID)
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
	operations, err := s.pool.Exec(ctx, fmt.Sprintf(`WITH doomed AS (SELECT proxy_id,request_id FROM %s WHERE state='complete' AND updated_at<$1 ORDER BY updated_at LIMIT $2) DELETE FROM %s WHERE (proxy_id,request_id) IN (SELECT proxy_id,request_id FROM doomed)`, s.operations, s.operations), before, batch)
	if err != nil {
		return 0, err
	}
	callbacks, err := s.pool.Exec(ctx, fmt.Sprintf(`WITH doomed AS (SELECT proxy_id,delivery_id FROM %s WHERE completed=true AND updated_at<$1 ORDER BY updated_at LIMIT $2) DELETE FROM %s WHERE (proxy_id,delivery_id) IN (SELECT proxy_id,delivery_id FROM doomed)`, s.callbacks, s.callbacks), before, batch)
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

func sameCallbackEvent(left, right contracts.WebhookEvent) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && jsonEqual(a, b)
}

func sameCallbackResponse(left, right contracts.WebhookResponse) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && jsonEqual(a, b)
}

var _ Store = (*PostgresStore)(nil)
var _ CallbackStore = (*PostgresStore)(nil)
