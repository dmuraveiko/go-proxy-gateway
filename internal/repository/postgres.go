package repository

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNoPermit = errors.New("host limit reached")
var ErrRequestConflict = errors.New("request_id already belongs to another request")

type Repository struct{ pool *pgxpool.Pool }

type Operation struct {
	Request       contracts.HTTPRequest
	Attempts      int
	DispatchToken string
}

type Delivery struct {
	ID, Kind, ClientID, Subject, MessageType string
	Payload                                  []byte
}

type WebhookRoute struct {
	ID, OwnerClientID, Name, Mode, ResponderClientID string
	TokenHash                                        []byte
	StaticResponse                                   contracts.StaticHTTPResponse
	ResponseTimeout                                  time.Duration
	MaxBodyBytes                                     int64
	Enabled                                          bool
}

type WebhookControlOperation struct {
	CommandID, ClientID, CommandType, Action string
	WebhookID, SubscriberID                  string
	Payload                                  []byte
	Result                                   contracts.WebhookControlResult
	Route                                    WebhookRoute
	Subscribers                              []string
}

type Stats struct {
	AwaitingACK, Ready, Dispatching, Completed, Unknown, Deliveries int64
}

//go:embed migrations/*.sql
var migrations embed.FS

func Open(ctx context.Context, dsn string, maxConnections int) (*Repository, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = int32(maxConnections)
	pool, err := pgxpool.NewWithConfig(ctx, config)
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

func (r *Repository) BindProxyID(ctx context.Context, proxyID string) error {
	tag, err := r.pool.Exec(ctx, `INSERT INTO proxy_identity(singleton,proxy_id) VALUES(true,$1) ON CONFLICT(singleton) DO NOTHING`, proxyID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var existing string
	if err = r.pool.QueryRow(ctx, `SELECT proxy_id FROM proxy_identity WHERE singleton=true`).Scan(&existing); err != nil {
		return err
	}
	if existing != proxyID {
		return fmt.Errorf("database belongs to proxy_id %q, not %q", existing, proxyID)
	}
	return nil
}

func (r *Repository) Migrate(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS proxy_schema_migrations(version bigint PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Name() < entries[right].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		version, parseErr := strconv.ParseInt(strings.SplitN(entry.Name(), "_", 2)[0], 10, 64)
		if parseErr != nil {
			return parseErr
		}
		var exists bool
		if err = r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM proxy_schema_migrations WHERE version=$1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, readErr := migrations.ReadFile("migrations/" + entry.Name())
		if readErr != nil {
			return readErr
		}
		transaction, beginErr := r.pool.Begin(ctx)
		if beginErr != nil {
			return beginErr
		}
		if _, err = transaction.Exec(ctx, string(sql)); err == nil {
			_, err = transaction.Exec(ctx, `INSERT INTO proxy_schema_migrations(version) VALUES($1)`, version)
		}
		if err == nil {
			err = transaction.Commit(ctx)
		} else {
			_ = transaction.Rollback(ctx)
		}
		if err != nil {
			return fmt.Errorf("migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func pgInterval(duration time.Duration) string { return fmt.Sprintf("%f seconds", duration.Seconds()) }

func jsonEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	normalizedLeft, _ := json.Marshal(leftValue)
	normalizedRight, _ := json.Marshal(rightValue)
	return string(normalizedLeft) == string(normalizedRight)
}

func sameHTTPRequest(existing []byte, incoming contracts.HTTPRequest) bool {
	var stored contracts.HTTPRequest
	if json.Unmarshal(existing, &stored) != nil {
		return false
	}
	stored.CreatedAt = time.Time{}
	incoming.CreatedAt = time.Time{}
	left, _ := json.Marshal(stored)
	right, _ := json.Marshal(incoming)
	return jsonEqual(left, right)
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
