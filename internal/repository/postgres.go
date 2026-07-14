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

	"github.com/jackc/pgx/v5/pgxpool"
	"proxy-server/internal/contracts"
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
type Stats struct {
	AwaitingACK, Ready, Dispatching, Completed, Unknown, Deliveries int64
}

//go:embed migrations/*.sql
var migrations embed.FS

func Open(ctx context.Context, dsn string, maxConns int) (*Repository, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(maxConns)
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
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		version, err := strconv.ParseInt(strings.SplitN(e.Name(), "_", 2)[0], 10, 64)
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
		sql, err := migrations.ReadFile("migrations/" + e.Name())
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
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
		if err != nil {
			return fmt.Errorf("migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (r *Repository) AcceptHTTPRequest(ctx context.Context, req contracts.HTTPRequest, acceptance contracts.Acceptance, subject string) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	ab, err := json.Marshal(acceptance)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO proxy_http_requests(request_id,client_id,proxy_id,request,status) VALUES($1,$2,$3,$4,'awaiting_acceptance_ack') ON CONFLICT(request_id) DO NOTHING`, req.RequestID, req.ClientID, req.ProxyID, b)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var client, proxy string
		var existing []byte
		if err = tx.QueryRow(ctx, `SELECT client_id,proxy_id,request FROM proxy_http_requests WHERE request_id=$1`, req.RequestID).Scan(&client, &proxy, &existing); err != nil {
			return err
		}
		if client != req.ClientID || proxy != req.ProxyID || !sameHTTPRequest(existing, req) {
			return ErrRequestConflict
		}
		// A restarted client replays the same request to resume the handshakes.
		// Reopening these deliveries never re-executes HTTP.
		if _, err = tx.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=NULL,next_attempt_at=now(),lease_until=NULL WHERE request_id=$1 AND kind IN ('acceptance','result')`, req.RequestID); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,request_id) VALUES($1,'acceptance',$2,$3,$4,$5,$6) ON CONFLICT(delivery_id) DO NOTHING`, acceptance.DeliveryID, req.ClientID, subject, contracts.TypeAcceptance, ab, req.RequestID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ConfirmAcceptance(ctx context.Context, ack contracts.DeliveryACK) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var requestID, clientID, kind string
	if err = tx.QueryRow(ctx, `SELECT request_id,client_id,kind FROM proxy_deliveries WHERE delivery_id=$1 FOR UPDATE`, ack.DeliveryID).Scan(&requestID, &clientID, &kind); err != nil {
		return err
	}
	if kind != "acceptance" || clientID != ack.ClientID || requestID != ack.RequestID {
		return errors.New("acceptance ACK does not match delivery")
	}
	if _, err = tx.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1`, ack.DeliveryID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE proxy_http_requests SET status='ready',updated_at=now() WHERE request_id=$1 AND status='awaiting_acceptance_ack'`, ack.RequestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ReserveRequest(ctx context.Context, lease time.Duration) (Operation, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Operation{}, err
	}
	defer tx.Rollback(ctx)
	var op Operation
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT request_id,request,attempts FROM proxy_http_requests WHERE status IN ('ready','retry_wait') AND next_attempt_at<=now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&op.Request.RequestID, &raw, &op.Attempts)
	if err != nil {
		return op, err
	}
	if err = json.Unmarshal(raw, &op.Request); err != nil {
		return op, err
	}
	op.DispatchToken = randomID()
	_, err = tx.Exec(ctx, `UPDATE proxy_http_requests SET status='reserved',dispatch_token=$2,lease_until=now()+$3::interval,updated_at=now() WHERE request_id=$1`, op.Request.RequestID, op.DispatchToken, pgInterval(lease))
	if err != nil {
		return op, err
	}
	if err = tx.Commit(ctx); err != nil {
		return op, err
	}
	return op, nil
}

func (r *Repository) BeginDispatch(ctx context.Context, requestID, token string, lease time.Duration) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_http_requests SET status='dispatching',attempts=attempts+1,lease_until=now()+$3::interval,updated_at=now() WHERE request_id=$1 AND status='reserved' AND dispatch_token=$2`, requestID, token, pgInterval(lease))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("dispatch ownership lost")
	}
	return nil
}
func (r *Repository) ReleaseReserved(ctx context.Context, requestID, token string) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_http_requests SET status='ready',dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1 AND status='reserved' AND dispatch_token=$2`, requestID, token)
	return err
}

func (r *Repository) SaveHTTPResult(ctx context.Context, token, clientID, subject string, result contracts.HTTPResult) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	state := "http_completed"
	if result.State == "unknown" {
		state = "unknown"
	}
	tag, err := tx.Exec(ctx, `UPDATE proxy_http_requests SET status=$4,result=$3,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1 AND status='dispatching' AND dispatch_token=$2`, result.RequestID, token, b, state)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var state string
		if err = tx.QueryRow(ctx, `SELECT status FROM proxy_http_requests WHERE request_id=$1`, result.RequestID).Scan(&state); err != nil {
			return err
		}
		if state != "http_completed" && state != "result_delivered" && state != "unknown" {
			return errors.New("cannot save result after dispatch ownership was lost")
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,request_id) VALUES($1,'result',$2,$3,$4,$5,$6) ON CONFLICT(delivery_id) DO NOTHING`, result.ResultID, clientID, subject, contracts.TypeHTTPResult, b, result.RequestID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (r *Repository) ScheduleRetry(ctx context.Context, requestID, token, reason string, next time.Time) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_http_requests SET status='retry_wait',last_error=$3,next_attempt_at=$4,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1 AND status='dispatching' AND dispatch_token=$2`, requestID, token, reason, next)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("retry ownership lost")
	}
	return err
}
func (r *Repository) MarkUnknown(ctx context.Context, requestID, token, reason string) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_http_requests SET status='unknown',last_error=$3,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1 AND status='dispatching' AND dispatch_token=$2`, requestID, token, reason)
	return err
}

func (r *Repository) ConfirmResult(ctx context.Context, ack contracts.DeliveryACK) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var requestID, clientID, kind string
	if err = tx.QueryRow(ctx, `SELECT request_id,client_id,kind FROM proxy_deliveries WHERE delivery_id=$1 FOR UPDATE`, ack.DeliveryID).Scan(&requestID, &clientID, &kind); err != nil {
		return err
	}
	if kind != "result" || clientID != ack.ClientID || requestID != ack.RequestID {
		return errors.New("result ACK does not match delivery")
	}
	if _, err = tx.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1`, ack.DeliveryID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE proxy_http_requests SET status='result_delivered',updated_at=now() WHERE request_id=$1 AND status='http_completed'`, requestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ClaimDelivery(ctx context.Context, lease time.Duration) (Delivery, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Delivery{}, err
	}
	defer tx.Rollback(ctx)
	var d Delivery
	err = tx.QueryRow(ctx, `SELECT delivery_id,kind,client_id,subject,message_type,payload FROM proxy_deliveries WHERE acknowledged_at IS NULL AND next_attempt_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&d.ID, &d.Kind, &d.ClientID, &d.Subject, &d.MessageType, &d.Payload)
	if err != nil {
		return d, err
	}
	_, err = tx.Exec(ctx, `UPDATE proxy_deliveries SET lease_until=now()+$2::interval,attempts=attempts+1 WHERE delivery_id=$1`, d.ID, pgInterval(lease))
	if err != nil {
		return d, err
	}
	if err = tx.Commit(ctx); err != nil {
		return d, err
	}
	return d, nil
}
func (r *Repository) RescheduleDelivery(ctx context.Context, id string, after time.Duration) error {
	_, err := r.pool.Exec(ctx, `UPDATE proxy_deliveries SET next_attempt_at=now()+$2::interval,lease_until=NULL WHERE delivery_id=$1 AND acknowledged_at IS NULL`, id, pgInterval(after))
	return err
}

func (r *Repository) AcquireHostPermit(ctx context.Context, host string, rps, concurrency int, lease time.Duration) (string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, host); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM proxy_host_permits WHERE lease_until<now()`); err != nil {
		return "", err
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM proxy_host_permits WHERE host=$1`, host).Scan(&active); err != nil {
		return "", err
	}
	if active >= concurrency {
		return "", ErrNoPermit
	}
	tag, err := tx.Exec(ctx, `INSERT INTO proxy_host_rate_windows(host,window_start,used) VALUES($1,date_trunc('second',now()),1) ON CONFLICT(host,window_start) DO UPDATE SET used=proxy_host_rate_windows.used+1 WHERE proxy_host_rate_windows.used<$2`, host, rps)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNoPermit
	}
	token := randomID()
	if _, err = tx.Exec(ctx, `INSERT INTO proxy_host_permits(token,host,lease_until) VALUES($1,$2,now()+$3::interval)`, token, host, pgInterval(lease)); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return token, nil
}
func (r *Repository) ReleaseHostPermit(ctx context.Context, token string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM proxy_host_permits WHERE token=$1`, token)
	return err
}

func (r *Repository) RegisterWebhook(ctx context.Context, route WebhookRoute, subscribers []string) error {
	b, err := json.Marshal(route.StaticResponse)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO proxy_webhook_routes(webhook_id,owner_client_id,name,path_token_hash,mode,static_response,responder_client_id,response_timeout_ms,max_body_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(webhook_id) DO NOTHING`, route.ID, route.OwnerClientID, route.Name, route.TokenHash, route.Mode, b, nullString(route.ResponderClientID), route.ResponseTimeout.Milliseconds(), route.MaxBodyBytes)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var owner string
		var token []byte
		if err = tx.QueryRow(ctx, `SELECT owner_client_id,path_token_hash FROM proxy_webhook_routes WHERE webhook_id=$1`, route.ID).Scan(&owner, &token); err != nil {
			return err
		}
		if owner != route.OwnerClientID || hex.EncodeToString(token) != hex.EncodeToString(route.TokenHash) {
			return errors.New("webhook command conflicts with an existing route")
		}
	}
	for _, clientID := range subscribers {
		if _, err = tx.Exec(ctx, `INSERT INTO proxy_webhook_subscribers(webhook_id,client_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, route.ID, clientID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (r *Repository) GetWebhookRoute(ctx context.Context, id string) (WebhookRoute, error) {
	var x WebhookRoute
	var raw []byte
	var ms int64
	var responder *string
	err := r.pool.QueryRow(ctx, `SELECT webhook_id,owner_client_id,name,path_token_hash,mode,static_response,responder_client_id,response_timeout_ms,max_body_bytes,enabled FROM proxy_webhook_routes WHERE webhook_id=$1`, id).Scan(&x.ID, &x.OwnerClientID, &x.Name, &x.TokenHash, &x.Mode, &raw, &responder, &ms, &x.MaxBodyBytes, &x.Enabled)
	if err != nil {
		return x, err
	}
	if responder != nil {
		x.ResponderClientID = *responder
	}
	x.ResponseTimeout = time.Duration(ms) * time.Millisecond
	err = json.Unmarshal(raw, &x.StaticResponse)
	return x, err
}
func (r *Repository) SubscribeWebhook(ctx context.Context, owner, webhookID, subscriber string) error {
	tag, err := r.pool.Exec(ctx, `INSERT INTO proxy_webhook_subscribers(webhook_id,client_id) SELECT webhook_id,$3 FROM proxy_webhook_routes WHERE webhook_id=$1 AND owner_client_id=$2 ON CONFLICT DO NOTHING`, webhookID, owner, subscriber)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("webhook not found or not owned by client")
	}
	return err
}
func (r *Repository) DeleteWebhook(ctx context.Context, owner, webhookID string) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_webhook_routes SET enabled=false,updated_at=now() WHERE webhook_id=$1 AND owner_client_id=$2`, webhookID, owner)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("webhook not found or not owned by client")
	}
	return err
}

func (r *Repository) SaveWebhookEvent(ctx context.Context, event contracts.WebhookEvent, route WebhookRoute, subject func(string) string, replySubject string) ([]Delivery, error) {
	base, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	state := "received"
	if route.Mode == "delegated" {
		state = "awaiting_response"
	}
	_, err = tx.Exec(ctx, `INSERT INTO proxy_webhook_events(event_id,webhook_id,request,status) VALUES($1,$2,$3,$4)`, event.EventID, event.WebhookID, base, state)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT client_id FROM proxy_webhook_subscribers WHERE webhook_id=$1`, event.WebhookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clients []string
	for rows.Next() {
		var c string
		if err = rows.Scan(&c); err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if route.Mode == "delegated" && !contains(clients, route.ResponderClientID) {
		clients = append(clients, route.ResponderClientID)
	}
	var out []Delivery
	for _, clientID := range clients {
		e := event
		e.DeliveryID = "wdel_" + randomID()
		if clientID == route.ResponderClientID && route.Mode == "delegated" {
			e.ReplySubject = replySubject
		}
		payload, _ := json.Marshal(e)
		d := Delivery{ID: e.DeliveryID, Kind: "webhook", ClientID: clientID, Subject: subject(clientID), MessageType: contracts.TypeWebhookEvent, Payload: payload}
		_, err = tx.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,webhook_event_id) VALUES($1,'webhook',$2,$3,$4,$5,$6)`, d.ID, d.ClientID, d.Subject, d.MessageType, d.Payload, event.EventID)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
func (r *Repository) ConfirmWebhook(ctx context.Context, ack contracts.DeliveryACK) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1 AND client_id=$2 AND kind='webhook'`, ack.DeliveryID, ack.ClientID)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("webhook ACK does not match delivery")
	}
	return err
}
func (r *Repository) SaveWebhookResponse(ctx context.Context, response contracts.WebhookResponse) error {
	b, err := json.Marshal(response)
	if err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE proxy_webhook_events e SET response=$2,status='responded',updated_at=now() FROM proxy_webhook_routes r WHERE e.event_id=$1 AND r.webhook_id=e.webhook_id AND r.mode='delegated' AND r.responder_client_id=$3 AND e.status='awaiting_response'`, response.EventID, b, response.ClientID)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("client is not the delegated webhook responder")
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE webhook_event_id=$1 AND client_id=$2 AND kind='webhook'`, response.EventID, response.ClientID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (r *Repository) MarkWebhookTimedOut(ctx context.Context, eventID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE proxy_webhook_events SET status='timed_out',updated_at=now() WHERE event_id=$1 AND status='awaiting_response'`, eventID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE proxy_deliveries d SET acknowledged_at=COALESCE(d.acknowledged_at,now()),lease_until=NULL FROM proxy_webhook_events e,proxy_webhook_routes r WHERE e.event_id=$1 AND r.webhook_id=e.webhook_id AND d.webhook_event_id=e.event_id AND d.client_id=r.responder_client_id`, eventID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) RecoverExpiredDispatches(ctx context.Context) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	reserved, err := tx.Exec(ctx, `UPDATE proxy_http_requests SET status='ready',dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE status='reserved' AND lease_until<now()`)
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT request_id,client_id,proxy_id,attempts FROM proxy_http_requests WHERE status='dispatching' AND lease_until<now() FOR UPDATE`)
	if err != nil {
		return 0, err
	}
	type stale struct {
		requestID, clientID, proxyID string
		attempts                     int
	}
	var items []stale
	for rows.Next() {
		var x stale
		if err = rows.Scan(&x.requestID, &x.clientID, &x.proxyID, &x.attempts); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, x)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, x := range items {
		result := contracts.HTTPResult{ResultID: "result_" + x.requestID, RequestID: x.requestID, ProxyID: x.proxyID, State: "unknown", ErrorCode: "proxy_restarted_during_http", Error: "HTTP outcome could not be recovered", Attempts: x.attempts, CompletedAt: time.Now().UTC()}
		b, _ := json.Marshal(result)
		if _, err = tx.Exec(ctx, `UPDATE proxy_http_requests SET status='unknown',result=$2,last_error=$3,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1`, x.requestID, b, result.Error); err != nil {
			return 0, err
		}
		subject := "client." + x.clientID + ".proxy." + x.proxyID + ".results"
		if _, err = tx.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,request_id) VALUES($1,'result',$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, result.ResultID, x.clientID, subject, contracts.TypeHTTPResult, b, x.requestID); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return reserved.RowsAffected() + int64(len(items)), nil
}
func (r *Repository) Cleanup(ctx context.Context, finishedBefore time.Time, unknownBefore *time.Time, batch int) (int64, error) {
	var unknown any = nil
	if unknownBefore != nil {
		unknown = *unknownBefore
	}
	tag, err := r.pool.Exec(ctx, `WITH doomed AS (SELECT request_id FROM proxy_http_requests WHERE ((status IN ('result_delivered','failed','canceled') AND updated_at<$1) OR ($2::timestamptz IS NOT NULL AND status='unknown' AND updated_at<$2)) ORDER BY updated_at LIMIT $3), del_delivery AS (DELETE FROM proxy_deliveries WHERE request_id IN (SELECT request_id FROM doomed)) DELETE FROM proxy_http_requests WHERE request_id IN (SELECT request_id FROM doomed)`, finishedBefore, unknown, batch)
	if err != nil {
		return 0, err
	}
	_, _ = r.pool.Exec(ctx, `DELETE FROM proxy_host_rate_windows WHERE window_start<now()-interval '1 hour'`)
	_, _ = r.pool.Exec(ctx, `WITH old_events AS (SELECT event_id FROM proxy_webhook_events e WHERE e.updated_at<$1 AND NOT EXISTS(SELECT 1 FROM proxy_deliveries d WHERE d.webhook_event_id=e.event_id AND d.acknowledged_at IS NULL) ORDER BY e.updated_at LIMIT $2), del_delivery AS (DELETE FROM proxy_deliveries WHERE webhook_event_id IN (SELECT event_id FROM old_events)) DELETE FROM proxy_webhook_events WHERE event_id IN (SELECT event_id FROM old_events)`, finishedBefore, batch)
	return tag.RowsAffected(), nil
}
func (r *Repository) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := r.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='awaiting_acceptance_ack'),count(*) FILTER(WHERE status IN ('ready','reserved','retry_wait')),count(*) FILTER(WHERE status='dispatching'),count(*) FILTER(WHERE status IN ('http_completed','result_delivered')),count(*) FILTER(WHERE status='unknown'),(SELECT count(*) FROM proxy_deliveries WHERE acknowledged_at IS NULL) FROM proxy_http_requests`).Scan(&s.AwaitingACK, &s.Ready, &s.Dispatching, &s.Completed, &s.Unknown, &s.Deliveries)
	return s, err
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func pgInterval(d time.Duration) string { return fmt.Sprintf("%f seconds", d.Seconds()) }
func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	nx, _ := json.Marshal(x)
	ny, _ := json.Marshal(y)
	return string(nx) == string(ny)
}
func sameHTTPRequest(existing []byte, incoming contracts.HTTPRequest) bool {
	var stored contracts.HTTPRequest
	if json.Unmarshal(existing, &stored) != nil {
		return false
	}
	stored.CreatedAt = time.Time{}
	incoming.CreatedAt = time.Time{}
	a, _ := json.Marshal(stored)
	b, _ := json.Marshal(incoming)
	return jsonEqual(a, b)
}
func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
