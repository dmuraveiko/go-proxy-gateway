package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

func (r *Repository) RecoverExpiredDispatches(ctx context.Context) (int64, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer transaction.Rollback(ctx)
	reserved, err := transaction.Exec(ctx, `UPDATE proxy_http_requests SET status='ready',dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE status='reserved' AND lease_until<now()`)
	if err != nil {
		return 0, err
	}
	rows, err := transaction.Query(ctx, `SELECT request_id,client_id,proxy_id,attempts FROM proxy_http_requests WHERE status='dispatching' AND lease_until<now() FOR UPDATE`)
	if err != nil {
		return 0, err
	}
	type staleDispatch struct {
		requestID, clientID, proxyID string
		attempts                     int
	}
	var stale []staleDispatch
	for rows.Next() {
		var dispatch staleDispatch
		if err = rows.Scan(&dispatch.requestID, &dispatch.clientID, &dispatch.proxyID, &dispatch.attempts); err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, dispatch)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, dispatch := range stale {
		result := contracts.HTTPResult{
			ResultID: "result_" + dispatch.requestID, RequestID: dispatch.requestID,
			ProxyID: dispatch.proxyID, State: "unknown", ErrorCode: "proxy_restarted_during_http",
			Error: "HTTP outcome could not be recovered", Attempts: dispatch.attempts,
			CompletedAt: time.Now().UTC(),
		}
		payload, _ := json.Marshal(result)
		if _, err = transaction.Exec(ctx, `UPDATE proxy_http_requests SET status='unknown',result=$2,last_error=$3,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1`, dispatch.requestID, payload, result.Error); err != nil {
			return 0, err
		}
		subject := "client." + dispatch.clientID + ".proxy." + dispatch.proxyID + ".results"
		if _, err = transaction.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,request_id) VALUES($1,'result',$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, result.ResultID, dispatch.clientID, subject, contracts.TypeHTTPResult, payload, dispatch.requestID); err != nil {
			return 0, err
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return 0, err
	}
	return reserved.RowsAffected() + int64(len(stale)), nil
}

func (r *Repository) Cleanup(ctx context.Context, finishedBefore time.Time, batch int) (int64, error) {
	tag, err := r.pool.Exec(ctx, `WITH doomed AS (SELECT request_id FROM proxy_http_requests WHERE status IN ('result_delivered','failed','canceled') AND updated_at<$1 ORDER BY updated_at LIMIT $2), del_delivery AS (DELETE FROM proxy_deliveries WHERE request_id IN (SELECT request_id FROM doomed)) DELETE FROM proxy_http_requests WHERE request_id IN (SELECT request_id FROM doomed)`, finishedBefore, batch)
	if err != nil {
		return 0, err
	}
	_, _ = r.pool.Exec(ctx, `DELETE FROM proxy_host_rate_windows WHERE window_start<now()-interval '1 hour'`)
	_, _ = r.pool.Exec(ctx, `DELETE FROM proxy_host_last_dispatch WHERE dispatched_at<now()-interval '24 hours'`)
	_, _ = r.pool.Exec(ctx, `WITH old_events AS (SELECT event_id FROM proxy_webhook_events e WHERE e.updated_at<$1 AND NOT EXISTS(SELECT 1 FROM proxy_deliveries d WHERE d.webhook_event_id=e.event_id AND d.acknowledged_at IS NULL) ORDER BY e.updated_at LIMIT $2), del_delivery AS (DELETE FROM proxy_deliveries WHERE webhook_event_id IN (SELECT event_id FROM old_events)) DELETE FROM proxy_webhook_events WHERE event_id IN (SELECT event_id FROM old_events)`, finishedBefore, batch)
	_, _ = r.pool.Exec(ctx, `WITH old_commands AS (SELECT command_id FROM proxy_webhook_commands c WHERE c.created_at<$1 AND NOT EXISTS(SELECT 1 FROM proxy_deliveries d WHERE d.command_id=c.command_id AND d.acknowledged_at IS NULL) ORDER BY c.created_at LIMIT $2), del_delivery AS (DELETE FROM proxy_deliveries WHERE command_id IN (SELECT command_id FROM old_commands)) DELETE FROM proxy_webhook_commands WHERE command_id IN (SELECT command_id FROM old_commands)`, finishedBefore, batch)
	_, _ = r.pool.Exec(ctx, `DELETE FROM proxy_webhook_routes r WHERE r.enabled=false AND r.updated_at<$1 AND NOT EXISTS(SELECT 1 FROM proxy_webhook_events e WHERE e.webhook_id=r.webhook_id)`, finishedBefore)
	return tag.RowsAffected(), nil
}

func (r *Repository) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := r.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='awaiting_acceptance_ack'),count(*) FILTER(WHERE status IN ('ready','reserved','retry_wait')),count(*) FILTER(WHERE status='dispatching'),count(*) FILTER(WHERE status IN ('http_completed','result_delivered')),count(*) FILTER(WHERE status='unknown'),(SELECT count(*) FROM proxy_deliveries WHERE acknowledged_at IS NULL) FROM proxy_http_requests`).Scan(&stats.AwaitingACK, &stats.Ready, &stats.Dispatching, &stats.Completed, &stats.Unknown, &stats.Deliveries)
	return stats, err
}
