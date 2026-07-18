package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

func (r *Repository) AcceptHTTPRequest(ctx context.Context, request contracts.HTTPRequest, acceptance contracts.Acceptance, subject string) error {
	requestPayload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	acceptancePayload, err := json.Marshal(acceptance)
	if err != nil {
		return err
	}
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	tag, err := transaction.Exec(ctx, `INSERT INTO proxy_http_requests(request_id,client_id,proxy_id,request,status) VALUES($1,$2,$3,$4,'awaiting_acceptance_ack') ON CONFLICT(request_id) DO NOTHING`, request.RequestID, request.ClientID, request.ProxyID, requestPayload)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var clientID, proxyID string
		var existing []byte
		if err = transaction.QueryRow(ctx, `SELECT client_id,proxy_id,request FROM proxy_http_requests WHERE request_id=$1`, request.RequestID).Scan(&clientID, &proxyID, &existing); err != nil {
			return err
		}
		if clientID != request.ClientID || proxyID != request.ProxyID || !sameHTTPRequest(existing, request) {
			return ErrRequestConflict
		}
		// Reopen only NATS handshakes. The HTTP request is never re-executed here.
		if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=NULL,next_attempt_at=now(),lease_until=NULL WHERE request_id=$1 AND kind IN ('acceptance','result')`, request.RequestID); err != nil {
			return err
		}
	}
	if _, err = transaction.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,request_id) VALUES($1,'acceptance',$2,$3,$4,$5,$6) ON CONFLICT(delivery_id) DO NOTHING`, acceptance.DeliveryID, request.ClientID, subject, contracts.TypeAcceptance, acceptancePayload, request.RequestID); err != nil {
		return err
	}
	return transaction.Commit(ctx)
}

func (r *Repository) ConfirmAcceptance(ctx context.Context, ack contracts.DeliveryACK) error {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	var requestID, clientID, kind string
	if err = transaction.QueryRow(ctx, `SELECT request_id,client_id,kind FROM proxy_deliveries WHERE delivery_id=$1 FOR UPDATE`, ack.DeliveryID).Scan(&requestID, &clientID, &kind); err != nil {
		return err
	}
	if kind != "acceptance" || clientID != ack.ClientID || requestID != ack.RequestID {
		return errors.New("acceptance ACK does not match delivery")
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1`, ack.DeliveryID); err != nil {
		return err
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_http_requests SET status='ready',updated_at=now() WHERE request_id=$1 AND status='awaiting_acceptance_ack'`, ack.RequestID); err != nil {
		return err
	}
	return transaction.Commit(ctx)
}

func (r *Repository) ReserveRequest(ctx context.Context, lease time.Duration) (Operation, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return Operation{}, err
	}
	defer transaction.Rollback(ctx)
	var operation Operation
	var rawRequest []byte
	err = transaction.QueryRow(ctx, `SELECT request_id,request,attempts FROM proxy_http_requests WHERE status IN ('ready','retry_wait') AND next_attempt_at<=now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&operation.Request.RequestID, &rawRequest, &operation.Attempts)
	if err != nil {
		return operation, err
	}
	if err = json.Unmarshal(rawRequest, &operation.Request); err != nil {
		return operation, err
	}
	operation.DispatchToken = randomID()
	if _, err = transaction.Exec(ctx, `UPDATE proxy_http_requests SET status='reserved',dispatch_token=$2,lease_until=now()+$3::interval,updated_at=now() WHERE request_id=$1`, operation.Request.RequestID, operation.DispatchToken, pgInterval(lease)); err != nil {
		return operation, err
	}
	return operation, transaction.Commit(ctx)
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
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	state := "http_completed"
	if result.State == "unknown" {
		state = "unknown"
	}
	tag, err := transaction.Exec(ctx, `UPDATE proxy_http_requests SET status=$4,result=$3,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1 AND status='dispatching' AND dispatch_token=$2`, result.RequestID, token, payload, state)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var currentState string
		if err = transaction.QueryRow(ctx, `SELECT status FROM proxy_http_requests WHERE request_id=$1`, result.RequestID).Scan(&currentState); err != nil {
			return err
		}
		if currentState != "http_completed" && currentState != "result_delivered" && currentState != "unknown" {
			return errors.New("cannot save result after dispatch ownership was lost")
		}
	}
	if _, err = transaction.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,request_id) VALUES($1,'result',$2,$3,$4,$5,$6) ON CONFLICT(delivery_id) DO NOTHING`, result.ResultID, clientID, subject, contracts.TypeHTTPResult, payload, result.RequestID); err != nil {
		return err
	}
	return transaction.Commit(ctx)
}

func (r *Repository) ScheduleRetry(ctx context.Context, requestID, token, reason string, next time.Time) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_http_requests SET status='retry_wait',last_error=$3,next_attempt_at=$4,dispatch_token=NULL,lease_until=NULL,updated_at=now() WHERE request_id=$1 AND status='dispatching' AND dispatch_token=$2`, requestID, token, reason, next)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("retry ownership lost")
	}
	return err
}

func (r *Repository) ConfirmResult(ctx context.Context, ack contracts.DeliveryACK) error {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	var requestID, clientID, kind string
	if err = transaction.QueryRow(ctx, `SELECT request_id,client_id,kind FROM proxy_deliveries WHERE delivery_id=$1 FOR UPDATE`, ack.DeliveryID).Scan(&requestID, &clientID, &kind); err != nil {
		return err
	}
	if kind != "result" || clientID != ack.ClientID || requestID != ack.RequestID {
		return errors.New("result ACK does not match delivery")
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1`, ack.DeliveryID); err != nil {
		return err
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_http_requests SET status='result_delivered',updated_at=now() WHERE request_id=$1 AND status IN ('http_completed','unknown')`, requestID); err != nil {
		return err
	}
	return transaction.Commit(ctx)
}
