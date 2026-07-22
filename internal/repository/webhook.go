package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (r *Repository) ApplyWebhookControl(ctx context.Context, operation WebhookControlOperation, subject string) (contracts.WebhookControlResult, error) {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		metrics.DBRequests.WithLabelValues("out", "error", "callback_registration").Inc()
		return contracts.WebhookControlResult{}, err
	}
	defer transaction.Rollback(ctx)
	metrics.DBRequests.WithLabelValues("out", "success", "callback_registration").Inc()

	var existingClientID, existingType string
	var existingPayload, existingResult []byte
	err = transaction.QueryRow(ctx, `SELECT client_id,command_type,payload,result FROM proxy_webhook_commands WHERE command_id=$1 FOR UPDATE`, operation.CommandID).Scan(&existingClientID, &existingType, &existingPayload, &existingResult)
	if err == nil {
		if existingClientID != operation.ClientID || existingType != operation.CommandType || !jsonEqual(existingPayload, operation.Payload) {
			return contracts.WebhookControlResult{}, ErrRequestConflict
		}
		var result contracts.WebhookControlResult
		if err = json.Unmarshal(existingResult, &result); err != nil {
			return result, err
		}
		if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=NULL,next_attempt_at=now(),lease_until=NULL WHERE command_id=$1 AND kind='control'`, operation.CommandID); err != nil {
			return result, err
		}
		return result, transaction.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return contracts.WebhookControlResult{}, err
	}

	switch operation.Action {
	case "register":
		err = insertWebhookRoute(ctx, transaction, operation.Route)
		if err == nil {
			for _, clientID := range operation.Subscribers {
				if _, err = transaction.Exec(ctx, `INSERT INTO proxy_webhook_subscribers(webhook_id,client_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, operation.Route.ID, clientID); err != nil {
					break
				}
			}
		}
	case "update":
		var staticResponse []byte
		staticResponse, err = json.Marshal(operation.Route.StaticResponse)
		if err == nil {
			var tag pgconn.CommandTag
			tag, err = transaction.Exec(ctx, `UPDATE proxy_webhook_routes SET name=$3,mode=$4,static_response=$5,responder_client_id=$6,response_timeout_ms=$7,max_body_bytes=$8,enabled=true,updated_at=now() WHERE webhook_id=$1 AND owner_client_id=$2`, operation.WebhookID, operation.ClientID, operation.Route.Name, operation.Route.Mode, staticResponse, nullString(operation.Route.ResponderClientID), operation.Route.ResponseTimeout.Milliseconds(), operation.Route.MaxBodyBytes)
			if err == nil && tag.RowsAffected() == 0 {
				failWebhookControl(&operation.Result)
			}
		}
	case "subscribe", "unsubscribe":
		var ownerClientID string
		err = transaction.QueryRow(ctx, `SELECT owner_client_id FROM proxy_webhook_routes WHERE webhook_id=$1`, operation.WebhookID).Scan(&ownerClientID)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && ownerClientID != operation.ClientID) {
			err = nil
			failWebhookControl(&operation.Result)
		}
		if err == nil && operation.Result.Success {
			if operation.Action == "subscribe" {
				_, err = transaction.Exec(ctx, `INSERT INTO proxy_webhook_subscribers(webhook_id,client_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, operation.WebhookID, operation.SubscriberID)
			} else {
				_, err = transaction.Exec(ctx, `DELETE FROM proxy_webhook_subscribers WHERE webhook_id=$1 AND client_id=$2`, operation.WebhookID, operation.SubscriberID)
			}
		}
	case "delete":
		var tag pgconn.CommandTag
		tag, err = transaction.Exec(ctx, `UPDATE proxy_webhook_routes SET enabled=false,updated_at=now() WHERE webhook_id=$1 AND owner_client_id=$2`, operation.WebhookID, operation.ClientID)
		if err == nil && tag.RowsAffected() == 0 {
			failWebhookControl(&operation.Result)
		}
	case "error":
		// Validation failures are stored and delivered through the same durable path.
	default:
		err = errors.New("unsupported webhook control action")
	}
	if err != nil {
		return contracts.WebhookControlResult{}, err
	}
	resultPayload, err := json.Marshal(operation.Result)
	if err != nil {
		return contracts.WebhookControlResult{}, err
	}
	if _, err = transaction.Exec(ctx, `INSERT INTO proxy_webhook_commands(command_id,client_id,command_type,payload,result) VALUES($1,$2,$3,$4,$5)`, operation.CommandID, operation.ClientID, operation.CommandType, operation.Payload, resultPayload); err != nil {
		return contracts.WebhookControlResult{}, err
	}
	if _, err = transaction.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,command_id) VALUES($1,'control',$2,$3,$4,$5,$6)`, operation.Result.DeliveryID, operation.ClientID, subject, contracts.TypeWebhookControlResult, resultPayload, operation.CommandID); err != nil {
		return contracts.WebhookControlResult{}, err
	}
	return operation.Result, transaction.Commit(ctx)
}

func insertWebhookRoute(ctx context.Context, transaction pgx.Tx, route WebhookRoute) error {
	staticResponse, err := json.Marshal(route.StaticResponse)
	if err != nil {
		return err
	}
	_, err = transaction.Exec(ctx, `INSERT INTO proxy_webhook_routes(webhook_id,owner_client_id,name,path_token_hash,mode,static_response,responder_client_id,response_timeout_ms,max_body_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, route.ID, route.OwnerClientID, route.Name, route.TokenHash, route.Mode, staticResponse, nullString(route.ResponderClientID), route.ResponseTimeout.Milliseconds(), route.MaxBodyBytes)
	return err
}

func failWebhookControl(result *contracts.WebhookControlResult) {
	result.Success = false
	result.ErrorCode = "webhook_not_found"
	result.Error = "webhook not found or not owned by client"
}

func (r *Repository) ConfirmWebhookControl(ctx context.Context, ack contracts.DeliveryACK) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1 AND client_id=$2 AND kind='control'`, ack.DeliveryID, ack.ClientID)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("webhook control ACK does not match delivery")
	}
	return err
}

func (r *Repository) GetWebhookRoute(ctx context.Context, webhookID string) (WebhookRoute, error) {
	var route WebhookRoute
	var staticResponse []byte
	var responseTimeoutMilliseconds int64
	var responderClientID *string
	err := r.pool.QueryRow(ctx, `SELECT webhook_id,owner_client_id,name,path_token_hash,mode,static_response,responder_client_id,response_timeout_ms,max_body_bytes,enabled FROM proxy_webhook_routes WHERE webhook_id=$1`, webhookID).Scan(&route.ID, &route.OwnerClientID, &route.Name, &route.TokenHash, &route.Mode, &staticResponse, &responderClientID, &responseTimeoutMilliseconds, &route.MaxBodyBytes, &route.Enabled)
	if err != nil {
		metrics.DBRequests.WithLabelValues("in", "error", "callback").Inc()
		return route, err
	}
	metrics.DBRequests.WithLabelValues("in", "success", "callback").Inc()
	if responderClientID != nil {
		route.ResponderClientID = *responderClientID
	}
	route.ResponseTimeout = time.Duration(responseTimeoutMilliseconds) * time.Millisecond
	err = json.Unmarshal(staticResponse, &route.StaticResponse)
	return route, err
}

func (r *Repository) SaveWebhookEvent(ctx context.Context, event contracts.WebhookEvent, route WebhookRoute, subject func(string) string, replySubject string) ([]Delivery, error) {
	basePayload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		metrics.DBRequests.WithLabelValues("out", "error", "callback").Inc()
		return nil, err
	}
	defer transaction.Rollback(ctx)
	metrics.DBRequests.WithLabelValues("out", "success", "callback").Inc()
	state := "received"
	if route.Mode == "delegated" {
		state = "awaiting_response"
	}
	if _, err = transaction.Exec(ctx, `INSERT INTO proxy_webhook_events(event_id,webhook_id,request,status) VALUES($1,$2,$3,$4)`, event.EventID, event.WebhookID, basePayload, state); err != nil {
		return nil, err
	}
	rows, err := transaction.Query(ctx, `SELECT client_id FROM proxy_webhook_subscribers WHERE webhook_id=$1`, event.WebhookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clientIDs []string
	for rows.Next() {
		var clientID string
		if err = rows.Scan(&clientID); err != nil {
			return nil, err
		}
		clientIDs = append(clientIDs, clientID)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if route.Mode == "delegated" && !contains(clientIDs, route.ResponderClientID) {
		clientIDs = append(clientIDs, route.ResponderClientID)
	}
	var deliveries []Delivery
	for _, clientID := range clientIDs {
		deliveryEvent := event
		deliveryEvent.DeliveryID = "wdel_" + randomID()
		if clientID == route.ResponderClientID && route.Mode == "delegated" {
			deliveryEvent.ReplySubject = replySubject
		}
		payload, _ := json.Marshal(deliveryEvent)
		delivery := Delivery{ID: deliveryEvent.DeliveryID, Kind: "webhook", ClientID: clientID, Subject: subject(clientID), MessageType: contracts.TypeWebhookEvent, Payload: payload}
		if _, err = transaction.Exec(ctx, `INSERT INTO proxy_deliveries(delivery_id,kind,client_id,subject,message_type,payload,webhook_event_id) VALUES($1,'webhook',$2,$3,$4,$5,$6)`, delivery.ID, delivery.ClientID, delivery.Subject, delivery.MessageType, delivery.Payload, event.EventID); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	if err = transaction.Commit(ctx); err != nil {
		return nil, err
	}
	return deliveries, nil
}

func (r *Repository) ConfirmWebhook(ctx context.Context, ack contracts.DeliveryACK) error {
	tag, err := r.pool.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1 AND client_id=$2 AND kind='webhook'`, ack.DeliveryID, ack.ClientID)
	if err == nil && tag.RowsAffected() == 0 {
		return errors.New("webhook ACK does not match delivery")
	}
	return err
}

func (r *Repository) SaveWebhookResponse(ctx context.Context, response contracts.WebhookResponse) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	var deliveryExists bool
	if err = transaction.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM proxy_deliveries WHERE delivery_id=$1 AND webhook_event_id=$2 AND client_id=$3 AND kind='webhook')`, response.DeliveryID, response.EventID, response.ClientID).Scan(&deliveryExists); err != nil {
		return err
	}
	if !deliveryExists {
		return errors.New("webhook response does not match its delivery")
	}
	tag, err := transaction.Exec(ctx, `UPDATE proxy_webhook_events e SET response=$2,status='responded',updated_at=now() FROM proxy_webhook_routes r WHERE e.event_id=$1 AND r.webhook_id=e.webhook_id AND r.mode='delegated' AND r.responder_client_id=$3 AND e.status='awaiting_response'`, response.EventID, payload, response.ClientID)
	if err == nil && tag.RowsAffected() == 0 {
		var existing []byte
		if scanErr := transaction.QueryRow(ctx, `SELECT response FROM proxy_webhook_events e JOIN proxy_webhook_routes r ON r.webhook_id=e.webhook_id WHERE e.event_id=$1 AND r.mode='delegated' AND r.responder_client_id=$2 AND e.status='responded'`, response.EventID, response.ClientID).Scan(&existing); scanErr != nil || !jsonEqual(existing, payload) {
			return errors.New("client is not the delegated webhook responder or response conflicts")
		}
	}
	if err != nil {
		return err
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries SET acknowledged_at=COALESCE(acknowledged_at,now()),lease_until=NULL WHERE delivery_id=$1 AND webhook_event_id=$2 AND client_id=$3 AND kind='webhook'`, response.DeliveryID, response.EventID, response.ClientID); err != nil {
		return err
	}
	return transaction.Commit(ctx)
}

func (r *Repository) MarkWebhookTimedOut(ctx context.Context, eventID string) error {
	transaction, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer transaction.Rollback(ctx)
	if _, err = transaction.Exec(ctx, `UPDATE proxy_webhook_events SET status='timed_out',updated_at=now() WHERE event_id=$1 AND status='awaiting_response'`, eventID); err != nil {
		return err
	}
	if _, err = transaction.Exec(ctx, `UPDATE proxy_deliveries d SET acknowledged_at=COALESCE(d.acknowledged_at,now()),lease_until=NULL FROM proxy_webhook_events e,proxy_webhook_routes r WHERE e.event_id=$1 AND r.webhook_id=e.webhook_id AND d.webhook_event_id=e.event_id AND d.client_id=r.responder_client_id`, eventID); err != nil {
		return err
	}
	return transaction.Commit(ctx)
}
