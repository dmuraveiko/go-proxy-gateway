package transport

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/repository"
	"github.com/nats-io/nats.go"
)

func (c *Core) webhookCommandSubject() string { return "proxy." + c.proxyID + ".webhooks.commands" }
func (c *Core) webhookACKSubject() string     { return "proxy." + c.proxyID + ".webhooks.acks" }
func (c *Core) webhookControlACKSubject() string {
	return "proxy." + c.proxyID + ".webhooks.control_acks"
}
func (c *Core) webhookResponseSubject() string {
	return "proxy." + c.proxyID + ".instance." + c.instanceID + ".webhooks.responses"
}

func (c *Core) handleWebhookCommand(message *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientID, payload, messageType, err := c.decodeAny(message)
	if err != nil {
		c.reject("webhook command", err)
		return
	}
	operation, err := c.webhookControlOperation(clientID, messageType, payload)
	if err != nil {
		c.reject("webhook command", err)
		return
	}
	if _, err = c.repo.ApplyWebhookControl(ctx, operation, c.clientSubject(clientID, "webhooks.control_results")); err != nil {
		c.reject("webhook command", err)
	}
}

func (c *Core) webhookControlOperation(clientID, messageType string, payload []byte) (repository.WebhookControlOperation, error) {
	operation := repository.WebhookControlOperation{ClientID: clientID, CommandType: messageType, Payload: append([]byte(nil), payload...)}
	invalid := func(commandID, action, code string, cause error) (repository.WebhookControlOperation, error) {
		if commandID == "" {
			return operation, cause
		}
		operation.CommandID, operation.Action = commandID, "error"
		operation.Result = contracts.WebhookControlResult{CommandID: commandID, DeliveryID: "control_" + commandID, Action: action, Success: false, ErrorCode: code, Error: cause.Error()}
		return operation, nil
	}

	switch messageType {
	case contracts.TypeWebhookRegister:
		var command contracts.WebhookRegister
		if err := json.Unmarshal(payload, &command); err != nil {
			return operation, err
		}
		if command.ClientID != clientID || command.CommandID == "" {
			return invalid(command.CommandID, "register", "invalid_identity", errors.New("command identity mismatch"))
		}
		if command.Mode != "static" && command.Mode != "delegated" {
			return invalid(command.CommandID, "register", "invalid_mode", errors.New("mode must be static or delegated"))
		}
		if command.Mode == "delegated" && command.ResponderID == "" {
			command.ResponderID = clientID
		}
		for _, subscriberID := range append(command.SubscriberIDs, command.ResponderID) {
			if subscriberID != "" && !c.allowedClient(subscriberID) {
				return invalid(command.CommandID, "register", "subscriber_not_allowed", errors.New("subscriber is not allowed"))
			}
		}
		token := c.webhookToken(clientID, command.CommandID)
		route := repository.WebhookRoute{
			ID: "wh_" + stableID(clientID+"\n"+command.CommandID), OwnerClientID: clientID,
			Name: command.Name, Mode: command.Mode, ResponderClientID: command.ResponderID,
			TokenHash: tokenHash(token), StaticResponse: command.StaticResponse,
			ResponseTimeout: command.ResponseTimeout, MaxBodyBytes: command.MaxBodyBytes, Enabled: true,
		}
		c.applyWebhookDefaults(&route)
		operation.CommandID, operation.Action, operation.Route = command.CommandID, "register", route
		operation.Subscribers = dedupe(append(command.SubscriberIDs, clientID))
		operation.Result = contracts.WebhookControlResult{CommandID: command.CommandID, DeliveryID: "control_" + command.CommandID, Action: "register", Success: true, WebhookID: route.ID, URL: c.publicBaseURL + "/v1/webhooks/" + route.ID + "/" + token}
		return operation, nil

	case contracts.TypeWebhookUpdate:
		var command contracts.WebhookUpdate
		if err := json.Unmarshal(payload, &command); err != nil {
			return operation, err
		}
		if command.ClientID != clientID || command.CommandID == "" || command.WebhookID == "" {
			return invalid(command.CommandID, "update", "invalid_identity", errors.New("command identity mismatch"))
		}
		if command.Mode != "static" && command.Mode != "delegated" {
			return invalid(command.CommandID, "update", "invalid_mode", errors.New("mode must be static or delegated"))
		}
		if command.Mode == "delegated" && command.ResponderID == "" {
			command.ResponderID = clientID
		}
		if command.ResponderID != "" && !c.allowedClient(command.ResponderID) {
			return invalid(command.CommandID, "update", "responder_not_allowed", errors.New("responder is not allowed"))
		}
		route := repository.WebhookRoute{ID: command.WebhookID, OwnerClientID: clientID, Name: command.Name, Mode: command.Mode, ResponderClientID: command.ResponderID, StaticResponse: command.StaticResponse, ResponseTimeout: command.ResponseTimeout, MaxBodyBytes: command.MaxBodyBytes, Enabled: true}
		c.applyWebhookDefaults(&route)
		operation.CommandID, operation.Action, operation.WebhookID, operation.Route = command.CommandID, "update", command.WebhookID, route
		operation.Result = contracts.WebhookControlResult{CommandID: command.CommandID, DeliveryID: "control_" + command.CommandID, Action: "update", Success: true, WebhookID: command.WebhookID}
		return operation, nil

	case contracts.TypeWebhookSubscribe, contracts.TypeWebhookUnsubscribe:
		var command contracts.WebhookSubscribe
		if err := json.Unmarshal(payload, &command); err != nil {
			return operation, err
		}
		action := "subscribe"
		if messageType == contracts.TypeWebhookUnsubscribe {
			action = "unsubscribe"
		}
		if command.ClientID != clientID || command.CommandID == "" || command.WebhookID == "" {
			return invalid(command.CommandID, action, "invalid_identity", errors.New("command identity mismatch"))
		}
		if action == "subscribe" && !c.allowedClient(command.SubscriberID) {
			return invalid(command.CommandID, action, "subscriber_not_allowed", errors.New("subscriber is not allowed"))
		}
		operation.CommandID, operation.Action, operation.WebhookID, operation.SubscriberID = command.CommandID, action, command.WebhookID, command.SubscriberID
		operation.Result = contracts.WebhookControlResult{CommandID: command.CommandID, DeliveryID: "control_" + command.CommandID, Action: action, Success: true, WebhookID: command.WebhookID}
		return operation, nil

	case contracts.TypeWebhookDelete:
		var command contracts.WebhookDelete
		if err := json.Unmarshal(payload, &command); err != nil {
			return operation, err
		}
		if command.ClientID != clientID || command.CommandID == "" || command.WebhookID == "" {
			return invalid(command.CommandID, "delete", "invalid_identity", errors.New("command identity mismatch"))
		}
		operation.CommandID, operation.Action, operation.WebhookID = command.CommandID, "delete", command.WebhookID
		operation.Result = contracts.WebhookControlResult{CommandID: command.CommandID, DeliveryID: "control_" + command.CommandID, Action: "delete", Success: true, WebhookID: command.WebhookID}
		return operation, nil
	default:
		return operation, errors.New("unsupported webhook command type")
	}
}

func (c *Core) applyWebhookDefaults(route *repository.WebhookRoute) {
	if route.StaticResponse.StatusCode == 0 {
		route.StaticResponse.StatusCode = http.StatusOK
	}
	if route.ResponseTimeout <= 0 {
		route.ResponseTimeout = 10 * time.Second
	}
	if route.MaxBodyBytes <= 0 {
		route.MaxBodyBytes = 4 << 20
	}
}

func (c *Core) handleWebhookControlACK(message *nats.Msg) {
	c.handleWebhookDeliveryACK(message, contracts.TypeWebhookControlACK, c.repo.ConfirmWebhookControl)
}

func (c *Core) handleWebhookACK(message *nats.Msg) {
	c.handleWebhookDeliveryACK(message, contracts.TypeWebhookEventACK, c.repo.ConfirmWebhook)
}

func (c *Core) handleWebhookDeliveryACK(message *nats.Msg, messageType string, confirm func(context.Context, contracts.DeliveryACK) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientID, payload, err := c.decode(message, messageType)
	if err != nil {
		return
	}
	var ack contracts.DeliveryACK
	if json.Unmarshal(payload, &ack) != nil || ack.ClientID != clientID {
		return
	}
	if confirm(ctx, ack) == nil {
		_ = c.Publish(ctx, c.clientSubject(clientID, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: ack.DeliveryID, ConfirmedAt: time.Now().UTC()})
	}
}

func (c *Core) handleWebhookResponse(message *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientID, payload, err := c.decode(message, contracts.TypeWebhookDelegatedResponse)
	if err != nil {
		return
	}
	var response contracts.WebhookResponse
	if json.Unmarshal(payload, &response) != nil || response.ClientID != clientID {
		return
	}
	if err = c.repo.SaveWebhookResponse(ctx, response); err != nil {
		c.reject("webhook response", err)
		return
	}
	_ = c.Publish(ctx, c.clientSubject(clientID, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: response.DeliveryID, ConfirmedAt: time.Now().UTC()})
	c.mu.Lock()
	waiter := c.webhookWaiters[response.EventID]
	c.mu.Unlock()
	if waiter != nil {
		select {
		case waiter <- response:
		default:
		}
	}
}

// DeliverWebhook is the callback direction: public HTTP -> durable DB fan-out ->
// NATS clients -> optional delegated HTTP response.
func (c *Core) DeliverWebhook(ctx context.Context, event contracts.WebhookEvent, route repository.WebhookRoute) (contracts.WebhookResponse, error) {
	event.ProxyID = c.proxyID
	replySubject := c.webhookResponseSubject()
	var responseChannel chan contracts.WebhookResponse
	if route.Mode == "delegated" {
		responseChannel = make(chan contracts.WebhookResponse, 1)
		c.mu.Lock()
		c.webhookWaiters[event.EventID] = responseChannel
		c.mu.Unlock()
		defer func() {
			c.mu.Lock()
			delete(c.webhookWaiters, event.EventID)
			c.mu.Unlock()
		}()
	}
	deliveries, err := c.repo.SaveWebhookEvent(ctx, event, route, func(clientID string) string {
		return c.clientSubject(clientID, "webhooks.events")
	}, replySubject)
	if err != nil {
		return contracts.WebhookResponse{}, err
	}
	if route.Mode != "delegated" {
		return contracts.WebhookResponse{}, nil
	}
	// Give the responder a low-latency first delivery. The durable delivery loop
	// continues retrying the same delivery until the response/ACK is committed.
	for _, delivery := range deliveries {
		if delivery.ClientID == route.ResponderClientID {
			_ = c.PublishRaw(ctx, delivery.Subject, delivery.MessageType, delivery.Payload)
			break
		}
	}
	select {
	case response := <-responseChannel:
		return response, nil
	case <-ctx.Done():
		_ = c.repo.MarkWebhookTimedOut(context.WithoutCancel(ctx), event.EventID)
		return contracts.WebhookResponse{}, ctx.Err()
	}
}

func tokenHash(value string) []byte {
	hash := sha256.Sum256([]byte(value))
	return hash[:]
}

func stableID(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:12])
}

func (c *Core) webhookToken(clientID, commandID string) string {
	key := []byte(c.signer)
	if len(key) == 0 {
		key = []byte("insecure-development-key")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(clientID + "\n" + commandID))
	return hex.EncodeToString(mac.Sum(nil))
}

func dedupe(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}
