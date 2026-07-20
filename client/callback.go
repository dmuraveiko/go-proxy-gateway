package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/nats-io/nats.go"
)

// Callback describes a registered public webhook endpoint.
type Callback struct {
	WebhookID string
	URL       string
}

func (c *Client) RegisterCallback(ctx context.Context, command WebhookRegister) (Callback, error) {
	command.ClientID = c.cfg.ClientID
	if command.CommandID == "" {
		command.CommandID = "cmd_" + randomID()
	}
	result, err := c.webhookControl(ctx, contracts.TypeWebhookRegister, command.CommandID, command)
	return Callback{WebhookID: result.WebhookID, URL: result.URL}, err
}

func (c *Client) UpdateCallback(ctx context.Context, command WebhookUpdate) error {
	command.ClientID = c.cfg.ClientID
	if command.CommandID == "" {
		command.CommandID = "cmd_" + randomID()
	}
	_, err := c.webhookControl(ctx, contracts.TypeWebhookUpdate, command.CommandID, command)
	return err
}

func (c *Client) SubscribeCallback(ctx context.Context, webhookID, subscriberID string) error {
	command := contracts.WebhookSubscribe{CommandID: "cmd_" + randomID(), ClientID: c.cfg.ClientID, WebhookID: webhookID, SubscriberID: subscriberID}
	_, err := c.webhookControl(ctx, contracts.TypeWebhookSubscribe, command.CommandID, command)
	return err
}

func (c *Client) UnsubscribeCallback(ctx context.Context, webhookID, subscriberID string) error {
	command := contracts.WebhookUnsubscribe{CommandID: "cmd_" + randomID(), ClientID: c.cfg.ClientID, WebhookID: webhookID, SubscriberID: subscriberID}
	_, err := c.webhookControl(ctx, contracts.TypeWebhookUnsubscribe, command.CommandID, command)
	return err
}

func (c *Client) DeleteCallback(ctx context.Context, webhookID string) error {
	command := contracts.WebhookDelete{CommandID: "cmd_" + randomID(), ClientID: c.cfg.ClientID, WebhookID: webhookID}
	_, err := c.webhookControl(ctx, contracts.TypeWebhookDelete, command.CommandID, command)
	return err
}

func (c *Client) webhookControl(ctx context.Context, typ, commandID string, command any) (contracts.WebhookControlResult, error) {
	ch := make(chan contracts.WebhookControlResult, 4)
	c.mu.Lock()
	if _, exists := c.control[commandID]; exists {
		c.mu.Unlock()
		return contracts.WebhookControlResult{}, errors.New("webhook command is already running")
	}
	c.control[commandID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.control, commandID)
		c.mu.Unlock()
	}()

	delay := c.cfg.RetryInterval
	for {
		_ = c.publish(ctx, "proxy."+c.cfg.ProxyID+".webhooks.commands", typ, command)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return contracts.WebhookControlResult{}, ctx.Err()
		case result := <-ch:
			timer.Stop()
			ack := contracts.DeliveryACK{DeliveryID: result.DeliveryID, ClientID: c.cfg.ClientID}
			if err := c.ackUntilConfirmed(ctx, "proxy."+c.cfg.ProxyID+".webhooks.control_acks", contracts.TypeWebhookControlACK, ack); err != nil {
				return contracts.WebhookControlResult{}, err
			}
			if !result.Success {
				return result, fmt.Errorf("proxy rejected webhook %s (%s): %s", result.Action, result.ErrorCode, result.Error)
			}
			return result, nil
		case <-timer.C:
			delay = nextBackoff(delay, c.cfg.MaxRetryInterval)
		}
	}
}

// ServeCallbacks routes durable webhook events to a standard http.Handler.
// Calling it once is enough for both static subscribers and delegated responders.
func (c *Client) ServeCallbacks(handler http.Handler) error {
	if handler == nil {
		return errors.New("callback handler is required")
	}
	store, ok := c.store.(CallbackStore)
	if !ok {
		return errors.New("client store does not implement CallbackStore")
	}
	c.mu.Lock()
	if c.callbackSub != nil {
		c.mu.Unlock()
		return errors.New("callback handler is already running")
	}
	c.callbackQueue = make(chan *nats.Msg, c.cfg.CallbackWorkers*64)
	for range c.cfg.CallbackWorkers {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-c.ctx.Done():
					return
				case message := <-c.callbackQueue:
					c.handleCallback(c.ctx, store, handler, message)
				}
			}
		}()
	}
	subject := c.subject("webhooks.events")
	queue := "natsproxyclient-" + c.cfg.ClientID + "-" + c.cfg.ProxyID
	sub, err := c.nc.QueueSubscribe(subject, queue, func(message *nats.Msg) {
		select {
		case c.callbackQueue <- message:
		default: // no ACK: Proxy retries after local backpressure clears.
		}
	})
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.callbackSub = sub
	c.mu.Unlock()
	if err = c.nc.Flush(); err != nil {
		return err
	}
	c.resumeCallbacks(store)
	return nil
}

func (c *Client) handleCallback(ctx context.Context, store CallbackStore, handler http.Handler, message *nats.Msg) {
	var event contracts.WebhookEvent
	if c.decode(message, contracts.TypeWebhookEvent, &event) != nil || event.DeliveryID == "" || event.ProxyID != c.cfg.ProxyID {
		return
	}
	if !c.beginCallback(event.DeliveryID) {
		return
	}
	defer c.endCallback(event.DeliveryID)
	stored, err := store.SaveCallback(ctx, event)
	if err != nil {
		return // no ACK: Proxy will deliver it again.
	}
	if stored.Completed {
		_ = c.confirmCallback(ctx, event)
		return
	}
	if stored.Response != nil {
		_ = c.deliverCallbackResponse(ctx, store, event, *stored.Response, false)
		return
	}
	request, err := http.NewRequestWithContext(ctx, event.Method, event.RequestURI, bytes.NewReader(event.Body))
	if err != nil {
		return
	}
	request.Header = make(http.Header)
	for _, field := range event.Headers {
		switch strings.ToLower(field.Name) {
		case "host":
			request.Host = field.Value
		case "content-length":
			if length, parseErr := strconv.ParseInt(field.Value, 10, 64); parseErr == nil {
				request.ContentLength = length
			}
		default:
			request.Header.Add(field.Name, field.Value)
		}
	}
	request.RequestURI = event.RequestURI
	recorder := newCallbackResponse()
	handler.ServeHTTP(recorder, request)

	if event.ReplySubject != "" {
		response := contracts.WebhookResponse{
			EventID: event.EventID, DeliveryID: event.DeliveryID, ClientID: c.cfg.ClientID,
			StatusCode: recorder.status, Headers: requestHeadersFromMap(recorder.header), Body: recorder.body.Bytes(),
		}
		if err = c.deliverCallbackResponse(ctx, store, event, response, true); err != nil {
			return
		}
		return
	}
	if err = c.confirmCallback(ctx, event); err == nil {
		_ = c.markCallbackComplete(ctx, store, event.DeliveryID)
	}
}

func (c *Client) confirmCallback(ctx context.Context, event contracts.WebhookEvent) error {
	ack := contracts.DeliveryACK{RequestID: event.EventID, DeliveryID: event.DeliveryID, ClientID: c.cfg.ClientID}
	return c.ackUntilConfirmed(ctx, "proxy."+c.cfg.ProxyID+".webhooks.acks", contracts.TypeWebhookEventACK, ack)
}

func (c *Client) deliverCallbackResponse(ctx context.Context, store CallbackStore, event contracts.WebhookEvent, response contracts.WebhookResponse, persist bool) error {
	ch := make(chan struct{}, 2)
	c.mu.Lock()
	c.confirmed[event.DeliveryID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.confirmed, event.DeliveryID)
		c.mu.Unlock()
	}()
	// The deliberate exceptional ordering is handler -> NATS -> client DB. It gives
	// Proxy a chance to persist the response even while the client DB is unavailable.
	c.publishCallbackResponse(ctx, event.ReplySubject, response)
	saved := !persist
	confirmed := false
	delay := c.cfg.RetryInterval
	for {
		if !saved {
			saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			saveErr := store.SaveCallbackResponse(saveCtx, event.ProxyID, response)
			cancel()
			saved = saveErr == nil
			if errors.Is(saveErr, ErrOperationNotFound) || errors.Is(saveErr, ErrRequestConflict) {
				return saveErr
			}
		}
		if saved && confirmed {
			return c.markCallbackComplete(ctx, store, event.DeliveryID)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-ch:
			timer.Stop()
			confirmed = true
		case <-timer.C:
			if !confirmed {
				c.publishCallbackResponse(ctx, event.ReplySubject, response)
			}
			delay = nextBackoff(delay, c.cfg.MaxRetryInterval)
		}
	}
}

func (c *Client) publishCallbackResponse(ctx context.Context, subject string, response contracts.WebhookResponse) {
	// Core NATS has no broker-side ACK. Bound Flush so an unavailable NATS cannot
	// prevent the next durable step (saving the response in the client database).
	publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = c.publish(publishCtx, subject, contracts.TypeWebhookDelegatedResponse, response)
}

func (c *Client) markCallbackComplete(ctx context.Context, store CallbackStore, deliveryID string) error {
	delay := c.cfg.RetryInterval
	for {
		if err := store.MarkCallbackComplete(ctx, c.cfg.ProxyID, deliveryID); err == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			delay = nextBackoff(delay, c.cfg.MaxRetryInterval)
		}
	}
}

func (c *Client) resumeCallbacks(store CallbackStore) {
	callbacks, err := store.ListPendingCallbacks(c.ctx, c.cfg.ProxyID, 10_000)
	if err != nil {
		return
	}
	for _, stored := range callbacks {
		if stored.Response == nil || stored.Completed || !c.beginCallback(stored.Event.DeliveryID) {
			continue
		}
		stored := stored
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer c.endCallback(stored.Event.DeliveryID)
			_ = c.deliverCallbackResponse(c.ctx, store, stored.Event, *stored.Response, false)
		}()
	}
}

func (c *Client) beginCallback(deliveryID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, running := c.callbackRuns[deliveryID]; running {
		return false
	}
	c.callbackRuns[deliveryID] = struct{}{}
	return true
}

func (c *Client) endCallback(deliveryID string) {
	c.mu.Lock()
	delete(c.callbackRuns, deliveryID)
	c.mu.Unlock()
}

type callbackResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
	wrote  bool
}

func newCallbackResponse() *callbackResponse {
	return &callbackResponse{header: make(http.Header), status: http.StatusOK}
}
func (r *callbackResponse) Header() http.Header { return r.header }
func (r *callbackResponse) WriteHeader(status int) {
	if !r.wrote && status >= 100 && status <= 999 {
		r.status = status
		r.wrote = true
	}
}
func (r *callbackResponse) Write(body []byte) (int, error) {
	r.wrote = true
	return r.body.Write(body)
}

func requestHeadersFromMap(header http.Header) []HeaderField {
	request := &http.Request{Header: header, ContentLength: -1}
	return requestHeaders(request)
}

var _ http.ResponseWriter = (*callbackResponse)(nil)
var _ io.Writer = (*callbackResponse)(nil)
