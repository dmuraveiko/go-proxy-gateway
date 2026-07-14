package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"proxy-server/internal/contracts"
	"proxy-server/internal/message"
	"proxy-server/internal/repository"
)

type Core struct {
	nc                                 *nats.Conn
	repo                               *repository.Repository
	log                                *slog.Logger
	proxyID, instanceID, publicBaseURL string
	signer                             ed25519.PrivateKey
	keys                               []ed25519.PublicKey
	clientByKeyID                      map[string]string
	allowed                            map[string]bool
	requireSignature                   bool
	deliveryWorkers                    int
	deliveryRetry                      time.Duration
	maxMessageBytes                    int64
	maxRequestBytes                    int64
	ready                              atomic.Bool
	mu                                 sync.Mutex
	webhookWaiters                     map[string]chan contracts.WebhookResponse
}

type Config struct {
	ProxyID, InstanceID, PublicBaseURL string
	DeliveryWorkers                    int
	DeliveryRetry                      time.Duration
	MaxMessageBytes                    int64
	MaxRequestBytes                    int64
	RequireSignature                   bool
}

func New(nc *nats.Conn, repo *repository.Repository, log *slog.Logger, cfg Config, signer ed25519.PrivateKey, keys map[string]ed25519.PublicKey, clientByKeyID map[string]string, allowed map[string]bool) *Core {
	flat := make([]ed25519.PublicKey, 0, len(keys))
	for _, k := range keys {
		flat = append(flat, k)
	}
	return &Core{nc: nc, repo: repo, log: log, proxyID: cfg.ProxyID, instanceID: cfg.InstanceID, publicBaseURL: strings.TrimRight(cfg.PublicBaseURL, "/"), signer: signer, keys: flat, clientByKeyID: clientByKeyID, allowed: allowed, requireSignature: cfg.RequireSignature, deliveryWorkers: cfg.DeliveryWorkers, deliveryRetry: cfg.DeliveryRetry, maxMessageBytes: cfg.MaxMessageBytes, maxRequestBytes: cfg.MaxRequestBytes, webhookWaiters: map[string]chan contracts.WebhookResponse{}}
}
func (c *Core) Ready() bool                   { return c.ready.Load() && c.nc.IsConnected() }
func (c *Core) requestSubject() string        { return "proxy." + c.proxyID + ".requests" }
func (c *Core) acceptedACKSubject() string    { return "proxy." + c.proxyID + ".accepted_acks" }
func (c *Core) resultACKSubject() string      { return "proxy." + c.proxyID + ".result_acks" }
func (c *Core) webhookCommandSubject() string { return "proxy." + c.proxyID + ".webhooks.commands" }
func (c *Core) webhookACKSubject() string     { return "proxy." + c.proxyID + ".webhooks.acks" }
func (c *Core) webhookResponseSubject() string {
	return "proxy." + c.proxyID + ".instance." + c.instanceID + ".webhooks.responses"
}
func (c *Core) clientSubject(clientID, suffix string) string {
	return "client." + clientID + ".proxy." + c.proxyID + "." + suffix
}

func (c *Core) Run(ctx context.Context) error {
	queue := "proxy-" + c.proxyID
	handlers := []struct {
		subject string
		queue   string
		fn      nats.MsgHandler
	}{
		{c.requestSubject(), queue, c.handleRequest}, {c.acceptedACKSubject(), queue, c.handleAcceptanceACK}, {c.resultACKSubject(), queue, c.handleResultACK},
		{c.webhookCommandSubject(), queue, c.handleWebhookCommand}, {c.webhookACKSubject(), queue, c.handleWebhookACK}, {c.webhookResponseSubject(), "", c.handleWebhookResponse},
	}
	var subs []*nats.Subscription
	for _, h := range handlers {
		var s *nats.Subscription
		var err error
		if h.queue == "" {
			s, err = c.nc.Subscribe(h.subject, h.fn)
		} else {
			s, err = c.nc.QueueSubscribe(h.subject, h.queue, h.fn)
		}
		if err != nil {
			return err
		}
		subs = append(subs, s)
	}
	if err := c.nc.Flush(); err != nil {
		return err
	}
	c.ready.Store(true)
	errch := make(chan error, c.deliveryWorkers)
	for range c.deliveryWorkers {
		go func() { errch <- c.deliveryLoop(ctx) }()
	}
	select {
	case <-ctx.Done():
		c.ready.Store(false)
		for _, s := range subs {
			_ = s.Drain()
		}
		return nil
	case err := <-errch:
		c.ready.Store(false)
		return err
	}
}

func (c *Core) handleRequest(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(m, contracts.TypeHTTPRequest)
	if err != nil {
		c.reject("http request", err)
		return
	}
	var req contracts.HTTPRequest
	if err = json.Unmarshal(payload, &req); err != nil {
		c.reject("http request", err)
		return
	}
	if err = c.validateRequest(req, client); err != nil {
		c.rejectRequest(ctx, client, req.RequestID, "invalid_request", err)
		return
	}
	deliveryID := "accept_" + req.RequestID
	acceptance := contracts.Acceptance{RequestID: req.RequestID, DeliveryID: deliveryID, ProxyID: c.proxyID, Accepted: true, AcceptedAt: time.Now().UTC()}
	if err = c.repo.AcceptHTTPRequest(ctx, req, acceptance, c.clientSubject(client, "accepted")); err != nil {
		if errors.Is(err, repository.ErrRequestConflict) {
			c.rejectRequest(ctx, client, req.RequestID, "request_id_conflict", err)
			return
		}
		c.reject("accept request", err)
	}
}
func (c *Core) rejectRequest(ctx context.Context, clientID, requestID, code string, cause error) {
	c.reject("http request", cause)
	_ = c.Publish(ctx, c.clientSubject(clientID, "accepted"), contracts.TypeAcceptance, contracts.Acceptance{RequestID: requestID, ProxyID: c.proxyID, Accepted: false, ErrorCode: code, Error: cause.Error(), AcceptedAt: time.Now().UTC()})
}
func (c *Core) validateRequest(req contracts.HTTPRequest, client string) error {
	if req.RequestID == "" || req.ClientID != client || req.ProxyID != c.proxyID {
		return errors.New("request identity mismatch")
	}
	if !c.allowedClient(client) {
		return errors.New("client is not allowed on this proxy")
	}
	if int64(len(req.Body)) > c.maxRequestBytes {
		return errors.New("request body exceeds NATS contract limit")
	}
	if req.Method == "" {
		return errors.New("method is required")
	}
	u, err := url.Parse(req.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("absolute HTTP/HTTPS URL is required")
	}
	if req.Retry.MaxAttempts < 0 || req.Retry.MaxAttempts > 20 {
		return errors.New("invalid retry attempts")
	}
	return nil
}
func (c *Core) handleAcceptanceACK(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(m, contracts.TypeAcceptanceACK)
	if err != nil {
		c.reject("acceptance ACK", err)
		return
	}
	var ack contracts.DeliveryACK
	if err = json.Unmarshal(payload, &ack); err != nil {
		return
	}
	if ack.ClientID != client {
		return
	}
	if err = c.repo.ConfirmAcceptance(ctx, ack); err != nil {
		c.reject("acceptance ACK", err)
		return
	}
	_ = c.Publish(ctx, c.clientSubject(client, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: ack.DeliveryID, ConfirmedAt: time.Now().UTC()})
}
func (c *Core) handleResultACK(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(m, contracts.TypeResultACK)
	if err != nil {
		c.reject("result ACK", err)
		return
	}
	var ack contracts.DeliveryACK
	if json.Unmarshal(payload, &ack) != nil || ack.ClientID != client {
		return
	}
	if err = c.repo.ConfirmResult(ctx, ack); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return
		} // direct result can arrive before its DB row; client retries ACK.
		c.reject("result ACK", err)
		return
	}
	_ = c.Publish(ctx, c.clientSubject(client, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: ack.DeliveryID, ConfirmedAt: time.Now().UTC()})
}

func (c *Core) handleWebhookCommand(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, typ, err := c.decodeAny(m)
	if err != nil {
		c.reject("webhook command", err)
		return
	}
	switch typ {
	case contracts.TypeWebhookRegister:
		var cmd contracts.WebhookRegister
		if json.Unmarshal(payload, &cmd) != nil || cmd.ClientID != client || cmd.CommandID == "" {
			return
		}
		if cmd.Mode != "static" && cmd.Mode != "delegated" {
			return
		}
		if cmd.Mode == "delegated" && cmd.ResponderID == "" {
			cmd.ResponderID = client
		}
		for _, id := range append(cmd.SubscriberIDs, cmd.ResponderID) {
			if id != "" && !c.allowedClient(id) {
				c.reject("webhook register", errors.New("subscriber is not allowed"))
				return
			}
		}
		token := c.webhookToken(client, cmd.CommandID)
		route := repository.WebhookRoute{ID: "wh_" + stableID(client+"\n"+cmd.CommandID), OwnerClientID: client, Name: cmd.Name, Mode: cmd.Mode, ResponderClientID: cmd.ResponderID, TokenHash: tokenHash(token), StaticResponse: cmd.StaticResponse, ResponseTimeout: cmd.ResponseTimeout, MaxBodyBytes: cmd.MaxBodyBytes, Enabled: true}
		if route.StaticResponse.StatusCode == 0 {
			route.StaticResponse.StatusCode = http.StatusOK
		}
		if route.ResponseTimeout <= 0 {
			route.ResponseTimeout = 10 * time.Second
		}
		if route.MaxBodyBytes <= 0 {
			route.MaxBodyBytes = 4 << 20
		}
		subs := dedupe(append(cmd.SubscriberIDs, client))
		if err = c.repo.RegisterWebhook(ctx, route, subs); err != nil {
			c.reject("webhook register", err)
			return
		}
		res := contracts.WebhookRegisterResult{CommandID: cmd.CommandID, WebhookID: route.ID, URL: c.publicBaseURL + "/v1/webhooks/" + route.ID + "/" + token}
		_ = c.Publish(ctx, c.clientSubject(client, "webhooks.control_results"), contracts.TypeWebhookRegisterResult, res)
	case contracts.TypeWebhookSubscribe:
		var cmd contracts.WebhookSubscribe
		if json.Unmarshal(payload, &cmd) != nil || cmd.ClientID != client {
			return
		}
		if !c.allowedClient(cmd.SubscriberID) {
			return
		}
		if err = c.repo.SubscribeWebhook(ctx, client, cmd.WebhookID, cmd.SubscriberID); err != nil {
			c.reject("webhook subscribe", err)
		}
	case contracts.TypeWebhookDelete:
		var cmd contracts.WebhookDelete
		if json.Unmarshal(payload, &cmd) != nil || cmd.ClientID != client {
			return
		}
		if err = c.repo.DeleteWebhook(ctx, client, cmd.WebhookID); err != nil {
			c.reject("webhook delete", err)
		}
	default:
		c.reject("webhook command", errors.New("unsupported command type"))
	}
}
func (c *Core) handleWebhookACK(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(m, contracts.TypeWebhookEventACK)
	if err != nil {
		return
	}
	var ack contracts.DeliveryACK
	if json.Unmarshal(payload, &ack) != nil || ack.ClientID != client {
		return
	}
	if c.repo.ConfirmWebhook(ctx, ack) == nil {
		_ = c.Publish(ctx, c.clientSubject(client, "ack_confirmed"), contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: ack.DeliveryID, ConfirmedAt: time.Now().UTC()})
	}
}
func (c *Core) handleWebhookResponse(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, payload, err := c.decode(m, contracts.TypeWebhookDelegatedResponse)
	if err != nil {
		return
	}
	var response contracts.WebhookResponse
	if json.Unmarshal(payload, &response) != nil || response.ClientID != client {
		return
	}
	if err = c.repo.SaveWebhookResponse(ctx, response); err != nil {
		c.reject("webhook response", err)
		return
	}
	c.mu.Lock()
	ch := c.webhookWaiters[response.EventID]
	c.mu.Unlock()
	if ch != nil {
		select {
		case ch <- response:
		default:
		}
	}
}

func (c *Core) deliveryLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		d, err := c.repo.ClaimDelivery(ctx, 10*time.Second)
		if errors.Is(err, pgx.ErrNoRows) {
			if !wait(ctx, 100*time.Millisecond) {
				return nil
			}
			continue
		}
		if err != nil {
			if !wait(ctx, time.Second) {
				return nil
			}
			continue
		}
		err = c.PublishRaw(ctx, d.Subject, d.MessageType, d.Payload)
		if err != nil {
			c.log.Warn("core NATS delivery failed", "delivery_id", d.ID, "error", err)
		}
		if e := c.repo.RescheduleDelivery(ctx, d.ID, c.deliveryRetry); e != nil && ctx.Err() == nil {
			return e
		}
	}
}
func (c *Core) Publish(ctx context.Context, subject, typ string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.PublishRaw(ctx, subject, typ, b)
}
func (c *Core) PublishRaw(ctx context.Context, subject, typ string, payload []byte) error {
	var raw json.RawMessage = payload
	env, err := message.NewEnvelope(typ, raw, c.signer)
	if err != nil {
		return err
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if int64(len(b)) > c.maxMessageBytes {
		return errors.New("NATS message exceeds configured limit")
	}
	if err = c.nc.Publish(subject, b); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return c.nc.FlushWithContext(flushCtx)
	}
	return c.nc.FlushWithContext(ctx)
}

func (c *Core) DeliverWebhook(ctx context.Context, event contracts.WebhookEvent, route repository.WebhookRoute) (contracts.WebhookResponse, error) {
	reply := c.webhookResponseSubject()
	var ch chan contracts.WebhookResponse
	if route.Mode == "delegated" {
		ch = make(chan contracts.WebhookResponse, 1)
		c.mu.Lock()
		c.webhookWaiters[event.EventID] = ch
		c.mu.Unlock()
		defer func() { c.mu.Lock(); delete(c.webhookWaiters, event.EventID); c.mu.Unlock() }()
	}
	deliveries, err := c.repo.SaveWebhookEvent(ctx, event, route, func(client string) string { return c.clientSubject(client, "webhooks.events") }, reply)
	if err != nil {
		return contracts.WebhookResponse{}, err
	}
	if route.Mode != "delegated" {
		return contracts.WebhookResponse{}, nil
	}
	for _, d := range deliveries {
		if d.ClientID == route.ResponderClientID {
			_ = c.PublishRaw(ctx, d.Subject, d.MessageType, d.Payload)
			break
		}
	}
	select {
	case response := <-ch:
		return response, nil
	case <-ctx.Done():
		_ = c.repo.MarkWebhookTimedOut(context.WithoutCancel(ctx), event.EventID)
		return contracts.WebhookResponse{}, ctx.Err()
	}
}
func (c *Core) PublishHTTPResult(ctx context.Context, clientID string, result contracts.HTTPResult) error {
	return c.Publish(ctx, c.clientSubject(clientID, "results"), contracts.TypeHTTPResult, result)
}

func (c *Core) decode(m *nats.Msg, expected string) (string, json.RawMessage, error) {
	client, payload, typ, err := c.decodeAny(m)
	if err == nil && typ != expected {
		err = fmt.Errorf("expected %s, got %s", expected, typ)
	}
	return client, payload, err
}
func (c *Core) decodeAny(m *nats.Msg) (string, json.RawMessage, string, error) {
	if int64(len(m.Data)) > c.maxMessageBytes {
		return "", nil, "", errors.New("message too large")
	}
	var env message.Envelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		return "", nil, "", err
	}
	client := ""
	if c.requireSignature {
		keyID, err := env.VerifyKey(c.keys, 5*time.Minute)
		if err != nil {
			return "", nil, "", err
		}
		client = c.clientByKeyID[keyID]
	} else {
		var identity struct {
			ClientID string `json:"client_id"`
		}
		_ = json.Unmarshal(env.Payload, &identity)
		client = identity.ClientID
	}
	if !c.allowedClient(client) {
		return "", nil, "", errors.New("client is not allowed")
	}
	return client, env.Payload, env.Type, nil
}
func (c *Core) allowedClient(id string) bool { return id != "" && c.allowed[id] }
func (c *Core) reject(kind string, err error) {
	c.log.Warn("NATS message rejected", "kind", kind, "error", err)
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func randomToken() string       { var b [18]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func tokenHash(v string) []byte { x := sha256.Sum256([]byte(v)); return x[:] }
func stableID(v string) string  { x := sha256.Sum256([]byte(v)); return hex.EncodeToString(x[:12]) }
func (c *Core) webhookToken(clientID, commandID string) string {
	key := []byte(c.signer)
	if len(key) == 0 {
		key = []byte("insecure-development-key")
	}
	m := hmac.New(sha256.New, key)
	_, _ = m.Write([]byte(clientID + "\n" + commandID))
	return hex.EncodeToString(m.Sum(nil))
}
func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
