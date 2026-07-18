package transport

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/message"
	"github.com/dmuraveiko/go-proxy-gateway/internal/repository"
	"github.com/nats-io/nats.go"
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
	for _, key := range keys {
		flat = append(flat, key)
	}
	return &Core{
		nc: nc, repo: repo, log: log,
		proxyID: cfg.ProxyID, instanceID: cfg.InstanceID,
		publicBaseURL: strings.TrimRight(cfg.PublicBaseURL, "/"), signer: signer,
		keys: flat, clientByKeyID: clientByKeyID, allowed: allowed,
		requireSignature: cfg.RequireSignature, deliveryWorkers: cfg.DeliveryWorkers,
		deliveryRetry: cfg.DeliveryRetry, maxMessageBytes: cfg.MaxMessageBytes,
		maxRequestBytes: cfg.MaxRequestBytes,
		webhookWaiters:  map[string]chan contracts.WebhookResponse{},
	}
}

func (c *Core) Ready() bool { return c.ready.Load() && c.nc.IsConnected() }

func (c *Core) Run(ctx context.Context) error {
	queue := "proxy-" + c.proxyID
	handlers := []struct {
		subject string
		queue   string
		fn      nats.MsgHandler
	}{
		{c.requestSubject(), queue, c.handleRequest},
		{c.acceptedACKSubject(), queue, c.handleAcceptanceACK},
		{c.resultACKSubject(), queue, c.handleResultACK},
		{c.webhookCommandSubject(), queue, c.handleWebhookCommand},
		{c.webhookControlACKSubject(), queue, c.handleWebhookControlACK},
		{c.webhookACKSubject(), queue, c.handleWebhookACK},
		{c.webhookResponseSubject(), "", c.handleWebhookResponse},
	}
	var subscriptions []*nats.Subscription
	for _, handler := range handlers {
		var subscription *nats.Subscription
		var err error
		if handler.queue == "" {
			subscription, err = c.nc.Subscribe(handler.subject, handler.fn)
		} else {
			subscription, err = c.nc.QueueSubscribe(handler.subject, handler.queue, handler.fn)
		}
		if err != nil {
			return err
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := c.nc.Flush(); err != nil {
		return err
	}
	c.ready.Store(true)
	errorsChannel := make(chan error, c.deliveryWorkers)
	for range c.deliveryWorkers {
		go func() { errorsChannel <- c.deliveryLoop(ctx) }()
	}
	select {
	case <-ctx.Done():
		c.ready.Store(false)
		for _, subscription := range subscriptions {
			_ = subscription.Drain()
		}
		return nil
	case err := <-errorsChannel:
		c.ready.Store(false)
		return err
	}
}

func (c *Core) Publish(ctx context.Context, subject, messageType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.PublishRaw(ctx, subject, messageType, data)
}

func (c *Core) PublishRaw(ctx context.Context, subject, messageType string, payload []byte) error {
	var raw json.RawMessage = payload
	envelope, err := message.NewEnvelope(messageType, raw, c.signer)
	if err != nil {
		return err
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	if int64(len(data)) > c.maxMessageBytes {
		return errors.New("NATS message exceeds configured limit")
	}
	if err = c.nc.Publish(subject, data); err != nil {
		return err
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		flushContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return c.nc.FlushWithContext(flushContext)
	}
	return c.nc.FlushWithContext(ctx)
}

func (c *Core) PublishHTTPResult(ctx context.Context, clientID string, result contracts.HTTPResult) error {
	return c.Publish(ctx, c.clientSubject(clientID, "results"), contracts.TypeHTTPResult, result)
}

func (c *Core) decode(messageValue *nats.Msg, expectedType string) (string, json.RawMessage, error) {
	clientID, payload, actualType, err := c.decodeAny(messageValue)
	if err == nil && actualType != expectedType {
		err = fmt.Errorf("expected %s, got %s", expectedType, actualType)
	}
	return clientID, payload, err
}

func (c *Core) decodeAny(messageValue *nats.Msg) (string, json.RawMessage, string, error) {
	if int64(len(messageValue.Data)) > c.maxMessageBytes {
		return "", nil, "", errors.New("message too large")
	}
	var envelope message.Envelope
	if err := json.Unmarshal(messageValue.Data, &envelope); err != nil {
		return "", nil, "", err
	}
	clientID := ""
	if c.requireSignature {
		keyID, err := envelope.VerifyKey(c.keys, 5*time.Minute)
		if err != nil {
			return "", nil, "", err
		}
		clientID = c.clientByKeyID[keyID]
	} else {
		var identity struct {
			ClientID string `json:"client_id"`
		}
		_ = json.Unmarshal(envelope.Payload, &identity)
		clientID = identity.ClientID
	}
	if !c.allowedClient(clientID) {
		return "", nil, "", errors.New("client is not allowed")
	}
	return clientID, envelope.Payload, envelope.Type, nil
}

func (c *Core) clientSubject(clientID, suffix string) string {
	return "client." + clientID + ".proxy." + c.proxyID + "." + suffix
}

func (c *Core) allowedClient(clientID string) bool { return clientID != "" && c.allowed[clientID] }

func (c *Core) reject(kind string, err error) {
	c.log.Warn("NATS message rejected", "kind", kind, "error", err)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
