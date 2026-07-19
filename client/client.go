// Package client provides standard net/http adapters for HTTP-NATS Proxy.
package client

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/message"
	"github.com/nats-io/nats.go"
)

type Request = contracts.HTTPRequest
type Result = contracts.HTTPResult
type HeaderField = contracts.HeaderField
type RetryPolicy = contracts.RetryPolicy
type StaticHTTPResponse = contracts.StaticHTTPResponse
type WebhookRegister = contracts.WebhookRegister
type WebhookUpdate = contracts.WebhookUpdate
type WebhookSubscribe = contracts.WebhookSubscribe
type WebhookUnsubscribe = contracts.WebhookUnsubscribe
type WebhookDelete = contracts.WebhookDelete
type WebhookEvent = contracts.WebhookEvent

const (
	StateOutgoing    = "outgoing"
	StateAccepted    = "accepted"
	StateResultSaved = "result_saved"
	StateComplete    = "complete"
)

var ErrOperationNotFound = errors.New("proxy client operation not found")
var ErrRequestConflict = errors.New("request_id already belongs to another request")

type StoredOperation struct {
	Request Request
	Result  *Result
	State   string
}

// StoredCallback is the durable client-side state of one Proxy delivery.
// Response is set after the handler has completed. A non-nil response means
// recovery must only resend it and must never call the handler again.
type StoredCallback struct {
	Event     WebhookEvent
	Response  *contracts.WebhookResponse
	Completed bool
}

// Store is the durable boundary on the client side. PostgresStore is the default
// implementation; applications only need a custom implementation for another DB.
type Store interface {
	SaveOutgoing(context.Context, Request) error
	Load(context.Context, string) (StoredOperation, error)
	ListPending(context.Context, int) ([]StoredOperation, error)
	MarkAccepted(context.Context, string) error
	SaveResult(context.Context, Result) (Result, error)
	MarkComplete(context.Context, string) error
}

type CallbackStore interface {
	SaveCallback(context.Context, contracts.WebhookEvent) (StoredCallback, error)
	SaveCallbackResponse(context.Context, contracts.WebhookResponse) error
	ListPendingCallbacks(context.Context, int) ([]StoredCallback, error)
	MarkCallbackComplete(context.Context, string) error
}

type Config struct {
	ClientID, ProxyID     string
	Signer                ed25519.PrivateKey
	ProxyPublicKey        ed25519.PublicKey
	RequireProxySignature bool
	RetryInterval         time.Duration
	MaxRetryInterval      time.Duration
	Retention             time.Duration
	CleanupInterval       time.Duration
	CallbackWorkers       int
}

type operationOutcome struct {
	result Result
	err    error
}
type operationRun struct {
	done    chan struct{}
	outcome operationOutcome
}

type Client struct {
	nc    *nats.Conn
	store Store
	cfg   Config

	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	acceptance    map[string]chan contracts.Acceptance
	results       map[string]chan Result
	confirmed     map[string]chan struct{}
	control       map[string]chan contracts.WebhookControlResult
	running       map[string]*operationRun
	callbackRuns  map[string]struct{}
	subs          []*nats.Subscription
	callbackSub   *nats.Subscription
	callbackQueue chan *nats.Msg
	closeStore    func()
	wg            sync.WaitGroup
}

func New(nc *nats.Conn, store Store, cfg Config) (*Client, error) {
	if nc == nil || store == nil {
		return nil, errors.New("NATS connection and durable store are required")
	}
	if cfg.ClientID == "" || cfg.ProxyID == "" {
		return nil, errors.New("client_id and proxy_id are required")
	}
	if !natsToken(cfg.ClientID) || !natsToken(cfg.ProxyID) {
		return nil, errors.New("client_id and proxy_id must each be a single NATS token")
	}
	if len(cfg.Signer) != ed25519.PrivateKeySize {
		return nil, errors.New("client signing key is required")
	}
	if cfg.RequireProxySignature && len(cfg.ProxyPublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("proxy public key is required")
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = time.Second
	}
	if cfg.MaxRetryInterval <= 0 {
		cfg.MaxRetryInterval = 30 * time.Second
	}
	if cfg.Retention <= 0 {
		cfg.Retention = 30 * 24 * time.Hour
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Hour
	}
	if cfg.CallbackWorkers <= 0 {
		cfg.CallbackWorkers = 8
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		nc: nc, store: store, cfg: cfg, ctx: ctx, cancel: cancel,
		acceptance: map[string]chan contracts.Acceptance{},
		results:    map[string]chan Result{}, confirmed: map[string]chan struct{}{},
		control: map[string]chan contracts.WebhookControlResult{}, running: map[string]*operationRun{},
		callbackRuns: map[string]struct{}{},
	}
	if err := c.subscribe(); err != nil {
		cancel()
		return nil, err
	}
	c.resumePending()
	if cleaner, ok := store.(interface {
		Cleanup(context.Context, time.Time, int) (int64, error)
	}); ok {
		c.wg.Add(1)
		go c.cleanupLoop(cleaner)
	}
	return c, nil
}

// NewTransport is the preferred constructor for use as http.Client.Transport.
func NewTransport(nc *nats.Conn, store Store, cfg Config) (*Client, error) {
	return New(nc, store, cfg)
}

// OpenTransport is the shortest production setup: it creates/migrates the
// built-in client tables and returns an http.RoundTripper.
func OpenTransport(ctx context.Context, nc *nats.Conn, dsn string, cfg Config, options ...PostgresStoreOption) (*Client, error) {
	store, err := OpenPostgresStore(ctx, dsn, options...)
	if err != nil {
		return nil, err
	}
	client, err := New(nc, store, cfg)
	if err != nil {
		store.Close()
		return nil, err
	}
	client.closeStore = store.Close
	return client, nil
}

func (c *Client) Close() error {
	c.cancel()
	c.mu.Lock()
	subs := append([]*nats.Subscription(nil), c.subs...)
	if c.callbackSub != nil {
		subs = append(subs, c.callbackSub)
	}
	c.mu.Unlock()
	for _, sub := range subs {
		_ = sub.Drain()
	}
	c.wg.Wait()
	if c.closeStore != nil {
		c.closeStore()
	}
	return nil
}

func (c *Client) subscribe() error {
	handlers := []struct {
		suffix string
		fn     nats.MsgHandler
	}{
		{"accepted", c.handleAcceptance},
		{"results", c.handleResult},
		{"ack_confirmed", c.handleConfirmed},
		{"webhooks.control_results", c.handleControlResult},
	}
	for _, h := range handlers {
		sub, err := c.nc.Subscribe(c.subject(h.suffix), h.fn)
		if err != nil {
			return err
		}
		c.subs = append(c.subs, sub)
	}
	return c.nc.Flush()
}

func (c *Client) handleAcceptance(m *nats.Msg) {
	var value contracts.Acceptance
	if c.decode(m, contracts.TypeAcceptance, &value) != nil {
		return
	}
	c.mu.Lock()
	ch := c.acceptance[value.RequestID]
	c.mu.Unlock()
	trySend(ch, value)
}

func (c *Client) handleResult(m *nats.Msg) {
	var value Result
	if c.decode(m, contracts.TypeHTTPResult, &value) != nil {
		return
	}
	c.mu.Lock()
	ch := c.results[value.RequestID]
	c.mu.Unlock()
	trySend(ch, value)
}

func (c *Client) handleConfirmed(m *nats.Msg) {
	var value contracts.ACKConfirmed
	if c.decode(m, contracts.TypeACKConfirmed, &value) != nil {
		return
	}
	c.mu.Lock()
	ch := c.confirmed[value.DeliveryID]
	c.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (c *Client) handleControlResult(m *nats.Msg) {
	var value contracts.WebhookControlResult
	if c.decode(m, contracts.TypeWebhookControlResult, &value) != nil {
		return
	}
	c.mu.Lock()
	ch := c.control[value.CommandID]
	c.mu.Unlock()
	trySend(ch, value)
}

func (c *Client) publish(ctx context.Context, subject, typ string, payload any) error {
	env, err := message.NewEnvelope(typ, payload, c.cfg.Signer)
	if err != nil {
		return err
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if err = c.nc.Publish(subject, b); err != nil {
		return err
	}
	return c.nc.FlushWithContext(ctx)
}

func (c *Client) decode(m *nats.Msg, typ string, out any) error {
	var env message.Envelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		return err
	}
	if env.Type != typ {
		return errors.New("unexpected message type")
	}
	if c.cfg.RequireProxySignature {
		if err := env.Verify([]ed25519.PublicKey{c.cfg.ProxyPublicKey}, 5*time.Minute); err != nil {
			return err
		}
	}
	return json.Unmarshal(env.Payload, out)
}

func (c *Client) subject(suffix string) string {
	return "client." + c.cfg.ClientID + ".proxy." + c.cfg.ProxyID + "." + suffix
}

func (c *Client) cleanupLoop(cleaner interface {
	Cleanup(context.Context, time.Time, int) (int64, error)
}) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		_, _ = cleaner.Cleanup(c.ctx, time.Now().Add(-c.cfg.Retention), 1000)
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func trySend[T any](ch chan T, value T) {
	if ch == nil {
		return
	}
	select {
	case ch <- value:
	default:
	}
}

func natsToken(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character == '.' || character == '*' || character == '>' || character == ' ' || character == '\t' || character == '\r' || character == '\n' {
			return false
		}
	}
	return true
}

type OutcomeUnknownError struct{ RequestID, Cause string }

func (e *OutcomeUnknownError) Error() string {
	return "HTTP outcome is unknown for " + e.RequestID + ": " + e.Cause
}
