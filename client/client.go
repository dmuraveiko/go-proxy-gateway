// Package client provides the synchronous Go API used by internal services.
// Transport is asynchronous Core NATS, while Do blocks until the final HTTP result.
package client

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"proxy-server/internal/contracts"
	"proxy-server/internal/message"
)

type Request = contracts.HTTPRequest
type Result = contracts.HTTPResult
type HeaderField = contracts.HeaderField
type RetryPolicy = contracts.RetryPolicy

// Store is the client's durable boundary. Production services must implement it
// with their own database transactionally with the surrounding business operation.
type Store interface {
	SaveOutgoing(context.Context, Request) error
	MarkAccepted(context.Context, string) error
	SaveResult(context.Context, Result) error
	MarkComplete(context.Context, string) error
}

type Config struct {
	ClientID, ProxyID     string
	Signer                ed25519.PrivateKey
	ProxyPublicKey        ed25519.PublicKey
	RequireProxySignature bool
	RetryInterval         time.Duration
}
type Client struct {
	nc    *nats.Conn
	store Store
	cfg   Config
}

func New(nc *nats.Conn, store Store, cfg Config) (*Client, error) {
	if nc == nil || store == nil {
		return nil, errors.New("NATS connection and durable store are required")
	}
	if cfg.ClientID == "" || cfg.ProxyID == "" {
		return nil, errors.New("client_id and proxy_id are required")
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
	return &Client{nc: nc, store: store, cfg: cfg}, nil
}

// Do persists the request before its first publish and returns only after the
// result is persisted by Store and the result-ACK handshake is complete.
func (c *Client) Do(ctx context.Context, req Request) (Result, error) {
	if req.RequestID == "" {
		return Result{}, errors.New("request_id is required")
	}
	req.ClientID = c.cfg.ClientID
	req.ProxyID = c.cfg.ProxyID
	if req.CreatedAt.IsZero() {
		req.CreatedAt = time.Now().UTC()
	}
	if err := c.store.SaveOutgoing(ctx, req); err != nil {
		return Result{}, fmt.Errorf("persist outgoing request: %w", err)
	}
	accepted, err := c.nc.SubscribeSync(c.subject("accepted"))
	if err != nil {
		return Result{}, err
	}
	defer accepted.Unsubscribe()
	results, err := c.nc.SubscribeSync(c.subject("results"))
	if err != nil {
		return Result{}, err
	}
	defer results.Unsubscribe()
	confirmed, err := c.nc.SubscribeSync(c.subject("ack_confirmed"))
	if err != nil {
		return Result{}, err
	}
	defer confirmed.Unsubscribe()
	if err = c.nc.Flush(); err != nil {
		return Result{}, err
	}
	var acceptance contracts.Acceptance
	for acceptance.RequestID == "" {
		if err = c.publish(ctx, "proxy."+c.cfg.ProxyID+".requests", contracts.TypeHTTPRequest, req); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return Result{}, err
		}
		deadline := time.Now().Add(c.cfg.RetryInterval)
		for time.Now().Before(deadline) {
			m, e := nextMessage(ctx, accepted, time.Until(deadline))
			if e != nil {
				if errors.Is(e, nats.ErrTimeout) || errors.Is(e, context.DeadlineExceeded) {
					break
				}
				return Result{}, e
			}
			if c.decode(m, contracts.TypeAcceptance, &acceptance) == nil && acceptance.RequestID == req.RequestID {
				break
			}
		}
	}
	if !acceptance.Accepted {
		return Result{}, fmt.Errorf("proxy rejected request (%s): %s", acceptance.ErrorCode, acceptance.Error)
	}
	if err = c.store.MarkAccepted(ctx, req.RequestID); err != nil {
		return Result{}, fmt.Errorf("persist acceptance: %w", err)
	}
	acceptACK := contracts.DeliveryACK{RequestID: req.RequestID, DeliveryID: acceptance.DeliveryID, ClientID: c.cfg.ClientID}
	if err = c.ackUntilConfirmed(ctx, "proxy."+c.cfg.ProxyID+".accepted_acks", contracts.TypeAcceptanceACK, acceptACK, confirmed); err != nil {
		return Result{}, err
	}
	for {
		m, e := results.NextMsgWithContext(ctx)
		if e != nil {
			return Result{}, e
		}
		var result Result
		if c.decode(m, contracts.TypeHTTPResult, &result) != nil || result.RequestID != req.RequestID {
			continue
		}
		if err = c.store.SaveResult(ctx, result); err != nil {
			return Result{}, fmt.Errorf("persist HTTP result: %w", err)
		}
		ack := contracts.DeliveryACK{RequestID: req.RequestID, DeliveryID: result.ResultID, ClientID: c.cfg.ClientID}
		if err = c.ackUntilConfirmed(ctx, "proxy."+c.cfg.ProxyID+".result_acks", contracts.TypeResultACK, ack, confirmed); err != nil {
			return Result{}, err
		}
		if err = c.store.MarkComplete(ctx, req.RequestID); err != nil {
			return Result{}, err
		}
		if result.State == "unknown" {
			return result, &OutcomeUnknownError{RequestID: req.RequestID, Cause: result.Error}
		}
		return result, nil
	}
}
func (c *Client) ackUntilConfirmed(ctx context.Context, subject, typ string, ack contracts.DeliveryACK, sub *nats.Subscription) error {
	for {
		if err := c.publish(ctx, subject, typ, ack); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		deadline := time.Now().Add(c.cfg.RetryInterval)
		for time.Now().Before(deadline) {
			m, err := nextMessage(ctx, sub, time.Until(deadline))
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
					break
				}
				return err
			}
			var confirmation contracts.ACKConfirmed
			if c.decode(m, contracts.TypeACKConfirmed, &confirmation) == nil && confirmation.DeliveryID == ack.DeliveryID {
				return nil
			}
		}
	}
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
func nextMessage(ctx context.Context, sub *nats.Subscription, timeout time.Duration) (*nats.Msg, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return sub.NextMsgWithContext(waitCtx)
}

type OutcomeUnknownError struct{ RequestID, Cause string }

func (e *OutcomeUnknownError) Error() string {
	return "HTTP outcome is unknown for " + e.RequestID + ": " + e.Cause
}

// MemoryStore is only for local development and tests; it is not restart-safe.
type MemoryStore struct {
	mu       sync.Mutex
	Requests map[string]Request
	Results  map[string]Result
	States   map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{Requests: map[string]Request{}, Results: map[string]Result{}, States: map[string]string{}}
}
func (s *MemoryStore) SaveOutgoing(_ context.Context, r Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Requests[r.RequestID] = r
	s.States[r.RequestID] = "outgoing"
	return nil
}
func (s *MemoryStore) MarkAccepted(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.States[id] = "accepted"
	return nil
}
func (s *MemoryStore) SaveResult(_ context.Context, r Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Results[r.RequestID] = r
	s.States[r.RequestID] = "result_saved"
	return nil
}
func (s *MemoryStore) MarkComplete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.States[id] = "complete"
	return nil
}
