package jsonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"proxy-server/internal/integration"
	"proxy-server/internal/security"
)

type TransactionStatus struct {
	endpoint, apiKeyHeader, apiKey string
	client                         *http.Client
	slots                          chan struct{}
	failures                       atomic.Int32
	openUntil                      atomic.Int64
	globalLimit                    func(context.Context) error
}
type transactionRequest struct {
	TransactionID string `json:"transaction_id"`
}

func NewTransactionStatus(endpoint, apiKeyHeader, apiKey string, timeout time.Duration, rps int, globalLimit ...func(context.Context) error) (*TransactionStatus, error) {
	host, err := endpointHost(endpoint)
	if err != nil {
		return nil, err
	}
	policy := security.NewAllowlist([]string{host})
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = policy.DialContext
	h := &TransactionStatus{endpoint: endpoint, apiKeyHeader: apiKeyHeader, apiKey: apiKey, client: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, max(1, rps))}
	if len(globalLimit) > 0 {
		h.globalLimit = globalLimit[0]
	}
	for range cap(h.slots) {
		h.slots <- struct{}{}
	}
	go func() {
		t := time.NewTicker(time.Second / time.Duration(max(1, rps)))
		defer t.Stop()
		for range t.C {
			select {
			case h.slots <- struct{}{}:
			default:
			}
		}
	}()
	return h, nil
}
func (h *TransactionStatus) Type() string { return "blockchain.transaction_status.get" }
func (h *TransactionStatus) Version() int { return 1 }
func (h *TransactionStatus) RetryPolicy() integration.RetryPolicy {
	return integration.RetryPolicy{MaxAttempts: 8, InitialBackoff: time.Second, MaxBackoff: 2 * time.Minute}
}
func (h *TransactionStatus) Validate(raw json.RawMessage) error {
	var in transactionRequest
	if err := json.Unmarshal(raw, &in); err != nil {
		return err
	}
	if in.TransactionID == "" || len(in.TransactionID) > 256 {
		return errors.New("transaction_id is required")
	}
	return nil
}
func (h *TransactionStatus) Execute(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	if until := time.Unix(0, h.openUntil.Load()); time.Now().Before(until) {
		return nil, integration.Retryable("provider_circuit_open", errors.New("provider circuit breaker is open"))
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.slots:
	}
	if h.globalLimit != nil {
		if err := h.globalLimit(ctx); err != nil {
			return nil, integration.Retryable("rate_limit_unavailable", err)
		}
	}
	var in transactionRequest
	_ = json.Unmarshal(raw, &in)
	body, _ := json.Marshal(map[string]string{"transaction_id": in.TransactionID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, integration.Permanent("request_invalid", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if h.apiKey != "" {
		req.Header.Set(h.apiKeyHeader, h.apiKey)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.recordFailure()
		return nil, integration.Retryable("provider_unavailable", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, integration.Retryable("provider_read_failed", err)
	}
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		h.recordFailure()
		return nil, integration.Retryable("provider_temporary_error", fmt.Errorf("provider returned %s", resp.Status))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, integration.Permanent("provider_rejected", fmt.Errorf("provider returned %s", resp.Status))
	}
	if !json.Valid(b) {
		h.recordFailure()
		return nil, integration.Retryable("provider_invalid_response", errors.New("provider returned invalid JSON"))
	}
	h.failures.Store(0)
	return b, nil
}
func (h *TransactionStatus) recordFailure() {
	if h.failures.Add(1) >= 5 {
		h.openUntil.Store(time.Now().Add(30 * time.Second).UnixNano())
		h.failures.Store(0)
	}
}
func endpointHost(raw string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil || req.URL.Hostname() == "" || req.URL.Scheme != "https" {
		return "", errors.New("transaction status endpoint must be a valid HTTPS URL")
	}
	return req.URL.Hostname(), nil
}
