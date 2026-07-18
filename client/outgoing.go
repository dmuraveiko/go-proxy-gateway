package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

type requestIDKey struct{}
type retryPolicyKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func WithRetryPolicy(ctx context.Context, policy RetryPolicy) context.Context {
	return context.WithValue(ctx, retryPolicyKey{}, policy)
}

func (c *Client) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("nil HTTP request or URL")
	}
	var body []byte
	var err error
	if req.Body != nil {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read request body: %w", err)
		}
	}
	if req.Body != nil {
		_ = req.Body.Close()
	}
	id, _ := req.Context().Value(requestIDKey{}).(string)
	if id == "" {
		id = "req_" + randomID()
	}
	policy, _ := req.Context().Value(retryPolicyKey{}).(RetryPolicy)
	timeout := time.Duration(0)
	if deadline, ok := req.Context().Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout < 0 {
			return nil, context.DeadlineExceeded
		}
	}
	request := Request{
		RequestID: id, ClientID: c.cfg.ClientID, ProxyID: c.cfg.ProxyID,
		Method: req.Method, URL: req.URL.String(), Headers: requestHeaders(req), Body: body,
		Timeout: timeout, Retry: policy, CreatedAt: time.Now().UTC(),
	}
	run, err := c.start(req.Context(), request)
	if err != nil {
		return nil, err
	}
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-run.done:
		if run.outcome.err != nil {
			return nil, run.outcome.err
		}
		result := run.outcome.result
		response := &http.Response{
			StatusCode: result.StatusCode,
			Status:     strconv.Itoa(result.StatusCode) + " " + http.StatusText(result.StatusCode),
			Header:     fieldsToHeader(result.Headers),
			Body:       io.NopCloser(bytes.NewReader(result.Body)),
			Request:    req,
		}
		response.ContentLength = int64(len(result.Body))
		return response, nil
	}
}

// Do remains as a low-level API for recovery/tests. New integrations should use
// http.Client with Client as its Transport.
func (c *Client) Do(ctx context.Context, request Request) (Result, error) {
	if request.RequestID == "" {
		request.RequestID = "req_" + randomID()
	}
	request.ClientID, request.ProxyID = c.cfg.ClientID, c.cfg.ProxyID
	if request.CreatedAt.IsZero() {
		request.CreatedAt = time.Now().UTC()
	}
	run, err := c.start(ctx, request)
	if err != nil {
		return Result{}, err
	}
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-run.done:
		return run.outcome.result, run.outcome.err
	}
}

func (c *Client) start(initialCtx context.Context, request Request) (*operationRun, error) {
	if request.RequestID == "" {
		return nil, errors.New("request_id is required")
	}
	if err := c.store.SaveOutgoing(initialCtx, request); err != nil {
		return nil, fmt.Errorf("persist outgoing request: %w", err)
	}
	stored, err := c.store.Load(initialCtx, request.RequestID)
	if err != nil {
		return nil, fmt.Errorf("load outgoing request: %w", err)
	}
	// A stable ID always reuses the originally persisted wire contract. Values
	// derived from a later caller context must not turn recovery into a conflict.
	request = stored.Request
	c.mu.Lock()
	if existing := c.running[request.RequestID]; existing != nil {
		c.mu.Unlock()
		return existing, nil
	}
	run := &operationRun{done: make(chan struct{})}
	if stored.State == StateComplete && stored.Result != nil {
		run.outcome.result = *stored.Result
		if stored.Result.State == "unknown" {
			run.outcome.err = &OutcomeUnknownError{RequestID: request.RequestID, Cause: stored.Result.Error}
		}
		close(run.done)
		c.mu.Unlock()
		return run, nil
	}
	c.running[request.RequestID] = run
	c.acceptance[request.RequestID] = make(chan contracts.Acceptance, 4)
	c.results[request.RequestID] = make(chan Result, 4)
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		run.outcome.result, run.outcome.err = c.execute(c.ctx, request)
		c.mu.Lock()
		delete(c.running, request.RequestID)
		delete(c.acceptance, request.RequestID)
		delete(c.results, request.RequestID)
		close(run.done)
		c.mu.Unlock()
	}()
	return run, nil
}

func (c *Client) execute(ctx context.Context, req Request) (Result, error) {
	c.mu.Lock()
	acceptedCh, resultCh := c.acceptance[req.RequestID], c.results[req.RequestID]
	c.mu.Unlock()

	var acceptance contracts.Acceptance
	delay := c.cfg.RetryInterval
	for acceptance.RequestID == "" {
		_ = c.publish(ctx, "proxy."+c.cfg.ProxyID+".requests", contracts.TypeHTTPRequest, req)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Result{}, ctx.Err()
		case acceptance = <-acceptedCh:
			timer.Stop()
		case <-timer.C:
			delay = nextBackoff(delay, c.cfg.MaxRetryInterval)
		}
	}
	if !acceptance.Accepted {
		return Result{}, fmt.Errorf("proxy rejected request (%s): %s", acceptance.ErrorCode, acceptance.Error)
	}
	// From durable acceptance onward the protocol finishes independently from the
	// caller's request context. This is required to persist/ACK a late result.
	if err := c.store.MarkAccepted(ctx, req.RequestID); err != nil {
		return Result{}, fmt.Errorf("persist acceptance: %w", err)
	}
	acceptACK := contracts.DeliveryACK{RequestID: req.RequestID, DeliveryID: acceptance.DeliveryID, ClientID: c.cfg.ClientID}
	if err := c.ackUntilConfirmed(ctx, "proxy."+c.cfg.ProxyID+".accepted_acks", contracts.TypeAcceptanceACK, acceptACK); err != nil {
		return Result{}, err
	}

	for {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case result := <-resultCh:
			effective, err := c.store.SaveResult(ctx, result)
			if err != nil {
				return Result{}, fmt.Errorf("persist HTTP result: %w", err)
			}
			ack := contracts.DeliveryACK{RequestID: req.RequestID, DeliveryID: result.ResultID, ClientID: c.cfg.ClientID}
			if err = c.ackUntilConfirmed(ctx, "proxy."+c.cfg.ProxyID+".result_acks", contracts.TypeResultACK, ack); err != nil {
				return Result{}, err
			}
			if err = c.store.MarkComplete(ctx, req.RequestID); err != nil {
				return Result{}, err
			}
			if effective.State == "unknown" {
				return effective, &OutcomeUnknownError{RequestID: req.RequestID, Cause: effective.Error}
			}
			return effective, nil
		}
	}
}

func (c *Client) ackUntilConfirmed(ctx context.Context, subject, typ string, ack contracts.DeliveryACK) error {
	ch := make(chan struct{}, 2)
	c.mu.Lock()
	c.confirmed[ack.DeliveryID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.confirmed, ack.DeliveryID)
		c.mu.Unlock()
	}()
	delay := c.cfg.RetryInterval
	for {
		_ = c.publish(ctx, subject, typ, ack)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-ch:
			timer.Stop()
			return nil
		case <-timer.C:
			delay = nextBackoff(delay, c.cfg.MaxRetryInterval)
		}
	}
}

func (c *Client) resumePending() {
	operations, err := c.store.ListPending(c.ctx, 10_000)
	if err != nil {
		return
	}
	for _, operation := range operations {
		_, _ = c.start(c.ctx, operation.Request)
	}
}

func requestHeaders(req *http.Request) []HeaderField {
	keys := make([]string, 0, len(req.Header))
	for key := range req.Header {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]HeaderField, 0, len(keys)+2)
	for _, key := range keys {
		for _, value := range req.Header.Values(key) {
			fields = append(fields, HeaderField{Name: key, Value: value})
		}
	}
	if req.Host != "" {
		fields = append(fields, HeaderField{Name: "Host", Value: req.Host})
	}
	if req.ContentLength >= 0 {
		fields = append(fields, HeaderField{Name: "Content-Length", Value: strconv.FormatInt(req.ContentLength, 10)})
	}
	return fields
}

func fieldsToHeader(fields []HeaderField) http.Header {
	header := make(http.Header)
	for _, field := range fields {
		header.Add(field.Name, field.Value)
	}
	return header
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current <= 0 {
		current = time.Second
	}
	next := current * 2
	if next < current || next > maximum {
		return maximum
	}
	return next
}

var _ http.RoundTripper = (*Client)(nil)
