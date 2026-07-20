package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/message"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

func integrationClient(t *testing.T) (*Client, *nats.Conn) {
	t.Helper()
	nc, err := nats.Connect(os.Getenv("PROXY_INTEGRATION_NATS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	config := Config{ClientID: "dev-client", ProxyID: "proxy-main", Signer: privateKey, RetryInterval: 100 * time.Millisecond}
	var client *Client
	if dsn := os.Getenv("PROXY_INTEGRATION_DATABASE_URL"); dsn != "" {
		client, err = OpenTransport(context.Background(), nc, dsn, config)
	} else {
		client, err = New(nc, NewMemoryStore(), config)
	}
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(); nc.Close() })
	return client, nc
}

func TestIntegrationRoundTripper(t *testing.T) {
	target := os.Getenv("PROXY_INTEGRATION_HTTP_URL")
	if os.Getenv("PROXY_INTEGRATION_NATS_URL") == "" || target == "" {
		t.Skip("integration environment is not configured")
	}
	transport, _ := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	requestID := "smoke-" + time.Now().Format("150405.000000000")
	request, err := http.NewRequestWithContext(WithRequestID(ctx, requestID), http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "proxy-smoke-ok") {
		t.Fatalf("unexpected response: %d %q", response.StatusCode, body)
	}

	// The same stable request ID returns the stored result and never repeats HTTP.
	replay, _ := http.NewRequestWithContext(WithRequestID(ctx, requestID), http.MethodGet, target, nil)
	resumed, err := (&http.Client{Transport: transport}).Do(replay)
	if err != nil || resumed.StatusCode != response.StatusCode {
		t.Fatalf("resume failed: response=%+v err=%v", resumed, err)
	}
}

func TestIntegrationStaticWebhookHandler(t *testing.T) {
	proxyURL := os.Getenv("PROXY_INTEGRATION_PROXY_URL")
	if os.Getenv("PROXY_INTEGRATION_NATS_URL") == "" || proxyURL == "" {
		t.Skip("integration environment is not configured")
	}
	client, _ := integrationClient(t)
	eventBody := make(chan string, 1)
	if err := client.ServeCallbacks(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		eventBody <- string(body)
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	callback, err := client.RegisterCallback(ctx, WebhookRegister{
		CommandID: "static-" + time.Now().Format("150405.000000000"), Name: "smoke", Mode: "static",
		SubscriberIDs: []string{"dev-client"}, StaticResponse: StaticHTTPResponse{StatusCode: http.StatusAccepted, Body: []byte("accepted")},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(proxyURL+callbackPath(callback.URL), "application/octet-stream", strings.NewReader("webhook-body"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || string(body) != "accepted" {
		t.Fatalf("static response %d %q", response.StatusCode, body)
	}
	select {
	case body := <-eventBody:
		if body != "webhook-body" {
			t.Fatalf("event body %q", body)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestIntegrationDelegatedWebhookHandler(t *testing.T) {
	proxyURL := os.Getenv("PROXY_INTEGRATION_PROXY_URL")
	if os.Getenv("PROXY_INTEGRATION_NATS_URL") == "" || proxyURL == "" {
		t.Skip("integration environment is not configured")
	}
	client, _ := integrationClient(t)
	if err := client.ServeCallbacks(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("X-Delegated", "yes")
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte("client-response"))
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	callback, err := client.RegisterCallback(ctx, WebhookRegister{
		CommandID: "delegated-" + time.Now().Format("150405.000000000"), Name: "delegated", Mode: "delegated",
		ResponderID: "dev-client", ResponseTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(proxyURL+callbackPath(callback.URL), "text/plain", strings.NewReader("question"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusCreated || response.Header.Get("X-Delegated") != "yes" || string(body) != "client-response" {
		t.Fatalf("delegated response %d %q", response.StatusCode, body)
	}
}

func TestIntegrationSavedCallbackResponseSkipsHandler(t *testing.T) {
	if os.Getenv("PROXY_INTEGRATION_NATS_URL") == "" || os.Getenv("PROXY_INTEGRATION_DATABASE_URL") == "" {
		t.Skip("integration environment is not configured")
	}
	client, nc := integrationClient(t)
	store := client.store.(CallbackStore)
	unique := time.Now().Format("150405.000000000")
	event := contracts.WebhookEvent{
		EventID: "recover-event-" + unique, DeliveryID: "recover-delivery-" + unique,
		ProxyID: "proxy-main", WebhookID: "recover-webhook", Method: http.MethodPost, RequestURI: "/recover",
		ReplySubject: "test.callback.responses." + unique,
	}
	if _, err := store.SaveCallback(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	response := contracts.WebhookResponse{
		EventID: event.EventID, DeliveryID: event.DeliveryID, ClientID: "dev-client",
		StatusCode: http.StatusCreated, Body: []byte("saved-response"),
	}
	if err := store.SaveCallbackResponse(context.Background(), event.ProxyID, response); err != nil {
		t.Fatal(err)
	}

	received := make(chan struct{}, 1)
	_, err := nc.Subscribe(event.ReplySubject, func(natsMessage *nats.Msg) {
		var envelope message.Envelope
		if json.Unmarshal(natsMessage.Data, &envelope) != nil || envelope.Type != contracts.TypeWebhookDelegatedResponse {
			return
		}
		confirmation, envelopeErr := message.NewEnvelope(contracts.TypeACKConfirmed, contracts.ACKConfirmed{DeliveryID: event.DeliveryID, ConfirmedAt: time.Now().UTC()}, nil)
		if envelopeErr != nil {
			return
		}
		data, _ := json.Marshal(confirmation)
		_ = nc.Publish("client.dev-client.proxy.proxy-main.ack_confirmed", data)
		_ = nc.Flush()
		select {
		case received <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var handlerCalls atomic.Int32
	if err = client.ServeCallbacks(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		handlerCalls.Add(1)
	})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("saved callback response was not resent")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, listErr := store.ListPendingCallbacks(context.Background(), event.ProxyID, 100)
		if listErr == nil {
			found := false
			for _, callback := range pending {
				found = found || callback.Event.DeliveryID == event.DeliveryID
			}
			if !found {
				if handlerCalls.Load() != 0 {
					t.Fatalf("handler was called %d times", handlerCalls.Load())
				}
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("saved callback response was not marked complete")
}

func TestIntegrationPostgresStoreSeparatesTheSameIDByProxy(t *testing.T) {
	dsn := os.Getenv("PROXY_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("integration PostgreSQL is not configured")
	}
	store, err := OpenPostgresStore(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	requestID := "shared-id-" + time.Now().Format("150405.000000000")
	requestA := Request{RequestID: requestID, ProxyID: "proxy-a", ClientID: "dev-client", Method: http.MethodGet, URL: "https://example.test/a"}
	requestB := Request{RequestID: requestID, ProxyID: "proxy-b", ClientID: "dev-client", Method: http.MethodGet, URL: "https://example.test/b"}
	if err = store.SaveOutgoing(context.Background(), requestA); err != nil {
		t.Fatal(err)
	}
	if err = store.SaveOutgoing(context.Background(), requestB); err != nil {
		t.Fatal(err)
	}
	loadedA, err := store.Load(context.Background(), requestA.ProxyID, requestID)
	if err != nil || loadedA.Request.URL != requestA.URL {
		t.Fatalf("proxy A operation: %+v err=%v", loadedA, err)
	}
	loadedB, err := store.Load(context.Background(), requestB.ProxyID, requestID)
	if err != nil || loadedB.Request.URL != requestB.URL {
		t.Fatalf("proxy B operation: %+v err=%v", loadedB, err)
	}

	deliveryID := "shared-delivery-" + requestID
	eventA := contracts.WebhookEvent{EventID: "event-a-" + requestID, DeliveryID: deliveryID, ProxyID: "proxy-a", WebhookID: "webhook-a"}
	eventB := contracts.WebhookEvent{EventID: "event-b-" + requestID, DeliveryID: deliveryID, ProxyID: "proxy-b", WebhookID: "webhook-b"}
	if _, err = store.SaveCallback(context.Background(), eventA); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SaveCallback(context.Background(), eventB); err != nil {
		t.Fatal(err)
	}
	callbacksA, err := store.ListPendingCallbacks(context.Background(), eventA.ProxyID, 100)
	if err != nil {
		t.Fatal(err)
	}
	callbacksB, err := store.ListPendingCallbacks(context.Background(), eventB.ProxyID, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !containsCallback(callbacksA, eventA) || containsCallback(callbacksA, eventB) || !containsCallback(callbacksB, eventB) || containsCallback(callbacksB, eventA) {
		t.Fatalf("callbacks are not isolated: proxy-a=%+v proxy-b=%+v", callbacksA, callbacksB)
	}
}

func TestIntegrationPostgresStoreMigratesLegacyPrimaryKeys(t *testing.T) {
	dsn := os.Getenv("PROXY_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("integration PostgreSQL is not configured")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	prefix := fmt.Sprintf("migration_%d_", time.Now().UnixNano())
	operations := quoteIdentifier(prefix + "operations")
	callbacks := quoteIdentifier(prefix + "callback_events")
	defer func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s,%s`, operations, callbacks))
	}()
	legacySchema := fmt.Sprintf(`
CREATE TABLE %s (
  request_id text PRIMARY KEY,
  request jsonb NOT NULL,
  result jsonb,
  state text NOT NULL CHECK (state IN ('outgoing','accepted','result_saved','complete')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE %s (
  delivery_id text PRIMARY KEY,
  event jsonb NOT NULL,
  completed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);`, operations, callbacks)
	if _, err = pool.Exec(ctx, legacySchema); err != nil {
		t.Fatal(err)
	}
	legacyRequest := Request{RequestID: "legacy-id", ProxyID: "proxy-a", ClientID: "client-a", Method: http.MethodGet, URL: "https://example.test/legacy"}
	requestPayload, _ := json.Marshal(legacyRequest)
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(request_id,request,state) VALUES($1,$2,'outgoing')`, operations), legacyRequest.RequestID, requestPayload); err != nil {
		t.Fatal(err)
	}
	legacyEvent := contracts.WebhookEvent{EventID: "legacy-event", DeliveryID: "legacy-delivery", WebhookID: "legacy-webhook"}
	eventPayload, _ := json.Marshal(legacyEvent)
	if _, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(delivery_id,event) VALUES($1,$2)`, callbacks), legacyEvent.DeliveryID, eventPayload); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(ctx, pool, WithTablePrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Load(ctx, legacyRequest.ProxyID, legacyRequest.RequestID); err != nil {
		t.Fatalf("legacy operation was not assigned to its proxy: %v", err)
	}
	secondProxy := legacyRequest
	secondProxy.ProxyID = "proxy-b"
	secondProxy.URL = "https://example.test/second"
	if err = store.SaveOutgoing(ctx, secondProxy); err != nil {
		t.Fatalf("composite operation key was not installed: %v", err)
	}
	newEvent := contracts.WebhookEvent{EventID: "new-event", DeliveryID: legacyEvent.DeliveryID, ProxyID: "proxy-b", WebhookID: "new-webhook"}
	if _, err = store.SaveCallback(ctx, newEvent); err != nil {
		t.Fatalf("composite callback key was not installed: %v", err)
	}
}

func containsCallback(callbacks []StoredCallback, event contracts.WebhookEvent) bool {
	for _, callback := range callbacks {
		if callback.Event.ProxyID == event.ProxyID && callback.Event.DeliveryID == event.DeliveryID && callback.Event.EventID == event.EventID {
			return true
		}
	}
	return false
}

func callbackPath(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return parsed.RequestURI()
}
