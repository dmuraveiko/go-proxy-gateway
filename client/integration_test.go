package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

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

func callbackPath(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return parsed.RequestURI()
}
