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
	"proxy-server/internal/contracts"
)

func TestIntegrationDo(t *testing.T) {
	natsURL := os.Getenv("PROXY_INTEGRATION_NATS_URL")
	target := os.Getenv("PROXY_INTEGRATION_HTTP_URL")
	if natsURL == "" || target == "" {
		t.Skip("integration environment is not configured")
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	client, err := New(nc, store, Config{ClientID: "dev-client", ProxyID: "proxy-main", Signer: privateKey, RetryInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	request := Request{RequestID: "smoke-" + time.Now().Format("150405.000000000"), Method: "GET", URL: target}
	result, err := client.Do(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != 200 || !strings.Contains(string(result.Body), "proxy-smoke-ok") {
		t.Fatalf("unexpected result: %+v", result)
	}
	// A restarted service can replay its durable request and resume both ACK
	// handshakes without executing the external HTTP request again.
	resumed, err := client.Do(ctx, request)
	if err != nil || resumed.ResultID != result.ResultID {
		t.Fatalf("resume failed: result=%+v err=%v", resumed, err)
	}
}

func TestIntegrationStaticWebhook(t *testing.T) {
	natsURL := os.Getenv("PROXY_INTEGRATION_NATS_URL")
	proxyURL := os.Getenv("PROXY_INTEGRATION_PROXY_URL")
	if natsURL == "" || proxyURL == "" {
		t.Skip("integration environment is not configured")
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	c, err := New(nc, NewMemoryStore(), Config{ClientID: "dev-client", ProxyID: "proxy-main", Signer: privateKey, RetryInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	control, err := nc.SubscribeSync(c.subject("webhooks.control_results"))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Unsubscribe()
	events, err := nc.SubscribeSync(c.subject("webhooks.events"))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Unsubscribe()
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	commandID := "webhook-smoke-" + time.Now().Format("150405.000000000")
	cmd := contracts.WebhookRegister{CommandID: commandID, ClientID: "dev-client", Name: "smoke", Mode: "static", SubscriberIDs: []string{"dev-client"}, StaticResponse: contracts.StaticHTTPResponse{StatusCode: 202, Body: []byte("accepted")}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = c.publish(ctx, "proxy.proxy-main.webhooks.commands", contracts.TypeWebhookRegister, cmd); err != nil {
		t.Fatal(err)
	}
	m, err := control.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var registered contracts.WebhookRegisterResult
	if err = c.decode(m, contracts.TypeWebhookRegisterResult, &registered); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(registered.URL)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(proxyURL+u.Path, "application/octet-stream", strings.NewReader("webhook-body"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 202 || string(body) != "accepted" {
		t.Fatalf("static response %d %q", response.StatusCode, body)
	}
	eventMsg, err := events.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event contracts.WebhookEvent
	if err = c.decode(eventMsg, contracts.TypeWebhookEvent, &event); err != nil {
		t.Fatal(err)
	}
	if string(event.Body) != "webhook-body" {
		t.Fatalf("event body %q", event.Body)
	}
	ack := contracts.DeliveryACK{DeliveryID: event.DeliveryID, ClientID: "dev-client"}
	if err = c.publish(ctx, "proxy.proxy-main.webhooks.acks", contracts.TypeWebhookEventACK, ack); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationDelegatedWebhook(t *testing.T) {
	natsURL, proxyURL := os.Getenv("PROXY_INTEGRATION_NATS_URL"), os.Getenv("PROXY_INTEGRATION_PROXY_URL")
	if natsURL == "" || proxyURL == "" {
		t.Skip("integration environment is not configured")
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	c, err := New(nc, NewMemoryStore(), Config{ClientID: "dev-client", ProxyID: "proxy-main", Signer: privateKey, RetryInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	control, _ := nc.SubscribeSync(c.subject("webhooks.control_results"))
	defer control.Unsubscribe()
	events, _ := nc.SubscribeSync(c.subject("webhooks.events"))
	defer events.Unsubscribe()
	_ = nc.Flush()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := contracts.WebhookRegister{CommandID: "delegated-" + time.Now().Format("150405.000000000"), ClientID: "dev-client", Name: "delegated", Mode: "delegated", ResponderID: "dev-client", ResponseTimeout: 5 * time.Second}
	if err = c.publish(ctx, "proxy.proxy-main.webhooks.commands", contracts.TypeWebhookRegister, cmd); err != nil {
		t.Fatal(err)
	}
	m, err := control.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var registered contracts.WebhookRegisterResult
	if err = c.decode(m, contracts.TypeWebhookRegisterResult, &registered); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(registered.URL)
	responderDone := make(chan error, 1)
	go func() {
		msg, e := events.NextMsgWithContext(ctx)
		if e != nil {
			responderDone <- e
			return
		}
		var event contracts.WebhookEvent
		if e = c.decode(msg, contracts.TypeWebhookEvent, &event); e != nil {
			responderDone <- e
			return
		}
		e = c.publish(ctx, event.ReplySubject, contracts.TypeWebhookDelegatedResponse, contracts.WebhookResponse{EventID: event.EventID, ClientID: "dev-client", StatusCode: 201, Headers: []contracts.HeaderField{{Name: "X-Delegated", Value: "yes"}}, Body: []byte("client-response")})
		responderDone <- e
	}()
	response, err := http.Post(proxyURL+u.Path, "text/plain", strings.NewReader("question"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 201 || response.Header.Get("X-Delegated") != "yes" || string(body) != "client-response" {
		t.Fatalf("delegated response %d %q", response.StatusCode, body)
	}
	if err = <-responderDone; err != nil {
		t.Fatal(err)
	}
}
