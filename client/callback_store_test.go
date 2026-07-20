package client

import (
	"context"
	"errors"
	"testing"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

func TestCallbackStoreKeepsHandlerResponseForRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	event := contracts.WebhookEvent{
		EventID: "event-1", DeliveryID: "delivery-1", ProxyID: "proxy-a", WebhookID: "webhook-1",
		Method: "POST", RequestURI: "/callback", Body: []byte("request"),
		ReplySubject: "proxy.proxy-a.instance.one.webhooks.responses",
	}
	stored, err := store.SaveCallback(ctx, event)
	if err != nil || stored.Response != nil || stored.Completed {
		t.Fatalf("save callback: stored=%+v err=%v", stored, err)
	}
	response := contracts.WebhookResponse{
		EventID: event.EventID, DeliveryID: event.DeliveryID, ClientID: "client-a",
		StatusCode: 201, Body: []byte("response"),
	}
	if err = store.SaveCallbackResponse(ctx, event.ProxyID, response); err != nil {
		t.Fatal(err)
	}

	pending, err := store.ListPendingCallbacks(ctx, event.ProxyID, 10)
	if err != nil || len(pending) != 1 || pending[0].Response == nil {
		t.Fatalf("pending callback was not recovered: callbacks=%+v err=%v", pending, err)
	}
	if !sameCallbackResponse(*pending[0].Response, response) {
		t.Fatalf("recovered another response: %+v", pending[0].Response)
	}

	// Re-saving the same response is an idempotent retry. A different response
	// under the same delivery ID is rejected instead of calling the handler again.
	if err = store.SaveCallbackResponse(ctx, event.ProxyID, response); err != nil {
		t.Fatal(err)
	}
	conflict := response
	conflict.StatusCode = 500
	if err = store.SaveCallbackResponse(ctx, event.ProxyID, conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("expected response conflict, got %v", err)
	}

	if err = store.MarkCallbackComplete(ctx, event.ProxyID, event.DeliveryID); err != nil {
		t.Fatal(err)
	}
	pending, err = store.ListPendingCallbacks(ctx, event.ProxyID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("completed callback remains pending: callbacks=%+v err=%v", pending, err)
	}
}

func TestCallbackDeliveryRunsOnlyOnceAtATime(t *testing.T) {
	client := &Client{callbackRuns: map[string]struct{}{}}
	if !client.beginCallback("delivery-1") {
		t.Fatal("first delivery must start")
	}
	if client.beginCallback("delivery-1") {
		t.Fatal("duplicate delivery must not run concurrently")
	}
	client.endCallback("delivery-1")
	if !client.beginCallback("delivery-1") {
		t.Fatal("delivery must be available after the first run ends")
	}
}

func TestMemoryStoreSeparatesTheSameIDsByProxy(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	requestA := Request{RequestID: "same-request", ProxyID: "proxy-a", ClientID: "client-a", Method: "GET", URL: "https://example.test/a"}
	requestB := Request{RequestID: "same-request", ProxyID: "proxy-b", ClientID: "client-a", Method: "GET", URL: "https://example.test/b"}
	if err := store.SaveOutgoing(ctx, requestA); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveOutgoing(ctx, requestB); err != nil {
		t.Fatal(err)
	}
	loadedA, err := store.Load(ctx, requestA.ProxyID, requestA.RequestID)
	if err != nil || loadedA.Request.URL != requestA.URL {
		t.Fatalf("load proxy A: operation=%+v err=%v", loadedA, err)
	}
	loadedB, err := store.Load(ctx, requestB.ProxyID, requestB.RequestID)
	if err != nil || loadedB.Request.URL != requestB.URL {
		t.Fatalf("load proxy B: operation=%+v err=%v", loadedB, err)
	}
	pendingA, err := store.ListPending(ctx, requestA.ProxyID, 10)
	if err != nil || len(pendingA) != 1 || pendingA[0].Request.ProxyID != requestA.ProxyID {
		t.Fatalf("proxy A pending operations are mixed: %+v err=%v", pendingA, err)
	}

	eventA := contracts.WebhookEvent{EventID: "event-a", DeliveryID: "same-delivery", ProxyID: "proxy-a", WebhookID: "webhook-a"}
	eventB := contracts.WebhookEvent{EventID: "event-b", DeliveryID: "same-delivery", ProxyID: "proxy-b", WebhookID: "webhook-b"}
	if _, err = store.SaveCallback(ctx, eventA); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SaveCallback(ctx, eventB); err != nil {
		t.Fatal(err)
	}
	callbacksA, err := store.ListPendingCallbacks(ctx, eventA.ProxyID, 10)
	if err != nil || len(callbacksA) != 1 || callbacksA[0].Event.EventID != eventA.EventID {
		t.Fatalf("proxy A callbacks are mixed: %+v err=%v", callbacksA, err)
	}
	callbacksB, err := store.ListPendingCallbacks(ctx, eventB.ProxyID, 10)
	if err != nil || len(callbacksB) != 1 || callbacksB[0].Event.EventID != eventB.EventID {
		t.Fatalf("proxy B callbacks are mixed: %+v err=%v", callbacksB, err)
	}
}
