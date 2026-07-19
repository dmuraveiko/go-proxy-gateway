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
		EventID: "event-1", DeliveryID: "delivery-1", WebhookID: "webhook-1",
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
	if err = store.SaveCallbackResponse(ctx, response); err != nil {
		t.Fatal(err)
	}

	pending, err := store.ListPendingCallbacks(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Response == nil {
		t.Fatalf("pending callback was not recovered: callbacks=%+v err=%v", pending, err)
	}
	if !sameCallbackResponse(*pending[0].Response, response) {
		t.Fatalf("recovered another response: %+v", pending[0].Response)
	}

	// Re-saving the same response is an idempotent retry. A different response
	// under the same delivery ID is rejected instead of calling the handler again.
	if err = store.SaveCallbackResponse(ctx, response); err != nil {
		t.Fatal(err)
	}
	conflict := response
	conflict.StatusCode = 500
	if err = store.SaveCallbackResponse(ctx, conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("expected response conflict, got %v", err)
	}

	if err = store.MarkCallbackComplete(ctx, event.DeliveryID); err != nil {
		t.Fatal(err)
	}
	pending, err = store.ListPendingCallbacks(ctx, 10)
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
