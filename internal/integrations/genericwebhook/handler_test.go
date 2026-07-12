package genericwebhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"testing"
)

func TestAuthenticationAndParsing(t *testing.T) {
	body := []byte(`{"id":"evt-1","type":"transaction.confirmed","payload":{"ok":true}}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write(body)
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("X-Webhook-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	h := New("provider", "secret")
	if err := h.Authenticate(r, body); err != nil {
		t.Fatal(err)
	}
	events, err := h.Parse(body)
	if err != nil || len(events) != 1 || events[0].ID != "evt-1" {
		t.Fatalf("events=%v err=%v", events, err)
	}
}

func TestEventTypeMappingIsFailClosed(t *testing.T) {
	h := New("provider", "secret", map[string]string{"external.confirmed": "transaction.confirmed"})
	events, err := h.Parse([]byte(`{"id":"1","type":"external.confirmed","payload":{}}`))
	if err != nil || events[0].Type != "transaction.confirmed" {
		t.Fatalf("events=%v err=%v", events, err)
	}
	if _, err = h.Parse([]byte(`{"id":"2","type":"external.unknown","payload":{}}`)); err == nil {
		t.Fatal("unknown provider event accepted")
	}
}
