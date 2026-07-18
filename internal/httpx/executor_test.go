package httpx

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

func TestExecutorPreservesApplicationDataAndDoesNotRedirect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "raw-body" {
			t.Errorf("body=%q", body)
		}
		if r.RequestURI != "/call/%2Fkeep?b=2&a=%2F&a=1" {
			t.Errorf("request URI changed: %q", r.RequestURI)
		}
		mac := hmac.New(sha256.New, []byte("secret"))
		_, _ = mac.Write([]byte(r.RequestURI))
		_, _ = mac.Write(body)
		if r.Header.Get("X-Signature") != hex.EncodeToString(mac.Sum(nil)) {
			t.Error("URL/body signature became invalid")
		}
		if got := r.Header.Values("X-Duplicate"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
			t.Errorf("duplicate headers=%v", got)
		}
		w.Header().Add("X-Reply", "first")
		w.Header().Add("X-Reply", "second")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("raw-response"))
	}))
	defer server.Close()
	executor := New(1024, 10, 10, time.Minute)
	defer executor.CloseIdleConnections()
	requestURI := "/call/%2Fkeep?b=2&a=%2F&a=1"
	mac := hmac.New(sha256.New, []byte("secret"))
	_, _ = mac.Write([]byte(requestURI))
	_, _ = mac.Write([]byte("raw-body"))
	result, err := executor.Do(context.Background(), contracts.HTTPRequest{Method: http.MethodPost, URL: server.URL + requestURI, Headers: []contracts.HeaderField{{Name: "X-Duplicate", Value: "one"}, {Name: "X-Duplicate", Value: "two"}, {Name: "X-Signature", Value: hex.EncodeToString(mac.Sum(nil))}}, Body: []byte("raw-body")})
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusCreated || string(result.Body) != "raw-response" {
		t.Fatalf("unexpected result: %+v", result)
	}
	redirect, err := executor.Do(context.Background(), contracts.HTTPRequest{Method: http.MethodGet, URL: server.URL + "/redirect"})
	if err != nil {
		t.Fatal(err)
	}
	if redirect.StatusCode != http.StatusFound {
		t.Fatalf("redirect status=%d", redirect.StatusCode)
	}
	if calls.Load() != 2 {
		t.Fatalf("redirect was followed, calls=%d", calls.Load())
	}
}

func TestExecutorRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("too large")) }))
	defer server.Close()
	executor := New(3, 1, 1, time.Minute)
	_, err := executor.Do(context.Background(), contracts.HTTPRequest{Method: http.MethodGet, URL: server.URL})
	if err == nil {
		t.Fatal("expected size error")
	}
}
