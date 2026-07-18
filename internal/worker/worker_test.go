package worker

import (
	"net/http"
	"testing"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

func TestRetrySafety(t *testing.T) {
	p := &Pool{}
	base := contracts.HTTPRequest{Retry: contracts.RetryPolicy{MaxAttempts: 3, RetryNetworkFail: true, RetryStatuses: []int{503}}}
	base.Method = http.MethodPost
	if p.canRetry(base, 1, true, 0) {
		t.Fatal("unsafe POST must not retry")
	}
	base.Retry.Idempotent = true
	if !p.canRetry(base, 1, true, 0) {
		t.Fatal("idempotent POST should retry")
	}
	base.Method = http.MethodGet
	base.Retry.Idempotent = false
	if !p.canRetry(base, 1, false, 503) {
		t.Fatal("GET should retry configured status")
	}
	if p.canRetry(base, 3, true, 0) {
		t.Fatal("max attempts must stop retries")
	}
}
