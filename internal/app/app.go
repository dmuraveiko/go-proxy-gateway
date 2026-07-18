package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/repository"
)

type WebhookTransport interface {
	Ready() bool
	DeliverWebhook(context.Context, contracts.WebhookEvent, repository.WebhookRoute) (contracts.WebhookResponse, error)
}
type App struct {
	log       *slog.Logger
	repo      *repository.Repository
	transport WebhookTransport
	addr      string
	maxBody   int64
	ready     atomic.Bool
}

func New(log *slog.Logger, repo *repository.Repository, transport WebhookTransport, addr string, maxBody int64) *App {
	return &App{log: log, repo: repo, transport: transport, addr: addr, maxBody: maxBody}
}
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /health/ready", a.readiness)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("/v1/webhooks/{id}/{token}", a.webhook)
	return http.MaxBytesHandler(mux, a.maxBody)
}
func (a *App) Run(ctx context.Context) error {
	srv := &http.Server{Addr: a.addr, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 60 * time.Second}
	a.ready.Store(true)
	ch := make(chan error, 1)
	go func() { ch <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		a.ready.Store(false)
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	case err := <-ch:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
func (a *App) readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if !a.ready.Load() || !a.transport.Ready() || a.repo.Ping(ctx) != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (a *App) metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	s, err := a.repo.Stats(ctx)
	if err != nil {
		http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "proxy_requests{status=\"awaiting_ack\"} %d\nproxy_requests{status=\"ready\"} %d\nproxy_requests{status=\"dispatching\"} %d\nproxy_requests{status=\"completed\"} %d\nproxy_requests{status=\"unknown\"} %d\nproxy_deliveries_pending %d\n", s.AwaitingACK, s.Ready, s.Dispatching, s.Completed, s.Unknown, s.Deliveries)
}
func (a *App) webhook(w http.ResponseWriter, r *http.Request) {
	route, err := a.repo.GetWebhookRoute(r.Context(), r.PathValue("id"))
	if err != nil || !route.Enabled || !tokenOK(r.PathValue("token"), route.TokenHash) {
		http.NotFound(w, r)
		return
	}
	limit := route.MaxBodyBytes
	if limit <= 0 || limit > a.maxBody {
		limit = a.maxBody
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	eventHeaders := r.Header.Clone()
	if r.Host != "" {
		eventHeaders.Set("Host", r.Host)
	}
	event := contracts.WebhookEvent{EventID: "event_" + randomID(), WebhookID: route.ID, Method: r.Method, RequestURI: r.URL.RequestURI(), Headers: headers(eventHeaders), Body: body, ReceivedAt: time.Now().UTC()}
	ctx := r.Context()
	if route.Mode == "delegated" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, route.ResponseTimeout)
		defer cancel()
	}
	response, err := a.transport.DeliverWebhook(ctx, event, route)
	if err != nil {
		a.log.Warn("webhook delivery", "webhook_id", route.ID, "error", err)
		http.Error(w, "upstream handler unavailable", http.StatusGatewayTimeout)
		return
	}
	if route.Mode == "static" {
		writeResponse(w, route.StaticResponse.StatusCode, route.StaticResponse.Headers, route.StaticResponse.Body)
		return
	}
	writeResponse(w, response.StatusCode, response.Headers, response.Body)
}
func writeResponse(w http.ResponseWriter, status int, headers []contracts.HeaderField, body []byte) {
	for _, h := range headers {
		if !hopByHop(h.Name) {
			w.Header().Add(h.Name, h.Value)
		}
	}
	if status < 100 || status > 599 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
func headers(h http.Header) []contracts.HeaderField {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []contracts.HeaderField
	for _, k := range keys {
		for _, v := range h.Values(k) {
			out = append(out, contracts.HeaderField{Name: k, Value: v})
		}
	}
	return out
}
func hopByHop(v string) bool {
	switch strings.ToLower(v) {
	case "connection", "proxy-connection", "keep-alive", "transfer-encoding", "upgrade", "te", "trailer":
		return true
	}
	return false
}
func tokenOK(token string, want []byte) bool {
	sum := sha256.Sum256([]byte(token))
	return len(want) == len(sum) && subtle.ConstantTimeCompare(sum[:], want) == 1
}
func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%x", b[:])
}
