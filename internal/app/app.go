package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"proxy-server/internal/integration"
	"proxy-server/internal/repository"
)

type App struct {
	log         *slog.Logger
	repo        *repository.Repository
	registry    *integration.Registry
	eventPrefix string
	maxBody     int64
	ready       atomic.Bool
	natsReady   func() bool
}

func New(log *slog.Logger, repo *repository.Repository, registry *integration.Registry, eventPrefix string, natsReady func() bool) *App {
	return &App{log: log, repo: repo, registry: registry, eventPrefix: eventPrefix, maxBody: 2 << 20, natsReady: natsReady}
}
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /health/ready", a.readiness)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("POST /v1/webhooks/{provider}", a.webhook)
	return http.MaxBytesHandler(mux, a.maxBody)
}
func (a *App) Run(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	a.ready.Store(true)
	errch := make(chan error, 1)
	go func() { errch <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		a.ready.Store(false)
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	case err := <-errch:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
func (a *App) readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if !a.ready.Load() || !a.natsReady() || a.repo.Ping(ctx) != nil {
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
	fmt.Fprintf(w, "proxy_operations{status=\"pending\"} %d\nproxy_operations{status=\"retrying\"} %d\nproxy_operations{status=\"processing\"} %d\nproxy_operations{status=\"failed\"} %d\nproxy_oldest_pending_seconds %f\nproxy_outbox_pending %d\n", s.Pending, s.Retrying, s.Processing, s.Failed, s.OldestPendingSeconds, s.OutboxPending)
}
func (a *App) webhook(w http.ResponseWriter, r *http.Request) {
	handler, err := a.registry.Webhook(r.PathValue("provider"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, a.maxBody+1))
	if err != nil || int64(len(body)) > a.maxBody {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err = handler.Authenticate(r, body); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	events, err := handler.Parse(body)
	if err != nil {
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
	}
	if err = a.repo.SaveWebhook(r.Context(), events, a.eventPrefix); err != nil {
		a.log.Error("save webhook", "error", err)
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
