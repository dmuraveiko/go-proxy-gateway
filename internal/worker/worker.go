package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/config"
	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
	"github.com/dmuraveiko/go-proxy-gateway/internal/httpx"
	"github.com/dmuraveiko/go-proxy-gateway/internal/metrics"
	"github.com/dmuraveiko/go-proxy-gateway/internal/repository"
	"github.com/jackc/pgx/v5"
)

type Publisher interface {
	PublishHTTPResult(context.Context, string, contracts.HTTPResult) error
}
type Pool struct {
	repo      *repository.Repository
	executor  *httpx.Executor
	publisher Publisher
	cfg       config.Config
	log       *slog.Logger
}

func New(repo *repository.Repository, executor *httpx.Executor, publisher Publisher, cfg config.Config, log *slog.Logger) *Pool {
	return &Pool{repo: repo, executor: executor, publisher: publisher, cfg: cfg, log: log}
}
func (p *Pool) Run(ctx context.Context) error {
	ch := make(chan error, p.cfg.Workers)
	for range p.cfg.Workers {
		go func() { ch <- p.run(ctx) }()
	}
	for range p.cfg.Workers {
		if err := <-ch; err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
}
func (p *Pool) run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		op, err := p.repo.ReserveRequest(ctx, p.cfg.DispatchLease)
		if errors.Is(err, pgx.ErrNoRows) {
			if !wait(ctx, 100*time.Millisecond) {
				return nil
			}
			continue
		}
		if err != nil {
			if !wait(ctx, time.Second) {
				return nil
			}
			continue
		}
		if err = p.execute(ctx, op); err != nil && ctx.Err() == nil {
			p.log.Error("execute operation", "request_id", op.Request.RequestID, "error", err)
		}
	}
}
func (p *Pool) execute(ctx context.Context, op repository.Operation) error {
	host := strings.ToLower(mustHost(op.Request.URL))
	limit := p.cfg.LimitForHost(host)
	permit, err := p.repo.AcquireHostPermit(ctx, host, limit.RPS, limit.Concurrency, limit.MinInterval, p.cfg.MaxRequestTimeout+time.Minute)
	if errors.Is(err, repository.ErrNoPermit) {
		_ = p.repo.ReleaseReserved(ctx, op.Request.RequestID, op.DispatchToken)
		wait(ctx, 50*time.Millisecond)
		return nil
	}
	if err != nil {
		_ = p.repo.ReleaseReserved(ctx, op.Request.RequestID, op.DispatchToken)
		return err
	}
	defer func() { _ = p.repo.ReleaseHostPermit(context.WithoutCancel(ctx), permit) }()
	if err = p.repo.BeginDispatch(ctx, op.Request.RequestID, op.DispatchToken, p.cfg.DispatchLease); err != nil {
		return err
	}
	timeout := op.Request.Timeout
	if timeout <= 0 {
		timeout = p.cfg.RequestTimeout
	}
	if timeout > p.cfg.MaxRequestTimeout {
		timeout = p.cfg.MaxRequestTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	result, callErr := p.executor.Do(callCtx, op.Request)
	cancel()
	attempt := op.Attempts + 1
	if callErr != nil {
		metrics.HTTPDispatches.WithLabelValues("error").Inc()
		if p.canRetry(op.Request, attempt, true, 0) {
			return p.repo.ScheduleRetry(ctx, op.Request.RequestID, op.DispatchToken, callErr.Error(), time.Now().Add(backoff(op.Request.Retry, attempt)))
		}
		result = contracts.HTTPResult{State: "unknown", ErrorCode: "http_outcome_unknown", Error: callErr.Error()}
	} else if p.canRetry(op.Request, attempt, false, result.StatusCode) {
		metrics.HTTPDispatches.WithLabelValues("retry").Inc()
		return p.repo.ScheduleRetry(ctx, op.Request.RequestID, op.DispatchToken, fmt.Sprintf("HTTP %d", result.StatusCode), time.Now().Add(backoff(op.Request.Retry, attempt)))
	} else {
		metrics.HTTPDispatches.WithLabelValues("success").Inc()
		result.State = "http_completed"
	}
	result.ResultID = "result_" + op.Request.RequestID
	result.RequestID = op.Request.RequestID
	result.ProxyID = p.cfg.ProxyID
	result.Attempts = attempt
	result.CompletedAt = time.Now().UTC()
	// The agreed exceptional ordering: first give the client a chance to persist the
	// response, then persist it locally. HTTP is never re-executed because this write fails.
	directCtx, cancelDirect := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	_ = p.publisher.PublishHTTPResult(directCtx, op.Request.ClientID, result)
	cancelDirect()
	for {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = p.repo.SaveHTTPResult(saveCtx, op.DispatchToken, op.Request.ClientID, "client."+op.Request.ClientID+".proxy."+p.cfg.ProxyID+".results", result)
		cancel()
		if err == nil {
			return nil
		}
		p.log.Error("persist HTTP result; request will not be re-sent", "request_id", op.Request.RequestID, "error", err)
		if !wait(ctx, time.Second) {
			return nil
		}
	}
}
func (p *Pool) canRetry(req contracts.HTTPRequest, attempt int, network bool, status int) bool {
	max := req.Retry.MaxAttempts
	if max <= 0 {
		max = 1
	}
	if attempt >= max {
		return false
	}
	safe := req.Method == http.MethodGet || req.Method == http.MethodHead || req.Method == http.MethodOptions || req.Retry.Idempotent
	if !safe {
		return false
	}
	if network {
		return req.Retry.RetryNetworkFail
	}
	for _, s := range req.Retry.RetryStatuses {
		if s == status {
			return true
		}
	}
	return false
}
func backoff(policy contracts.RetryPolicy, attempt int) time.Duration {
	d := policy.InitialBackoff
	if d <= 0 {
		d = time.Second
	}
	for i := 1; i < attempt && i < 20; i++ {
		d *= 2
	}
	if policy.MaxBackoff > 0 && d > policy.MaxBackoff {
		d = policy.MaxBackoff
	}
	return d + time.Duration(rand.Int64N(int64(d/2)+1))
}
func mustHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Host
}
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
