package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"proxy-server/internal/contracts"
	"proxy-server/internal/integration"
	"proxy-server/internal/repository"
)

type Pool struct {
	repo         *repository.Repository
	registry     *integration.Registry
	log          *slog.Logger
	count        int
	resultPrefix string
	dlqPrefix    string
	lease        time.Duration
}

func New(repo *repository.Repository, registry *integration.Registry, log *slog.Logger, count int, resultPrefix, dlqPrefix string, lease time.Duration) *Pool {
	return &Pool{repo: repo, registry: registry, log: log, count: count, resultPrefix: resultPrefix, dlqPrefix: dlqPrefix, lease: lease}
}
func (p *Pool) Run(ctx context.Context) error {
	errch := make(chan error, p.count)
	for range p.count {
		go func() { errch <- p.run(ctx) }()
	}
	for range p.count {
		if err := <-errch; err != nil && ctx.Err() == nil {
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
		op, err := p.repo.ClaimOperation(ctx, p.lease)
		if errors.Is(err, pgx.ErrNoRows) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			return err
		}
		p.execute(ctx, op)
	}
}
func (p *Pool) execute(ctx context.Context, op repository.Operation) {
	h, err := p.registry.Command(op.Command.Type, op.Command.Version)
	if err == nil {
		err = h.Validate(op.Command.Payload)
	}
	var payload json.RawMessage
	if err == nil {
		payload, err = h.Execute(ctx, op.Command.Payload)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
		if e := p.repo.Release(context.WithoutCancel(ctx), op.Command.ID); e != nil {
			p.log.Error("release canceled operation", "error", e)
		}
		return
	}
	result := contracts.Result{CommandID: op.Command.ID, CorrelationID: op.Command.CorrelationID, Type: op.Command.Type + ".result", Payload: payload, Attempts: op.Attempts, FinishedAt: time.Now().UTC()}
	subject := p.resultPrefix + "." + op.Command.Type
	if err == nil {
		result.Status = "succeeded"
		if e := p.repo.Complete(ctx, result, subject); e != nil {
			p.log.Error("complete operation", "error", e)
		}
		return
	}
	policy := integration.RetryPolicy{MaxAttempts: 1}
	if h != nil {
		policy = h.RetryPolicy()
	}
	code, retryable := "execution_failed", false
	var executionErr *integration.ExecutionError
	if errors.As(err, &executionErr) {
		code = executionErr.Code
		retryable = executionErr.Retryable
	}
	if retryable && op.Attempts < policy.MaxAttempts {
		delay := backoff(policy, op.Attempts)
		if e := p.repo.Retry(ctx, op.Command.ID, code, err.Error(), time.Now().Add(delay)); e != nil {
			p.log.Error("schedule retry", "error", e)
		}
		return
	}
	result.Status = "failed"
	result.Error = &contracts.Problem{Code: code, Message: err.Error(), Retryable: false}
	if e := p.repo.Fail(ctx, result, subject, p.dlqPrefix+"."+op.Command.Type); e != nil {
		p.log.Error("fail operation", "error", e)
	}
}
func backoff(p integration.RetryPolicy, attempt int) time.Duration {
	d := p.InitialBackoff << min(attempt-1, 20)
	if d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	if d <= 0 {
		d = time.Second
	}
	return d + time.Duration(rand.Int64N(int64(d/2)+1))
}
