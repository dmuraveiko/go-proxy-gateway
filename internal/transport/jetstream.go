package transport

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"proxy-server/internal/contracts"
	"proxy-server/internal/message"
	"proxy-server/internal/repository"
)

var safeType = regexp.MustCompile(`^[a-z0-9_]+(?:\.[a-z0-9_]+)*$`)

type StreamSpec struct {
	Name, Subject string
	MaxAge        time.Duration
	MaxBytes      int64
	MaxMsgSize    int32
	Discard       nats.DiscardPolicy
}
type Config struct {
	Commands, Results, Events, DLQ StreamSpec
	Durable                        string
	Replicas                       int
	AckWait                        time.Duration
	MaxAckPending, FetchBatch      int
}
type JetStream struct {
	js               nats.JetStreamContext
	repo             *repository.Repository
	log              *slog.Logger
	cfg              Config
	signer           ed25519.PrivateKey
	verifiers        []ed25519.PublicKey
	permissions      map[string][]string
	requireSignature bool
	healthy          func(bool)
}

func New(js nats.JetStreamContext, repo *repository.Repository, log *slog.Logger, cfg Config, signer ed25519.PrivateKey, verifiers []ed25519.PublicKey, permissions map[string][]string, requireSignature bool, healthy func(bool)) *JetStream {
	return &JetStream{js: js, repo: repo, log: log, cfg: cfg, signer: signer, verifiers: verifiers, permissions: permissions, requireSignature: requireSignature, healthy: healthy}
}
func (t *JetStream) Ensure(ctx context.Context) error {
	for _, s := range []StreamSpec{t.cfg.Commands, t.cfg.Results, t.cfg.Events, t.cfg.DLQ} {
		desired := &nats.StreamConfig{Name: s.Name, Subjects: []string{s.Subject}, Retention: nats.LimitsPolicy, Storage: nats.FileStorage, Replicas: t.cfg.Replicas, MaxAge: s.MaxAge, MaxBytes: s.MaxBytes, MaxMsgSize: s.MaxMsgSize, Discard: s.Discard, Duplicates: 24 * time.Hour}
		info, err := t.js.StreamInfo(s.Name, nats.Context(ctx))
		if errors.Is(err, nats.ErrStreamNotFound) {
			if _, err = t.js.AddStream(desired, nats.Context(ctx)); err != nil {
				return fmt.Errorf("create stream %s: %w", s.Name, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		current := info.Config
		if len(current.Subjects) != 1 || current.Subjects[0] != s.Subject {
			return fmt.Errorf("stream %s has incompatible subjects", s.Name)
		}
		current.Replicas = desired.Replicas
		current.MaxAge = desired.MaxAge
		current.MaxBytes = desired.MaxBytes
		current.MaxMsgSize = desired.MaxMsgSize
		current.Discard = desired.Discard
		current.Duplicates = desired.Duplicates
		if _, err = t.js.UpdateStream(&current, nats.Context(ctx)); err != nil {
			return fmt.Errorf("reconcile stream %s: %w", s.Name, err)
		}
	}
	consumer := &nats.ConsumerConfig{Durable: t.cfg.Durable, FilterSubject: t.cfg.Commands.Subject, AckPolicy: nats.AckExplicitPolicy, AckWait: t.cfg.AckWait, MaxAckPending: t.cfg.MaxAckPending, MaxDeliver: -1, ReplayPolicy: nats.ReplayInstantPolicy, Replicas: t.cfg.Replicas}
	info, err := t.js.ConsumerInfo(t.cfg.Commands.Name, t.cfg.Durable, nats.Context(ctx))
	if errors.Is(err, nats.ErrConsumerNotFound) {
		_, err = t.js.AddConsumer(t.cfg.Commands.Name, consumer, nats.Context(ctx))
	} else if err == nil && !sameConsumer(info.Config, *consumer) {
		_, err = t.js.UpdateConsumer(t.cfg.Commands.Name, consumer, nats.Context(ctx))
	}
	if err != nil {
		return fmt.Errorf("ensure consumer: %w", err)
	}
	return nil
}
func sameConsumer(a, b nats.ConsumerConfig) bool {
	return a.Durable == b.Durable && a.FilterSubject == b.FilterSubject && a.AckPolicy == b.AckPolicy && a.AckWait == b.AckWait && a.MaxAckPending == b.MaxAckPending && a.MaxDeliver == b.MaxDeliver && a.ReplayPolicy == b.ReplayPolicy && a.Replicas == b.Replicas
}
func (t *JetStream) Consume(ctx context.Context) error {
	sub, err := t.js.PullSubscribe(t.cfg.Commands.Subject, t.cfg.Durable, nats.Bind(t.cfg.Commands.Name, t.cfg.Durable))
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	t.healthy(true)
	defer t.healthy(false)
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := sub.Fetch(t.cfg.FetchBatch, nats.MaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return err
		}
		for _, m := range msgs {
			t.accept(ctx, m)
		}
	}
}
func (t *JetStream) accept(ctx context.Context, m *nats.Msg) {
	var env message.Envelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		t.reject(m, "invalid_envelope", err)
		return
	}
	keyID := ""
	if t.requireSignature {
		var err error
		if keyID, err = env.VerifyKey(t.verifiers, 5*time.Minute); err != nil {
			t.reject(m, "invalid_signature", err)
			return
		}
	}
	var c contracts.Command
	if err := json.Unmarshal(env.Payload, &c); err != nil || c.ID == "" || !safeType.MatchString(c.Type) || c.Version < 1 {
		t.reject(m, "invalid_command", errors.New("invalid command contract"))
		return
	}
	if t.requireSignature && !allowed(t.permissions[keyID], c.Type) {
		t.reject(m, "command_not_authorized", errors.New("signing key cannot execute command type"))
		return
	}
	if err := t.repo.AcceptCommand(ctx, c); err != nil {
		_ = m.NakWithDelay(time.Second)
		return
	}
	_ = m.AckSync()
}
func allowed(patterns []string, kind string) bool {
	for _, p := range patterns {
		if p == "*" || p == kind || (strings.HasSuffix(p, ".*") && strings.HasPrefix(kind, strings.TrimSuffix(p, "*"))) {
			return true
		}
	}
	return false
}
func (t *JetStream) reject(m *nats.Msg, reason string, cause error) {
	t.log.Warn("command rejected", "reason", reason, "error", cause)
	dlq := nats.NewMsg("proxy.dlq.rejected")
	dlq.Data = m.Data
	dlq.Header.Set("X-Rejection-Reason", reason)
	dlq.Header.Set(nats.MsgIdHdr, "rejected:"+m.Header.Get(nats.MsgIdHdr))
	if _, err := t.js.PublishMsg(dlq); err != nil {
		_ = m.NakWithDelay(time.Second)
		return
	}
	_ = m.Term()
}
func (t *JetStream) PublishOutbox(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		item, err := t.repo.ClaimOutbox(ctx, 30*time.Second)
		if errors.Is(err, pgx.ErrNoRows) {
			if !wait(ctx, 100*time.Millisecond) {
				return nil
			}
			continue
		}
		if err != nil {
			return err
		}
		env, err := message.NewEnvelope("integration.delivery", json.RawMessage(item.Payload), t.signer)
		if err != nil {
			return err
		}
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		msg := nats.NewMsg(item.Subject)
		msg.Data = data
		msg.Header.Set(nats.MsgIdHdr, item.ID)
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = t.js.PublishMsg(msg, nats.Context(publishCtx))
		cancel()
		if err != nil {
			t.log.Warn("outbox publish failed", "id", item.ID, "error", err)
			continue
		}
		if err = t.repo.MarkPublished(ctx, item.ID); err != nil {
			return err
		}
	}
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
