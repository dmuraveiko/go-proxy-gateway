package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"proxy-server/internal/app"
	"proxy-server/internal/config"
	"proxy-server/internal/integration"
	"proxy-server/internal/integrations/genericwebhook"
	"proxy-server/internal/integrations/jsonapi"
	"proxy-server/internal/message"
	"proxy-server/internal/repository"
	"proxy-server/internal/transport"
	"proxy-server/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	signer, err := message.LoadPrivateKey(cfg.SigningPrivateKeyFile)
	if err != nil {
		return err
	}
	verifiers, err := message.ParsePublicKeys(cfg.VerifyPublicKeys)
	if err != nil {
		return err
	}
	if cfg.RequireSignature {
		for _, key := range verifiers {
			if len(cfg.KeyPermissions[message.PublicKeyID(key)]) == 0 {
				return errors.New("every verification key must have command permissions")
			}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	repo, err := repository.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer repo.Close()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		return repo.Migrate(ctx)
	}
	if cfg.AutoMigrate {
		if err = repo.Migrate(ctx); err != nil {
			return err
		}
	}
	if _, err = repo.Stats(ctx); err != nil {
		return errors.New("database is not migrated; run `proxy migrate`")
	}
	opts := []nats.Option{nats.Name("integration-gateway"), nats.Timeout(5 * time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second)}
	if cfg.NATSCredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.NATSCredsFile))
	}
	if cfg.NATSCACert != "" {
		opts = append(opts, nats.RootCAs(cfg.NATSCACert))
	}
	if cfg.NATSClientCert != "" {
		opts = append(opts, nats.ClientCert(cfg.NATSClientCert, cfg.NATSClientKey))
	}
	nc, err := nats.Connect(cfg.NATSURL, opts...)
	if err != nil {
		return err
	}
	defer nc.Drain()
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))
	if err != nil {
		return err
	}
	registry := integration.NewRegistry()
	if cfg.TransactionEndpoint != "" {
		handler, handlerErr := jsonapi.NewTransactionStatus(cfg.TransactionEndpoint, cfg.ProviderAPIKeyHeader, cfg.ProviderAPIKey, cfg.RequestTimeout, cfg.ProviderRPS, func(ctx context.Context) error {
			return repo.AcquireRateToken(ctx, "transaction-status", cfg.ProviderRPS)
		})
		if handlerErr != nil {
			return handlerErr
		}
		if err = registry.RegisterCommand(handler); err != nil {
			return err
		}
	}
	if cfg.WebhookProvider != "" {
		if err = registry.RegisterWebhook(genericwebhook.New(cfg.WebhookProvider, cfg.WebhookSecret, cfg.WebhookEventTypes)); err != nil {
			return err
		}
	}
	streamCfg := transport.Config{
		Commands: transport.StreamSpec{Name: cfg.CommandsStream, Subject: cfg.CommandSubject, MaxAge: 7 * 24 * time.Hour, MaxBytes: cfg.StreamMaxBytes, MaxMsgSize: cfg.MaxMessageBytes, Discard: nats.DiscardNew},
		Results:  transport.StreamSpec{Name: cfg.ResultsStream, Subject: cfg.ResultPrefix + ".>", MaxAge: 30 * 24 * time.Hour, MaxBytes: cfg.StreamMaxBytes, MaxMsgSize: cfg.MaxMessageBytes, Discard: nats.DiscardNew},
		Events:   transport.StreamSpec{Name: cfg.EventsStream, Subject: cfg.EventPrefix + ".>", MaxAge: 30 * 24 * time.Hour, MaxBytes: cfg.StreamMaxBytes, MaxMsgSize: cfg.MaxMessageBytes, Discard: nats.DiscardNew},
		DLQ:      transport.StreamSpec{Name: cfg.DLQStream, Subject: cfg.DLQPrefix + ".>", MaxAge: 90 * 24 * time.Hour, MaxBytes: cfg.StreamMaxBytes, MaxMsgSize: cfg.MaxMessageBytes, Discard: nats.DiscardNew},
		Durable:  cfg.CommandDurable, Replicas: cfg.StreamReplicas, AckWait: cfg.AckWait, MaxAckPending: cfg.MaxAckPending, FetchBatch: cfg.FetchBatch,
	}
	var natsReady atomic.Bool
	transportLayer := transport.New(js, repo, log, streamCfg, signer, verifiers, cfg.KeyPermissions, cfg.RequireSignature, natsReady.Store)
	if err = transportLayer.Ensure(ctx); err != nil {
		return err
	}
	workers := worker.New(repo, registry, log, cfg.Workers, cfg.ResultPrefix, cfg.DLQPrefix, cfg.RequestTimeout+time.Minute)
	httpApp := app.New(log, repo, registry, cfg.EventPrefix, natsReady.Load)
	serviceCount := 3 + cfg.OutboxWorkers
	errch := make(chan error, serviceCount)
	go func() { errch <- transportLayer.Consume(ctx) }()
	for range cfg.OutboxWorkers {
		go func() { errch <- transportLayer.PublishOutbox(ctx) }()
	}
	go func() { errch <- workers.Run(ctx) }()
	go func() { errch <- httpApp.Run(ctx, cfg.HTTPAddr) }()
	go maintenance(ctx, repo, log, cfg.Retention)
	select {
	case <-ctx.Done():
		return drainServices(errch, serviceCount, cfg.ShutdownTimeout, nil)
	case err = <-errch:
		stop()
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		return drainServices(errch, serviceCount-1, cfg.ShutdownTimeout, err)
	}
}
func drainServices(ch <-chan error, count int, timeout time.Duration, first error) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for range count {
		select {
		case err := <-ch:
			if first == nil && err != nil && !errors.Is(err, context.Canceled) {
				first = err
			}
		case <-timer.C:
			if first != nil {
				return first
			}
			return errors.New("graceful shutdown timed out")
		}
	}
	return first
}
func maintenance(ctx context.Context, repo *repository.Repository, log *slog.Logger, retention time.Duration) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := repo.Cleanup(ctx, time.Now().Add(-retention))
			if err != nil {
				log.Error("retention cleanup", "error", err)
			} else {
				log.Info("retention cleanup", "deleted_operations", n)
			}
		}
	}
}
