package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dmuraveiko/go-proxy-gateway/internal/app"
	"github.com/dmuraveiko/go-proxy-gateway/internal/config"
	"github.com/dmuraveiko/go-proxy-gateway/internal/httpx"
	"github.com/dmuraveiko/go-proxy-gateway/internal/message"
	"github.com/dmuraveiko/go-proxy-gateway/internal/repository"
	"github.com/dmuraveiko/go-proxy-gateway/internal/transport"
	"github.com/dmuraveiko/go-proxy-gateway/internal/worker"
	"github.com/nats-io/nats.go"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("proxy stopped", "error", err)
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
	clientKeys, clientByKeyID, err := message.ParseClientPublicKeys(cfg.ClientPublicKeys)
	if err != nil {
		return err
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
	if err = repo.BindProxyID(ctx, cfg.ProxyID); err != nil {
		return err
	}
	opts := []nats.Option{nats.Name("http-nats-proxy:" + cfg.ProxyID + ":" + cfg.InstanceID), nats.Timeout(5 * time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second)}
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
	defer nc.Close()
	core := transport.New(nc, repo, log, transport.Config{ProxyID: cfg.ProxyID, InstanceID: cfg.InstanceID, PublicBaseURL: cfg.PublicBaseURL, DeliveryWorkers: cfg.DeliveryWorkers, DeliveryRetry: cfg.DeliveryRetry, MaxMessageBytes: cfg.MaxMessageBytes, MaxRequestBytes: cfg.MaxRequestBytes, RequireSignature: cfg.RequireSignature}, signer, clientKeys, clientByKeyID, cfg.AllowedClients)
	executor := httpx.New(cfg.MaxResponseBytes, 256, 32, 90*time.Second)
	defer executor.CloseIdleConnections()
	workers := worker.New(repo, executor, core, cfg, log)
	httpApp := app.New(log, repo, core, cfg.HTTPAddr, cfg.WebhookDefaultMaxBody)
	errch := make(chan error, 3)
	go func() { errch <- core.Run(ctx) }()
	go func() { errch <- workers.Run(ctx) }()
	go func() { errch <- httpApp.Run(ctx) }()
	go maintenance(ctx, repo, log, cfg)
	select {
	case <-ctx.Done():
		return drain(errch, 3, cfg.ShutdownTimeout, nil)
	case err = <-errch:
		stop()
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		return drain(errch, 2, cfg.ShutdownTimeout, err)
	}
}
func maintenance(ctx context.Context, repo *repository.Repository, log *slog.Logger, cfg config.Config) {
	recoveryTicker := time.NewTicker(5 * time.Second)
	cleanupTicker := time.NewTicker(cfg.CleanupInterval)
	defer recoveryTicker.Stop()
	defer cleanupTicker.Stop()
	recoverExpired := func() {
		if n, err := repo.RecoverExpiredDispatches(ctx); err != nil {
			log.Error("recover expired dispatches", "error", err)
		} else if n > 0 {
			log.Warn("expired dispatches recovered", "count", n)
		}
	}
	cleanup := func() {
		before := time.Now().Add(-cfg.Retention)
		if n, err := repo.Cleanup(ctx, before, 1000); err != nil {
			log.Error("retention cleanup", "error", err)
		} else if n > 0 {
			log.Info("retention cleanup", "deleted", n)
		}
	}
	recoverExpired()
	cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case <-recoveryTicker.C:
			recoverExpired()
		case <-cleanupTicker.C:
			cleanup()
		}
	}
}
func drain(ch <-chan error, count int, timeout time.Duration, first error) error {
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
