package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr, DatabaseURL, NATSURL, NATSCredsFile                        string
	NATSCACert, NATSClientCert, NATSClientKey                            string
	CommandsStream, ResultsStream, EventsStream, DLQStream               string
	CommandSubject, CommandDurable, ResultPrefix, EventPrefix, DLQPrefix string
	StreamReplicas                                                       int
	StreamMaxBytes                                                       int64
	MaxMessageBytes                                                      int32
	AckWait                                                              time.Duration
	MaxAckPending, FetchBatch, OutboxWorkers                             int
	SigningPrivateKeyFile                                                string
	VerifyPublicKeys                                                     []string
	KeyPermissions                                                       map[string][]string
	RequireSignature                                                     bool
	AutoMigrate                                                          bool
	Workers                                                              int
	DBMaxConns                                                           int32
	RequestTimeout, ShutdownTimeout, Retention                           time.Duration
	TransactionEndpoint, ProviderAPIKeyHeader, ProviderAPIKey            string
	ProviderRPS                                                          int
	WebhookProvider, WebhookSecret                                       string
	WebhookEventTypes                                                    map[string]string
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("PROXY_HTTP_ADDR", ":8080"), DatabaseURL: os.Getenv("PROXY_DATABASE_URL"), NATSURL: env("PROXY_NATS_URL", "nats://127.0.0.1:4222"), NATSCredsFile: os.Getenv("PROXY_NATS_CREDS_FILE"), NATSCACert: os.Getenv("PROXY_NATS_CA_CERT"), NATSClientCert: os.Getenv("PROXY_NATS_CLIENT_CERT"), NATSClientKey: os.Getenv("PROXY_NATS_CLIENT_KEY"), CommandsStream: env("PROXY_COMMANDS_STREAM", "PROXY_COMMANDS"), ResultsStream: env("PROXY_RESULTS_STREAM", "PROXY_RESULTS"), EventsStream: env("PROXY_EVENTS_STREAM", "PROXY_EVENTS"), DLQStream: env("PROXY_DLQ_STREAM", "PROXY_DLQ"), CommandSubject: env("PROXY_COMMAND_SUBJECT", "proxy.commands.>"), CommandDurable: env("PROXY_COMMAND_DURABLE", "proxy-gateway"), ResultPrefix: env("PROXY_RESULT_PREFIX", "proxy.results"), EventPrefix: env("PROXY_EVENT_PREFIX", "proxy.events"), DLQPrefix: env("PROXY_DLQ_PREFIX", "proxy.dlq"), SigningPrivateKeyFile: os.Getenv("PROXY_SIGNING_PRIVATE_KEY_FILE"), VerifyPublicKeys: csv(os.Getenv("PROXY_VERIFY_PUBLIC_KEYS")), KeyPermissions: permissions(os.Getenv("PROXY_KEY_PERMISSIONS")), TransactionEndpoint: os.Getenv("PROXY_TRANSACTION_STATUS_ENDPOINT"), ProviderAPIKeyHeader: env("PROXY_PROVIDER_API_KEY_HEADER", "Authorization"), ProviderAPIKey: os.Getenv("PROXY_PROVIDER_API_KEY"), WebhookProvider: os.Getenv("PROXY_WEBHOOK_PROVIDER"), WebhookSecret: os.Getenv("PROXY_WEBHOOK_SECRET"), WebhookEventTypes: stringMap(os.Getenv("PROXY_WEBHOOK_EVENT_TYPES"))}
	var err error
	if c.StreamReplicas, err = intVal("PROXY_STREAM_REPLICAS", 3); err != nil {
		return c, err
	}
	if c.StreamMaxBytes, err = int64Val("PROXY_STREAM_MAX_BYTES", 10<<30); err != nil {
		return c, err
	}
	msg, e := intVal("PROXY_MAX_MESSAGE_BYTES", 2<<20)
	if e != nil {
		return c, e
	}
	c.MaxMessageBytes = int32(msg)
	if c.MaxAckPending, err = intVal("PROXY_MAX_ACK_PENDING", 1024); err != nil {
		return c, err
	}
	if c.FetchBatch, err = intVal("PROXY_FETCH_BATCH", 64); err != nil {
		return c, err
	}
	if c.OutboxWorkers, err = intVal("PROXY_OUTBOX_WORKERS", 4); err != nil {
		return c, err
	}
	if c.Workers, err = intVal("PROXY_WORKERS", 16); err != nil {
		return c, err
	}
	db, e := intVal("PROXY_DB_MAX_CONNS", 32)
	if e != nil {
		return c, e
	}
	c.DBMaxConns = int32(db)
	if c.ProviderRPS, err = intVal("PROXY_PROVIDER_RPS", 20); err != nil {
		return c, err
	}
	if c.RequireSignature, err = boolVal("PROXY_REQUIRE_SIGNATURE", true); err != nil {
		return c, err
	}
	if c.AutoMigrate, err = boolVal("PROXY_AUTO_MIGRATE", false); err != nil {
		return c, err
	}
	if c.AckWait, err = durationVal("PROXY_ACK_WAIT", time.Minute); err != nil {
		return c, err
	}
	if c.RequestTimeout, err = durationVal("PROXY_REQUEST_TIMEOUT", 30*time.Second); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = durationVal("PROXY_SHUTDOWN_TIMEOUT", 30*time.Second); err != nil {
		return c, err
	}
	if c.Retention, err = durationVal("PROXY_RETENTION", 30*24*time.Hour); err != nil {
		return c, err
	}
	if c.DatabaseURL == "" {
		return c, errors.New("PROXY_DATABASE_URL is required")
	}
	if c.Workers < 1 || c.OutboxWorkers < 1 || c.DBMaxConns < 4 || c.StreamReplicas < 1 || c.StreamReplicas > 5 {
		return c, errors.New("invalid sizing configuration")
	}
	if c.RequireSignature && (c.SigningPrivateKeyFile == "" || len(c.VerifyPublicKeys) == 0 || len(c.KeyPermissions) == 0) {
		return c, errors.New("signing private key and verification public keys are required")
	}
	if (c.WebhookProvider == "") != (c.WebhookSecret == "") {
		return c, errors.New("webhook provider and secret must be configured together")
	}
	if c.WebhookProvider != "" && len(c.WebhookEventTypes) == 0 {
		return c, errors.New("PROXY_WEBHOOK_EVENT_TYPES is required with webhook provider")
	}
	if (c.NATSClientCert == "") != (c.NATSClientKey == "") {
		return c, errors.New("NATS client certificate and key must be configured together")
	}
	return c, nil
}
func permissions(v string) map[string][]string {
	out := map[string][]string{}
	for _, entry := range strings.Split(v, ";") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) == 2 && parts[0] != "" {
			out[parts[0]] = strings.Split(parts[1], "|")
		}
	}
	return out
}
func stringMap(v string) map[string]string {
	out := map[string]string{}
	for _, entry := range strings.Split(v, ";") {
		p := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(p) == 2 && p[0] != "" && p[1] != "" {
			out[p[0]] = p[1]
		}
	}
	return out
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func intVal(k string, d int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	n, e := strconv.Atoi(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}
func int64Val(k string, d int64) (int64, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	n, e := strconv.ParseInt(v, 10, 64)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}
func boolVal(k string, d bool) (bool, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	b, e := strconv.ParseBool(v)
	if e != nil {
		return false, fmt.Errorf("%s: %w", k, e)
	}
	return b, nil
}
func durationVal(k string, d time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	x, e := time.ParseDuration(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return x, nil
}
func csv(v string) []string {
	var o []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			o = append(o, s)
		}
	}
	return o
}
