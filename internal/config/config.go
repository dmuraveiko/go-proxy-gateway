package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type HostLimit struct {
	RPS         int
	Concurrency int
	MinInterval time.Duration
}

type Config struct {
	ProxyID, InstanceID, HTTPAddr, PublicBaseURL, DatabaseURL, NATSURL string
	NATSCredsFile, NATSCACert, NATSClientCert, NATSClientKey           string
	SigningPrivateKeyFile                                              string
	ClientPublicKeys                                                   map[string]string
	AllowedClients                                                     map[string]bool
	RequireSignature, AutoMigrate                                      bool
	Workers, DeliveryWorkers, DBMaxConns                               int
	MaxMessageBytes, MaxRequestBytes, MaxResponseBytes                 int64
	RequestTimeout, MaxRequestTimeout, DeliveryRetry                   time.Duration
	DispatchLease, ShutdownTimeout, Retention                          time.Duration
	CleanupInterval                                                    time.Duration
	DefaultHostLimit                                                   HostLimit
	HostLimits                                                         map[string]HostLimit
	WebhookDefaultMaxBody                                              int64
	WebhookDefaultTimeout                                              time.Duration
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	c := Config{
		ProxyID: env("PROXY_ID", "default"), InstanceID: env("PROXY_INSTANCE_ID", host),
		HTTPAddr: env("PROXY_HTTP_ADDR", ":8080"), PublicBaseURL: env("PROXY_PUBLIC_BASE_URL", "http://localhost:8080"),
		DatabaseURL: os.Getenv("PROXY_DATABASE_URL"), NATSURL: env("PROXY_NATS_URL", "nats://127.0.0.1:4222"),
		NATSCredsFile: os.Getenv("PROXY_NATS_CREDS_FILE"), NATSCACert: os.Getenv("PROXY_NATS_CA_CERT"),
		NATSClientCert: os.Getenv("PROXY_NATS_CLIENT_CERT"), NATSClientKey: os.Getenv("PROXY_NATS_CLIENT_KEY"),
		SigningPrivateKeyFile: os.Getenv("PROXY_SIGNING_PRIVATE_KEY_FILE"),
		ClientPublicKeys:      clientKeys(os.Getenv("PROXY_CLIENT_PUBLIC_KEYS")), AllowedClients: set(os.Getenv("PROXY_ALLOWED_CLIENTS")),
		HostLimits: map[string]HostLimit{},
	}
	var err error
	if c.Workers, err = intVal("PROXY_WORKERS", 16); err != nil {
		return c, err
	}
	if c.DeliveryWorkers, err = intVal("PROXY_DELIVERY_WORKERS", 4); err != nil {
		return c, err
	}
	if c.DBMaxConns, err = intVal("PROXY_DB_MAX_CONNS", 32); err != nil {
		return c, err
	}
	if c.MaxMessageBytes, err = int64Val("PROXY_MAX_MESSAGE_BYTES", 8<<20); err != nil {
		return c, err
	}
	if c.MaxRequestBytes, err = int64Val("PROXY_MAX_REQUEST_BYTES", 4<<20); err != nil {
		return c, err
	}
	if c.MaxResponseBytes, err = int64Val("PROXY_MAX_RESPONSE_BYTES", 4<<20); err != nil {
		return c, err
	}
	if c.WebhookDefaultMaxBody, err = int64Val("PROXY_WEBHOOK_MAX_BODY_BYTES", 4<<20); err != nil {
		return c, err
	}
	if c.DefaultHostLimit.RPS, err = intVal("PROXY_DEFAULT_HOST_RPS", 20); err != nil {
		return c, err
	}
	if c.DefaultHostLimit.Concurrency, err = intVal("PROXY_DEFAULT_HOST_CONCURRENCY", 8); err != nil {
		return c, err
	}
	if c.DefaultHostLimit.MinInterval, err = durationVal("PROXY_DEFAULT_HOST_MIN_INTERVAL", 0); err != nil {
		return c, err
	}
	if c.HostLimits, err = hostLimits(os.Getenv("PROXY_HOST_LIMITS")); err != nil {
		return c, err
	}
	if c.RequireSignature, err = boolVal("PROXY_REQUIRE_SIGNATURE", true); err != nil {
		return c, err
	}
	if c.AutoMigrate, err = boolVal("PROXY_AUTO_MIGRATE", false); err != nil {
		return c, err
	}
	if c.RequestTimeout, err = durationVal("PROXY_REQUEST_TIMEOUT", 30*time.Second); err != nil {
		return c, err
	}
	if c.MaxRequestTimeout, err = durationVal("PROXY_MAX_REQUEST_TIMEOUT", 2*time.Minute); err != nil {
		return c, err
	}
	if c.DeliveryRetry, err = durationVal("PROXY_DELIVERY_RETRY", time.Second); err != nil {
		return c, err
	}
	if c.DispatchLease, err = durationVal("PROXY_DISPATCH_LEASE", 3*time.Minute); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = durationVal("PROXY_SHUTDOWN_TIMEOUT", 30*time.Second); err != nil {
		return c, err
	}
	if c.Retention, err = durationVal("PROXY_RETENTION", 30*24*time.Hour); err != nil {
		return c, err
	}
	if c.CleanupInterval, err = durationVal("PROXY_CLEANUP_INTERVAL", time.Hour); err != nil {
		return c, err
	}
	if c.WebhookDefaultTimeout, err = durationVal("PROXY_WEBHOOK_RESPONSE_TIMEOUT", 10*time.Second); err != nil {
		return c, err
	}
	if c.DatabaseURL == "" {
		return c, errors.New("PROXY_DATABASE_URL is required")
	}
	if c.ProxyID == "" || strings.ContainsAny(c.ProxyID, ".*> ") {
		return c, errors.New("PROXY_ID must be a single NATS token")
	}
	if c.InstanceID == "" || strings.ContainsAny(c.InstanceID, ".*> ") {
		return c, errors.New("PROXY_INSTANCE_ID must be a single NATS token")
	}
	if c.Workers < 1 || c.DeliveryWorkers < 1 || c.DBMaxConns < 4 || c.DefaultHostLimit.RPS < 1 || c.DefaultHostLimit.Concurrency < 1 || c.DefaultHostLimit.MinInterval < 0 {
		return c, errors.New("invalid sizing configuration")
	}
	if c.MaxRequestTimeout < c.RequestTimeout || c.DispatchLease <= c.MaxRequestTimeout {
		return c, errors.New("dispatch lease must exceed max request timeout")
	}
	publicURL, parseErr := url.Parse(c.PublicBaseURL)
	if parseErr != nil || publicURL.Host == "" || (publicURL.Scheme != "http" && publicURL.Scheme != "https") {
		if parseErr == nil {
			parseErr = errors.New("absolute HTTP/HTTPS URL is required")
		}
		err = parseErr
		return c, fmt.Errorf("PROXY_PUBLIC_BASE_URL: %w", err)
	}
	if c.RequireSignature && (c.SigningPrivateKeyFile == "" || len(c.ClientPublicKeys) == 0 || len(c.AllowedClients) == 0) {
		return c, errors.New("signing key, client public keys and allowed clients are required")
	}
	for clientID := range c.AllowedClients {
		if clientID == "" || strings.ContainsAny(clientID, ".*> ") {
			return c, fmt.Errorf("invalid client ID %q", clientID)
		}
		if c.RequireSignature {
			if _, ok := c.ClientPublicKeys[clientID]; !ok {
				return c, fmt.Errorf("allowed client %q has no public key", clientID)
			}
		}
	}
	needed := max(c.MaxRequestBytes, c.MaxResponseBytes)*4/3 + 64<<10
	if c.MaxMessageBytes < needed {
		return c, fmt.Errorf("PROXY_MAX_MESSAGE_BYTES must be at least %d for base64 payloads", needed)
	}
	if (c.NATSClientCert == "") != (c.NATSClientKey == "") {
		return c, errors.New("NATS client certificate and key must be configured together")
	}
	return c, nil
}

func (c Config) LimitForHost(host string) HostLimit {
	if v, ok := c.HostLimits[strings.ToLower(host)]; ok {
		return v
	}
	return c.DefaultHostLimit
}

func clientKeys(v string) map[string]string {
	out := map[string]string{}
	for _, entry := range strings.Split(v, ";") {
		p := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(p) == 2 && p[0] != "" && p[1] != "" {
			out[p[0]] = p[1]
		}
	}
	return out
}
func set(v string) map[string]bool {
	out := map[string]bool{}
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out[x] = true
		}
	}
	return out
}
func hostLimits(v string) (map[string]HostLimit, error) {
	out := map[string]HostLimit{}
	for _, entry := range strings.Split(v, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		p := strings.SplitN(entry, "=", 2)
		if len(p) != 2 {
			return nil, fmt.Errorf("invalid host limit %q", entry)
		}
		n := strings.Split(p[1], ":")
		if len(n) < 2 || len(n) > 3 {
			return nil, fmt.Errorf("invalid host limit %q", entry)
		}
		rps, e1 := strconv.Atoi(n[0])
		concurrency, e2 := strconv.Atoi(n[1])
		if e1 != nil || e2 != nil || rps < 1 || concurrency < 1 {
			return nil, fmt.Errorf("invalid host limit %q", entry)
		}
		var minInterval time.Duration
		if len(n) == 3 {
			minInterval, e1 = time.ParseDuration(n[2])
			if e1 != nil || minInterval < 0 {
				return nil, fmt.Errorf("invalid host limit %q", entry)
			}
		}
		out[strings.ToLower(p[0])] = HostLimit{RPS: rps, Concurrency: concurrency, MinInterval: minInterval}
	}
	return out, nil
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
	n, e := strconv.ParseBool(v)
	if e != nil {
		return false, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}
func durationVal(k string, d time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return d, nil
	}
	n, e := time.ParseDuration(v)
	if e != nil {
		return 0, fmt.Errorf("%s: %w", k, e)
	}
	return n, nil
}
