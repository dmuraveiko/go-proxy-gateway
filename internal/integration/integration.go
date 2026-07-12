package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"proxy-server/internal/contracts"
)

type RetryPolicy struct {
	MaxAttempts                int
	InitialBackoff, MaxBackoff time.Duration
}
type ExecutionError struct {
	Code      string
	Err       error
	Retryable bool
}

func (e *ExecutionError) Error() string      { return e.Err.Error() }
func Permanent(code string, err error) error { return &ExecutionError{Code: code, Err: err} }
func Retryable(code string, err error) error {
	return &ExecutionError{Code: code, Err: err, Retryable: true}
}

type CommandHandler interface {
	Type() string
	Version() int
	Validate(json.RawMessage) error
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
	RetryPolicy() RetryPolicy
}

type WebhookHandler interface {
	Provider() string
	Authenticate(*http.Request, []byte) error
	Parse([]byte) ([]contracts.Event, error)
}

type Registry struct {
	mu       sync.RWMutex
	commands map[string]CommandHandler
	webhooks map[string]WebhookHandler
}

func NewRegistry() *Registry {
	return &Registry{commands: map[string]CommandHandler{}, webhooks: map[string]WebhookHandler{}}
}
func key(kind string, version int) string { return fmt.Sprintf("%s:v%d", kind, version) }
func (r *Registry) RegisterCommand(h CommandHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key(h.Type(), h.Version())
	if _, ok := r.commands[k]; ok {
		return fmt.Errorf("command handler %s already registered", k)
	}
	r.commands[k] = h
	return nil
}
func (r *Registry) RegisterWebhook(h WebhookHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.webhooks[h.Provider()]; ok {
		return fmt.Errorf("webhook handler %s already registered", h.Provider())
	}
	r.webhooks[h.Provider()] = h
	return nil
}
func (r *Registry) Command(kind string, version int) (CommandHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.commands[key(kind, version)]
	if !ok {
		return nil, errors.New("unsupported command type or version")
	}
	return h, nil
}
func (r *Registry) Webhook(provider string) (WebhookHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.webhooks[provider]
	if !ok {
		return nil, errors.New("unknown webhook provider")
	}
	return h, nil
}
