package client

import (
	"context"
	"sync"

	"github.com/dmuraveiko/go-proxy-gateway/internal/contracts"
)

// MemoryStore is only for tests and local development; it is not restart-safe.
type MemoryStore struct {
	mu         sync.Mutex
	Operations map[string]StoredOperation
	Callbacks  map[string]bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{Operations: map[string]StoredOperation{}, Callbacks: map[string]bool{}}
}

func (s *MemoryStore) SaveCallback(_ context.Context, event contracts.WebhookEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	complete := s.Callbacks[event.DeliveryID]
	if _, exists := s.Callbacks[event.DeliveryID]; !exists {
		s.Callbacks[event.DeliveryID] = false
	}
	return complete, nil
}

func (s *MemoryStore) MarkCallbackComplete(_ context.Context, deliveryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.Callbacks[deliveryID]; !exists {
		return ErrOperationNotFound
	}
	s.Callbacks[deliveryID] = true
	return nil
}

func (s *MemoryStore) SaveOutgoing(_ context.Context, request Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.Operations[request.RequestID]; ok {
		if sameRequest(existing.Request, request) {
			return nil
		}
		return ErrRequestConflict
	}
	s.Operations[request.RequestID] = StoredOperation{Request: request, State: StateOutgoing}
	return nil
}

func (s *MemoryStore) Load(_ context.Context, id string) (StoredOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.Operations[id]
	if !ok {
		return StoredOperation{}, ErrOperationNotFound
	}
	return cloneOperation(value), nil
}

func (s *MemoryStore) ListPending(_ context.Context, limit int) ([]StoredOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([]StoredOperation, 0)
	for _, value := range s.Operations {
		if value.State != StateComplete {
			values = append(values, cloneOperation(value))
			if len(values) == limit {
				break
			}
		}
	}
	return values, nil
}

func (s *MemoryStore) MarkAccepted(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.Operations[id]
	if !ok {
		return ErrOperationNotFound
	}
	value.State = StateAccepted
	s.Operations[id] = value
	return nil
}

func (s *MemoryStore) SaveResult(_ context.Context, result Result) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.Operations[result.RequestID]
	if !ok {
		return Result{}, ErrOperationNotFound
	}
	if value.Result != nil && value.Result.State != "unknown" && result.State == "unknown" {
		return *value.Result, nil
	}
	copyResult := result
	value.Result = &copyResult
	value.State = StateResultSaved
	s.Operations[result.RequestID] = value
	return result, nil
}

func (s *MemoryStore) MarkComplete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.Operations[id]
	if !ok {
		return ErrOperationNotFound
	}
	value.State = StateComplete
	s.Operations[id] = value
	return nil
}

func cloneOperation(value StoredOperation) StoredOperation {
	if value.Result != nil {
		copyResult := *value.Result
		value.Result = &copyResult
	}
	return value
}

var _ Store = (*MemoryStore)(nil)
var _ CallbackStore = (*MemoryStore)(nil)
