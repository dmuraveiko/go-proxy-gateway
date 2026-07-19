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
	Callbacks  map[string]StoredCallback
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{Operations: map[string]StoredOperation{}, Callbacks: map[string]StoredCallback{}}
}

func (s *MemoryStore) SaveCallback(_ context.Context, event contracts.WebhookEvent) (StoredCallback, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, exists := s.Callbacks[event.DeliveryID]
	if exists {
		if !sameCallbackEvent(stored.Event, event) {
			return StoredCallback{}, ErrRequestConflict
		}
		return cloneCallback(stored), nil
	}
	stored = StoredCallback{Event: event}
	s.Callbacks[event.DeliveryID] = stored
	return cloneCallback(stored), nil
}

func (s *MemoryStore) SaveCallbackResponse(_ context.Context, response contracts.WebhookResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, exists := s.Callbacks[response.DeliveryID]
	if !exists {
		return ErrOperationNotFound
	}
	if stored.Event.EventID != response.EventID {
		return ErrRequestConflict
	}
	if stored.Response != nil && !sameCallbackResponse(*stored.Response, response) {
		return ErrRequestConflict
	}
	copyResponse := response
	stored.Response = &copyResponse
	s.Callbacks[response.DeliveryID] = stored
	return nil
}

func (s *MemoryStore) ListPendingCallbacks(_ context.Context, limit int) ([]StoredCallback, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 1000
	}
	values := make([]StoredCallback, 0)
	for _, value := range s.Callbacks {
		if !value.Completed {
			values = append(values, cloneCallback(value))
			if len(values) == limit {
				break
			}
		}
	}
	return values, nil
}

func (s *MemoryStore) MarkCallbackComplete(_ context.Context, deliveryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, exists := s.Callbacks[deliveryID]
	if !exists {
		return ErrOperationNotFound
	}
	stored.Completed = true
	s.Callbacks[deliveryID] = stored
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

func cloneCallback(value StoredCallback) StoredCallback {
	value.Event.Headers = append([]HeaderField(nil), value.Event.Headers...)
	value.Event.Body = append([]byte(nil), value.Event.Body...)
	if value.Response != nil {
		response := *value.Response
		response.Headers = append([]HeaderField(nil), response.Headers...)
		response.Body = append([]byte(nil), response.Body...)
		value.Response = &response
	}
	return value
}

var _ Store = (*MemoryStore)(nil)
var _ CallbackStore = (*MemoryStore)(nil)
