package contracts

import (
	"encoding/json"
	"time"
)

type Command struct {
	ID            string          `json:"id"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id,omitempty"`
	TraceParent   string          `json:"traceparent,omitempty"`
	TenantID      string          `json:"tenant_id,omitempty"`
	Type          string          `json:"type"`
	Version       int             `json:"version"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

type Result struct {
	CommandID     string          `json:"command_id"`
	Type          string          `json:"type"`
	Status        string          `json:"status"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Error         *Problem        `json:"error,omitempty"`
	Attempts      int             `json:"attempts"`
	FinishedAt    time.Time       `json:"finished_at"`
	CorrelationID string          `json:"correlation_id"`
}

type Event struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	Provider   string          `json:"provider"`
	Payload    json.RawMessage `json:"payload"`
	ReceivedAt time.Time       `json:"received_at"`
}

type Problem struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}
