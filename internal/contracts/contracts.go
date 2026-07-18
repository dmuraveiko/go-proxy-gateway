package contracts

import "time"

const (
	TypeHTTPRequest              = "http.request.v1"
	TypeAcceptance               = "http.accepted.v1"
	TypeAcceptanceACK            = "http.accepted_ack.v1"
	TypeHTTPResult               = "http.result.v1"
	TypeResultACK                = "http.result_ack.v1"
	TypeACKConfirmed             = "ack.confirmed.v1"
	TypeWebhookRegister          = "webhook.register.v1"
	TypeWebhookControlResult     = "webhook.control_result.v1"
	TypeWebhookRegisterResult    = TypeWebhookControlResult
	TypeWebhookControlACK        = "webhook.control_ack.v1"
	TypeWebhookSubscribe         = "webhook.subscribe.v1"
	TypeWebhookUnsubscribe       = "webhook.unsubscribe.v1"
	TypeWebhookUpdate            = "webhook.update.v1"
	TypeWebhookDelete            = "webhook.delete.v1"
	TypeWebhookEvent             = "webhook.event.v1"
	TypeWebhookEventACK          = "webhook.event_ack.v1"
	TypeWebhookDelegatedResponse = "webhook.response.v1"
)

// HeaderField is deliberately a list item rather than a map entry. It preserves
// duplicate values at the NATS contract boundary. net/http does not promise wire order.
type HeaderField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type RetryPolicy struct {
	MaxAttempts      int           `json:"max_attempts"`
	InitialBackoff   time.Duration `json:"initial_backoff"`
	MaxBackoff       time.Duration `json:"max_backoff"`
	RetryStatuses    []int         `json:"retry_statuses,omitempty"`
	RetryNetworkFail bool          `json:"retry_network_fail"`
	Idempotent       bool          `json:"idempotent"`
}

type HTTPRequest struct {
	RequestID   string        `json:"request_id"`
	ClientID    string        `json:"client_id"`
	ProxyID     string        `json:"proxy_id"`
	Method      string        `json:"method"`
	URL         string        `json:"url"`
	Headers     []HeaderField `json:"headers,omitempty"`
	Body        []byte        `json:"body,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`
	Retry       RetryPolicy   `json:"retry"`
	CreatedAt   time.Time     `json:"created_at"`
	TraceParent string        `json:"traceparent,omitempty"`
}

type Acceptance struct {
	RequestID  string    `json:"request_id"`
	DeliveryID string    `json:"delivery_id"`
	ProxyID    string    `json:"proxy_id"`
	Accepted   bool      `json:"accepted"`
	ErrorCode  string    `json:"error_code,omitempty"`
	Error      string    `json:"error,omitempty"`
	AcceptedAt time.Time `json:"accepted_at"`
}

type DeliveryACK struct {
	RequestID  string `json:"request_id"`
	DeliveryID string `json:"delivery_id"`
	ClientID   string `json:"client_id"`
}

type ACKConfirmed struct {
	DeliveryID  string    `json:"delivery_id"`
	ConfirmedAt time.Time `json:"confirmed_at"`
}

type HTTPResult struct {
	ResultID    string        `json:"result_id"`
	RequestID   string        `json:"request_id"`
	ProxyID     string        `json:"proxy_id"`
	State       string        `json:"state"`
	StatusCode  int           `json:"status_code,omitempty"`
	Headers     []HeaderField `json:"headers,omitempty"`
	Body        []byte        `json:"body,omitempty"`
	ErrorCode   string        `json:"error_code,omitempty"`
	Error       string        `json:"error,omitempty"`
	Attempts    int           `json:"attempts"`
	CompletedAt time.Time     `json:"completed_at"`
}

type StaticHTTPResponse struct {
	StatusCode int           `json:"status_code"`
	Headers    []HeaderField `json:"headers,omitempty"`
	Body       []byte        `json:"body,omitempty"`
}

type WebhookRegister struct {
	CommandID       string             `json:"command_id"`
	ClientID        string             `json:"client_id"`
	Name            string             `json:"name"`
	Mode            string             `json:"mode"` // static or delegated
	StaticResponse  StaticHTTPResponse `json:"static_response"`
	ResponderID     string             `json:"responder_id,omitempty"`
	SubscriberIDs   []string           `json:"subscriber_ids,omitempty"`
	ResponseTimeout time.Duration      `json:"response_timeout,omitempty"`
	MaxBodyBytes    int64              `json:"max_body_bytes,omitempty"`
}

type WebhookControlResult struct {
	CommandID  string `json:"command_id"`
	DeliveryID string `json:"delivery_id"`
	Action     string `json:"action"`
	Success    bool   `json:"success"`
	WebhookID  string `json:"webhook_id,omitempty"`
	URL        string `json:"url,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
	Error      string `json:"error,omitempty"`
}

type WebhookRegisterResult = WebhookControlResult

type WebhookSubscribe struct {
	CommandID    string `json:"command_id"`
	ClientID     string `json:"client_id"`
	WebhookID    string `json:"webhook_id"`
	SubscriberID string `json:"subscriber_id"`
}

type WebhookUnsubscribe = WebhookSubscribe

type WebhookUpdate struct {
	CommandID       string             `json:"command_id"`
	ClientID        string             `json:"client_id"`
	WebhookID       string             `json:"webhook_id"`
	Name            string             `json:"name"`
	Mode            string             `json:"mode"`
	StaticResponse  StaticHTTPResponse `json:"static_response"`
	ResponderID     string             `json:"responder_id,omitempty"`
	ResponseTimeout time.Duration      `json:"response_timeout,omitempty"`
	MaxBodyBytes    int64              `json:"max_body_bytes,omitempty"`
}

type WebhookDelete struct {
	CommandID string `json:"command_id"`
	ClientID  string `json:"client_id"`
	WebhookID string `json:"webhook_id"`
}

type WebhookEvent struct {
	EventID      string        `json:"event_id"`
	DeliveryID   string        `json:"delivery_id"`
	WebhookID    string        `json:"webhook_id"`
	Method       string        `json:"method"`
	RequestURI   string        `json:"request_uri"`
	Headers      []HeaderField `json:"headers,omitempty"`
	Body         []byte        `json:"body,omitempty"`
	ReceivedAt   time.Time     `json:"received_at"`
	ReplySubject string        `json:"reply_subject,omitempty"`
}

type WebhookResponse struct {
	EventID    string        `json:"event_id"`
	DeliveryID string        `json:"delivery_id"`
	ClientID   string        `json:"client_id"`
	StatusCode int           `json:"status_code"`
	Headers    []HeaderField `json:"headers,omitempty"`
	Body       []byte        `json:"body,omitempty"`
}
