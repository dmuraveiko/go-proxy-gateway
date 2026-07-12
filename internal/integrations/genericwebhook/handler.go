package genericwebhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"proxy-server/internal/contracts"
)

var safeType = regexp.MustCompile(`^[a-z0-9_]+(?:\.[a-z0-9_]+)*$`)

type Handler struct {
	provider, secret string
	eventTypes       map[string]string
}

func New(provider, secret string, eventTypes ...map[string]string) *Handler {
	m := map[string]string{}
	if len(eventTypes) > 0 {
		m = eventTypes[0]
	}
	return &Handler{provider: provider, secret: secret, eventTypes: m}
}
func (h *Handler) Provider() string { return h.provider }
func (h *Handler) Authenticate(r *http.Request, body []byte) error {
	got := strings.TrimPrefix(r.Header.Get("X-Webhook-Signature"), "sha256=")
	decoded, err := hex.DecodeString(got)
	if err != nil {
		return errors.New("invalid webhook signature")
	}
	mac := hmac.New(sha256.New, []byte(h.secret))
	mac.Write(body)
	if !hmac.Equal(decoded, mac.Sum(nil)) {
		return errors.New("invalid webhook signature")
	}
	return nil
}
func (h *Handler) Parse(body []byte) ([]contracts.Event, error) {
	var raw struct {
		ID      string          `json:"id"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	if raw.ID == "" || !safeType.MatchString(raw.Type) || len(raw.Payload) == 0 {
		return nil, errors.New("id, type and payload are required")
	}
	if len(h.eventTypes) > 0 {
		mapped, ok := h.eventTypes[raw.Type]
		if !ok {
			return nil, errors.New("event type is not allowed")
		}
		raw.Type = mapped
	}
	if !safeType.MatchString(raw.Type) {
		return nil, errors.New("mapped event type is invalid")
	}
	return []contracts.Event{{ID: raw.ID, Type: raw.Type, Version: 1, Provider: h.provider, Payload: raw.Payload, ReceivedAt: time.Now().UTC()}}, nil
}
