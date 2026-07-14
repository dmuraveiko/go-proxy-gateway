package message

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type HTTPRequest struct {
	ID, Method, URL string
	Headers         map[string]string
	Body            []byte
	WebhookURL      string
	CreatedAt       time.Time
}
type HTTPResult struct {
	ID         string            `json:"id"`
	RequestID  string            `json:"request_id"`
	StatusCode int               `json:"status_code"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       []byte            `json:"body,omitempty"`
	Error      string            `json:"error,omitempty"`
	Attempt    int               `json:"attempt"`
	FinishedAt time.Time         `json:"finished_at"`
}
type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
	KeyID     string          `json:"key_id,omitempty"`
	Signature string          `json:"signature,omitempty"`
}

func NewEnvelope(kind string, payload any, signer ed25519.PrivateKey) (Envelope, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	idb := make([]byte, 16)
	if _, err = rand.Read(idb); err != nil {
		return Envelope{}, err
	}
	e := Envelope{ID: base64.RawURLEncoding.EncodeToString(idb), Type: kind, Timestamp: time.Now().UTC(), Payload: b}
	if len(signer) > 0 {
		e.KeyID = base64.RawURLEncoding.EncodeToString(signer.Public().(ed25519.PublicKey)[:8])
		e.Signature = base64.RawStdEncoding.EncodeToString(ed25519.Sign(signer, e.signingBytes()))
	}
	return e, nil
}
func (e Envelope) Verify(keys []ed25519.PublicKey, maxAge time.Duration) error {
	_, err := e.VerifyKey(keys, maxAge)
	return err
}
func (e Envelope) VerifyKey(keys []ed25519.PublicKey, maxAge time.Duration) (string, error) {
	if e.ID == "" || e.Type == "" || e.Timestamp.IsZero() {
		return "", errors.New("invalid envelope")
	}
	if d := time.Since(e.Timestamp); d > maxAge || d < -time.Minute {
		return "", errors.New("expired envelope")
	}
	sig, err := base64.RawStdEncoding.DecodeString(e.Signature)
	if err != nil {
		return "", errors.New("invalid signature encoding")
	}
	for _, k := range keys {
		keyID := base64.RawURLEncoding.EncodeToString(k[:8])
		if keyID == e.KeyID && ed25519.Verify(k, e.signingBytes(), sig) {
			return keyID, nil
		}
	}
	return "", errors.New("signature verification failed")
}
func (e Envelope) signingBytes() []byte {
	return []byte(e.ID + "\n" + e.Type + "\n" + e.Timestamp.UTC().Format(time.RFC3339Nano) + "\n" + string(e.Payload))
}
func LoadPrivateKey(path string) (ed25519.PrivateKey, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw, err := base64.RawStdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, err
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("private key length %d", len(raw))
	}
	return ed25519.PrivateKey(raw), nil
}
func ParsePublicKeys(values []string) ([]ed25519.PublicKey, error) {
	out := make([]ed25519.PublicKey, 0, len(values))
	for _, v := range values {
		b, err := base64.RawStdEncoding.DecodeString(v)
		if err != nil || len(b) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid public key")
		}
		out = append(out, ed25519.PublicKey(b))
	}
	return out, nil
}
func ParseClientPublicKeys(values map[string]string) (map[string]ed25519.PublicKey, map[string]string, error) {
	byClient := make(map[string]ed25519.PublicKey, len(values))
	clientByKeyID := make(map[string]string, len(values))
	for clientID, value := range values {
		b, err := base64.RawStdEncoding.DecodeString(strings.TrimSpace(value))
		if err != nil || len(b) != ed25519.PublicKeySize {
			return nil, nil, fmt.Errorf("invalid public key for client %q", clientID)
		}
		key := ed25519.PublicKey(b)
		keyID := PublicKeyID(key)
		if previous := clientByKeyID[keyID]; previous != "" {
			return nil, nil, fmt.Errorf("clients %q and %q use the same key", previous, clientID)
		}
		byClient[clientID] = key
		clientByKeyID[keyID] = clientID
	}
	return byClient, clientByKeyID, nil
}
func PublicKeyID(k ed25519.PublicKey) string {
	if len(k) < 8 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(k[:8])
}
