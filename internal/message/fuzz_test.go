package message

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"
)

func FuzzEnvelopeVerifyDoesNotPanic(f *testing.F) {
	f.Add([]byte(`{"id":"x"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var e Envelope
		_ = json.Unmarshal(data, &e)
		_ = e.Verify([]ed25519.PublicKey{make([]byte, ed25519.PublicKeySize)}, time.Minute)
	})
}
