package message

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"
)

func TestEnvelopeSignature(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	env, err := NewEnvelope("test", map[string]string{"ok": "yes"}, priv)
	if err != nil {
		t.Fatal(err)
	}
	if err = env.Verify([]ed25519.PublicKey{pub}, time.Minute); err != nil {
		t.Fatal(err)
	}
	env.Payload[0] ^= 1
	if err = env.Verify([]ed25519.PublicKey{pub}, time.Minute); err == nil {
		t.Fatal("tampered payload was accepted")
	}
}

func TestEnvelopeRejectsWrongKeyID(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	env, err := NewEnvelope("test", map[string]bool{"ok": true}, priv)
	if err != nil {
		t.Fatal(err)
	}
	env.KeyID = "different"
	env.Signature = base64.RawStdEncoding.EncodeToString(ed25519.Sign(priv, env.signingBytes()))
	if err = env.Verify([]ed25519.PublicKey{pub}, time.Minute); err == nil {
		t.Fatal("wrong key id accepted")
	}
}
