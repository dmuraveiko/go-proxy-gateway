package security

import (
	"context"
	"testing"
)

func TestRejectsUnsafeAndUnknownDestinations(t *testing.T) {
	a := NewAllowlist([]string{"127.0.0.1", "api.example.com"})
	for _, raw := range []string{"http://api.example.com/path", "https://other.example.com", "https://127.0.0.1/path"} {
		if err := a.Validate(context.Background(), raw); err == nil {
			t.Fatalf("expected %s to be rejected", raw)
		}
	}
}
