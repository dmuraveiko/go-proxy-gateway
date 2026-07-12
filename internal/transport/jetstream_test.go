package transport

import "testing"

func TestAllowedCommandPatterns(t *testing.T) {
	for _, tc := range []struct {
		patterns []string
		kind     string
		want     bool
	}{{[]string{"blockchain.*"}, "blockchain.transaction.get", true}, {[]string{"payments.create"}, "payments.create", true}, {[]string{"payments.read"}, "payments.create", false}, {nil, "anything", false}} {
		if got := allowed(tc.patterns, tc.kind); got != tc.want {
			t.Fatalf("allowed(%v,%q)=%v", tc.patterns, tc.kind, got)
		}
	}
}
