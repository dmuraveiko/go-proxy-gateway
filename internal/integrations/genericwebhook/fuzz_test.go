package genericwebhook

import "testing"

func FuzzParseDoesNotPanic(f *testing.F) {
	f.Add([]byte(`{"id":"1","type":"event.ok","payload":{}}`))
	h := New("test", "secret")
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = h.Parse(b) })
}
