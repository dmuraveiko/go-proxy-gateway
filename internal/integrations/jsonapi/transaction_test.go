package jsonapi

import "testing"

func TestEndpointMustBeHTTPS(t *testing.T) {
	if _, err := NewTransactionStatus("http://example.com", "Authorization", "", 1, 1); err == nil {
		t.Fatal("HTTP endpoint accepted")
	}
	if _, err := NewTransactionStatus("https://example.com", "Authorization", "", 1, 1); err != nil {
		t.Fatal(err)
	}
}
