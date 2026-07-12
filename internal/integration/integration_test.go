package integration

import (
	"context"
	"encoding/json"
	"testing"
)

type fakeCommand struct{}

func (fakeCommand) Type() string                   { return "test.run" }
func (fakeCommand) Version() int                   { return 1 }
func (fakeCommand) Validate(json.RawMessage) error { return nil }
func (fakeCommand) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}
func (fakeCommand) RetryPolicy() RetryPolicy { return RetryPolicy{} }

func TestRegistryRejectsDuplicatesAndVersions(t *testing.T) {
	r := NewRegistry()
	if err := r.RegisterCommand(fakeCommand{}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterCommand(fakeCommand{}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if _, err := r.Command("test.run", 2); err == nil {
		t.Fatal("unknown version accepted")
	}
}
