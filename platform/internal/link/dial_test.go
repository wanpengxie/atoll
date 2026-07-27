package link

// Dialer construction contract tests.

import (
	"context"
	"testing"
)

// The process-level shared ledger is the one remote session truth: Dial
// refuses to manufacture a private registry per connection.
func TestDialRequiresSharedSessionLedger(t *testing.T) {
	if _, err := Dial(context.Background(), "ws://127.0.0.1:1", DialConfig{}, nil); err == nil ||
		err.Error() != "link: DialConfig.SessionLedger is required" {
		t.Fatalf("err=%v want ledger required", err)
	}
}
