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

// A second accepted attach reply after a successful adoption is refused at
// the adoption point and never rewrites the installed session.
func TestDuplicateAcceptedAttachReplyIsRefusedAtAdoption(t *testing.T) {
	d := &Dialer{
		sessions:      NewRemoteSessionLedger(nil).registry,
		pendingAttach: newPendingReplies[AttachReply](),
		done:          make(chan struct{}),
	}
	first, err := mintSessionGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.adoptAttachReply(AttachReply{
		Accepted: true, ChannelID: "chan", DaemonID: "daemon-1", Generation: first,
	}); err != nil {
		t.Fatalf("first adoption: %v", err)
	}
	second, err := mintSessionGeneration()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.adoptAttachReply(AttachReply{
		Accepted: true, ChannelID: "chan", DaemonID: "daemon-1", Generation: second,
	}); err == nil {
		t.Fatal("duplicate accepted attach reply adopted")
	}
	d.mu.Lock()
	installed := d.session
	d.mu.Unlock()
	if installed == nil || installed.generation != first {
		t.Fatalf("installed session=%v want the first generation", installed)
	}
}
