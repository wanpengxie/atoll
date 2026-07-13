package compute

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// TestCellCancelForwarderNilSafe: the daemon-side caller-cancel forwarder fires its
// Canceller even when the daemon is currently DISCONNECTED (no Dialer bound, or an
// explicit nil Rebind) — a best-effort drop, never a panic. This is the crux of the
// atomic-Dialer design: a caller's Cancel must be safe at every wire state (the
// ledger still commits the caller's own terminal regardless), and after a reconnect
// Rebind installs the live Dialer the same closure forwards through it — the closure
// never captures a stale Dialer (双线审 F5).
func TestCellCancelForwarderNilSafe(t *testing.T) {
	f := newCellCancelForwarder()
	c := f.cancellerFor(actor.ActorID("caller-1"))

	// No Dialer bound yet (never connected): a fired Canceller drops silently.
	c(actor.ActorID("target"), message.ID("req-1"))

	// Explicit disconnected state: Rebind(nil) then fire again — still a safe no-op.
	f.Rebind(nil)
	c(actor.ActorID("target"), message.ID("req-2"))
}
