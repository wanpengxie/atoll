package placement

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// TestStateClosedSet locks the L2 §1.4.11.2 closed enum: 4 values,
// spec order. Drift on either side trips.
func TestStateClosedSet(t *testing.T) {
	t.Parallel()

	if got := len(AllStates); got != 4 {
		t.Fatalf("AllStates len = %d, want 4", got)
	}
	want := []State{StateCreating, StateActive, StateOrphan, StateStale}
	for i, s := range AllStates {
		if s != want[i] {
			t.Errorf("AllStates[%d] = %q, want %q", i, s, want[i])
		}
	}
}

// TestTransitionMatrix locks the L2 §1.4.11.2 state-machine transition
// rules. Every (from, to) pair from the spec table must be Allowed; any
// pair not on the spec table must be Forbidden.
//
// This is the verification artifact called out by m1.5-tickets.md §T1
// acceptance criteria — "状态转换矩阵".
func TestTransitionMatrix(t *testing.T) {
	t.Parallel()

	allowed := map[transition]bool{
		// ∅ → creating  (server reserve)
		{from: "", to: StateCreating}: true,
		// creating → active  (ACK match)
		{from: StateCreating, to: StateActive}: true,
		// creating → orphan  (create_timeout)
		{from: StateCreating, to: StateOrphan}: true,
		// active → stale  (heartbeat timeout + grace passed)
		{from: StateActive, to: StateStale}: true,
		// active → ∅      (server unbind + ACK; row deleted)
		{from: StateActive, to: ""}: true,
		// orphan → creating  (retry new daemon, owner_epoch+1)
		{from: StateOrphan, to: StateCreating}: true,
		// stale → creating  (server reclaim, owner_epoch+1, fresh fencing)
		{from: StateStale, to: StateCreating}: true,
		// stale → orphan  (stale_timeout — M2+ migration)
		{from: StateStale, to: StateOrphan}: true,
	}

	all := append([]State{""}, AllStates...) // include ∅
	for _, from := range all {
		for _, to := range all {
			tr := transition{from: from, to: to}
			want := allowed[tr]
			got := CanTransition(from, to)
			if got != want {
				t.Errorf("CanTransition(%q → %q) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestPlacementTransitionMatrix_BareStaleToActiveRejected(t *testing.T) {
	t.Parallel()

	if CanTransition(StateStale, StateActive) {
		t.Fatal("bare stale -> active transition must be rejected; reclaim must pass through creating")
	}
	if !CanTransition(StateStale, StateCreating) {
		t.Fatal("stale reclaim path should transition stale -> creating")
	}
	if !CanTransition(StateCreating, StateActive) {
		t.Fatal("reclaim completion should transition creating -> active")
	}
}

// TestACKMatchExactFieldSet locks the proto-foundation §3.3.3 +
// impl-layer2 §3.2.2 rule: Match is the saga-identifier pre-check
// (channel_id + create_request_id + daemon_id), NOT a comparison of
// owner_epoch / fencing_token. The fencing tuple is the daemon's
// authoritative output and arrives FROM the ack — it is written into
// the placement row by the Phase 3 CAS, never compared against a
// pre-existing value.
func TestACKMatchExactFieldSet(t *testing.T) {
	t.Parallel()

	// Phase 1 placement: owner_epoch=0, fencing_token="" per
	// proto-foundation §3.3.3.
	base := Placement{
		ChannelID:       channel.ID("chan-A"),
		DaemonID:        "daemon-1",
		State:           StateCreating,
		OwnerEpoch:      0,
		FencingToken:    "",
		CreateRequestID: "req-uuid-x",
	}
	// Phase 2 ack: daemon-generated owner_epoch=1 + fresh fencing_token.
	ackOK := CreateChannelAck{
		FrameID:         "f-1",
		ChannelID:       channel.ID("chan-A"),
		CreateRequestID: "req-uuid-x",
		OwnerEpoch:      1,
		FencingToken:    "tok-daemon-generated",
		DaemonID:        "daemon-1",
		DaemonEpoch:     1,
		Result:          CreateChannelAccepted,
	}
	if !ackOK.Match(base) {
		t.Fatalf("baseline ACK should match placement; got false\n  ack: %+v\n  pl:  %+v", ackOK, base)
	}

	cases := []struct {
		name string
		mod  func(*CreateChannelAck)
	}{
		{
			name: "channel_id mismatch",
			mod:  func(a *CreateChannelAck) { a.ChannelID = "chan-other" },
		},
		{
			name: "create_request_id mismatch",
			mod:  func(a *CreateChannelAck) { a.CreateRequestID = "req-uuid-other" },
		},
		{
			name: "daemon_id mismatch",
			mod:  func(a *CreateChannelAck) { a.DaemonID = "daemon-2" },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ack := ackOK
			tc.mod(&ack)
			if ack.Match(base) {
				t.Errorf("Match returned true after %s — should be false", tc.name)
			}
		})
	}

	// Daemon_epoch + Result must NOT factor into the match
	// decision (they are recorded for audit / state, not for fencing).
	t.Run("daemon_epoch difference is informational", func(t *testing.T) {
		ack := ackOK
		ack.DaemonEpoch = 999
		if !ack.Match(base) {
			t.Errorf("daemon_epoch should not affect Match; got false")
		}
	})
	t.Run("empty result does not affect Match", func(t *testing.T) {
		// Match only checks saga identifiers; downstream logic uses
		// Result to decide whether to call CASActivate. Asserting Match
		// true here proves the saga-identifier rule is exact.
		ack := ackOK
		ack.Result = ""
		if !ack.Match(base) {
			t.Errorf("result should not affect Match; got false")
		}
	})
	// owner_epoch / fencing_token are daemon-generated outputs
	// (proto-foundation §3.3.3 Phase 2) — they MUST NOT factor into
	// Match. The Phase 3 CAS writes them into the placement row.
	t.Run("owner_epoch is a daemon output not a Match predicate", func(t *testing.T) {
		ack := ackOK
		ack.OwnerEpoch = 99
		if !ack.Match(base) {
			t.Errorf("owner_epoch should not affect Match (daemon output); got false")
		}
	})
	t.Run("fencing_token is a daemon output not a Match predicate", func(t *testing.T) {
		ack := ackOK
		ack.FencingToken = "some-other-token"
		if !ack.Match(base) {
			t.Errorf("fencing_token should not affect Match (daemon output); got false")
		}
	})
}

// TestErrPlacementExistsIsSentinel asserts Reserve callers can branch
// on ErrPlacementExists via type assertion (not just string match).
func TestErrPlacementExistsIsSentinel(t *testing.T) {
	t.Parallel()

	err := &ErrPlacementExists{ChannelID: "chan-A"}
	if err.Error() == "" {
		t.Errorf("ErrPlacementExists.Error returned empty string")
	}

	var perr *ErrPlacementExists
	switch v := error(err).(type) {
	case *ErrPlacementExists:
		perr = v
	default:
		t.Fatalf("type assertion failed; got %T", v)
	}
	if perr.ChannelID != "chan-A" {
		t.Errorf("type assertion lost channel id; got %q", perr.ChannelID)
	}
}

// TestCreateChannelAckResultClosedSet asserts the L2 accept-path result
// value.
func TestCreateChannelAckResultClosedSet(t *testing.T) {
	t.Parallel()

	if CreateChannelAccepted != "accepted" {
		t.Errorf("CreateChannelAccepted = %q, want %q", CreateChannelAccepted, "accepted")
	}
}

// transition is a (from, to) tuple used by TestTransitionMatrix.
type transition struct {
	from State
	to   State
}
