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
		// stale → active  (original daemon reclaim)
		{from: StateStale, to: StateActive}: true,
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

// TestACKMatchExactFieldSet locks the L2 §1.4.11.3 step 5 CAS rule:
// the ACK is only accepted when CreateRequestID + OwnerEpoch +
// FencingToken + DaemonID ALL match the placement record. Each field
// gets its own subtest so the failure message points to the broken
// field.
//
// This is the verification artifact called out by m1.5-tickets.md §T1
// acceptance criteria — "ACK 完整字段匹配".
func TestACKMatchExactFieldSet(t *testing.T) {
	t.Parallel()

	base := Placement{
		ChannelID:       channel.ID("chan-A"),
		DaemonID:        "daemon-1",
		State:           StateCreating,
		OwnerEpoch:      7,
		FencingToken:    "tok-7",
		CreateRequestID: "req-uuid-x",
	}
	ackOK := CreateChannelAck{
		FrameID:         "f-1",
		ChannelID:       channel.ID("chan-A"),
		CreateRequestID: "req-uuid-x",
		OwnerEpoch:      7,
		FencingToken:    "tok-7",
		DaemonID:        "daemon-1",
		DaemonEpoch:     1,
		Status:          AckBound,
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
			name: "owner_epoch mismatch (off-by-one)",
			mod:  func(a *CreateChannelAck) { a.OwnerEpoch = 8 },
		},
		{
			name: "fencing_token mismatch",
			mod:  func(a *CreateChannelAck) { a.FencingToken = "tok-8" },
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

	// Daemon_epoch + Status + Reason must NOT factor into the match
	// decision (they are recorded for audit / state, not for fencing).
	t.Run("daemon_epoch difference is informational", func(t *testing.T) {
		ack := ackOK
		ack.DaemonEpoch = 999
		if !ack.Match(base) {
			t.Errorf("daemon_epoch should not affect Match; got false")
		}
	})
	t.Run("status rejected does not affect Match", func(t *testing.T) {
		// Match only checks the field-quad; downstream logic uses Status
		// to decide whether to call CASActivate. Asserting Match true
		// here proves the field-quad rule is exact.
		ack := ackOK
		ack.Status = AckRejected
		if !ack.Match(base) {
			t.Errorf("status should not affect Match; got false")
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

// TestAckStatusClosedSet asserts the L2 §1.4.11.3 step 4 AckStatus has
// exactly the two spec values (bound | rejected) with the wire form
// matching.
func TestAckStatusClosedSet(t *testing.T) {
	t.Parallel()

	if AckBound != "bound" {
		t.Errorf("AckBound = %q, want %q", AckBound, "bound")
	}
	if AckRejected != "rejected" {
		t.Errorf("AckRejected = %q, want %q", AckRejected, "rejected")
	}
}

// transition is a (from, to) tuple used by TestTransitionMatrix.
type transition struct {
	from State
	to   State
}
