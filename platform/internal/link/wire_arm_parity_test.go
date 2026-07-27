package link

// Cell/port PARITY and IDENTITY WELDING over a real wire round trip.
//
// The contract these pin (relaywire.go / accessdoor.AccessHandle's doc /
// protocol/access.Invocation.Caller's doc): an out-of-process cell holding a
// wire proxy must observe exactly what an in-process cell holding the local
// handle observes — same verdict fields, same (product, error) split — while
// the SUBJECT of every operation comes from the authenticated endpoint, never
// from anything the frame says about itself.

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// TestWireAccessArmRoundTripsVerdictsWithCellParity drives all four access
// verbs plus both Invoke scopes across the wire and asserts BOTH directions:
// the operand the home decodes is the operand the cell passed, and the product
// the cell receives is the product the home door returned — field for field,
// including the ones a lossy encoder would most plausibly drop (Grant, Route,
// Found on a false-but-accepted read, the Query arms' Reject verdicts).
func TestWireAccessArmRoundTripsVerdictsWithCellParity(t *testing.T) {
	t.Parallel()
	ing := &wireArmIngress{}
	rig := newWireArmRig(t, ing)
	ctx := context.Background()

	t.Run("resource_face_invoke", func(t *testing.T) {
		grant := &access.Grant{
			GranteeKind: access.GranteeActor,
			Grantee:     "agent:peer",
			Ops:         []access.Operation{access.OpRead},
		}
		want := accessdoor.Outcome{
			Value: []byte("bytes-back"),
			Found: true,
			Route: &accessdoor.FileRoute{
				Local: true, Token: "tok-7", Mode: access.OpWrite,
				ReservationID: "res-9", Dir: false,
			},
		}
		ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
			return remoteingress.AccessResponse{Outcome: want}, nil
		})

		got, err := rig.access.Invoke(ctx, access.OpRead, "res:doc", []byte("args"), grant)
		if err != nil {
			t.Fatalf("Invoke: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("outcome = %+v, want %+v (wire proxy is not cell-parity)", got, want)
		}

		calls := ing.recordedAccess()
		last := calls[len(calls)-1]
		wantReq := remoteingress.AccessRequest{
			Kind: remoteingress.AccessInvoke, Scope: remoteingress.ScopeChannel,
			Operation: access.OpRead, Resource: "res:doc",
			Args: []byte("args"), Grant: grant,
		}
		if !reflect.DeepEqual(last.req, wantReq) {
			t.Fatalf("home decoded %+v, want %+v", last.req, wantReq)
		}
	})

	t.Run("state_face_invoke_carries_the_state_scope", func(t *testing.T) {
		ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
			return remoteingress.AccessResponse{
				Outcome: accessdoor.Outcome{Value: []byte("state-value")},
			}, nil
		})
		got, err := rig.state.Invoke(ctx, access.OpWrite, "res:self", []byte("v"), nil)
		if err != nil {
			t.Fatalf("state Invoke: %v", err)
		}
		if string(got.Value) != "state-value" {
			t.Fatalf("state outcome value = %q, want state-value", got.Value)
		}
		calls := ing.recordedAccess()
		last := calls[len(calls)-1]
		// The two faces are ONE wire arm distinguished only by Scope: a state
		// call arriving as channel-scoped would silently redirect an actor's
		// own belongings onto the channel tree.
		if last.req.Scope != remoteingress.ScopeState {
			t.Fatalf("state face arrived with scope %q, want %q", last.req.Scope, remoteingress.ScopeState)
		}
		if last.req.Kind != remoteingress.AccessInvoke {
			t.Fatalf("state face arrived as kind %q, want invoke", last.req.Kind)
		}
	})

	t.Run("create_arm_carries_the_spec_not_invocation_args", func(t *testing.T) {
		spec := accessdoor.CreateSpec{
			Kind: accessdoor.KindFile, Dir: false, WithContent: true,
			SourceChannelID: "chan:src", SourceResourceID: "res:src",
		}
		want := accessdoor.Outcome{
			Route: &accessdoor.FileRoute{Local: false, Token: "create-tok", Mode: access.OpWrite},
		}
		ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
			return remoteingress.AccessResponse{Outcome: want}, nil
		})

		got, err := rig.access.Create(ctx, "res:new", spec, []byte("initial"))
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("create outcome = %+v, want %+v", got, want)
		}
		calls := ing.recordedAccess()
		last := calls[len(calls)-1]
		if last.req.Kind != remoteingress.AccessCreate {
			t.Fatalf("create arrived as kind %q", last.req.Kind)
		}
		if !reflect.DeepEqual(last.req.Spec, spec) {
			t.Fatalf("spec crossed as %+v, want %+v", last.req.Spec, spec)
		}
		if string(last.req.Initial) != "initial" {
			t.Fatalf("initial bytes crossed as %q, want initial", last.req.Initial)
		}
		// Carrier law: the spec rides the dedicated Create arm, never
		// access.Invocation.Args — so a create that arrives must NOT also
		// present itself as an invocation operand.
		if last.req.Args != nil {
			t.Fatalf("create arrived with Invocation Args %q, want none", last.req.Args)
		}
	})

	t.Run("stat_reject_is_a_verdict_not_an_error", func(t *testing.T) {
		want := accessdoor.StatResult{
			Meta: accessdoor.StatMeta{
				Kind:              resourcespec.KindFile,
				PlacementKind:     resourcespec.PlacementDaemonLocal,
				PlacementDaemonID: "daemon-7",
				CreatedAt:         1700000000123,
				CreatedBy:         "agent:creator",
			},
			Ops:    accessdoor.OpSet{access.OpRead, access.OpWrite},
			Reject: accessdoor.QueryNotFound,
		}
		ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
			return remoteingress.AccessResponse{Stat: want}, nil
		})
		got, err := rig.access.Stat(ctx, "res:doc")
		if err != nil {
			t.Fatalf("Stat returned a Go error for a reject VERDICT: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("stat = %+v, want %+v", got, want)
		}
		calls := ing.recordedAccess()
		if last := calls[len(calls)-1]; last.req.Kind != remoteingress.AccessStat ||
			last.req.Resource != resource.ResourceID("res:doc") {
			t.Fatalf("stat arrived as %+v", last.req)
		}
	})

	t.Run("list_page_and_cursor", func(t *testing.T) {
		want := accessdoor.ListPage{
			Entries: []accessdoor.ListEntry{{
				ID: "res:a", Kind: resourcespec.KindKV, Ops: accessdoor.OpSet{access.OpRead},
			}},
			Next:   "cursor-2",
			Reject: "",
		}
		ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
			return remoteingress.AccessResponse{List: want}, nil
		})
		query := accessdoor.ListQuery{Prefix: "res:", Limit: 7, Cursor: "cursor-1"}
		got, err := rig.access.List(ctx, query)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("list page = %+v, want %+v", got, want)
		}
		calls := ing.recordedAccess()
		last := calls[len(calls)-1]
		if last.req.Kind != remoteingress.AccessList || !reflect.DeepEqual(last.req.List, query) {
			t.Fatalf("list query crossed as %+v (kind %q), want %+v",
				last.req.List, last.req.Kind, query)
		}
	})

	t.Run("definite_home_error_stays_an_error", func(t *testing.T) {
		ing.setAccess(func(remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
			return remoteingress.AccessResponse{}, remoteingress.ErrInvalidRequest
		})
		got, err := rig.access.Invoke(ctx, access.OpRead, "res:doc", nil, nil)
		if err == nil {
			t.Fatal("a definite home error crossed as a success")
		}
		if err.Error() != remoteingress.ErrInvalidRequest.Error() {
			t.Fatalf("err = %q, want %q", err, remoteingress.ErrInvalidRequest)
		}
		// A definite host verdict must NOT be dressed as the transport's
		// unknown — that word is reserved for genuinely unconfirmed work.
		if got.RejectReason == access.OutcomeUnknown {
			t.Fatal("a definite host error was reported as outcome_unknown")
		}
	})
}

// TestWireAccessArmWeldsAuthenticatedIdentityAndRejectsSelfReportedCaller pins
// the weld from both sides. Positive: the home hands the door the ENDPOINT's
// (id, key) even though no frame carries either. Negative: a frame that does
// try to name its own caller is refused outright — it never reaches the door,
// so an attacker cannot even provoke a decision under a borrowed identity.
func TestWireAccessArmWeldsAuthenticatedIdentityAndRejectsSelfReportedCaller(t *testing.T) {
	t.Parallel()
	ing := &wireArmIngress{}
	rig := newWireArmRig(t, ing)
	ctx := context.Background()

	if _, err := rig.access.Invoke(ctx, access.OpRead, "res:doc", nil, nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if _, err := rig.state.Invoke(ctx, access.OpWrite, "res:self", nil, nil); err != nil {
		t.Fatalf("state Invoke: %v", err)
	}
	calls := ing.recordedAccess()
	if len(calls) != 2 {
		t.Fatalf("home saw %d access calls, want 2", len(calls))
	}
	for i, call := range calls {
		if call.id != rig.id {
			t.Fatalf("call %d ran as %q, want the endpoint's authenticated %q", i, call.id, rig.id)
		}
		if call.key != rig.key {
			t.Fatalf("call %d ran under attempt key %q, want the endpoint's %q", i, call.key, rig.key)
		}
	}

	// The proxy self-reports nothing: every invocation leaves caller empty on
	// the wire (the home stamps it). Checked on the exact bytes that crossed.
	for i, raw := range rig.rawAccessFrames() {
		var frame accessRequest
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("frame %d undecodable: %v", i, err)
		}
		if frame.Inv == nil {
			t.Fatalf("frame %d carried no invocation arm", i)
		}
		if frame.Inv.Caller != "" {
			t.Fatalf("frame %d self-reported caller %q; the wire must leave it empty",
				i, frame.Inv.Caller)
		}
	}

	// A hand-crafted frame that DOES name a caller is rejected as malformed —
	// fail-fast, exactly like the pen refusing a pre-filled Sender, never
	// silently overwritten and never executed under the borrowed name.
	forged, err := json.Marshal(accessRequest{
		Kind:  accessKindInvocation,
		Scope: accessScopeChannel,
		Inv: &access.Invocation{
			Caller: "agent:impostor", Resource: "res:doc", Operation: access.OpWrite,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, ackErr, txErr := rig.accessRelay.roundTrip(ctx, forged)
	if txErr != nil {
		t.Fatalf("forged frame produced a transport error %v, want a definite refusal", txErr)
	}
	if ackErr == nil {
		t.Fatal("a self-reported caller was accepted")
	}
	if after := ing.recordedAccess(); len(after) != 2 {
		t.Fatalf("the forged frame reached the door (%d calls, want still 2): %+v",
			len(after), after[len(after)-1])
	}
}

// TestWireScheduleArmRoundTripsWithWeldedAuthorAndCorrelationID is the time
// axis's parity twin. The whole ScheduleReq crosses — CorrelationID included,
// since dropping the caller's causal coordinate would lose domain session
// semantics the substrate is obliged to relay — while the author is welded at
// the home mint: the frame has no author field to carry one, and the ingress
// arm's signature has no attempt key to compare (a timer belongs to the
// identity, not the term).
func TestWireScheduleArmRoundTripsWithWeldedAuthorAndCorrelationID(t *testing.T) {
	t.Parallel()
	ing := &wireArmIngress{}
	rig := newWireArmRig(t, ing)
	ctx := context.Background()

	req := schedule.ScheduleReq{
		Home:          schedule.TimerHomeDurable,
		FireAt:        1700000000999,
		Type:          "domain.reminder",
		Payload:       []byte(`{"note":"ping"}`),
		CorrelationID: "corr-abc-123",
	}
	ing.setSchedule(func(remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error) {
		return remoteingress.ScheduleResponse{ID: "timer:minted"}, nil
	})
	id, err := rig.sched.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if id != schedule.TimerID("timer:minted") {
		t.Fatalf("TimerID = %q, want timer:minted (the minted id did not cross back)", id)
	}

	calls := ing.recordedSchedule()
	if len(calls) != 1 {
		t.Fatalf("home saw %d schedule calls, want 1", len(calls))
	}
	if calls[0].id != rig.id {
		t.Fatalf("schedule ran as %q, want the endpoint's authenticated %q", calls[0].id, rig.id)
	}
	if calls[0].req.Method != remoteingress.ScheduleSet {
		t.Fatalf("method = %q, want schedule", calls[0].req.Method)
	}
	if !reflect.DeepEqual(calls[0].req.Req, req) {
		t.Fatalf("ScheduleReq crossed as %+v, want %+v", calls[0].req.Req, req)
	}

	// Cancel and Ack carry the id only, and a home error stays an error.
	ing.setSchedule(func(remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error) {
		return remoteingress.ScheduleResponse{}, remoteingress.ErrInvalidRequest
	})
	if err := rig.sched.Cancel(ctx, "timer:minted"); err == nil ||
		err.Error() != remoteingress.ErrInvalidRequest.Error() {
		t.Fatalf("Cancel err = %v, want the home's definite error", err)
	}
	ing.setSchedule(func(remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error) {
		return remoteingress.ScheduleResponse{}, nil
	})
	if err := rig.sched.Ack(ctx, "timer:minted"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	calls = ing.recordedSchedule()
	if len(calls) != 3 {
		t.Fatalf("home saw %d schedule calls, want 3", len(calls))
	}
	if calls[1].req.Method != remoteingress.ScheduleCancel || calls[1].req.ID != "timer:minted" {
		t.Fatalf("cancel arrived as %+v", calls[1].req)
	}
	if calls[2].req.Method != remoteingress.ScheduleAck || calls[2].req.ID != "timer:minted" {
		t.Fatalf("ack arrived as %+v", calls[2].req)
	}

	// Structural half of the weld: the schedule frame has no field in which an
	// author could ride, so there is nothing for the home to have to ignore.
	for i, raw := range rig.rawScheduleFrames() {
		var generic map[string]json.RawMessage
		if err := json.Unmarshal(raw, &generic); err != nil {
			t.Fatalf("schedule frame %d undecodable: %v", i, err)
		}
		for _, forbidden := range []string{"author", "caller", "attempt_key", "attempt"} {
			if _, found := generic[forbidden]; found {
				t.Fatalf("schedule frame %d carries a self-reported %q field", i, forbidden)
			}
		}
	}
	// And the payload it DOES carry keeps the causal coordinate intact.
	var first scheduleRequest
	if err := json.Unmarshal(rig.rawScheduleFrames()[0], &first); err != nil {
		t.Fatal(err)
	}
	if first.Req.CorrelationID != "corr-abc-123" {
		t.Fatalf("CorrelationID on the wire = %q, want corr-abc-123", first.Req.CorrelationID)
	}
}
