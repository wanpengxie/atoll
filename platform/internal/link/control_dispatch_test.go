package link

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestEveryControlKindHasAValidParsePath(t *testing.T) {
	control := func(frame controlFrame) []byte {
		raw, err := encodeControl(frame)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	storage := func(frame storageControlFrame) []byte {
		raw, err := encodeStorageControl(frame)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	lane := func(frame laneControlFrame) []byte {
		raw, err := encodeLaneControl(frame)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	tests := []struct {
		name  string
		parse func([]byte) (controlMessage, error)
		raw   []byte
	}{
		{"attach", parseAttach, control(controlFrame{RequestID: "r", Kind: ctrlAttach, Attach: &AttachRequest{Proto: 2}})},
		{"attach_reply", parseAttachReply, control(controlFrame{RequestID: "r", Kind: ctrlAttachReply, AttachReply: &AttachReply{Reason: "rejected"}})},
		{"plan_pull", parsePlanPull, control(controlFrame{RequestID: "r", Kind: ctrlPlanPull, PlanPull: &PlanPull{}})},
		{"plan_reply", parsePlanReply, control(controlFrame{RequestID: "r", Kind: ctrlPlanReply, PlanReply: &PlanReply{}})},
		{"plan_poke", parsePlanPoke, encodePlanPoke()},
		{"probe", parseProbe, control(controlFrame{Kind: ctrlProbe, Probe: &Probe{Nonce: "n"}})},
		{"probe_reply", parseProbeReply, control(controlFrame{Kind: ctrlProbeReply, ProbeReply: &ProbeReply{Nonce: "n"}})},
		{"alloc_request", parseAllocRequest, storage(storageControlFrame{Kind: ctrlAllocRequest, AllocRequest: &AllocRequest{RequestID: "r", ChannelID: "c", Coord: "x"}})},
		{"alloc_reply", parseAllocReply, storage(storageControlFrame{Kind: ctrlAllocReply, AllocReply: &AllocReply{RequestID: "r", OK: true}})},
		{"committed", parseCommitted, storage(storageControlFrame{Kind: ctrlCommitted, Committed: &Committed{RequestID: "r", ReservationID: "res"}})},
		{"committed_reply", parseCommittedReply, storage(storageControlFrame{Kind: ctrlCommittedReply, CommittedReply: &CommittedReply{RequestID: "r"}})},
		{"reclaim_ack", parseReclaimAck, storage(storageControlFrame{Kind: ctrlReclaimAck, ReclaimAck: &ReclaimAck{RequestID: "r", TombstoneID: "t"}})},
		{"reclaim_ack_reply", parseReclaimAckReply, storage(storageControlFrame{Kind: ctrlReclaimAckReply, ReclaimAckReply: &ReclaimAckReply{RequestID: "r"}})},
		{"reconcile_pull", parseReconcilePull, storage(storageControlFrame{Kind: ctrlReconcilePull, ReconcilePull: &ReconcilePull{RequestID: "r"}})},
		{"reconcile_pull_reply", parseReconcilePullReply, storage(storageControlFrame{Kind: ctrlReconcilePullReply, ReconcilePullReply: &ReconcilePullReply{RequestID: "r"}})},
		{"reclaim_request", parseReclaimRequest, storage(storageControlFrame{Kind: ctrlReclaimRequest, ReclaimRequest: &ReclaimRequest{RequestID: "r", Coord: "x"}})},
		{"reclaim_reply", parseReclaimReply, storage(storageControlFrame{Kind: ctrlReclaimReply, ReclaimReply: &ReclaimReply{RequestID: "r", OK: true}})},
		{"resolve_coord", parseResolveCoord, lane(laneControlFrame{Kind: ctrlResolveCoord, ResolveCoord: &ResolveCoordRequest{RequestID: "r", Token: "t"}})},
		{"resolve_coord_reply", parseResolveCoordReply, lane(laneControlFrame{Kind: ctrlResolveCoordReply, ResolveCoordReply: &ResolveCoordReply{RequestID: "r", OK: true, Coord: "x", Mode: access.OpRead}})},
	}
	if len(tests) != 19 {
		t.Fatalf("normal-path cases = %d, want 19", len(tests))
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.parse(test.raw); err != nil {
				t.Fatalf("valid frame rejected: %v", err)
			}
		})
	}
}

func TestEndpointKnownKindListsCoverExactlyNineteenKinds(t *testing.T) {
	union := map[controlKind]struct{}{}
	for _, kinds := range [][]controlKind{homeKnownControlKinds, daemonKnownControlKinds} {
		for _, kind := range kinds {
			union[kind] = struct{}{}
		}
	}
	if len(union) != 19 {
		t.Fatalf("known control kind union = %d, want 19", len(union))
	}
	for _, kind := range []controlKind{
		ctrlAttach, ctrlAttachReply, ctrlPlanPull, ctrlPlanReply, ctrlPlanPoke,
		ctrlProbe, ctrlProbeReply, ctrlAllocRequest, ctrlAllocReply,
		ctrlCommitted, ctrlCommittedReply, ctrlReclaimAck, ctrlReclaimAckReply,
		ctrlReconcilePull, ctrlReconcilePullReply, ctrlReclaimRequest, ctrlReclaimReply,
		ctrlResolveCoord, ctrlResolveCoordReply,
	} {
		if _, ok := union[kind]; !ok {
			t.Errorf("known kind lists omit %q", kind)
		}
	}
}

func TestRequiredAndIllegalControlFieldsAreRejectedAtParse(t *testing.T) {
	attempt, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	validActor := map[string]any{
		"actor_id": "agent:a", "attempt_key": attempt,
		"kind": actor.KindAgent, "class": "worker",
	}
	tests := []struct {
		name  string
		parse func([]byte) (controlMessage, error)
		raw   string
	}{
		{"attach envelope request", parseAttach, `{"kind":"attach","attach":{"proto":2}}`},
		{"attach proto", parseAttach, `{"kind":"attach","request_id":"r","attach":{"proto":0}}`},
		{"attach reply envelope request", parseAttachReply, `{"kind":"attach_reply","attach_reply":{"reason":"no"}}`},
		{"attach reply rejected reason", parseAttachReply, `{"kind":"attach_reply","request_id":"r","attach_reply":{}}`},
		{"attach reply accepted channel", parseAttachReply, `{"kind":"attach_reply","request_id":"r","attach_reply":{"accepted":true,"generation":"` + string(attempt) + `","daemon_id":"d"}}`},
		{"attach reply accepted generation", parseAttachReply, `{"kind":"attach_reply","request_id":"r","attach_reply":{"accepted":true,"channel_id":"c","daemon_id":"d"}}`},
		{"attach reply illegal generation", parseAttachReply, `{"kind":"attach_reply","request_id":"r","attach_reply":{"accepted":true,"channel_id":"c","generation":"not-v7","daemon_id":"d"}}`},
		{"attach reply accepted daemon", parseAttachReply, `{"kind":"attach_reply","request_id":"r","attach_reply":{"accepted":true,"channel_id":"c","generation":"` + string(attempt) + `"}}`},
		{"plan pull envelope request", parsePlanPull, `{"kind":"plan_pull","plan_pull":{}}`},
		{"plan reply envelope request", parsePlanReply, `{"kind":"plan_reply","plan_reply":{}}`},
		{"plan actor id", parsePlanReply, string(mustJSON(t, map[string]any{"kind": "plan_reply", "request_id": "r", "plan_reply": map[string]any{"actors": []any{mapWithout(validActor, "actor_id")}}}))},
		{"plan actor attempt", parsePlanReply, string(mustJSON(t, map[string]any{"kind": "plan_reply", "request_id": "r", "plan_reply": map[string]any{"actors": []any{mapWithout(validActor, "attempt_key")}}}))},
		{"plan actor kind", parsePlanReply, string(mustJSON(t, map[string]any{"kind": "plan_reply", "request_id": "r", "plan_reply": map[string]any{"actors": []any{mapWithout(validActor, "kind")}}}))},
		{"plan actor class", parsePlanReply, string(mustJSON(t, map[string]any{"kind": "plan_reply", "request_id": "r", "plan_reply": map[string]any{"actors": []any{mapWithout(validActor, "class")}}}))},
		{"plan poke extra", parsePlanPoke, `{"kind":"plan_poke","extra":1}`},
		{"probe nonce", parseProbe, `{"kind":"session_probe","probe":{}}`},
		{"probe reply nonce", parseProbeReply, `{"kind":"session_probe_reply","probe_reply":{}}`},
		{"alloc request id", parseAllocRequest, `{"kind":"alloc_request","alloc_request":{"channel_id":"c","coord":"x"}}`},
		{"alloc request channel", parseAllocRequest, `{"kind":"alloc_request","alloc_request":{"request_id":"r","coord":"x"}}`},
		{"alloc request coord", parseAllocRequest, `{"kind":"alloc_request","alloc_request":{"request_id":"r","channel_id":"c"}}`},
		{"alloc reply id", parseAllocReply, `{"kind":"alloc_reply","alloc_reply":{"ok":true}}`},
		{"alloc reply reason", parseAllocReply, `{"kind":"alloc_reply","alloc_reply":{"request_id":"r"}}`},
		{"committed id", parseCommitted, `{"kind":"committed","committed":{"reservation_id":"x"}}`},
		{"committed reservation", parseCommitted, `{"kind":"committed","committed":{"request_id":"r"}}`},
		{"committed reply id", parseCommittedReply, `{"kind":"committed_reply","committed_reply":{}}`},
		{"committed reply lost invariant", parseCommittedReply, `{"kind":"committed_reply","committed_reply":{"request_id":"r","lost":true}}`},
		{"reclaim ack id", parseReclaimAck, `{"kind":"reclaim_ack","reclaim_ack":{"tombstone_id":"t"}}`},
		{"reclaim ack tombstone", parseReclaimAck, `{"kind":"reclaim_ack","reclaim_ack":{"request_id":"r"}}`},
		{"reclaim ack reply id", parseReclaimAckReply, `{"kind":"reclaim_ack_reply","reclaim_ack_reply":{}}`},
		{"reconcile pull id", parseReconcilePull, `{"kind":"reconcile_pull","reconcile_pull":{}}`},
		{"reconcile active coord", parseReconcilePull, `{"kind":"reconcile_pull","reconcile_pull":{"request_id":"r","active_coords":[""]}}`},
		{"reclaim request id", parseReclaimRequest, `{"kind":"reclaim_request","reclaim_request":{"coord":"x"}}`},
		{"reclaim request coord", parseReclaimRequest, `{"kind":"reclaim_request","reclaim_request":{"request_id":"r"}}`},
		{"reclaim reply id", parseReclaimReply, `{"kind":"reclaim_reply","reclaim_reply":{"ok":true}}`},
		{"reclaim reply reason", parseReclaimReply, `{"kind":"reclaim_reply","reclaim_reply":{"request_id":"r"}}`},
		{"reconcile reply id", parseReconcilePullReply, `{"kind":"reconcile_pull_reply","reconcile_pull_reply":{}}`},
		{"reconcile resource coord", parseReconcilePullReply, `{"kind":"reconcile_pull_reply","reconcile_pull_reply":{"request_id":"r","resources":[{}]}}`},
		{"reconcile reservation id", parseReconcilePullReply, `{"kind":"reconcile_pull_reply","reconcile_pull_reply":{"request_id":"r","pending_reservations":[{"coord":"x"}]}}`},
		{"reconcile reservation coord", parseReconcilePullReply, `{"kind":"reconcile_pull_reply","reconcile_pull_reply":{"request_id":"r","pending_reservations":[{"reservation_id":"x"}]}}`},
		{"reconcile tombstone id", parseReconcilePullReply, `{"kind":"reconcile_pull_reply","reconcile_pull_reply":{"request_id":"r","pending_tombstones":[{"coord":"x"}]}}`},
		{"reconcile tombstone coord", parseReconcilePullReply, `{"kind":"reconcile_pull_reply","reconcile_pull_reply":{"request_id":"r","pending_tombstones":[{"tombstone_id":"x"}]}}`},
		{"resolve request id", parseResolveCoord, `{"kind":"resolve_coord","resolve_coord":{"token":"t"}}`},
		{"resolve request token", parseResolveCoord, `{"kind":"resolve_coord","resolve_coord":{"request_id":"r"}}`},
		{"resolve reply id", parseResolveCoordReply, `{"kind":"resolve_coord_reply","resolve_coord_reply":{"reason":"no"}}`},
		{"resolve reject reason", parseResolveCoordReply, `{"kind":"resolve_coord_reply","resolve_coord_reply":{"request_id":"r"}}`},
		{"resolve success coord", parseResolveCoordReply, `{"kind":"resolve_coord_reply","resolve_coord_reply":{"request_id":"r","ok":true,"mode":"read"}}`},
		{"resolve success mode", parseResolveCoordReply, `{"kind":"resolve_coord_reply","resolve_coord_reply":{"request_id":"r","ok":true,"coord":"x","mode":"delete"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.parse([]byte(test.raw)); err == nil {
				t.Fatal("malformed frame passed parse")
			}
		})
	}
}

func mapWithout(source map[string]any, key string) map[string]any {
	copy := make(map[string]any, len(source)-1)
	for current, value := range source {
		if current != key {
			copy[current] = value
		}
	}
	return copy
}

func TestControlTableCompletenessRejectsEveryMissingRowAndCell(t *testing.T) {
	kinds := []controlKind{
		ctrlAttach, ctrlAttachReply, ctrlPlanPull, ctrlPlanReply, ctrlPlanPoke,
		ctrlProbe, ctrlProbeReply, ctrlAllocRequest, ctrlAllocReply,
		ctrlCommitted, ctrlCommittedReply, ctrlReclaimAck, ctrlReclaimAckReply,
		ctrlReconcilePull, ctrlReconcilePullReply, ctrlReclaimRequest, ctrlReclaimReply,
		ctrlResolveCoord, ctrlResolveCoordReply,
	}
	base := make(map[controlKind]controlRoute, len(kinds))
	for _, kind := range kinds {
		base[kind] = controlRoute{
			parse:     func([]byte) (controlMessage, error) { return controlMessage{}, nil },
			handle:    func(controlDispatchInput, controlMessage) {},
			execution: controlExecutionInline, gate: controlGateNone, state: controlStateNone,
		}
	}
	clone := func() map[controlKind]controlRoute {
		result := make(map[controlKind]controlRoute, len(base))
		for kind, row := range base {
			result[kind] = row
		}
		return result
	}
	if _, err := newControlRouter(kinds, clone(), nil, nil, nil); err != nil {
		t.Fatalf("complete table rejected: %v", err)
	}
	for _, kind := range kinds {
		t.Run("row/"+string(kind), func(t *testing.T) {
			rows := clone()
			delete(rows, kind)
			if _, err := newControlRouter(kinds, rows, nil, nil, nil); err == nil {
				t.Fatal("missing row accepted")
			}
		})
		for _, cell := range []string{"parse", "handle", "position", "worker_busy"} {
			t.Run(cell+"/"+string(kind), func(t *testing.T) {
				rows := clone()
				row := rows[kind]
				switch cell {
				case "parse":
					row.parse = nil
				case "handle":
					row.handle = nil
				case "position":
					row.execution = 0
				case "worker_busy":
					row.execution = controlExecutionWorker
					row.busy = nil
				}
				rows[kind] = row
				if _, err := newControlRouter(kinds, rows, nil, nil, nil); err == nil {
					t.Fatal("missing cell accepted")
				}
			})
		}
	}
}

func TestControlDispatcherThreeWayKindSemantics(t *testing.T) {
	var malformed, unknown int
	router, err := newControlRouter(
		[]controlKind{ctrlProbe},
		map[controlKind]controlRoute{ctrlProbe: {
			parse: parseProbe, handle: func(controlDispatchInput, controlMessage) {},
			execution: controlExecutionInline, gate: controlGateNone, state: controlStateProbe,
		}},
		nil,
		func(controlKind, error) { malformed++ },
		func(controlKind) { unknown++ },
	)
	if err != nil {
		t.Fatal(err)
	}
	router.dispatch(controlDispatchInput{}, []byte(`{}`))
	router.dispatch(controlDispatchInput{}, []byte(`{"kind":"session_probe","probe":{}}`))
	router.dispatch(controlDispatchInput{}, []byte(`{"kind":"future_kind"}`))
	if malformed != 2 || unknown != 1 {
		t.Fatalf("malformed=%d unknown=%d, want 2/1", malformed, unknown)
	}
}

func TestSessionGateRunsInsideWorkerAndIdentityIsPassed(t *testing.T) {
	link := &linkSession{controlTasks: newControlTaskPool(nil, nil)}
	release := make(chan struct{})
	for i := 0; i < controlTaskCapacity; i++ {
		link.controlTasks.submit(func() { <-release }, nil)
	}
	var gateCalls, handlerCalls, busyCalls int
	var peer string
	router, err := newControlRouter(
		[]controlKind{ctrlPlanPull},
		map[controlKind]controlRoute{ctrlPlanPull: {
			parse: parsePlanPull, execution: controlExecutionWorker,
			gate: controlGateSession, state: controlStateSessionGate,
			handle: func(in controlDispatchInput, _ controlMessage) {
				handlerCalls++
				peer = in.peerID
			},
			busy:       func(controlDispatchInput, controlMessage) { busyCalls++ },
			gateReject: func(controlDispatchInput, controlMessage) {},
		}},
		func() bool { gateCalls++; return true }, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	router.dispatch(
		controlDispatchInput{peerID: "daemon-authenticated", link: link},
		[]byte(`{"kind":"plan_pull","request_id":"r","plan_pull":{}}`),
	)
	if gateCalls != 0 || handlerCalls != 0 || busyCalls != 1 {
		t.Fatalf("saturated dispatch gate=%d handler=%d busy=%d", gateCalls, handlerCalls, busyCalls)
	}
	close(release)
	if !waitGroupWithin(&link.controlTasks.wg, time.Second) {
		t.Fatal("control workers did not drain")
	}
	router.dispatch(
		controlDispatchInput{peerID: "daemon-authenticated", link: link},
		[]byte(`{"kind":"plan_pull","request_id":"r2","plan_pull":{}}`),
	)
	if !waitGroupWithin(&link.controlTasks.wg, time.Second) {
		t.Fatal("gated worker did not finish")
	}
	if gateCalls != 1 || handlerCalls != 1 || peer != "daemon-authenticated" {
		t.Fatalf("gate=%d handler=%d peer=%q", gateCalls, handlerCalls, peer)
	}
}

func TestUncorrelatedAttachReplyClosesWithoutAdoptingSession(t *testing.T) {
	generation, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	d := &Dialer{
		sessions:      newSessionRegistry(nil),
		pendingAttach: newPendingReplies[AttachReply](),
		done:          make(chan struct{}),
	}
	router, err := buildDaemonControlRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeControl(controlFrame{
		RequestID: "not-pending", Kind: ctrlAttachReply,
		AttachReply: &AttachReply{
			Accepted: true, ChannelID: "c", DaemonID: "daemon",
			Generation: SessionGeneration(generation),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	router.dispatch(controlDispatchInput{}, raw)
	d.mu.Lock()
	closed, session := d.closed, d.session
	d.mu.Unlock()
	if !closed {
		t.Fatal("uncorrelated attach reply did not close the dial")
	}
	if session != nil || len(d.sessions.snapshots()) != 0 {
		t.Fatal("read-loop correlation failure polluted the session ledger")
	}
}

func TestMatchedAttachReplyCannotAdoptAfterLocalClose(t *testing.T) {
	generation, err := actorhost.NewAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	d := &Dialer{
		sessions:      newSessionRegistry(nil),
		pendingAttach: newPendingReplies[AttachReply](),
		done:          make(chan struct{}),
	}
	const requestID = "matched-before-close"
	waiter := d.pendingAttach.register(requestID)
	reply := AttachReply{
		Accepted: true, ChannelID: "channel", DaemonID: "daemon",
		Generation: SessionGeneration(generation),
	}
	if !d.pendingAttach.deliver(requestID, reply) {
		t.Fatal("attach reply did not match its pending request")
	}
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()

	delivered := <-waiter
	if err := d.adoptAttachReply(delivered); err == nil {
		t.Fatal("closed dial adopted an already-delivered attach reply")
	}
	d.mu.Lock()
	session := d.session
	d.mu.Unlock()
	if session != nil || len(d.sessions.snapshots()) != 0 {
		t.Fatal("close between delivery and adoption polluted the session ledger")
	}
}

func TestEndpointTablesDeclareExecutionGateAndDedicatedState(t *testing.T) {
	home, err := buildHomeControlRouter(
		&Acceptor{}, t.Context(), &sessionRecord{}, func() bool { return true },
	)
	if err != nil {
		t.Fatal(err)
	}
	daemon, err := buildDaemonControlRouter(&Dialer{})
	if err != nil {
		t.Fatal(err)
	}
	assertExecution := func(router *controlRouter, want controlExecution, kinds ...controlKind) {
		t.Helper()
		for _, kind := range kinds {
			if got := router.rows[kind].execution; got != want {
				t.Errorf("%s execution=%v want %v", kind, got, want)
			}
		}
	}
	assertExecution(home, controlExecutionWorker,
		ctrlAttach, ctrlPlanPull, ctrlCommitted, ctrlReclaimAck, ctrlReconcilePull)
	assertExecution(home, controlExecutionInline,
		ctrlAllocReply, ctrlReclaimReply, ctrlResolveCoord, ctrlProbe, ctrlProbeReply)
	assertExecution(daemon, controlExecutionWorker, ctrlAllocRequest, ctrlReclaimRequest)
	assertExecution(daemon, controlExecutionInline,
		ctrlAttachReply, ctrlPlanReply, ctrlPlanPoke,
		ctrlCommittedReply, ctrlReclaimAckReply, ctrlReconcilePullReply,
		ctrlResolveCoordReply, ctrlProbe, ctrlProbeReply)

	for kind, row := range home.rows {
		want := controlGateNone
		if kind == ctrlPlanPull {
			want = controlGateSession
		}
		if row.gate != want {
			t.Errorf("home %s gate=%v want %v", kind, row.gate, want)
		}
	}
	if home.rows[ctrlAttach].state != controlStateCandidate ||
		home.rows[ctrlPlanPull].state != controlStateSessionGate ||
		home.rows[ctrlProbe].state != controlStateProbe ||
		home.rows[ctrlProbeReply].state != controlStateProbe {
		t.Fatal("home table lost an explicit dedicated-state declaration")
	}
	if daemon.rows[ctrlProbe].state != controlStateProbe ||
		daemon.rows[ctrlProbeReply].state != controlStateProbe {
		t.Fatal("daemon table lost probe-state declarations")
	}
}

// The completeness constructor's session-gate contract: a gated row must be
// worker-positioned, carry a gateReject, and have a live sessionGate — each
// omission alone must fail construction.
func TestGatedRowCompletenessRejectsEachMissingRequirement(t *testing.T) {
	makeRows := func(mutate func(*controlRoute)) map[controlKind]controlRoute {
		row := controlRoute{
			parse:      func([]byte) (controlMessage, error) { return controlMessage{}, nil },
			handle:     func(controlDispatchInput, controlMessage) {},
			execution:  controlExecutionWorker,
			gate:       controlGateSession,
			state:      controlStateNone,
			busy:       func(controlDispatchInput, controlMessage) {},
			gateReject: func(controlDispatchInput, controlMessage) {},
		}
		mutate(&row)
		return map[controlKind]controlRoute{"gated": row}
	}
	gate := func() bool { return true }
	if _, err := newControlRouter(
		[]controlKind{"gated"}, makeRows(func(*controlRoute) {}), gate, nil, nil,
	); err != nil {
		t.Fatalf("complete gated row rejected: %v", err)
	}
	if _, err := newControlRouter(
		[]controlKind{"gated"},
		makeRows(func(r *controlRoute) { r.gateReject = nil }), gate, nil, nil,
	); err == nil {
		t.Fatal("gated row without gateReject accepted")
	}
	if _, err := newControlRouter(
		[]controlKind{"gated"},
		makeRows(func(r *controlRoute) { r.execution = controlExecutionInline; r.busy = nil }),
		gate, nil, nil,
	); err == nil {
		t.Fatal("inline gated row accepted")
	}
	if _, err := newControlRouter(
		[]controlKind{"gated"}, makeRows(func(*controlRoute) {}), nil, nil, nil,
	); err == nil {
		t.Fatal("gated row without a sessionGate accepted")
	}
}

// The daemon table has no session-gated row today; a future accidental gate
// addition must trip this regression, not slip in silently.
func TestDaemonTableHasNoSessionGatedRow(t *testing.T) {
	daemon, err := buildDaemonControlRouter(&Dialer{})
	if err != nil {
		t.Fatal(err)
	}
	for kind, row := range daemon.rows {
		if row.gate != controlGateNone {
			t.Errorf("daemon %s gate=%v want none", kind, row.gate)
		}
	}
}

// plan_poke's sole-key rule: a single-key frame whose key is not "kind" is
// rejected by the dedicated validator branch.
func TestPlanPokeMissingKindKeyIsInvalid(t *testing.T) {
	if validPlanPoke([]byte(`{"other":"x"}`)) {
		t.Fatal("single-key frame without kind accepted as plan_poke")
	}
}
