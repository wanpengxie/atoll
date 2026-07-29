package link

// Fork/EndSelf FIELD FIDELITY across the link wire.
//
// Two halves of one codec face each other and nothing else in the repo drives
// them against each other:
//
//	daemon: remoteActorLifecycle.Fork  (remote_lifecycle.go) flattens an
//	        actorcaps.ForkSpec — including its *channel.Placement — into an
//	        ipc.SpawnPayload;
//	home:   serverActorEndpoint.handleFork (server_stream.go) rebuilds the
//	        ForkSpec and the Placement pointer from that flat payload.
//
// Placement.Kind is the server/daemon 落位 axis (canonical
// control-model-harden/06 §3 "落位轴（Placement.Kind）单 daemon 下也承重"): a
// field lost or mutated in flight silently places a child in the wrong
// execution domain, and every later judgment inherits the wrong answer.
//
// The rig pairs the PRODUCTION objects on both ends over one in-memory pipe:
//
//	daemon: remoteActorLifecycle → relayCore (ipc.KindSpawn / ipc.KindEnd)
//	        → Dialer.streamReadLoop (the real ack router)
//	  wire: net.Pipe + ipc.Codec
//	  home: serverActorEndpoint.readLoop → handleFork / handleEnd
//	        → the fork/endSelf handlers (recorded here)
//
// The handlers are the observation point on purpose: production wires them to
// remoteingress.Fork/EndSelf, and what must be proven is exactly what the HOME
// hands that ingress — the spec's operand, plus the coordinate the ENDPOINT
// supplies (its authenticated (id, key)), never anything a frame carried.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/ipc"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

// forkWireTimeout only ever bounds events that MUST happen (a frame arriving,
// a read loop unwinding), never a duration being awaited, so a loaded machine
// cannot flake these.
const forkWireTimeout = 15 * time.Second

// forkWireCall is one recorded home-side arrival: the coordinate the ENDPOINT
// handed the ingress plus the decoded lifecycle operand.
type forkWireCall struct {
	id  actor.ActorID
	key actorhost.AttemptKey
	req remoteingress.ForkRequest
}

type forkWireEndCall struct {
	id  actor.ActorID
	key actorhost.AttemptKey
	req actorcaps.EndSelfRequest
}

type forkWireRig struct {
	t *testing.T

	// id/key are the endpoint's AUTHENTICATED coordinate, fixed at
	// construction exactly as accept.go fixes it from the handshake. No test
	// frame ever carries them.
	id  actor.ActorID
	key actorhost.AttemptKey

	lifecycle *remoteActorLifecycle
	endpoint  *serverActorEndpoint
	stream    *actorStream

	homeConn   net.Conn
	daemonConn net.Conn

	mu        sync.Mutex
	forks     []forkWireCall
	ends      []forkWireEndCall
	forkReply func(remoteingress.ForkRequest) (actor.ActorID, error)
	endReply  func(actorcaps.EndSelfRequest) error
}

const forkWireDefaultChild = actor.ActorID("agent:fork-wire-child")

// errForkWireRefused stands for any definite home-side refusal of a fork.
var errForkWireRefused = errors.New("link-test: home refused the fork")

// newForkWireRig builds a live daemon↔home lifecycle arm. wireHandlers=false
// reproduces the home that has no lifecycle ingress installed.
func newForkWireRig(t *testing.T, wireHandlers bool) *forkWireRig {
	t.Helper()
	homeConn, daemonConn := net.Pipe()
	rig := &forkWireRig{
		t: t, id: "agent:fork-wire", key: physicalKey(t),
		homeConn: homeConn, daemonConn: daemonConn,
	}

	var handlers serverActorHandlers
	if wireHandlers {
		handlers.fork = func(
			_ context.Context,
			id actor.ActorID,
			key actorhost.AttemptKey,
			req remoteingress.ForkRequest,
		) (actor.ActorID, error) {
			rig.mu.Lock()
			rig.forks = append(rig.forks, forkWireCall{id: id, key: key, req: req})
			reply := rig.forkReply
			rig.mu.Unlock()
			if reply == nil {
				return forkWireDefaultChild, nil
			}
			return reply(req)
		}
		handlers.endSelf = func(
			_ context.Context,
			id actor.ActorID,
			key actorhost.AttemptKey,
			req actorcaps.EndSelfRequest,
		) error {
			rig.mu.Lock()
			rig.ends = append(rig.ends, forkWireEndCall{id: id, key: key, req: req})
			reply := rig.endReply
			rig.mu.Unlock()
			if reply == nil {
				return nil
			}
			return reply(req)
		}
	}
	rig.endpoint = newServerActorEndpoint(
		context.Background(), rig.id, rig.key, homeConn, nil, handlers,
	)
	go func() { _ = rig.endpoint.Run(context.Background()) }()

	// Daemon side: the same objects OpenExactActorStream builds (dial.go), on
	// the real read loop so ack routing and transport-death teardown are the
	// production ones.
	codec := ipc.NewCodec(daemonConn, daemonConn)
	rig.lifecycle = newRemoteActorLifecycle(codec)
	dialer := testDialer()
	rig.stream = &actorStream{
		id: rig.id, stream: daemonConn, codec: codec,
		writer:      NewRemoteWriter(codec),
		access:      newRelayClient(codec, ipc.KindAccess),
		sched:       newRelayClient(codec, ipc.KindSchedule),
		lifecycleV2: rig.lifecycle,
		done:        make(chan struct{}),
	}
	go dialer.streamReadLoop(rig.stream, nil)

	t.Cleanup(func() {
		_ = daemonConn.Close()
		_ = homeConn.Close()
		select {
		case <-rig.stream.done:
		case <-time.After(forkWireTimeout):
			t.Error("daemon stream read loop never unwound after the transport closed")
		}
	})
	return rig
}

func (r *forkWireRig) recordedForks() []forkWireCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]forkWireCall(nil), r.forks...)
}

func (r *forkWireRig) recordedEnds() []forkWireEndCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]forkWireEndCall(nil), r.ends...)
}

func (r *forkWireRig) setForkReply(fn func(remoteingress.ForkRequest) (actor.ActorID, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forkReply = fn
}

// drainHome closes the daemon end and waits for the home endpoint's read loop
// to unwind, so every frame that ever crossed has been counted before a test
// asserts that none did.
func (r *forkWireRig) drainHome() {
	r.t.Helper()
	_ = r.daemonConn.Close()
	select {
	case <-r.endpoint.Done():
	case <-time.After(forkWireTimeout):
		r.t.Fatal("home endpoint never unwound after the daemon end closed")
	}
}

func forkWireContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), forkWireTimeout)
	t.Cleanup(cancel)
	return ctx
}

// TestForkSpecCrossesTheWireFieldForField is the round-trip proof for every
// ForkSpec field and every legal Placement shape: what the daemon-side handle
// was asked to fork is exactly what the home ingress is handed.
func TestForkSpecCrossesTheWireFieldForField(t *testing.T) {
	cases := []struct {
		name      string
		spec      actorcaps.ForkSpec
		placement *channel.Placement // want, after the round trip
	}{
		{
			name: "daemon placement with desired host",
			spec: actorcaps.ForkSpec{
				Kind: actor.KindAgent, Class: "worker", NameHint: "hint-α",
				Config:    json.RawMessage(`{"k":"vé","n":[1,2,3],"nested":{"deep":true}}`),
				Placement: &channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-7"},
			},
			placement: &channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-7"},
		},
		{
			name: "server placement carries no host",
			spec: actorcaps.ForkSpec{
				Kind: actor.KindTool, Class: "tool-class",
				Config:    json.RawMessage(`{}`),
				Placement: &channel.Placement{Kind: channel.PlacementServer},
			},
			placement: &channel.Placement{Kind: channel.PlacementServer},
		},
		{
			// Legal per channel.Placement.Validate: an empty host on a daemon
			// placement is resolved by the home's in-channel admission segment.
			// The wire must not "helpfully" collapse it into no placement.
			name: "daemon placement with unresolved host",
			spec: actorcaps.ForkSpec{
				Kind: actor.KindAgent, Class: "worker",
				Placement: &channel.Placement{Kind: channel.PlacementDaemon},
			},
			placement: &channel.Placement{Kind: channel.PlacementDaemon},
		},
		{
			name: "absent placement stays absent",
			spec: actorcaps.ForkSpec{
				Kind: actor.KindHuman, Class: "person", NameHint: "no-placement",
			},
			placement: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newForkWireRig(t, true)
			requestID := message.ID("req-" + tc.name)

			child, err := rig.lifecycle.Fork(forkWireContext(t), requestID, tc.spec)
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			if child != forkWireDefaultChild {
				t.Fatalf("child=%q want %q", child, forkWireDefaultChild)
			}

			calls := rig.recordedForks()
			if len(calls) != 1 {
				t.Fatalf("home fork arrivals=%d want 1", len(calls))
			}
			got := calls[0]

			// The coordinate is the endpoint's authenticated one. No frame
			// carries it, so a regression that started reading it off the
			// payload would have to invent it.
			if got.id != rig.id || got.key != rig.key {
				t.Fatalf("home coordinate=(%q,%q) want (%q,%q)",
					got.id, got.key, rig.id, rig.key)
			}
			if got.req.RequestID != requestID {
				t.Fatalf("request id=%q want %q", got.req.RequestID, requestID)
			}

			spec := got.req.Spec
			if spec.Kind != tc.spec.Kind {
				t.Fatalf("kind=%q want %q", spec.Kind, tc.spec.Kind)
			}
			if spec.Class != tc.spec.Class {
				t.Fatalf("class=%q want %q", spec.Class, tc.spec.Class)
			}
			if spec.NameHint != tc.spec.NameHint {
				t.Fatalf("name hint=%q want %q", spec.NameHint, tc.spec.NameHint)
			}
			if len(tc.spec.Config) == 0 {
				if len(spec.Config) != 0 {
					t.Fatalf("config=%s want empty", spec.Config)
				}
			} else if !bytes.Equal(spec.Config, tc.spec.Config) {
				t.Fatalf("config=%s want %s", spec.Config, tc.spec.Config)
			}

			switch {
			case tc.placement == nil:
				if spec.Placement != nil {
					t.Fatalf("placement=%+v want nil", *spec.Placement)
				}
			case spec.Placement == nil:
				t.Fatalf("placement=nil want %+v", *tc.placement)
			default:
				if *spec.Placement != *tc.placement {
					t.Fatalf("placement=%+v want %+v", *spec.Placement, *tc.placement)
				}
				// Rebuilt on the far side, never the caller's own struct:
				// a shortcut that passed the pointer through would make the
				// home's copy mutable from the daemon's behavior.
				if spec.Placement == tc.spec.Placement {
					t.Fatal("home placement aliases the daemon's struct")
				}
			}
		})
	}
}

// A non-nil but ZERO placement is the one lossy input of the flat encoding
// (both discriminator fields are empty, so the decoder cannot tell it from
// "absent"). It is not a legal channel.Placement — Placement.Validate rejects
// an empty Kind — so the collapse is a degenerate-input mapping, not a lost
// legal value. Pinned so a future encoding change has to face the question
// deliberately rather than discover it in production.
func TestZeroPlacementCollapsesToAbsentAcrossTheWire(t *testing.T) {
	zero := &channel.Placement{}
	if err := zero.Validate(); err == nil {
		t.Fatal("a zero placement became legal: the collapse below is now a lost value, not a degenerate input")
	}
	rig := newForkWireRig(t, true)
	if _, err := rig.lifecycle.Fork(forkWireContext(t), "req-zero-placement", actorcaps.ForkSpec{
		Kind: actor.KindAgent, Class: "worker", Placement: zero,
	}); err != nil {
		t.Fatalf("fork: %v", err)
	}
	calls := rig.recordedForks()
	if len(calls) != 1 {
		t.Fatalf("home fork arrivals=%d want 1", len(calls))
	}
	if p := calls[0].req.Spec.Placement; p != nil {
		t.Fatalf("zero placement arrived as %+v; the encoding gained a presence bit — assert the new contract", *p)
	}
}

// TestForkFailuresCrossTheWireWithoutFabricatingAChild: every home-side refusal
// must reach the daemon caller as an error, and no arm of the failure map may
// hand back a child id the home never minted.
func TestForkFailuresCrossTheWireWithoutFabricatingAChild(t *testing.T) {
	t.Run("home error text survives", func(t *testing.T) {
		rig := newForkWireRig(t, true)
		rig.setForkReply(func(remoteingress.ForkRequest) (actor.ActorID, error) {
			return "", errForkWireRefused
		})
		child, err := rig.lifecycle.Fork(forkWireContext(t), "req-refused", actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "worker",
		})
		if err == nil {
			t.Fatal("a refused fork returned no error")
		}
		if !strings.Contains(err.Error(), errForkWireRefused.Error()) {
			t.Fatalf("error=%v want it to carry %v", err, errForkWireRefused)
		}
		if child != "" {
			t.Fatalf("refused fork returned child %q", child)
		}
	})

	t.Run("empty child with no error is not success", func(t *testing.T) {
		rig := newForkWireRig(t, true)
		rig.setForkReply(func(remoteingress.ForkRequest) (actor.ActorID, error) {
			return "", nil
		})
		child, err := rig.lifecycle.Fork(forkWireContext(t), "req-empty-child", actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "worker",
		})
		if err == nil {
			t.Fatal("an empty child id was accepted as a successful fork")
		}
		if child != "" {
			t.Fatalf("child=%q want empty", child)
		}
	})

	t.Run("home without a lifecycle ingress refuses", func(t *testing.T) {
		rig := newForkWireRig(t, false)
		if _, err := rig.lifecycle.Fork(forkWireContext(t), "req-no-handler", actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "worker",
		}); err == nil {
			t.Fatal("a home with no fork handler answered success")
		}
	})

	t.Run("missing request id never reaches the wire", func(t *testing.T) {
		rig := newForkWireRig(t, true)
		if _, err := rig.lifecycle.Fork(forkWireContext(t), "", actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "worker",
		}); err == nil {
			t.Fatal("a fork with no request id was sent")
		}
		rig.drainHome()
		if calls := rig.recordedForks(); len(calls) != 0 {
			t.Fatalf("an unidentified fork still reached the home: %+v", calls)
		}
	})
}

// TestEndSelfCrossesTheWireAsSelfScoped: the daemon-side encoder carries the
// diagnostic reason and NOTHING about whom to end. The home's target gate
// (server_stream.go handleEnd) therefore passes on every frame this encoder
// produces — the self-scoping is structural, not a lucky value.
func TestEndSelfCrossesTheWireAsSelfScoped(t *testing.T) {
	rig := newForkWireRig(t, true)
	const reason = "behaviour asked to retire"
	if err := rig.lifecycle.EndSelf(forkWireContext(t), actorcaps.EndSelfRequest{Reason: reason}); err != nil {
		t.Fatalf("end self: %v", err)
	}
	ends := rig.recordedEnds()
	if len(ends) != 1 {
		t.Fatalf("home end arrivals=%d want 1", len(ends))
	}
	if ends[0].req.Reason != reason {
		t.Fatalf("reason=%q want %q", ends[0].req.Reason, reason)
	}
	if ends[0].id != rig.id || ends[0].key != rig.key {
		t.Fatalf("home coordinate=(%q,%q) want (%q,%q)",
			ends[0].id, ends[0].key, rig.id, rig.key)
	}
}

// TestLifecycleFramesCarryOperandsOnlyNeverAnIdentityClaim reads the RAW bytes
// the daemon encoder puts on the wire. The (id, key) coordinate is fixed on the
// endpoint at handshake time, so a lifecycle frame that started carrying an
// actor id / attempt key / end target would be a frame making a claim about who
// is issuing it — the exact thing server_stream.go's call table is shaped to
// make impossible. Field names are pinned here too: they are the compatibility
// surface between the two halves.
func TestLifecycleFramesCarryOperandsOnlyNeverAnIdentityClaim(t *testing.T) {
	homeConn, daemonConn := net.Pipe()
	t.Cleanup(func() { _ = homeConn.Close(); _ = daemonConn.Close() })
	homeCodec := ipc.NewCodec(homeConn, homeConn)
	lifecycle := newRemoteActorLifecycle(ipc.NewCodec(daemonConn, daemonConn))

	forkDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.Fork(forkWireContext(t), "req-raw", actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "worker", NameHint: "raw",
			Config:    json.RawMessage(`{"a":1}`),
			Placement: &channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-3"},
		})
		forkDone <- err
	}()

	frame := forkWireReadFrame(t, homeCodec)
	if frame.Kind != ipc.KindSpawn {
		t.Fatalf("frame kind=%q want %q", frame.Kind, ipc.KindSpawn)
	}
	fields := forkWireDecodeFields(t, frame.Payload)
	wantFork := map[string]any{
		"request_id":     "req-raw",
		"kind":           "agent",
		"class":          "worker",
		"name_hint":      "raw",
		"placement_kind": "daemon",
		"placement_host": "daemon-3",
	}
	for key, want := range wantFork {
		if got, ok := fields[key]; !ok || got != want {
			t.Fatalf("spawn payload %q=%v (present=%v) want %v", key, got, ok, want)
		}
	}
	if _, ok := fields["config"]; !ok {
		t.Fatal("spawn payload dropped config")
	}
	forkWireRejectIdentityKeys(t, "spawn", fields, len(wantFork)+1)

	// Nothing drives a daemon-side ack router in this raw-wire test (and the
	// pipe is synchronous, so an unread ack would wedge the home end), so the
	// arm is settled directly. The frame under test has already been observed
	// on the wire — that is this test's whole subject.
	lifecycle.fork.deliverAck(ipc.SpawnAckPayload{ChildID: forkWireDefaultChild})
	select {
	case err := <-forkDone:
		if err != nil {
			t.Fatalf("fork: %v", err)
		}
	case <-time.After(forkWireTimeout):
		t.Fatal("fork never resolved after its ack")
	}

	endDone := make(chan error, 1)
	go func() {
		endDone <- lifecycle.EndSelf(forkWireContext(t), actorcaps.EndSelfRequest{Reason: "done"})
	}()
	frame = forkWireReadFrame(t, homeCodec)
	if frame.Kind != ipc.KindEnd {
		t.Fatalf("frame kind=%q want %q", frame.Kind, ipc.KindEnd)
	}
	fields = forkWireDecodeFields(t, frame.Payload)
	if fields["reason"] != "done" {
		t.Fatalf("end payload reason=%v want %q", fields["reason"], "done")
	}
	if _, ok := fields["target"]; ok {
		t.Fatalf("end payload carries a target claim: %v", fields["target"])
	}
	forkWireRejectIdentityKeys(t, "end", fields, 1)

	lifecycle.end.deliverAck(ipc.EndAckPayload{})
	select {
	case err := <-endDone:
		if err != nil {
			t.Fatalf("end self: %v", err)
		}
	case <-time.After(forkWireTimeout):
		t.Fatal("end self never resolved after its ack")
	}
}

func forkWireReadFrame(t *testing.T, codec *ipc.Codec) ipc.Frame {
	t.Helper()
	type readResult struct {
		frame ipc.Frame
		err   error
	}
	out := make(chan readResult, 1)
	go func() {
		frame, err := codec.Read()
		out <- readResult{frame: frame, err: err}
	}()
	select {
	case got := <-out:
		if got.err != nil {
			t.Fatalf("read frame: %v", got.err)
		}
		return got.frame
	case <-time.After(forkWireTimeout):
		t.Fatal("no lifecycle frame reached the home end")
		return ipc.Frame{}
	}
}

func forkWireDecodeFields(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	fields := map[string]any{}
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("decode payload %s: %v", payload, err)
	}
	return fields
}

// forkWireRejectIdentityKeys fails on any key that would let a frame assert who
// is issuing the operation, and on any unexpected key at all (an unreviewed
// field is exactly how such a claim would arrive).
func forkWireRejectIdentityKeys(t *testing.T, what string, fields map[string]any, wantCount int) {
	t.Helper()
	for _, forbidden := range []string{
		"actor", "actor_id", "id", "lease_id", "attempt_key", "key",
		"caller", "sender", "author", "target", "principal",
	} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("%s payload carries identity claim %q: %v", what, forbidden, fields[forbidden])
		}
	}
	if len(fields) != wantCount {
		t.Fatalf("%s payload has %d fields %v, want %d — review the new field before widening this wall",
			what, len(fields), fields, wantCount)
	}
}
