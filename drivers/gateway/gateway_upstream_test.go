package gateway

// Upstream business-frame mapping (spec §3.1/§3.2 表①, DoD-4 六业务帧四码) + the
// 撤销×在途写 semantics (DoD-5 / DoD-7①, A 案 best-effort window).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// TestUpstreamSixFramesFourCodes (DoD-4): every one of the six business frames maps to
// the four error codes off the资格账 gate (统一映射 表①): missing channel_id →
// bad_payload; no eligibility → forbidden; lease-expired (paused) → unavailable; session
// closed → closed. None of these paths dereferences Route.Home (they short-circuit
// before slot lookup), so a bare session with a synthesised资格账 exercises the full
// gate.
func TestUpstreamSixFramesFourCodes(t *testing.T) {
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: newClock()})

	newSess := func(t *testing.T) *Session {
		s, err := g.Attach("u", nil)
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		return s
	}

	for _, typ := range businessFrames {
		typ := typ
		t.Run(string(typ), func(t *testing.T) {
			// bad_payload: channel_id absent (empty → required-field validator rejects).
			s := newSess(t)
			if code := codeOf(t, s.Upstream(mkBusiness(t, typ, ""))); code != subjectgate.CodeBadPayload {
				t.Fatalf("%s missing channel_id → want bad_payload, got %q", typ, code)
			}
			s.Close()

			// forbidden: named channel absent from the资格账 (no eligibility).
			s = newSess(t)
			s.elig.Store(&eligState{routes: map[channel.ID]Route{}, paused: map[channel.ID]struct{}{}})
			if code := codeOf(t, s.Upstream(mkBusiness(t, typ, "c"))); code != subjectgate.CodeForbidden {
				t.Fatalf("%s no eligibility → want forbidden, got %q", typ, code)
			}
			s.Close()

			// unavailable: channel in the paused set (lease expired) → retryable.
			s = newSess(t)
			s.elig.Store(&eligState{
				routes: map[channel.ID]Route{},
				paused: map[channel.ID]struct{}{"c": {}},
			})
			if code := codeOf(t, s.Upstream(mkBusiness(t, typ, "c"))); code != subjectgate.CodeUnavailable {
				t.Fatalf("%s paused lease → want unavailable, got %q", typ, code)
			}
			s.Close()

			// closed: a valid member route, but the session is closed → the delivery
			// permit gate refuses (Route.Home is never dereferenced here).
			s = newSess(t)
			s.elig.Store(&eligState{
				routes: map[channel.ID]Route{"c": {Channel: "c", Access: AccessMember}},
				paused: map[channel.ID]struct{}{},
			})
			s.Close()
			if code := codeOf(t, s.Upstream(mkBusiness(t, typ, "c"))); code != subjectgate.CodeClosed {
				t.Fatalf("%s on closed session → want closed, got %q", typ, code)
			}
		})
	}
}

func TestApplyRoutingSinglePath(t *testing.T) {
	res := newResolver()
	calls := 0
	routing := func(context.Context, channel.ID, message.Kind) ([]actor.ActorID, message.Kind, string, error) {
		calls++
		return []actor.ActorID{"agent-1"}, message.KindRequest, "", nil
	}
	g := newTestGateway(t, Config{Resolver: res, Routing: routing}, settings{clock: newClock()})
	s, _ := g.Attach("routing", nil)
	defer s.Close()

	explicit, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "explicit", subjectgate.SubmitPayload{
		ChannelID: "c", Kind: string(message.KindEvent), Audience: []string{"human-1"},
	})
	got, ferr := s.applyRouting("c", explicit)
	if ferr != nil || calls != 0 {
		t.Fatalf("explicit audience called Routing: calls=%d err=%v", calls, ferr)
	}
	var explicitPayload subjectgate.SubmitPayload
	_ = got.DecodePayload(&explicitPayload)
	if len(explicitPayload.Audience) != 1 || explicitPayload.Audience[0] != "human-1" {
		t.Fatalf("explicit audience changed: %v", explicitPayload.Audience)
	}

	empty, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "empty", subjectgate.SubmitPayload{ChannelID: "c"})
	got, ferr = s.applyRouting("c", empty)
	if ferr != nil || calls != 1 {
		t.Fatalf("empty audience routing: calls=%d err=%v", calls, ferr)
	}
	var routed subjectgate.SubmitPayload
	_ = got.DecodePayload(&routed)
	if len(routed.Audience) != 1 || routed.Audience[0] != "agent-1" || routed.Kind != string(message.KindRequest) {
		t.Fatalf("routed payload = audience %v kind %q", routed.Audience, routed.Kind)
	}
}

func TestApplyRoutingFailuresAreUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retryable string
		err       error
	}{
		{name: "policy", retryable: "no reachable brain"},
		{name: "internal", err: errors.New("routing store down")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routing := func(context.Context, channel.ID, message.Kind) ([]actor.ActorID, message.Kind, string, error) {
				return nil, "", tc.retryable, tc.err
			}
			g := newTestGateway(t, Config{Resolver: newResolver(), Routing: routing}, settings{clock: newClock()})
			s, _ := g.Attach("routing", nil)
			defer s.Close()
			f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{ChannelID: "c"})
			_, got := s.applyRouting("c", f)
			if got == nil || codeOf(t, *got) != subjectgate.CodeUnavailable {
				t.Fatalf("routing failure = %v, want unavailable frame", got)
			}
		})
	}
}

// TestUpstreamObserverForbidden: an observer route (read-only) may not drive a business
// frame — the gate refuses forbidden before any delivery (表① observer/absent →
// forbidden).
func TestUpstreamObserverForbidden(t *testing.T) {
	g := newTestGateway(t, Config{Resolver: newResolver()}, settings{clock: newClock()})
	s, _ := g.Attach("obs", nil)
	defer s.Close()
	s.elig.Store(&eligState{
		routes: map[channel.ID]Route{"c": {Channel: "c", Access: AccessObserver}},
		paused: map[channel.ID]struct{}{},
	})
	if code := codeOf(t, s.Upstream(mkBusiness(t, subjectgate.FrameSubmit, "c"))); code != subjectgate.CodeForbidden {
		t.Fatalf("observer business frame → want forbidden, got %q", code)
	}
}

// TestUpstreamNoOccupantUnavailable: a member route whose subject cell has NO attached
// interpreter → Deliver returns ErrNoOccupant → the gate maps it to unavailable
// (retryable, 表①). Exercises the real slot lookup + Deliver path.
func TestUpstreamNoOccupantUnavailable(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "kim"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	defer s.Close()
	s.reconcile() // establish eligibility; no interpreter is ever attached to the slot
	if code := codeOf(t, s.Upstream(mkBusiness(t, subjectgate.FrameSubmit, "c"))); code != subjectgate.CodeUnavailable {
		t.Fatalf("member route with no live cell → want unavailable, got %q", code)
	}
}

// TestRevocationInFlightThenRefused (DoD-5 / DoD-7① A 案): a delivery whose eligibility
// check has already passed and is in-flight (inside Deliver) STILL commits after the
// revocation lands (best-effort window — 撤销前已过检的在途操作可落账); a NEW frame after
// the资格账 re-resolves to absent is refused forbidden (撤销提交后的新检查必拒). No
// global write-count is asserted (A 案: no "全局至多一次").
func TestRevocationInFlightThenRefused(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "leo"
	// openHomeWired (六轮终审 P1-5, barrier authenticity): a REAL membership-change poke
	// wire, so this test's revocation drives the actual Remove→poke edge — not a
	// hand-called s.reconcile() standing in for it.
	h, id := openHomeWired(t, channel.ID("c"), principal, g)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	// Home's TempDir cleanup must not race the session pump's deferred subscription
	// cancel. Use the owning Gateway.Close join (not Session.Close's async signal), then
	// synchronously close the Home before TempDir removes sqlite files (R2 flaky
	// "directory not empty"). Both later t.Cleanup calls are idempotent.
	defer func() {
		_ = g.Close()
		_ = h.Close()
	}()
	s.StartFeed() // real running pump (StartFeed's synchronous first reconcile resolves "c")
	slot, _ := h.SubjectSlotFor(id)

	got := make(chan struct{}, 1)
	release := make(chan struct{})
	stop := blockingInterpreter(slot, got, release)
	defer stop()

	// Drive a submit; it passes the eligibility check and blocks in-flight in the cell.
	inflight := make(chan subjectgate.Frame, 1)
	go func() {
		inflight <- s.Upstream(mkBusiness(t, subjectgate.FrameSubmit, "c"))
	}()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery never reached the interpreter")
	}

	// The revocation lands NOW: the resolver drops the route AND a REAL Home.Remove
	// fires the real membership-change poke (六轮终审 P1-5) — no manual s.reconcile()
	// call. The running pump must self-discover this asynchronously off the poke.
	res.set(principal, nil, nil, nil)
	if err := h.Remove(context.Background(), id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	waitFor(t, func() bool {
		_, ok := eligRoutes(s)["c"]
		return !ok
	}, "real Remove→poke must retire the channel without a hand-called reconcile")

	// Release the in-flight delivery → it STILL commits (best-effort, 已过检可落账).
	close(release)
	select {
	case f := <-inflight:
		if f.Type != subjectgate.FrameReceipt {
			t.Fatalf("in-flight delivery must still commit after revocation (A 案), got %+v", f)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight delivery did not complete")
	}

	// A NEW frame after the revocation → the new check必拒 forbidden.
	if code := codeOf(t, s.Upstream(mkBusiness(t, subjectgate.FrameSubmit, "c"))); code != subjectgate.CodeForbidden {
		t.Fatalf("post-revocation frame must be forbidden (撤销提交后的新检查必拒), got %q", code)
	}
}
