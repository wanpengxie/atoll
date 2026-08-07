package gateway

// S4 test group (连接模型勘误期 spec §5 S4 / §8 DoD) shared harness.
//
// These are WHITE-BOX tests over the reconcile-model gateway (spec §3.2): the two
// reconcile drivers (资格对账 = the per-session read pump; 在场对账 = the gateway-wide
// presence loop) are exercised BOTH through their running goroutines (poke/wake edges,
// blocking-deliver barriers) AND by calling the reconcile FUNCTIONS directly
// (s.reconcile / g.presenceReconcile) for deterministic convergence assertions — one
// call = one 圈/轮, no dependence on the 30s backstop timers (fast-test discipline).

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type gatewayTestCompositionResolver struct{}

func (gatewayTestCompositionResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func (gatewayTestCompositionResolver) ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error) {
	return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
}
func (gatewayTestCompositionResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}

// logCapture is a slog.Handler that records every emitted message (for telemetry
// assertions — e.g. gateway.entitlement.paused/resumed, DoD-8).
type logCapture struct {
	mu   sync.Mutex
	msgs []string
	ints map[string]map[string]int64
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, r.Message)
	if c.ints == nil {
		c.ints = make(map[string]map[string]int64)
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Value.Kind() == slog.KindInt64 {
			m := c.ints[r.Message]
			if m == nil {
				m = make(map[string]int64)
				c.ints[r.Message] = m
			}
			m[a.Key] = a.Value.Int64()
		}
		return true
	})
	c.mu.Unlock()
	return nil
}
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }
func (c *logCapture) has(msg string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if m == msg {
			return true
		}
	}
	return false
}
func (c *logCapture) logger() *slog.Logger { return slog.New(c) }
func (c *logCapture) intAttr(msg, key string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.ints[msg][key]
	return v, ok
}

// fakeResolver is a controllable EntitlementResolver (spec §3.2 解析面): the test sets
// per-principal routes / per-channel failures / a whole-snapshot error, each read under
// a lock so a test goroutine can flip eligibility while a pump/loop reads it.
type fakeResolver struct {
	mu     sync.Mutex
	routes map[string][]Route
	failed map[string][]channel.ID
	err    map[string]error
	calls  int
}

func newResolver() *fakeResolver {
	return &fakeResolver{
		routes: map[string][]Route{},
		failed: map[string][]channel.ID{},
		err:    map[string]error{},
	}
}

func (r *fakeResolver) Snapshot(ctx context.Context, principal string) ([]Route, []channel.ID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.routes[principal], r.failed[principal], r.err[principal]
}

func (r *fakeResolver) set(principal string, routes []Route, failed []channel.ID, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[principal] = routes
	r.failed[principal] = failed
	r.err[principal] = err
}

func (r *fakeResolver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// fakeClock is an injectable wall clock (spec §3.2 lease anchoring): the T_stale lease
// and reconcile timestamps read it, so a test drives lease expiry by advancing it
// rather than sleeping 30s.
type fakeClock struct {
	mu      sync.Mutex
	t       time.Time
	pending []*fakeTimer
	arms    []time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Now() time.Time { return c.now() }

type fakeTimer struct {
	mu       sync.Mutex
	deadline time.Time
	ch       chan time.Time
	stopped  bool
	fired    bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}
func (t *fakeTimer) fire(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired || t.deadline.After(now) {
		return false
	}
	t.fired = true
	return true
}
func (t *fakeTimer) settled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped || t.fired
}

// NewTimer and advance form a deterministic injected clock, but the code under test
// still arms/receives a real Timer through the production loop's select. Tests never
// send timer channels or call reconcile directly to stand in for an alarm.
func (c *fakeClock) NewTimer(deadline time.Time) timer {
	c.mu.Lock()
	t := &fakeTimer{deadline: deadline, ch: make(chan time.Time, 1)}
	c.arms = append(c.arms, deadline)
	if !deadline.After(c.t) {
		t.fired = true
		t.ch <- c.t
	} else {
		c.pending = append(c.pending, t)
	}
	c.mu.Unlock()
	return t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	now := c.t
	var due []*fakeTimer
	remaining := c.pending[:0]
	for _, timer := range c.pending {
		if timer.fire(now) {
			due = append(due, timer)
		} else if !timer.settled() {
			remaining = append(remaining, timer)
		}
	}
	c.pending = remaining
	c.mu.Unlock()
	for _, timer := range due {
		timer.ch <- now
	}
}

func (c *fakeClock) armCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.arms)
}

func (c *fakeClock) lastDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.arms) == 0 {
		return time.Time{}
	}
	return c.arms[len(c.arms)-1]
}

type testChannel struct {
	channelhost.Bundle
	host      *channelhost.ChannelHost
	channelID channel.ID
	principal string
	memberID  actor.ActorID
	ownerID   actor.ActorID
	extras    *subjectgate.Registry
	sources   map[actor.ActorID]string
}

type testGatewayHitch struct {
	base   channelhost.GatewayHitch
	extras *subjectgate.Registry
}

func (h testGatewayHitch) SubjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	if slot, ok := h.extras.Slot(id); ok {
		return slot, true
	}
	return h.base.SubjectSlotFor(id)
}

func (h testGatewayHitch) Subscribe() (<-chan struct{}, func()) { return h.base.Subscribe() }

func (h *testChannel) Gateway() channelhost.GatewayHitch {
	return testGatewayHitch{base: h.Bundle.Gateway(), extras: h.extras}
}

func (h *testChannel) Close() error { return h.host.Close(context.Background()) }

func (h *testChannel) EnsureSubjectSlot(id actor.ActorID) *subjectgate.Slot {
	return h.extras.EnsureSlot(id)
}

func (h *testChannel) SubjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	return h.Gateway().SubjectSlotFor(id)
}

func (h *testChannel) View() channelhost.View { return h.Bundle.View() }

func (h *testChannel) Admit(ctx context.Context, _ actor.Kind, principal string) (actor.ActorID, error) {
	result, err := h.SysOp().Admit(ctx, channelspec.AdmitRequest{Ref: "gateway-test:admit:" + principal, Principal: principal})
	return result.ActorID, err
}

func (h *testChannel) Remove(ctx context.Context, id actor.ActorID) error {
	if h.ownerID == "" {
		return context.Canceled
	}
	_, err := h.SysOp().Remove(ctx, channelspec.RemoveRequest{Ref: "gateway-test:remove:" + string(id), Target: id, InitiatorActorID: h.ownerID})
	if err == nil {
		h.extras.Remove(id)
	}
	return err
}

func openTestChannel(t *testing.T, chID channel.ID, owner, member string, memberKind actor.Kind, wired *Gateway) (*testChannel, actor.ActorID) {
	t.Helper()
	deps := channelhost.HomeDeps{CompositionResolver: gatewayTestCompositionResolver{}, IntroductionResolver: gatewayTestCompositionResolver{}}
	if wired != nil {
		deps.OnRelationChange = func(_ channel.ID, deltas []channelspec.RelationDelta) {
			for _, delta := range deltas {
				if delta.Principal != "" {
					wired.Poke(delta.Principal)
				}
			}
		}
	}
	host, err := channelhost.New(t.TempDir(), deps)
	if err != nil {
		t.Fatal(err)
	}
	spec := channelhost.ProvisionSpec{ChannelID: chID, Type: "group", OwnerPrincipal: owner, CreatedAt: time.Now().UnixMilli()}
	source := ""
	if memberKind == actor.KindAgent {
		source = "gateway-test:" + member
		rendered, sealErr := (channelspec.RenderedSnapshot{
			Class: "gateway-test-unresolved", Placement: channel.Placement{Kind: channel.PlacementServer},
		}).Seal()
		if sealErr != nil {
			t.Fatal(sealErr)
		}
		spec.GenesisDeclarations = []channelhost.GenesisDeclaration{{DeclID: source, Kind: actor.KindAgent, Rendered: rendered}}
	}
	if _, err := host.Provision(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(context.Background(), channelhost.OpenSpec{ChannelID: chID, ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	bundle, ok := host.Acquire(chID)
	if !ok {
		t.Fatal("channel bundle unavailable")
	}
	var id actor.ActorID
	var found bool
	if memberKind == actor.KindAgent {
		var ids []actor.ActorID
		ids, err = bundle.View().DeclaredInstances(context.Background(), source)
		found = len(ids) != 0
		if found {
			id = ids[0]
		}
	} else {
		id, found, err = bundle.View().ResolvePrincipal(context.Background(), member)
	}
	if err != nil || !found {
		t.Fatalf("ResolvePrincipal(%s)=(%s,%v,%v)", member, id, found, err)
	}
	ownerID, ownerFound, ownerErr := bundle.View().ResolvePrincipal(context.Background(), owner)
	if ownerErr != nil || !ownerFound {
		t.Fatalf("ResolvePrincipal(owner %s)=(%s,%v,%v)", owner, ownerID, ownerFound, ownerErr)
	}
	h := &testChannel{Bundle: bundle, host: host, channelID: chID, principal: member, memberID: id, ownerID: ownerID, extras: subjectgate.NewRegistry(), sources: map[actor.ActorID]string{}}
	if source != "" {
		h.sources[id] = source
		h.EnsureSubjectSlot(id)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, id
}

func openHome(t *testing.T, chID channel.ID, principal string) (*testChannel, actor.ActorID) {
	return openTestChannel(t, chID, principal, principal, actor.KindHuman, nil)
}

func openHomeWired(t *testing.T, chID channel.ID, principal string, g *Gateway) (*testChannel, actor.ActorID) {
	return openTestChannel(t, chID, principal, principal, actor.KindHuman, g)
}

func openDeclaredAgentHomeWired(t *testing.T, chID channel.ID, principal string, g *Gateway) (*testChannel, actor.ActorID) {
	return openTestChannel(t, chID, "gateway-owner:"+principal, principal, actor.KindAgent, g)
}

func newTestGateway(t testing.TB, cfg Config, set settings) *Gateway {
	t.Helper()
	g, err := newGateway(cfg, set)
	if err != nil {
		t.Fatalf("newGateway: %v", err)
	}
	return g
}

// memberRoute builds a member Route to a channel bundle's admitted subject.
func memberRoute(chID channel.ID, h *testChannel, subj actor.ActorID, now time.Time) Route {
	return Route{Channel: chID, Bundle: h, SubjectID: subj}
}

// mkBusiness builds a business frame of type typ carrying channel_id=cid (empty cid →
// the required-field validator rejects it). Only the channel_id field is load-bearing
// for the routing/eligibility path under test.
func mkBusiness(t *testing.T, typ subjectgate.FrameType, cid string) subjectgate.Frame {
	t.Helper()
	var load any
	switch typ {
	case subjectgate.FrameSubmit:
		load = subjectgate.SubmitPayload{ChannelID: cid, MsgType: "human.message"}
	case subjectgate.FrameResolve:
		load = subjectgate.ResolvePayload{ChannelID: cid, ReqID: "r1", Decision: "approved"}
	case subjectgate.FrameCancel:
		load = subjectgate.CancelPayload{ChannelID: cid, ReqID: "r1"}
	case subjectgate.FrameAfter:
		load = subjectgate.AfterPayload{ChannelID: cid, DurationMs: 1000, MsgType: "wake"}
	case subjectgate.FrameCancelTimer:
		load = subjectgate.CancelTimerPayload{ChannelID: cid, TimerID: "t1"}
	case subjectgate.FrameResource:
		load = subjectgate.ResourcePayload{ChannelID: cid, Op: subjectgate.ResRead, ResourceID: "res:1"}
	default:
		t.Fatalf("mkBusiness: not a business frame: %s", typ)
	}
	f, err := subjectgate.NewFrame(typ, "ref", load)
	if err != nil {
		t.Fatalf("NewFrame(%s): %v", typ, err)
	}
	return f
}

// businessFrames is the closed set of six upstream business frame types (spec §3.1).
var businessFrames = []subjectgate.FrameType{
	subjectgate.FrameSubmit, subjectgate.FrameResolve, subjectgate.FrameCancel,
	subjectgate.FrameAfter, subjectgate.FrameCancelTimer, subjectgate.FrameResource,
}

// codeOf returns the error code of an error frame, or "" for a non-error frame.
func codeOf(t *testing.T, f subjectgate.Frame) string {
	t.Helper()
	if f.Type != subjectgate.FrameError {
		return ""
	}
	var p subjectgate.ErrorPayload
	if err := f.DecodePayload(&p); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	return p.Code
}

// waitFor polls cond until true or a 2s deadline (a real condition-wait, not a sleep
// 竞猜). Used to observe an async pump/loop edge deterministically.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal(msg)
		case <-tick.C:
		}
	}
}

// blockingInterpreter attaches an interpreter to slot that, for each job, signals
// `got` and then blocks until `release` is closed before replying with a receipt. It
// lets a test hold a delivery in-flight (past the eligibility check, inside Deliver)
// while it flips eligibility or closes the gateway. Returns a stop func.
func blockingInterpreter(slot *subjectgate.Slot, got chan<- struct{}, release <-chan struct{}) func() {
	ch, _, rel := slot.AttachInterpreter()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case job := <-ch:
				select {
				case got <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-stop:
					return
				}
				r, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, job.Frame.Ref, subjectgate.SubmitReceipt{MessageID: "m"})
				job.Reply(subjectgate.FrameResult{Frame: r})
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		rel()
		<-done
	}
}

// admitRows submits n public events through the real member slot to seed a
// visible feed backlog without reaching around the Bundle boundary.
func admitRows(t *testing.T, h *testChannel, n int) {
	t.Helper()
	slot, ok := h.SubjectSlotFor(h.memberID)
	if !ok {
		t.Fatal("member slot unavailable")
	}
	for i := 0; i < n; i++ {
		frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, "row", subjectgate.SubmitPayload{
			ChannelID: string(h.channelID), MsgType: "gateway.test.row", Kind: "event", Visibility: "public", Audience: []string{string(h.memberID)},
		})
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			result, deliverErr := slot.Deliver(context.Background(), frame)
			if deliverErr == nil && result.Frame.Type == subjectgate.FrameReceipt {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("submit row %d: result=%+v err=%v", i, result, deliverErr)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// feedObserver consumes the real downstream wire and records the exact feed sequence
// observed for each channel. Pump tests deliberately observe this public boundary:
// cursor is owner-only session state and must not be read from another goroutine.
type feedObserver struct {
	mu    sync.Mutex
	last  map[channel.ID]int64
	count map[channel.ID]int
	seqs  map[channel.ID][]int64
}

func (o *feedObserver) lastSeq(ch channel.ID) int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last[ch]
}

func (o *feedObserver) delivered(ch channel.ID) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.count[ch]
}

func (o *feedObserver) sequences(ch channel.ID) []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int64(nil), o.seqs[ch]...)
}

// observeFeed drains a session's downstream lane until Done closes. An optional
// callback runs after each observed feed frame; it lets a test change external truth
// at a real wire boundary without adding a scheduling seam to production code.
// The function returns the wire observer and a stop func that waits for its goroutine.
func observeFeed(s *Session, onFeed ...func(channel.ID, int)) (*feedObserver, func()) {
	observer := &feedObserver{
		last:  make(map[channel.ID]int64),
		count: make(map[channel.ID]int),
		seqs:  make(map[channel.ID][]int64),
	}
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(ready)
		for {
			select {
			case b, ok := <-s.Down():
				if !ok {
					return
				}
				f, err := subjectgate.ParseEnvelope(b)
				if err == nil && f.Type == subjectgate.FrameFeed {
					var payload subjectgate.FeedPayload
					if f.DecodePayload(&payload) != nil {
						continue
					}
					ch := channel.ID(payload.ChannelID)
					observer.mu.Lock()
					observer.count[ch]++
					observer.seqs[ch] = append(observer.seqs[ch], payload.Seq)
					count := observer.count[ch]
					if payload.Seq > observer.last[ch] {
						observer.last[ch] = payload.Seq
					}
					observer.mu.Unlock()
					for _, fn := range onFeed {
						fn(ch, count)
					}
				}
			case <-s.Done():
				return
			}
		}
	}()
	<-ready
	return observer, func() {
		<-done
	}
}
