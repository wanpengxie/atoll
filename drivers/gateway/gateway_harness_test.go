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
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type gatewayTestCompositionResolver struct{}

func (gatewayTestCompositionResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

type gatewayTestDaemonAuthority struct{}

func (gatewayTestDaemonAuthority) LockAndValidate(context.Context, string, channel.ID) (func(), error) {
	return func() {}, nil
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

// openHome opens a real channel Home and admits `principal` as a human member, whose
// per-identity slot Admit ensures synchronously (so a member Route resolves a real
// slot). Returns the Home and the channel-scoped subject id. The long reconcile
// interval keeps the Home's own background ticker from racing the test.
func openHome(t *testing.T, chID channel.ID, principal string) (*home.Home, actor.ActorID) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), string(chID)+".sqlite")
	h, err := home.Open(home.Config{
		ChannelID:           chID,
		DBPath:              dbPath,
		Bootstrap:           true,
		ReconcileInterval:   time.Hour,
		CompositionResolver: gatewayTestCompositionResolver{},
		DaemonAuthority:     gatewayTestDaemonAuthority{},
	})
	if err != nil {
		t.Fatalf("home.Open(%s): %v", chID, err)
	}
	t.Cleanup(func() { _ = h.Close() })
	id, err := h.Admit(context.Background(), actor.KindHuman, principal)
	if err != nil {
		t.Fatalf("Admit(%s): %v", principal, err)
	}
	return h, id
}

// openHomeWired is openHome plus a REAL membership-change poke wire (spec §3.2 表②):
// Home.Admit/Home.Remove fire g.Poke(principal) exactly as the assembly root bridges
// App → Gateway.Poke in production (app.go/cmd/server main.go). Tests that must
// exercise a genuine Remove→poke edge (not a hand-called s.reconcile()/g.Poke) use this
// instead of openHome (六轮终审 P1-5: barrier authenticity).
func openHomeWired(t *testing.T, chID channel.ID, principal string, g *Gateway) (*home.Home, actor.ActorID) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), string(chID)+".sqlite")
	h, err := home.Open(home.Config{
		ChannelID:           chID,
		DBPath:              dbPath,
		Bootstrap:           true,
		ReconcileInterval:   time.Hour,
		CompositionResolver: gatewayTestCompositionResolver{},
		DaemonAuthority:     gatewayTestDaemonAuthority{},
		OnMembershipChange:  g.Poke,
	})
	if err != nil {
		t.Fatalf("home.Open(%s): %v", chID, err)
	}
	t.Cleanup(func() { _ = h.Close() })
	id, err := h.Admit(context.Background(), actor.KindHuman, principal)
	if err != nil {
		t.Fatalf("Admit(%s): %v", principal, err)
	}
	return h, id
}

// openDormantDeclaredHomeWired is the no-body variant used by delivery-lane
// linearization tests. It installs a real durable principal and real
// Remove→OnMembershipChange wire, but deliberately declares an unresolved class,
// so Home cannot race the test-owned subject slot with a built-in human
// interpreter.
func openDormantDeclaredHomeWired(t *testing.T, chID channel.ID, principal string, g *Gateway) (*home.Home, actor.ActorID) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), string(chID)+".sqlite")
	h, err := home.Open(home.Config{
		ChannelID:           chID,
		DBPath:              dbPath,
		Bootstrap:           true,
		ReconcileInterval:   time.Hour,
		CompositionResolver: gatewayTestCompositionResolver{},
		DaemonAuthority:     gatewayTestDaemonAuthority{},
		OnMembershipChange:  g.Poke,
	})
	if err != nil {
		t.Fatalf("home.Open(%s): %v", chID, err)
	}
	t.Cleanup(func() { _ = h.Close() })
	declared, err := h.Declare(context.Background(), home.DeclareRequest{
		SourceDeclID: "gateway-test:" + principal,
		Principal:    principal,
		Kind:         actor.KindAgent,
		Class:        "gateway-test-unresolved",
		Placement:    storespec.NewServerPlacement(),
		CreatedAt:    time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Declare(%s): %v", principal, err)
	}
	return h, declared.Row.ID
}

var testRouting Routing = func(context.Context, channel.ID, message.Kind) ([]actor.ActorID, message.Kind, string, error) {
	return []actor.ActorID{"test-agent"}, message.KindRequest, "", nil
}

func newTestGateway(t testing.TB, cfg Config, set settings) *Gateway {
	t.Helper()
	if cfg.Routing == nil {
		cfg.Routing = testRouting
	}
	g, err := newGateway(cfg, set)
	if err != nil {
		t.Fatalf("newGateway: %v", err)
	}
	return g
}

// memberRoute builds a member Route to a Home's admitted subject.
func memberRoute(chID channel.ID, h *home.Home, subj actor.ActorID, now time.Time) Route {
	return Route{Channel: chID, Home: h, Access: AccessMember, SubjectID: subj}
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
				r, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, job.Frame.Ref, subjectgate.SubmitReceipt{MessageID: "m", Seq: 1})
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

// admitRows admits n extra humans into h to append n log rows (each Admit = one
// membership-mirror row, verified by probe) — a cheap way to seed a feed backlog.
func admitRows(t *testing.T, h *home.Home, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := h.Admit(context.Background(), actor.KindHuman, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("Admit row %d: %v", i, err)
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
				f, err := subjectgate.ParseFrame(b)
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
