package gateway

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// defaultEpoch mints the production process-lifetime gateway epoch.
func defaultEpoch() int64 { return time.Now().UnixNano() }

// feedBatch is the read pump's per-poll, per-channel row budget (照 ws.go wsTail 100).
const feedBatch = 100

const (
	defaultHistoryLimit       = 200
	maxHistoryLimit           = 200
	initialHistoryParallelism = 4
	maxHistoryInflight        = 4
	maxBackgroundHistory      = 3
	maxHistoryBytes           = 4 << 20
)

// subscription is one channel the session's read pump is currently following (spec
// §3.2 收敛对象甲, current = 订阅集). PUMP-OWNED — created/mutated/read only by the
// runFeed goroutine, so no lock. lastOK anchors the T_stale lease; paused = lease
// expired (streaming stopped, awaiting resume).
type subscription struct {
	route     Route
	reader    Reader
	temporary bool
	notify    <-chan struct{}
	cancel    func()
	lastOK    time.Time
	paused    bool
}

type controlKind uint8

const (
	controlObserve controlKind = iota + 1
	controlUnobserve
	controlHistoryBefore
	controlHistoryCancel
)

type controlCommand struct {
	kind       controlKind
	channel    channel.ID
	before     int64
	limit      int
	byteLimit  int
	generation uint64
	purpose    string
	priority   string
	ref        string
	targetRef  string
	readCtx    context.Context
	// start is a connector-owned publication barrier. A history read may not
	// enqueue backfill until its accepted receipt is already on the live lane.
	// Direct in-process callers leave it nil and retain the synchronous API.
	start <-chan struct{}
	reply chan controlResult
}

type controlResult struct {
	code     string
	detail   string
	accepted bool
}

type historyRequest struct {
	ref      string
	priority string
	cancel   context.CancelFunc
}

// eligState is the session's资格账 snapshot (spec §3.2 v0.8, 六轮 P1-2 修形): the pump
// SINGLE-writes it (atomic pointer), Upstream reads it — so a business frame's
// channel_id maps to its Route (member? which subject? which Home?) without touching
// the pump-owned subscription map. paused = channels with an EXISTING subscription
// whose lease expired (business frame → unavailable, retryable). failed = channels the
// most recent resolver call explicitly reported as a per-channel failure THIS ROUND,
// whether or not a subscription already existed for them (a first-seen/new channel that
// fails resolution has no subscription yet, so it can never land in paused — without
// this set it would silently fall through to "confirmed absent" → forbidden, which is
// the wrong verdict for "查得坏消息"). globalErr = the last resolver call was a
// whole-snapshot failure (no routes/failed are trustworthy at all) — any channel_id not
// already a known live route maps to unavailable rather than forbidden, because a
// forbidden verdict asserts confirmed absence and we cannot currently confirm anything.
// resolved = at least one reconcile has published this state. Attach seeds an
// empty UNRESOLVED ledger; MembershipSnapshot must never present that seed as
// "confirmed member of nothing" (empty + complete), so completeness requires it.
type eligState struct {
	routes    map[channel.ID]Route
	paused    map[channel.ID]struct{}
	failed    map[channel.ID]struct{}
	globalErr bool
	resolved  bool
}

// Session is one attached connection's handle into the gateway (spec §3.2 终形):
// {lane, 游标表(lane.cursor), 订阅集(subs), dirty/wake, closed 闩, 资格账(elig), 读泵}.
// 连接即人 — a session has NO频道身份色: eligibility is a per-frame/per-batch fact.
// The connector drives it: drains Down() to the wire, feeds each parsed upstream
// frame to Upstream, and calls Close on disconnect.
type Session struct {
	gw         *Gateway
	id         string
	label      string
	principal  string
	generation uint64
	lane       *lane

	// ctx is the session-lifetime context (spec §3.2统一会话闸): Close cancels it, and
	// the resolver / routing / Slot.Deliver all derive from it — so a已获准 blocking
	// delivery unblocks on Close (排水 reaches zero), never on the HTTP request ctx.
	ctx    context.Context
	cancel context.CancelFunc

	// mu is the统一会话闸 lock: closed + the delivery permit land under it, so
	// "获准递交" and "置闩" share one linearization point.
	mu     sync.Mutex
	closed bool

	// dirty/wake is the entitlement reconcile无丢边 protocol (spec §3.2, P1-2):
	// dirty = atomic.Bool; wake = buffered(1). poke → dirty.Store(true)+非阻塞 wake;
	// pump loop head → dirty.Swap(false)→收敛 (Swap防丢边). initial-dirty = true at
	// pump start (new session current=∅, first圈 does not wait 30s).
	dirty atomic.Bool
	wake  chan struct{}

	// subs is the订阅集 (PUMP-OWNED, no lock). elig is the資格账 the pump publishes
	// for Upstream (atomic single-writer).
	subs                   map[channel.ID]*subscription
	elig                   atomic.Pointer[eligState]
	controls               chan controlCommand
	historyMu              sync.Mutex
	historyInflight        map[channel.ID]historyRequest
	historyCount           int
	historyBackgroundCount int

	onceClose sync.Once
	historyWG sync.WaitGroup
}

// Attach opens a session for one authenticated connection (连接模型勘误期: the portal
// membrane resolved cookie→principal — no channel ACL at connection level). It seats
// the device (首入 → 踢在场圈) and hands back the session; the attach receipt is now
// EMPTY (报到 ack — attach is no longer a binding grant). Refused after Close.
func (g *Gateway) Attach(principal string, since map[channel.ID]int64, generation ...uint64) (*Session, error) {
	var connectionGeneration uint64
	if len(generation) > 0 {
		connectionGeneration = generation[0]
	}
	s := &Session{
		gw:              g,
		principal:       principal,
		generation:      connectionGeneration,
		lane:            newLane(newCursor(since)),
		wake:            make(chan struct{}, 1),
		subs:            map[channel.ID]*subscription{},
		controls:        make(chan controlCommand, 32),
		historyInflight: map[channel.ID]historyRequest{},
	}
	// A connection's own name, minted here because the client must not choose it:
	// a client that named itself could claim to be another screen, and every
	// "operate that one" would be aimable at a device its sender does not hold.
	s.id = "s-" + uuid.NewString()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.elig.Store(&eligState{routes: map[channel.ID]Route{}, paused: map[channel.ID]struct{}{}, failed: map[channel.ID]struct{}{}})
	if err := g.addDevice(principal, s); err != nil {
		s.cancel()
		return nil, err
	}
	return s, nil
}

// StartFeed launches the read pump. The connector calls it AFTER sending the attach
// receipt (so the receipt is not interleaved behind backfill). The pump join-track
// is taken in beginFeed under the same closed re-check as Close — a pump arriving
// after Close is refused (迟启泵 barrier) rather than left reading a closing Home.
//
// Before spawning the async pump it establishes initial eligibility SYNCHRONOUSLY (one
// reconcile) so a client that acts immediately after the attach receipt sees its
// channels already resolved (连接即人: connect-then-act must not race the first
// reconcile — this preserves the pre-勘误 synchronous-Attach eligibility semantics).
// The pump's own initial-dirty re-resolves harmlessly (idempotent); subs it builds here
// are pump-owned thereafter (this reconcile completes before the pump goroutine starts,
// so there is no concurrent access).
func (s *Session) StartFeed() {
	if !s.PrimeFeed() {
		return
	}
	s.LaunchFeed()
}

// PrimeFeed is the synchronous half of StartFeed: pump registration + the first
// eligibility reconcile, WITHOUT spawning the pump. Split out so a connector can
// read the资格账 (MembershipSnapshot) and put the answer into the attach receipt
// BEFORE any backfill can enter the lane — the pump is the only thing that pushes
// feed frames, and it does not exist until LaunchFeed. Returns false (and closes
// the session) if registration is refused.
func (s *Session) PrimeFeed() bool {
	if !s.beginFeed() {
		s.Close()
		return false
	}
	s.reconcile()
	return true
}

// LaunchFeed spawns the async pump after PrimeFeed. Defensively re-checks the
// closed latch before spawning: production resolvers must honor ctx; even a
// faulty late return cannot launch a post-Close goroutine.
func (s *Session) LaunchFeed() {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		s.teardownSubs()
		return
	}
	go s.runFeed()
}

// MembershipSnapshot reports the资格账 as of the last published reconcile: the
// confirmed membership routes, plus whether that round confirmed the FULL set
// (complete=false on a whole-snapshot resolver failure or any per-channel
// failure — then absence from routes means "unconfirmed", not "not a member").
// Read-only off the atomic elig pointer; safe from any goroutine.
func (s *Session) MembershipSnapshot() (routes []Route, complete bool) {
	st := s.elig.Load()
	if st == nil {
		return nil, false
	}
	routes = make([]Route, 0, len(st.routes))
	for _, r := range st.routes {
		routes = append(routes, r)
	}
	return routes, st.resolved && !st.globalErr && len(st.failed) == 0
}

// PrepareHistoryMetadata snapshots only the current head of each member
// channel. Subscriptions are already installed by PrimeFeed, so advancing the
// live cursor to the captured head produces an atomic seam: commits at or below
// the head are history; commits after it remain observable by the live pump.
// No message body enters the attach receipt.
func (s *Session) PrepareHistoryMetadata(focus channel.ID) []subjectgate.HistoryMetaEntry {
	type job struct {
		ch  channel.ID
		sub *subscription
	}
	type result struct {
		ch    channel.ID
		entry subjectgate.HistoryMetaEntry
	}
	jobs := make([]job, 0, len(s.subs))
	for ch, sub := range s.subs {
		if sub.temporary {
			continue
		}
		jobs = append(jobs, job{ch: ch, sub: sub})
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.gw.tRead)
	defer cancel()
	work := make(chan job)
	results := make(chan result, len(jobs))
	workers := min(initialHistoryParallelism, len(jobs))
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				entry := subjectgate.HistoryMetaEntry{ChannelID: string(item.ch)}
				if item.sub.paused {
					entry.ErrorCode, entry.ErrorDetail = subjectgate.CodeUnavailable, "channel eligibility unavailable — retry"
					results <- result{ch: item.ch, entry: entry}
					continue
				}
				active, err := item.sub.route.Bundle.View().IsActive(ctx, item.sub.reader.ActorID)
				if err != nil || !active {
					entry.ErrorCode, entry.ErrorDetail = subjectgate.CodeUnavailable, "channel eligibility unavailable — retry"
					results <- result{ch: item.ch, entry: entry}
					continue
				}
				rows, head, _, err := item.sub.route.Bundle.View().ReadVisibleBeforeSeq(ctx, 0, 1)
				if err != nil {
					entry.ErrorCode, entry.ErrorDetail = subjectgate.CodeUnavailable, "history unavailable — retry"
					results <- result{ch: item.ch, entry: entry}
					continue
				}
				entry.HeadSeq = head
				entry.HasRows = len(rows) > 0
				if len(rows) > 0 {
					entry.LastActivity = rows[len(rows)-1].Envelope.TS
				}
				results <- result{ch: item.ch, entry: entry}
			}
		}()
	}
	go func() {
		for _, item := range jobs {
			work <- item
		}
		close(work)
		wg.Wait()
		close(results)
	}()
	collected := make([]result, 0, len(jobs))
	for item := range results {
		collected = append(collected, item)
	}
	// Deterministic ordering is client policy input only: focus first, then the
	// most recently active channels. A slow channel never blocks the others;
	// metadata reads above run in four bounded workers.
	slices.SortFunc(collected, func(a, b result) int {
		if (a.ch == focus) != (b.ch == focus) {
			if a.ch == focus {
				return -1
			}
			return 1
		}
		if a.entry.LastActivity != b.entry.LastActivity {
			if a.entry.LastActivity > b.entry.LastActivity {
				return -1
			}
			return 1
		}
		return strings.Compare(string(a.ch), string(b.ch))
	})
	entries := make([]subjectgate.HistoryMetaEntry, 0, len(collected))
	for _, item := range collected {
		entries = append(entries, item.entry)
		if item.entry.ErrorCode != "" {
			s.gw.logger.Warn("gateway.history.attach_failed",
				"channel", string(item.ch),
				"code", item.entry.ErrorCode,
				"detail", item.entry.ErrorDetail,
			)
			continue
		}
		s.lane.cursor.anchor(item.ch, item.entry.HeadSeq)
	}
	return entries
}

// beginFeed is the SESSION half of泵登记 (统一会话闸, 五轮 P1-3): closed, 泵登记, and
// 递交许可 must all be gated by the SAME lock consulting the SAME closed flag. Before
// this fix beginFeed only checked the gateway's g.closed (under g.mu) — a session that
// had ALREADY been Close()d directly (s.closed=true under s.mu), while the gateway
// itself was still open, would still pass that check and register a pump for a dead
// session. Checking s.closed here, under s.mu, closes that gap: Close() sets s.closed
// under this same lock, so a beginFeed racing a Close either observes closed=true
// (refuse) or completes registration before Close can set it (registered, and Close's
// later s.Close() cancels the session ctx, unblocking the pump the normal way).
func (s *Session) beginFeed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if !s.gw.tryRegisterPump() {
		return false
	}
	return true
}

// Send serializes one downstream frame and queues it on the lane. A temporarily full
// lane applies backpressure; a closed lane tears the session down.
func (s *Session) Send(f subjectgate.Frame) {
	b, err := f.Marshal()
	if err != nil {
		return
	}
	if !s.lane.push(b) {
		s.Close()
	}
}

// SendBackfill serializes one historical frame onto the bounded low-priority lane.
func (s *Session) SendBackfill(f subjectgate.Frame) {
	b, err := f.Marshal()
	if err != nil {
		return
	}
	if !s.lane.pushBackfill(b) {
		s.Close()
	}
}

// SendHistory serializes one on-demand history page frame. It is below live
// traffic but above attach replay: a person's upward read must not queue behind
// unrelated channels being hydrated in the background.
func (s *Session) SendHistory(f subjectgate.Frame) {
	b, err := f.Marshal()
	if err != nil {
		return
	}
	if !s.lane.pushHistory(b) {
		s.Close()
	}
}

// LiveDown, HistoryDown and BackfillDown are the writer inputs. The connector performs
// strict-priority selection at every frame boundary.
func (s *Session) LiveDown() <-chan []byte     { return s.lane.live }
func (s *Session) HistoryDown() <-chan []byte  { return s.lane.history }
func (s *Session) BackfillDown() <-chan []byte { return s.lane.backfill }

// Down remains the live face for non-history gateway harnesses.
func (s *Session) Down() <-chan []byte { return s.lane.live }

// Done closes when the session is torn down (the connector unblocks its reader off
// this).
func (s *Session) Done() <-chan struct{} { return s.lane.closed }

// Close tears the session down:置闩 (统一会话闸), cancel the session ctx (unblocks a
// 已获准 blocking delivery + the read pump), close the lane, and drop the device
// (末出 → 踢在场圈). Idempotent.
func (s *Session) Close() {
	s.onceClose.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
		s.lane.close()
		s.gw.removeDevice(s.principal, s)
	})
}

// markDirty flags the pump to re-resolve subscriptions and wakes it (无丢边: Store
// before the非阻塞 wake; the pump's Swap re-arms if a poke lands mid-收敛).
func (s *Session) markDirty() {
	s.dirty.Store(true)
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// beginDeliver acquires a delivery permit (统一会话闸): under the session lock, refuse
// if closed, else Add to the gateway's递交 counter. Because the Add is under the same
// lock that Close sets closed with, and Close waits the counter only AFTER every
// session is closed, no Add ever races the Wait.
func (s *Session) beginDeliver() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.gw.delivering.Add(1)
	return true
}

func (s *Session) endDeliver() { s.gw.delivering.Done() }

func (s *Session) teardownSubs() {
	for ch, sub := range s.subs {
		sub.cancel()
		delete(s.subs, ch)
	}
	s.Close()
	// Runtime history reads are detached from the live pump so they can never
	// stall feed delivery. Session cancellation releases them; joining here keeps
	// their lifetime inside the pump registration owned by Gateway.Close.
	s.historyWG.Wait()
	// Pump registration retires LAST. Gateway.Close's join covers all teardown.
	s.gw.unregisterPump()
}

// runFeed is the read pump AND the entitlement reconcile loop (spec §3.2 收敛对象甲):
// resolve → converge subscriptions → pump each active channel one batch (round-robin,
// 公平) → 积压续跑 or wait on ctx/wake/sweep/Home-signals. On exit it closes every
// subscription, the lane, untracks the pump, and closes the session.
func (s *Session) runFeed() {
	defer s.teardownSubs()

	s.dirty.Store(true) // initial-dirty
	var sweep timer
	resetSweep := func() {
		if sweep != nil {
			sweep.Stop()
		}
		sweep = s.gw.clock.NewTimer(s.nextSweepDeadline())
	}
	resetSweep()
	defer func() {
		if sweep != nil {
			sweep.Stop()
		}
	}()
	rot := 0

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		// Control commands are drained at every loop head, including the hot
		// full-batch path that never reaches wait. This prevents observe receipts
		// from starving behind an indefinitely busy member feed.
		for {
			select {
			case command := <-s.controls:
				s.handleControl(command)
			default:
				goto controlsDrained
			}
		}
	controlsDrained:
		// 每批资格窄窗复核 (spec §2.1 #18/§3.2, P0-1): a poke or the periodic sweep must
		// still be OBSERVED even while the busy→continue path below keeps this loop
		// runnable indefinitely (a hot channel that returns a full feedBatch every poll
		// never reaches wait() — that is the only place the old code drained wake/
		// sweep.C). Without this non-blocking drain here, a revoked channel with a
		// sustained backlog would stream past its revocation with no bound: dirty never
		// gets set (no wait() to catch the poke) and the expired tSweep timer just sits
		// unread. Draining both here, every iteration, means a lost/delayed poke and the
		// sweep backstop both still force a re-resolve before the next batch — this IS
		// the promotion of 每批复核 from "leak backstop" to "撤销停流正门".
		select {
		case <-s.wake:
			s.dirty.Store(true)
		default:
		}
		select {
		case <-sweep.C():
			s.dirty.Store(true)
		default:
		}
		if s.dirty.Swap(false) {
			s.reconcile()
			resetSweep()
		}
		// Pump every active (non-paused) subscription one batch, round-robin fair.
		chans := s.activeChannels()
		busy := false
		if n := len(chans); n > 0 {
			for i := 0; i < n; i++ {
				ch := chans[(rot+i)%n]
				sub := s.subs[ch]
				if sub == nil || sub.paused {
					continue
				}
				full, ok := s.pumpChannel(ch, sub)
				if !ok {
					return // lane closed → stop the pump
				}
				if full {
					busy = true
				}
			}
			rot = (rot + 1) % n
		}
		if busy {
			continue // 积压续跑: stay runnable, next轮 continues the rotation
		}
		if !s.wait(sweep) {
			return
		}
	}
}

// activeChannels returns the current subscription channel ids (pump-owned).
func (s *Session) activeChannels() []channel.ID {
	out := make([]channel.ID, 0, len(s.subs))
	for ch := range s.subs {
		out = append(out, ch)
	}
	return out
}

// wait blocks the pump on ctx / wake(poke) / sweep / any Home commit signal (spec
// §3.2: 泵相位零新 goroutine — one reflect.Select over the dynamic subscription set).
// Returns false on ctx cancel. wake/sweep re-arm dirty (re-resolve); a bare Home
// signal just proceeds to pump.
func (s *Session) wait(sweep timer) bool {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.ctx.Done())},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.wake)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sweep.C())},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.controls)},
	}
	subs := s.activeChannels()
	for _, ch := range subs {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.subs[ch].notify)})
	}
	chosen, received, _ := reflect.Select(cases)
	switch chosen {
	case 0:
		return false // ctx done
	case 1:
		s.dirty.Store(true) // poke
	case 2:
		s.dirty.Store(true) // periodic sweep → re-resolve
	case 3:
		// reflect.Select consumed the command, so execute it here; commands
		// arriving on a hot loop are handled by the loop-head drain above.
		s.handleControl(received.Interface().(controlCommand))
	default:
		// a Home commit signal → pump (dirty untouched)
	}
	return true
}

// nextSweepDeadline returns the absolute next entitlement check. A successful route
// is never allowed to sleep past its own lastOK+T_stale: if the resolver has failed by
// that boundary the timer-driven reconcile pauses it at the boundary, even when a
// regular T_sweep fired an instant before it or T_sweep is configured larger.
func (s *Session) nextSweepDeadline() time.Time {
	now := s.gw.clock.Now()
	next := now.Add(s.gw.tSweep)
	for _, sub := range s.subs {
		if sub.paused {
			continue
		}
		leaseDeadline := sub.lastOK.Add(s.gw.tStale)
		if leaseDeadline.Before(next) {
			next = leaseDeadline
		}
	}
	return next
}

// reconcile resolves this principal's entitlement and converges the subscription set
// (spec §3.2 收敛对象甲 differences: 缺→订+补流 / 多→退订 / 换值→换订; failed→T_stale
// lease; confirmed-absent→退订 immediately). Then it publishes the资格账 for Upstream.
func (s *Session) reconcile() {
	rctx, cancel := context.WithTimeout(s.ctx, s.gw.tRead)
	routes, failed, err := s.gw.resolver.Snapshot(rctx, s.principal)
	cancel()
	// Defend against a faulty resolver returning after Close. Once the session lifetime
	// is canceled, never create/refresh subscriptions from that stale return;
	// StartFeed's post-reconcile latch check will retire the pump registration.
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return
	}
	// Anchor success/failure at CHECK COMPLETION, not before the resolver call. A
	// resolver begun just inside the lease may return after lastOK+T_stale; using its
	// start time would incorrectly permit one more batch after the true deadline.
	now := s.gw.clock.Now()

	if err != nil {
		// whole-snapshot failure → the entire prior snapshot rides its lease; any sub
		// past T_stale from its lastOK is paused. Never退订 on a transient blip. globalErr
		// = true tells Upstream that an UNKNOWN channel_id (never subscribed) must also
		// map to unavailable, not forbidden — we have confirmed nothing this round.
		for ch, sub := range s.subs {
			if sub.temporary {
				s.refreshObservation(ch, sub, now)
				continue
			}
			s.leaseOrPause(sub, now)
		}
		s.publishElig(true, nil)
		return
	}

	desired := make(map[channel.ID]Route, len(routes))
	for _, r := range routes {
		desired[r.Channel] = r
	}
	failedSet := make(map[channel.ID]struct{}, len(failed))
	for _, ch := range failed {
		failedSet[ch] = struct{}{}
	}

	// additions + home rebind + confirmed-live refresh.
	for ch, r := range desired {
		sub := s.subs[ch]
		switch {
		case sub == nil:
			s.subscribeMember(ch, r, now)
		case sub.temporary:
			s.endObservation(ch, subjectgate.ObserveEndedNowMember)
			s.subscribeMember(ch, r, now)
		case sub.route.Bundle.Generation() != r.Bundle.Generation():
			// Home 换代 (频道删后重开): 退旧订新 (do not assume the old notify chan closes).
			sub.cancel()
			s.subscribeMember(ch, r, now)
		default:
			if sub.paused {
				s.gw.logger.Info("gateway.entitlement.resumed", "channel", string(ch))
			}
			sub.route = r
			sub.lastOK = now
			sub.paused = false
		}
	}
	// removals: subscribed but not desired.
	for ch, sub := range s.subs {
		if sub.temporary {
			continue
		}
		if _, ok := desired[ch]; ok {
			continue
		}
		if _, isFailed := failedSet[ch]; isFailed {
			s.leaseOrPause(sub, now) // 查得坏消息 → lease
			continue
		}
		// confirmed no eligibility (absent from BOTH routes and failed) → 立即退订.
		sub.cancel()
		delete(s.subs, ch)
	}
	// Temporary observations use the same periodic/poke reconcile discipline
	// as membership subscriptions, but are re-evaluated through the injected
	// observer policy and never published into upstream write eligibility.
	for ch, sub := range s.subs {
		if !sub.temporary {
			continue
		}
		s.refreshObservation(ch, sub, now)
	}
	s.publishElig(false, failedSet)
}

// leaseOrPause keeps a failed channel served within T_stale of its last SUCCESSFUL
// check, then pauses it (streaming stops + telemetry) — the sub stays so a later
// success can resume it.
func (s *Session) leaseOrPause(sub *subscription, now time.Time) {
	if now.Before(sub.lastOK.Add(s.gw.tStale)) {
		return // within lease: keep serving from last good
	}
	if !sub.paused {
		sub.paused = true
		s.gw.logger.Info("gateway.entitlement.paused", "channel", string(sub.route.Channel), "reason", "lease_expired")
	}
}

// subscribe registers a Home commit subscription for ch and records the sub. Backfill
// happens in the pump loop (cursor at ch, default 0).
func (s *Session) subscribeMember(ch channel.ID, r Route, now time.Time) {
	notify, cancel := r.Bundle.Gateway().Subscribe()
	s.subs[ch] = &subscription{
		route: r, reader: Reader{ActorID: r.SubjectID, Mode: ReaderMember},
		notify: notify, cancel: cancel, lastOK: now,
	}
}

func (s *Session) handleControl(command controlCommand) {
	result := controlResult{}
	switch command.kind {
	case controlObserve:
		if sub := s.subs[command.channel]; sub != nil {
			if !sub.temporary {
				result.code = subjectgate.CodeNowMember
				result.detail = "members already receive this channel"
			}
			break
		}
		route, code, detail := s.resolveObservation(command.channel)
		if code != "" {
			result.code, result.detail = code, detail
			break
		}
		notify, cancel := route.Bundle.Gateway().Subscribe()
		s.subs[command.channel] = &subscription{
			route: Route{Channel: route.Channel, Bundle: route.Bundle}, reader: route.Reader,
			temporary: true, notify: notify, cancel: cancel, lastOK: s.gw.clock.Now(),
		}
	case controlUnobserve:
		if sub := s.subs[command.channel]; sub != nil && sub.temporary {
			sub.cancel()
			delete(s.subs, command.channel)
		}
	case controlHistoryBefore:
		sub := s.subs[command.channel]
		if sub == nil {
			result.code, result.detail = s.readEligibilityError(command.channel)
			break
		}
		if sub.paused {
			result.code, result.detail = subjectgate.CodeUnavailable, "channel eligibility unavailable — retry"
			break
		}
		if sub.temporary {
			s.refreshObservation(command.channel, sub, s.gw.clock.Now())
			sub = s.subs[command.channel]
			if sub == nil || !sub.temporary {
				result.code, result.detail = subjectgate.CodeUnavailable, "observation unavailable — retry"
				break
			}
		}
		if command.generation != s.generation {
			result.code, result.detail = subjectgate.CodeUnavailable, "stale connection generation"
			break
		}
		// Admission is connection-wide and channel-scoped. The actual read remains
		// asynchronous, but no connection can retain more than four View reads and
		// no channel can race two beforeSeq cursors.
		s.historyMu.Lock()
		if s.historyCount >= maxHistoryInflight {
			result.code, result.detail = subjectgate.CodeUnavailable, "history batch capacity reached — retry"
		} else if command.priority == "background" && s.historyBackgroundCount >= maxBackgroundHistory {
			result.code, result.detail = subjectgate.CodeUnavailable, "background history capacity reached — retry"
		} else if _, exists := s.historyInflight[command.channel]; exists {
			result.code, result.detail = subjectgate.CodeUnavailable, "history batch already in flight for channel"
		} else {
			readCtx, cancel := context.WithCancel(s.ctx)
			s.historyInflight[command.channel] = historyRequest{ref: command.ref, priority: command.priority, cancel: cancel}
			s.historyCount++
			if command.priority == "background" {
				s.historyBackgroundCount++
			}
			command.readCtx = readCtx
		}
		s.historyMu.Unlock()
		if result.code != "" {
			break
		}
		// The pump owns subscription/eligibility state, so it snapshots the
		// authorized route here. The potentially slow historical read runs outside
		// the pump; live commit notifications and feed batches remain runnable.
		route, reader := sub.route, sub.reader
		s.historyWG.Add(1)
		go func(route Route, reader Reader) {
			defer s.historyWG.Done()
			defer s.releaseHistory(command.channel, command.ref)
			if command.start != nil {
				select {
				case <-command.start:
				case <-s.ctx.Done():
					return
				}
			}
			s.readHistory(command.readCtx, command, route, reader)
		}(route, reader)
		result.accepted = true
	case controlHistoryCancel:
		if command.generation != s.generation {
			result.code, result.detail = subjectgate.CodeUnavailable, "stale connection generation"
			break
		}
		s.historyMu.Lock()
		request, exists := s.historyInflight[command.channel]
		if exists && request.ref == command.targetRef {
			request.cancel()
		}
		s.historyMu.Unlock()
		// Cancellation is idempotent. Completion may have won the race, which is
		// already the desired terminal state and must not turn cancel into an error.
		result.accepted = true
	default:
		result.code, result.detail = subjectgate.CodeBadPayload, "unknown session control command"
	}
	s.replyControl(command, result)
}

func (s *Session) releaseHistory(ch channel.ID, ref string) {
	s.historyMu.Lock()
	defer s.historyMu.Unlock()
	request, exists := s.historyInflight[ch]
	if !exists || request.ref != ref {
		return
	}
	request.cancel()
	delete(s.historyInflight, ch)
	s.historyCount--
	if request.priority == "background" {
		s.historyBackgroundCount--
	}
}

func (s *Session) readHistory(baseCtx context.Context, command controlCommand, route Route, reader Reader) {
	started := time.Now()
	rctx, cancel := context.WithTimeout(baseCtx, s.gw.tRead)
	defer cancel()
	if reader.Mode == ReaderMember {
		active, err := route.Bundle.View().IsActive(rctx, reader.ActorID)
		if err != nil {
			s.sendHistoryEnd(command, subjectgate.PageEndPayload{ErrorCode: subjectgate.CodeUnavailable, ErrorDetail: "channel eligibility unavailable — retry"})
			return
		}
		if !active {
			s.sendHistoryEnd(command, subjectgate.PageEndPayload{ErrorCode: subjectgate.CodeForbidden, ErrorDetail: "no eligibility for channel"})
			return
		}
	}
	// Every historical surface uses the same projection. Live feed remains raw,
	// but attach, upward paging and system.log.recent all hide housekeeping and
	// collapse provisional progress identically: completed turns keep their
	// terminal; an open turn keeps only its latest provisional. A single complete
	// root is enough here because the background reservoir, unlike the first
	// screen, is governed primarily by its bounded row waterline.
	minimumRoots := 1
	if command.purpose == "initial-tail" {
		// First paint must contain enough complete conversations to be useful for
		// channels with many tiny turns. Later hydration is governed by the bounded
		// row/byte reservoir and only needs one semantic root boundary.
		minimumRoots = 20
	}
	window, err := route.Bundle.View().ReadVisibleTurnWindowBeforeSeq(rctx, channelspec.HistoryWindowQuery{
		BeforeSeq: command.before, TargetRows: command.limit, MinimumCompleteRoots: minimumRoots,
	})
	if err != nil {
		if errors.Is(rctx.Err(), context.Canceled) {
			return
		}
		s.gw.logger.Warn("gateway.history.read_failed", "channel", string(command.channel), "ref", command.ref, "duration", time.Since(started), "error", err)
		s.sendHistoryEnd(command, subjectgate.PageEndPayload{ErrorCode: subjectgate.CodeUnavailable, ErrorDetail: "history unavailable — retry"})
		return
	}
	readAt := time.Now()
	s.gw.logger.Info("gateway.history.read", "channel", string(command.channel), "ref", command.ref, "before_seq", command.before, "limit", command.limit, "rows", len(window.Rows), "duration", readAt.Sub(started))
	rows := window.Rows
	if len(rows) > command.limit {
		rows = rows[len(rows)-command.limit:]
	}
	// Select a bounded suffix by encoded bytes as well as row count. Selecting
	// from newest to oldest preserves a contiguous cursor range; anything omitted
	// is fetched by the next batch through NextBeforeSeq.
	encoded := make([]boundedFeedResult, 0, len(rows))
	encodedBytes := 0
	for index := len(rows) - 1; index >= 0; index-- {
		row := rows[index]
		feed, feedErr := buildBoundedFeed(command.ref, command.channel, row.Seq, "history", command.generation, row.Envelope)
		if feedErr != nil {
			s.gw.logger.Error("gateway.history.row_untransportable", "channel", string(command.channel), "seq", row.Seq, "error", feedErr)
			s.sendHistoryEnd(command, subjectgate.PageEndPayload{ErrorCode: subjectgate.CodeUnavailable, ErrorDetail: "history row exceeds transport limit"})
			return
		}
		if feed.projected {
			s.gw.logger.Warn("gateway.history.row_projected", "channel", string(command.channel), "seq", row.Seq, "original_bytes", feed.originalBytes, "projected_bytes", len(feed.envelope))
		}
		rowBytes := len(feed.envelope)
		if encodedBytes > 0 && encodedBytes+rowBytes > command.byteLimit {
			break
		}
		encoded = append(encoded, feed)
		encodedBytes += rowBytes
	}
	slices.Reverse(encoded)
	nextBefore := window.OldestSeq
	oldest, newest := int64(0), int64(0)
	if len(encoded) > 0 {
		oldest, newest = encoded[0].seq, encoded[len(encoded)-1].seq
		if len(encoded) < len(window.Rows) {
			nextBefore = oldest
		}
	}
	end := subjectgate.PageEndPayload{
		Source: "history", Purpose: command.purpose, Generation: command.generation,
		ChannelID: string(command.channel), HeadSeq: window.HeadSeq,
		OldestSeq: oldest, NewestSeq: newest, ScanLowSeq: nextBefore,
		ScanHighSeq: historyScanHigh(command.before, window.HeadSeq), NextBeforeSeq: nextBefore,
		Rows: len(encoded), Bytes: encodedBytes, HasOlder: window.HasOlder || nextBefore > window.OldestSeq,
	}
	for _, row := range encoded {
		if rctx.Err() != nil {
			return
		}
		s.sendHistoryFrame(command, row.frame)
	}
	s.sendHistoryEnd(command, end)
	s.gw.logger.Info("gateway.history.queued", "channel", string(command.channel), "ref", command.ref, "rows", len(window.Rows), "queue_duration", time.Since(readAt), "total_duration", time.Since(started))
}

func (s *Session) sendHistoryEnd(command controlCommand, payload subjectgate.PageEndPayload) {
	payload.Source = "history"
	payload.ChannelID = string(command.channel)
	payload.Purpose = command.purpose
	payload.Generation = command.generation
	frame, err := subjectgate.NewFrame(subjectgate.FramePageEnd, command.ref, payload)
	if err == nil {
		s.sendHistoryFrame(command, frame)
	}
}

func (s *Session) sendHistoryFrame(command controlCommand, frame subjectgate.Frame) {
	if command.priority == "foreground" {
		s.SendHistory(frame)
		return
	}
	s.SendBackfill(frame)
}

func historyScanHigh(before, head int64) int64 {
	if before > 0 {
		return before - 1
	}
	return head
}

func (s *Session) replyControl(command controlCommand, result controlResult) {
	select {
	case command.reply <- result:
	case <-s.ctx.Done():
	}
}

func (s *Session) readEligibilityError(ch channel.ID) (string, string) {
	st := s.elig.Load()
	if st != nil {
		if _, ok := st.paused[ch]; ok {
			return subjectgate.CodeUnavailable, "channel eligibility unavailable — retry"
		}
		if _, ok := st.failed[ch]; ok || st.globalErr {
			return subjectgate.CodeUnavailable, "channel eligibility unavailable — retry"
		}
	}
	return subjectgate.CodeForbidden, "no eligibility for channel"
}

func normalizeHistoryLimit(limit int) int {
	if limit <= 0 {
		return defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		return maxHistoryLimit
	}
	return limit
}

func normalizeHistoryByteLimit(limit int) int {
	if limit <= 0 || limit > maxHistoryBytes {
		return maxHistoryBytes
	}
	return limit
}

func validHistoryPurpose(purpose string) bool {
	return purpose == "initial-tail" || purpose == "user-demand" || purpose == "hydrate"
}

func validHistoryPriority(priority string) bool {
	return priority == "foreground" || priority == "background"
}

func (s *Session) resolveObservation(ch channel.ID) (ObserverRoute, string, string) {
	if s.gw.observer == nil {
		return ObserverRoute{}, subjectgate.CodeCapabilityUnavailable, "observation resolver unavailable"
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.gw.tRead)
	defer cancel()
	route, reason, err := s.gw.observer.ResolveObservation(ctx, s.principal, ch)
	if err != nil {
		return ObserverRoute{}, subjectgate.CodeChannelUnavailable, err.Error()
	}
	if reason != "" {
		code := normalizeObservationCode(reason)
		return ObserverRoute{}, code, "observation refused: " + code
	}
	if route.Channel != ch || route.Bundle == nil || route.Reader.Mode != ReaderObserver {
		return ObserverRoute{}, subjectgate.CodeCapabilityUnavailable, "invalid observation route"
	}
	return route, "", ""
}

func (s *Session) refreshObservation(ch channel.ID, sub *subscription, now time.Time) {
	route, code, _ := s.resolveObservation(ch)
	if code != "" {
		s.endObservation(ch, observeEndedReason(code))
		return
	}
	if sub.route.Bundle.Generation() != route.Bundle.Generation() {
		sub.cancel()
		notify, cancel := route.Bundle.Gateway().Subscribe()
		sub.notify, sub.cancel = notify, cancel
	}
	sub.route = Route{Channel: ch, Bundle: route.Bundle}
	sub.reader = route.Reader
	sub.lastOK = now
}

func normalizeObservationCode(code string) string {
	switch code {
	case subjectgate.CodeNowMember, subjectgate.CodeChannelNotFound,
		subjectgate.CodeChannelUnavailable, subjectgate.CodeCapabilityUnavailable:
		return code
	default:
		return subjectgate.CodeCapabilityUnavailable
	}
}

func observeEndedReason(code string) subjectgate.ObserveEndedReason {
	switch normalizeObservationCode(code) {
	case subjectgate.CodeNowMember:
		return subjectgate.ObserveEndedNowMember
	case subjectgate.CodeChannelNotFound:
		return subjectgate.ObserveEndedChannelRetired
	case subjectgate.CodeChannelUnavailable:
		return subjectgate.ObserveEndedChannelUnavailable
	default:
		return subjectgate.ObserveEndedCapabilityUnavailable
	}
}

func (s *Session) endObservation(ch channel.ID, reason subjectgate.ObserveEndedReason) {
	sub := s.subs[ch]
	if sub == nil || !sub.temporary {
		return
	}
	sub.cancel()
	delete(s.subs, ch)
	frame, err := subjectgate.NewFrame(subjectgate.FrameObserveEnded, "", subjectgate.ObserveEndedPayload{
		ChannelID: string(ch), Reason: reason,
	})
	if err == nil {
		s.Send(frame)
	}
}

// publishElig snapshots the资格账 for Upstream (atomic single-write). Non-paused subs
// are eligible routes; paused subs go into the paused set (business frame →
// unavailable). A confirmed-absent channel is in neither (→ forbidden) UNLESS it is
// also in failedThisRound (a per-channel failure with no subscription yet — 六轮 P1-2)
// or globalErr is set (a whole-snapshot failure — nothing is confirmed this round),
// either of which must map to unavailable, not forbidden (表①: 查得坏消息 ≠ 查不到).
func (s *Session) publishElig(globalErr bool, failedThisRound map[channel.ID]struct{}) {
	routes := make(map[channel.ID]Route, len(s.subs))
	paused := map[channel.ID]struct{}{}
	for ch, sub := range s.subs {
		if sub.temporary {
			continue
		}
		if sub.paused {
			paused[ch] = struct{}{}
			continue
		}
		routes[ch] = sub.route
	}
	s.elig.Store(&eligState{routes: routes, paused: paused, failed: failedThisRound, globalErr: globalErr, resolved: true})
}

// pumpChannel drains up to feedBatch rows after ch's cursor into the lane as feed
// frames. Returns (full, ok): full = read a whole batch (积压续跑 → stay runnable);
// ok=false = lane closed. Only a feed frame's own seq (a READ position) moves
// the cursor; a submit receipt carries no seq to confuse it with.
func (s *Session) pumpChannel(ch channel.ID, sub *subscription) (full, ok bool) {
	if sub.temporary {
		s.refreshObservation(ch, sub, s.gw.clock.Now())
		current := s.subs[ch]
		if current == nil || !current.temporary {
			return false, true
		}
		sub = current
	}
	rctx, cancel := context.WithTimeout(s.ctx, s.gw.tRead)
	defer cancel()
	at := s.lane.cursor.at(ch)
	if sub.reader.Mode == ReaderMember {
		active, err := sub.route.Bundle.View().IsActive(rctx, sub.reader.ActorID)
		if err != nil || !active {
			return false, true
		}
	}
	rows, scanned, err := sub.route.Bundle.View().ReadVisibleAfterSeq(rctx, at, feedBatch)
	if err != nil || (len(rows) == 0 && scanned == at) {
		return false, true
	}
	for _, r := range rows {
		feed, feedErr := buildBoundedFeed("", ch, r.Seq, "live", s.generation, r.Envelope)
		if feedErr != nil {
			// Never publish a checkpoint across a visible row the transport did
			// not publish. Keeping the cursor before this row makes the next pump
			// retry it; advancing would turn an encoding failure into false cache
			// coverage on the browser.
			s.gw.logger.Error("gateway.live.row_untransportable", "channel", string(ch), "seq", r.Seq, "error", feedErr)
			return false, true
		}
		if feed.projected {
			s.gw.logger.Warn("gateway.live.row_projected", "channel", string(ch), "seq", r.Seq, "original_bytes", feed.originalBytes, "projected_bytes", len(feed.envelope))
		}
		if !s.lane.push(feed.encoded) {
			return false, false // Session/lane closed
		}
		s.lane.cursor.advance(ch, r.Seq)
	}
	if scanned > at {
		checkpoint, frameErr := subjectgate.NewFrame(subjectgate.FrameCheckpoint, "", subjectgate.CheckpointPayload{
			ChannelID: string(ch), ScanLowSeq: at + 1, ScannedSeq: scanned, Generation: s.generation,
		})
		if frameErr != nil {
			return false, true
		}
		encoded, marshalErr := checkpoint.Marshal()
		if marshalErr != nil {
			return false, true
		}
		if !s.lane.push(encoded) {
			return false, false
		}
		s.lane.cursor.advance(ch, scanned)
	}
	return len(rows) == feedBatch, true
}

// Upstream drives one parsed upstream business frame onto the channel's subject cell
// (through that channel's slot) and returns the receipt-or-error frame the connector
// writes back (spec §3.1/§3.2). The connection is channel-blind: the frame's payload
// channel_id names the channel; the gateway resolves it against the资格账 (member →
// deliver; observer/absent → forbidden; lease-expired → unavailable) and looks up the
// slot现场. The detach frame is整删 (no case). Error mapping照表①.
func (s *Session) Upstream(f subjectgate.Frame) subjectgate.Frame {
	return s.upstream(f, nil)
}

// Dispatch is the connector-facing upstream path. History is two-phase:
// validate/accept, publish the acceptance on the realtime lane, then release
// the asynchronous read. This makes `receipt → rows → page_end` structural;
// it does not depend on goroutine scheduling or the writer happening to notice
// the high-priority lane in time.
func (s *Session) Dispatch(f subjectgate.Frame) {
	if f.Type != subjectgate.FrameHistoryBefore {
		s.Send(s.Upstream(f))
		return
	}
	start := make(chan struct{})
	response := s.upstream(f, start)
	s.Send(response)
	close(start)
}

func (s *Session) upstream(f subjectgate.Frame, historyStart <-chan struct{}) subjectgate.Frame {
	errFrame := func(code, detail string) subjectgate.Frame {
		return subjectgate.NewErrorFrame(f.Ref, string(f.Type), code, detail)
	}
	switch f.Type {
	case subjectgate.FrameAttach:
		return errFrame(subjectgate.CodeBadPayload, "attach is the opening frame, not a mid-stream verb")
	case subjectgate.FrameObserve, subjectgate.FrameUnobserve, subjectgate.FrameHistoryBefore, subjectgate.FrameHistoryCancel:
		chID, derr := channelIDOf(f)
		if derr != nil || subjectgate.RequireChannelID(chID) != nil {
			return errFrame(subjectgate.CodeBadPayload, "missing required channel_id")
		}
		kind := controlObserve
		command := controlCommand{channel: channel.ID(chID)}
		if f.Type == subjectgate.FrameUnobserve {
			kind = controlUnobserve
		} else if f.Type == subjectgate.FrameHistoryCancel {
			kind = controlHistoryCancel
			var payload subjectgate.HistoryCancelPayload
			if err := f.DecodePayload(&payload); err != nil || payload.Generation == 0 || payload.TargetRef == "" {
				return errFrame(subjectgate.CodeBadPayload, "history cancel requires a positive generation and target_ref")
			}
			command.generation = payload.Generation
			command.targetRef = payload.TargetRef
		} else if f.Type == subjectgate.FrameHistoryBefore {
			kind = controlHistoryBefore
			var payload subjectgate.HistoryBeforePayload
			if err := f.DecodePayload(&payload); err != nil || payload.BeforeSeq < 0 || payload.Limit < 0 || payload.Limit > maxHistoryLimit || payload.ByteLimit < 0 || payload.ByteLimit > maxHistoryBytes || !validHistoryPurpose(payload.Purpose) || !validHistoryPriority(payload.Priority) || payload.Generation == 0 {
				return errFrame(subjectgate.CodeBadPayload, "history requires a positive generation, valid purpose and priority, non-negative cursor, limit <= 200 and byte_limit <= 4MiB")
			}
			command.before = payload.BeforeSeq
			command.limit = normalizeHistoryLimit(payload.Limit)
			command.byteLimit = normalizeHistoryByteLimit(payload.ByteLimit)
			command.generation = payload.Generation
			command.purpose = payload.Purpose
			command.priority = payload.Priority
			command.ref = f.Ref
			command.start = historyStart
		}
		reply := make(chan controlResult, 1)
		command.kind, command.reply = kind, reply
		select {
		case s.controls <- command:
		case <-s.ctx.Done():
			return errFrame(subjectgate.CodeClosed, "session closed")
		}
		select {
		case result := <-reply:
			if result.code != "" {
				return errFrame(result.code, result.detail)
			}
			if f.Type == subjectgate.FrameHistoryBefore {
				if !result.accepted {
					return errFrame(subjectgate.CodeUnavailable, "history unavailable — retry")
				}
				frame, err := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, subjectgate.HistoryAcceptedReceipt{
					Accepted: true, ChannelID: chID, Purpose: command.purpose, Priority: command.priority, Generation: command.generation,
				})
				if err != nil {
					return errFrame(subjectgate.CodeUnavailable, "history receipt unavailable")
				}
				return frame
			}
			if f.Type == subjectgate.FrameHistoryCancel {
				frame, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, subjectgate.HistoryCancelReceipt{
					ChannelID: chID, TargetRef: command.targetRef, Generation: command.generation,
				})
				return frame
			}
			if f.Type == subjectgate.FrameObserve {
				frame, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, subjectgate.ObserveReceipt{ChannelID: chID})
				return frame
			}
			frame, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, subjectgate.UnobserveReceipt{ChannelID: chID})
			return frame
		case <-s.ctx.Done():
			return errFrame(subjectgate.CodeClosed, "session closed")
		}
	case subjectgate.FrameSubmit, subjectgate.FrameResolve, subjectgate.FrameCancel,
		subjectgate.FrameAfter, subjectgate.FrameCancelTimer, subjectgate.FrameResource:
		// channel_id is required on every business frame, file resources
		// included: it says which channel the request is being made *in*, which
		// is a fact about the request and not about the object it names. A file
		// address separately says where the bytes live; the two are compared by
		// that channel's own door, which refuses an address naming anywhere else.
		st := s.elig.Load()
		chID, derr := channelIDOf(f)
		if derr != nil {
			return errFrame(subjectgate.CodeBadPayload, derr.Error())
		}
		if err := subjectgate.RequireChannelID(chID); err != nil {
			return errFrame(subjectgate.CodeBadPayload, "missing required channel_id")
		}
		cid := channel.ID(chID)
		r, ok := st.routes[cid]
		if !ok {
			if _, paused := st.paused[cid]; paused {
				return errFrame(subjectgate.CodeUnavailable, "channel eligibility unavailable — retry")
			}
			if _, failed := st.failed[cid]; failed {
				// 查得坏消息 for this channel THIS round, no prior subscription to lease
				// from (六轮 P1-2) — unavailable, not a confirmed-absence forbidden.
				return errFrame(subjectgate.CodeUnavailable, "channel eligibility unavailable — retry")
			}
			if st.globalErr {
				// The last resolver call was a whole-snapshot failure: nothing is
				// confirmed this round, so an unknown/never-subscribed channel_id must
				// not be told forbidden (that asserts confirmed absence).
				return errFrame(subjectgate.CodeUnavailable, "channel eligibility unavailable — retry")
			}
			return errFrame(subjectgate.CodeForbidden, "no eligibility for channel")
		}
		// Delivery permit (统一会话闸): a已获准 delivery blocks Close's排水 counter.
		if !s.beginDeliver() {
			return errFrame(subjectgate.CodeClosed, "session closed")
		}
		defer s.endDeliver()

		slot, ok := r.Bundle.Gateway().SubjectSlotFor(r.SubjectID)
		if !ok {
			return errFrame(subjectgate.CodeUnavailable, "subject cell unavailable — retry")
		}
		// Deliver derives from the SESSION ctx (not the HTTP request ctx) so Close's
		// cancel reaches a blocked delivery (排水归零). The bindingGen gate is gone (no
		// client-visible binding axis).
		res, derr := slot.Deliver(s.ctx, f)
		if derr != nil {
			if errors.Is(derr, subjectgate.ErrNoOccupant) {
				return errFrame(subjectgate.CodeUnavailable, "subject cell unavailable — retry")
			}
			if errors.Is(derr, context.Canceled) || errors.Is(derr, context.DeadlineExceeded) {
				return errFrame(subjectgate.CodeClosed, "session closed")
			}
			return errFrame(subjectgate.CodeUnavailable, "delivery unavailable — retry")
		}
		return res.Frame
	default:
		return errFrame(subjectgate.CodeBadPayload, "unknown frame_type: "+string(f.Type))
	}
}

// channelIDOf extracts the required channel_id from any business frame payload
// (all six carry it as `channel_id`). A payload that fails to decode → error
// (bad_payload).
func channelIDOf(f subjectgate.Frame) (string, error) {
	var p struct {
		ChannelID string `json:"channel_id"`
	}
	if err := f.DecodePayload(&p); err != nil {
		return "", err
	}
	return p.ChannelID, nil
}

// ID names this connection. Stable for its lifetime and unique across the node:
// a reconnect is a different screen as far as anything addressing it is
// concerned, because it is a different socket with its own view of the world.
func (s *Session) ID() string { return s.id }

// Label is the client's own words for itself, or "" if it did not say.
func (s *Session) Label() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.label
}

// SetLabel records what the client calls itself. Purely descriptive: it is shown
// to a person choosing between screens and is never used to address anything,
// so two clients claiming the same label is untidy rather than dangerous.
func (s *Session) SetLabel(label string) {
	s.mu.Lock()
	s.label = label
	s.mu.Unlock()
}
