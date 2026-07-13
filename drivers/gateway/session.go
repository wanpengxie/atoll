package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// defaultEpoch is the process-lifetime gateway epoch when Config.Epoch is 0.
func defaultEpoch() int64 { return time.Now().UnixNano() }

// feedBatch is the read pump's per-poll, per-channel row budget (照 ws.go wsTail 100).
const feedBatch = 100

// subscription is one channel the session's read pump is currently following (spec
// §3.2 收敛对象甲, current = 订阅集). PUMP-OWNED — created/mutated/read only by the
// runFeed goroutine, so no lock. lastOK anchors the T_stale lease; paused = lease
// expired (streaming stopped, awaiting resume).
type subscription struct {
	route  Route
	notify <-chan struct{}
	cancel func()
	lastOK time.Time
	paused bool
}

// eligState is the session's资格账 snapshot (spec §3.2 v0.8): the pump SINGLE-writes
// it (atomic pointer), Upstream reads it — so a business frame's channel_id maps to
// its Route (member? which subject? which Home?) without touching the pump-owned
// subscription map. paused = channels whose lease expired (business frame →
// unavailable, retryable). checkedAt is the last reconcile time.
type eligState struct {
	routes    map[channel.ID]Route
	paused    map[channel.ID]struct{}
	checkedAt time.Time
}

// Session is one attached connection's handle into the gateway (spec §3.2 终形):
// {lane, 游标表(lane.cursor), 订阅集(subs), dirty/wake, closed 闩, 资格账(elig), 读泵}.
// 连接即人 — a session has NO频道身份色: eligibility is a per-frame/per-batch fact.
// The connector drives it: drains Down() to the wire, feeds each parsed upstream
// frame to Upstream, and calls Close on disconnect.
type Session struct {
	gw        *Gateway
	principal string
	lane      *lane

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
	subs map[channel.ID]*subscription
	elig atomic.Pointer[eligState]

	onceClose sync.Once
}

// Attach opens a session for one authenticated connection (连接模型勘误期: the app
// membrane resolved cookie→principal — no channel ACL at connection level). It seats
// the device (首入 → 踢在场圈) and hands back the session; the attach receipt is now
// EMPTY (报到 ack — attach is no longer a binding grant). Refused after Close.
func (g *Gateway) Attach(ctx context.Context, principal string, since map[channel.ID]int64) (*Session, error) {
	s := &Session{
		gw:        g,
		principal: principal,
		lane:      newLane(newCursor(since)),
		wake:      make(chan struct{}, 1),
		subs:      map[channel.ID]*subscription{},
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.elig.Store(&eligState{routes: map[channel.ID]Route{}, paused: map[channel.ID]struct{}{}})
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
	if !s.gw.beginFeed() {
		s.Close()
		return
	}
	s.reconcile()
	go s.runFeed()
}

// Send serializes one downstream frame and queues it on the lane. A full lane (满 →
// 断连) tears the session down.
func (s *Session) Send(f subjectgate.Frame) {
	b, err := f.Marshal()
	if err != nil {
		return
	}
	if !s.lane.push(b) {
		s.Close()
	}
}

// Down is the connector's drain: the writer goroutine reads serialized downstream
// frames from here and writes them to the wire.
func (s *Session) Down() <-chan []byte { return s.lane.out }

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

// runFeed is the read pump AND the entitlement reconcile loop (spec §3.2 收敛对象甲):
// resolve → converge subscriptions → pump each active channel one batch (round-robin,
// 公平) → 积压续跑 or wait on ctx/wake/sweep/Home-signals. On exit it closes every
// subscription, the lane, untracks the pump, and closes the session.
func (s *Session) runFeed() {
	defer func() {
		for _, sub := range s.subs {
			sub.cancel()
		}
		s.gw.pumps.Done()
		s.Close()
	}()

	s.dirty.Store(true) // initial-dirty
	sweep := time.NewTimer(tSweep)
	defer sweep.Stop()
	rot := 0

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		if s.dirty.Swap(false) {
			s.reconcile()
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
					return // full lane → 断连
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
func (s *Session) wait(sweep *time.Timer) bool {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.ctx.Done())},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.wake)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sweep.C)},
	}
	subs := s.activeChannels()
	for _, ch := range subs {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.subs[ch].notify)})
	}
	chosen, _, _ := reflect.Select(cases)
	switch chosen {
	case 0:
		return false // ctx done
	case 1:
		s.dirty.Store(true) // poke
	case 2:
		sweep.Reset(tSweep)
		s.dirty.Store(true) // periodic sweep → re-resolve
	default:
		// a Home commit signal → pump (dirty untouched)
	}
	return true
}

// reconcile resolves this principal's entitlement and converges the subscription set
// (spec §3.2 收敛对象甲 differences: 缺→订+补流 / 多→退订 / 换值→换订; failed→T_stale
// lease; confirmed-absent→退订 immediately). Then it publishes the资格账 for Upstream.
func (s *Session) reconcile() {
	now := s.gw.clock()
	if s.gw.resolver == nil {
		s.publishElig(now) // no resolver → nothing eligible (every frame forbidden)
		return
	}
	rctx, cancel := context.WithTimeout(s.ctx, tRead)
	routes, failed, err := s.gw.resolver.Snapshot(rctx, s.principal)
	cancel()

	if err != nil {
		// whole-snapshot failure → the entire prior snapshot rides its lease; any sub
		// past T_stale from its lastOK is paused. Never退订 on a transient blip.
		for _, sub := range s.subs {
			s.leaseOrPause(sub, now)
		}
		s.publishElig(now)
		return
	}

	desired := make(map[channel.ID]Route, len(routes))
	for _, r := range routes {
		desired[r.Channel] = r
	}
	failedSet := make(map[channel.ID]struct{}, len(failed))
	for _, cf := range failed {
		failedSet[cf.Channel] = struct{}{}
	}

	// additions + home rebind + confirmed-live refresh.
	for ch, r := range desired {
		sub := s.subs[ch]
		switch {
		case sub == nil:
			s.subscribe(ch, r, now)
		case sub.route.Home != r.Home:
			// Home 换代 (频道删后重开): 退旧订新 (do not assume the old notify chan closes).
			sub.cancel()
			s.subscribe(ch, r, now)
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
	s.publishElig(now)
}

// leaseOrPause keeps a failed channel served within T_stale of its last SUCCESSFUL
// check, then pauses it (streaming stops + telemetry) — the sub stays so a later
// success can resume it.
func (s *Session) leaseOrPause(sub *subscription, now time.Time) {
	if now.Sub(sub.lastOK) <= tStale {
		return // within lease: keep serving from last good
	}
	if !sub.paused {
		sub.paused = true
		s.gw.logger.Info("gateway.entitlement.paused", "channel", string(sub.route.Channel), "reason", "lease_expired")
	}
}

// subscribe registers a Home commit subscription for ch and records the sub. Backfill
// happens in the pump loop (cursor at ch, default 0).
func (s *Session) subscribe(ch channel.ID, r Route, now time.Time) {
	notify, cancel := r.Home.Subscribe()
	s.subs[ch] = &subscription{route: r, notify: notify, cancel: cancel, lastOK: now}
}

// publishElig snapshots the资格账 for Upstream (atomic single-write). Non-paused subs
// are eligible routes; paused subs go into the paused set (business frame →
// unavailable). A confirmed-absent channel is in neither (→ forbidden).
func (s *Session) publishElig(now time.Time) {
	routes := make(map[channel.ID]Route, len(s.subs))
	paused := map[channel.ID]struct{}{}
	for ch, sub := range s.subs {
		if sub.paused {
			paused[ch] = struct{}{}
			continue
		}
		routes[ch] = sub.route
	}
	s.elig.Store(&eligState{routes: routes, paused: paused, checkedAt: now})
}

// pumpChannel drains up to feedBatch rows after ch's cursor into the lane as feed
// frames. Returns (full, ok): full = read a whole batch (积压续跑 → stay runnable);
// ok=false = full lane (→ 断连). receipt.seq is never folded here (write位≠读位).
func (s *Session) pumpChannel(ch channel.ID, sub *subscription) (full, ok bool) {
	rctx, cancel := context.WithTimeout(s.ctx, tRead)
	defer cancel()
	at := s.lane.cursor.at(ch)
	rows, err := sub.route.Home.View().ReadAfterSeq(rctx, at, feedBatch)
	if err != nil || len(rows) == 0 {
		return false, true
	}
	for _, r := range rows {
		env, merr := json.Marshal(r.Envelope)
		if merr != nil {
			continue
		}
		fr, ferr := subjectgate.NewFrame(subjectgate.FrameFeed, "", subjectgate.FeedPayload{
			ChannelID: string(ch),
			Seq:       r.Seq,
			Envelope:  env,
		})
		if ferr != nil {
			continue
		}
		b, berr := fr.Marshal()
		if berr != nil {
			continue
		}
		if !s.lane.push(b) {
			return false, false // full lane → 断连
		}
		s.lane.cursor.advance(ch, r.Seq)
	}
	return len(rows) == feedBatch, true
}

// Upstream drives one parsed upstream business frame onto the channel's subject cell
// (through that channel's slot) and returns the receipt-or-error frame the connector
// writes back (spec §3.1/§3.2). The connection is channel-blind: the frame's payload
// channel_id names the channel; the gateway resolves it against the资格账 (member →
// deliver; observer/absent → forbidden; lease-expired → unavailable) and looks up the
// slot现场. The detach frame is整删 (no case). Error mapping照表①.
func (s *Session) Upstream(ctx context.Context, f subjectgate.Frame) subjectgate.Frame {
	errFrame := func(code, detail string) subjectgate.Frame {
		fr, _ := subjectgate.NewFrame(subjectgate.FrameError, f.Ref, subjectgate.ErrorPayload{
			Frame: string(f.Type), Code: code, Detail: detail,
		})
		return fr
	}
	switch f.Type {
	case subjectgate.FrameAttach:
		return errFrame(subjectgate.CodeBadPayload, "attach is the opening frame, not a mid-stream verb")
	case subjectgate.FrameSubmit, subjectgate.FrameResolve, subjectgate.FrameCancel,
		subjectgate.FrameAfter, subjectgate.FrameCancelTimer, subjectgate.FrameResource:
		// channel_id is required on every business frame (连接模型勘误期 v2).
		chID, derr := channelIDOf(f)
		if derr != nil {
			return errFrame(subjectgate.CodeBadPayload, derr.Error())
		}
		if err := subjectgate.RequireChannelID(chID); err != nil {
			return errFrame(subjectgate.CodeBadPayload, "missing required channel_id")
		}
		cid := channel.ID(chID)
		st := s.elig.Load()
		r, ok := st.routes[cid]
		if !ok {
			if _, paused := st.paused[cid]; paused {
				return errFrame(subjectgate.CodeUnavailable, "channel eligibility unavailable — retry")
			}
			return errFrame(subjectgate.CodeForbidden, "no eligibility for channel")
		}
		if r.Access != AccessMember {
			return errFrame(subjectgate.CodeForbidden, "observer may not drive business frames")
		}
		// Delivery permit (统一会话闸): a已获准 delivery blocks Close's排水 counter.
		if !s.beginDeliver() {
			return errFrame(subjectgate.CodeClosed, "session closed")
		}
		defer s.endDeliver()

		if f.Type == subjectgate.FrameSubmit {
			routed, rerr := s.applyRouting(cid, f)
			if rerr != nil {
				return *rerr
			}
			f = routed
		}
		slot, ok := r.Home.SubjectSlotFor(r.SubjectID)
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

// channelIDOf extracts the required channel_id from any business frame payload (all
// six carry it as `channel_id`). A payload that fails to decode → error (bad_payload).
func channelIDOf(f subjectgate.Frame) (string, error) {
	var p struct {
		ChannelID string `json:"channel_id"`
	}
	if err := f.DecodePayload(&p); err != nil {
		return "", err
	}
	return p.ChannelID, nil
}

// applyRouting resolves an empty-audience submit through the injected app routing面
// and rewrites the payload with the concrete audience + kind (design §5.3: routing
// 政策留 app). An explicit audience is honoured as-is; a nil resolver leaves the frame
// untouched. A per-request routing condition (no reachable brain) → unavailable
// (never写黑洞). channel_id is preserved through the rewrite.
func (s *Session) applyRouting(cid channel.ID, f subjectgate.Frame) (subjectgate.Frame, *subjectgate.Frame) {
	var p subjectgate.SubmitPayload
	if err := f.DecodePayload(&p); err != nil {
		fr, _ := subjectgate.NewFrame(subjectgate.FrameError, f.Ref, subjectgate.ErrorPayload{
			Frame: string(f.Type), Code: subjectgate.CodeBadPayload, Detail: err.Error(),
		})
		return f, &fr
	}
	if len(p.Audience) > 0 || s.gw.routing == nil {
		return f, nil
	}
	aud, kind, retryable, err := s.gw.routing(s.ctx, cid, nil, message.Kind(p.Kind))
	if retryable != "" || err != nil {
		detail := retryable
		if detail == "" {
			s.gw.logger.Error("gateway.routing", "channel", string(cid), "err", err)
			detail = "routing unavailable"
		}
		fr, _ := subjectgate.NewFrame(subjectgate.FrameError, f.Ref, subjectgate.ErrorPayload{
			Frame: string(f.Type), Code: subjectgate.CodeUnavailable, Detail: detail,
		})
		return f, &fr
	}
	p.Audience = p.Audience[:0]
	for _, a := range aud {
		p.Audience = append(p.Audience, string(a))
	}
	p.Kind = string(kind)
	routed, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, f.Ref, p)
	return routed, nil
}
