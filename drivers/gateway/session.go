package gateway

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// defaultEpoch is the process-lifetime gateway epoch when Config.Epoch is 0.
func defaultEpoch() int64 { return time.Now().UnixNano() }

// feedBatch is the read pump's per-poll row budget (照 ws.go wsTail 100).
const feedBatch = 100

// Session is one attached connection's handle into the gateway session cross. The
// connector drives it: it drains Down() to the wire, feeds each parsed upstream
// frame to Upstream, and calls Close on disconnect. A member session carries a
// slot (write + presence) and a频道臂 (seal on revocation); a tail-only session
// (workspace observer, 看得见≠在里面) carries only a read pump — its业务 frames are
// refused not_member.
type Session struct {
	gw        *Gateway
	home      *platform.Home
	chID      channel.ID
	principal string
	subjectID actor.ActorID
	isMember  bool

	entry *userEntry            // nil for tail-only
	arm   *channelArm           // nil for tail-only
	slot  *platform.SubjectSlot // nil for tail-only
	lane  *lane

	ctx    context.Context
	cancel context.CancelFunc

	once sync.Once
}

// Attach opens a session for one authenticated connection (the connector calls it
// after the app membrane resolved session→principal + channel ACL). principal
// resolves to the subject; found → member (slot + presence + write); not found →
// tail-only observer. It ensures the slot (装配链 step②, before the cell ever races
// it — though the always-on cell already ensured its own, EnsureSubjectSlot is
// idempotent), advances the绑定世代, seats the device (首入 → online), and starts the
// read pump. Returns the session + the granted binding_gen for the attach receipt.
func (g *Gateway) Attach(ctx context.Context, home *platform.Home, chID channel.ID, principal string, since map[channel.ID]int64) (*Session, int64, error) {
	subjectID, found, err := home.ResolvePrincipal(ctx, actor.KindHuman, principal)
	if err != nil {
		return nil, 0, err
	}
	s := &Session{
		gw:        g,
		home:      home,
		chID:      chID,
		principal: principal,
		lane:      newLane(newCursor(since)),
	}
	var bindingGen int64
	if found {
		s.isMember = true
		s.subjectID = subjectID
		s.slot = home.EnsureSubjectSlot(subjectID)
		s.entry = g.ensureEntry(home, chID, subjectID, s.slot)
		s.arm = s.entry.arm
		bindingGen = s.arm.nextGen()
		s.ctx, s.cancel = context.WithCancel(s.arm.context())
		g.addDevice(s.entry, s)
	} else {
		// tail-only observer: its own lifecycle (no arm seal — revocation of a bare
		// observer is deferred, see build-spec申报; the read pump's per-batch recheck
		// is a member-only backstop).
		s.ctx, s.cancel = context.WithCancel(context.Background())
	}
	return s, bindingGen, nil
}

// StartFeed launches the read pump. The connector calls it AFTER sending the
// attach receipt so the receipt is not interleaved behind backfill on a cold
// channel. The arm join-track is taken HERE (paired with runFeed's untrack), so a
// seal never blocks on a pump that was counted but never launched.
func (s *Session) StartFeed() {
	if s.arm != nil {
		s.arm.track()
	}
	go s.runFeed()
}

// Send serializes one downstream frame and queues it on the lane (the single
// writer drains it). A full lane (满 → 断连) tears the session down.
func (s *Session) Send(f platform.Frame) {
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
// this — sets a past read deadline so ReadJSON errors and calls Close).
func (s *Session) Done() <-chan struct{} { return s.lane.closed }

// IsMember reports whether this session may drive business frames.
func (s *Session) IsMember() bool { return s.isMember }

// BindingGen returns the session's current层2 binding generation (0 for tail-only).
func (s *Session) BindingGen() int64 {
	if s.arm == nil {
		return 0
	}
	return s.arm.gen
}

// Close tears the session down: cancel its pump, close the lane (stops the
// connector writer), and drop the device from the entry (末出 → offline). For a
// member whose arm is being sealed, the pump is already stopping — Close is still
// what publishes the last-out offline + retires the entry. Idempotent.
func (s *Session) Close() {
	s.once.Do(func() {
		s.cancel()
		s.lane.close()
		if s.entry != nil {
			s.gw.removeDevice(s.entry, s)
		}
	})
}

// runFeed is the read pump (§5.6 lane恢复路径): backfill from the since cursor, then
// follow the commit Signal, pushing feed frames into the lane. A full lane (满 →
// 断连) or a lost-revocation reader-recheck failure tears the session down. On exit
// it closes the lane (connector writer stops) and, for a member, untracks from the
// arm join set.
func (s *Session) runFeed() {
	defer s.lane.close()
	if s.arm != nil {
		defer s.arm.untrack()
	}
	notify, cancelSub := s.home.Subscribe()
	defer cancelSub()
	if !s.pumpBatch() {
		return
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case _, ok := <-notify:
			if !ok {
				return
			}
			if !s.pumpBatch() {
				return
			}
		}
	}
}

// pumpBatch drains every log row after the lane cursor into the lane as feed
// frames. Returns false (→ tear down) on a full lane or a read-side reader recheck
// failure (漏 revocation 兜底, DoD-5). receipt.seq is never folded here — only feed
// rows advance the cursor (write位≠读位).
func (s *Session) pumpBatch() bool {
	if !s.readerStillValid() {
		return false
	}
	cur := s.lane.cursor
	for {
		select {
		case <-s.ctx.Done():
			return false
		default:
		}
		rows, err := s.home.View().ReadAfterSeq(context.Background(), cur.at(s.chID), feedBatch)
		if err != nil || len(rows) == 0 {
			return true
		}
		for _, r := range rows {
			env, merr := json.Marshal(r.Envelope)
			if merr != nil {
				continue
			}
			fr, ferr := platform.NewFrame(platform.FrameFeed, 0, "", platform.FeedPayload{
				ChannelID: string(s.chID),
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
				return false // full lane → 断连
			}
			cur.advance(s.chID, r.Seq)
		}
	}
}

// readerStillValid is the read-side每批 reader-resource recheck (design §5.5 漏事件
// 兜底): a member whose channel membership vanished (a lost revocation event) must
// stop being answered. Re-resolve the principal-bound subject; gone → stop. Tail-
// only sessions always pass here (bare-observer revocation is deferred).
func (s *Session) readerStillValid() bool {
	if !s.isMember {
		return true
	}
	if s.arm != nil && s.arm.isSealed() {
		return false
	}
	// Re-classify the subject through the principal membership read: an active
	// principal still resolves (found → keep serving); a removed one no longer
	// resolves (found=false → stop, a lost revocation caught here). A store error
	// keeps serving (never false-revoke on a transient blip).
	_, found, err := s.home.ResolvePrincipal(context.Background(), actor.KindHuman, s.principal)
	if err != nil {
		return true
	}
	return found
}

// Upstream drives one parsed upstream frame onto the subject's own cell (through
// the slot) and returns the receipt-or-error frame the connector writes back. It
// is the共享世代 admission gate's upstream half: a sealed arm refuses every business
// frame (stale_binding); a tail-only session refuses them (not_member); a submit's
// empty audience is routing-resolved (app政策 via the injected面); everything else
// is delivered to the cell whose from-log五步 does the real authorization.
func (s *Session) Upstream(ctx context.Context, f platform.Frame) platform.Frame {
	gen := s.BindingGen()
	errFrame := func(code, detail string) platform.Frame {
		fr, _ := platform.NewFrame(platform.FrameError, gen, f.Ref, platform.ErrorPayload{
			Frame: string(f.Type), Code: code, Detail: detail,
		})
		return fr
	}
	switch f.Type {
	case platform.FrameDetach:
		s.Close()
		fr, _ := platform.NewFrame(platform.FrameReceipt, gen, f.Ref, platform.DetachPayload{ChannelID: string(s.chID)})
		return fr
	case platform.FrameAttach:
		return errFrame(platform.CodeBadPayload, "attach is the opening frame, not a mid-stream verb")
	case platform.FrameSubmit, platform.FrameResolve, platform.FrameCancel,
		platform.FrameAfter, platform.FrameCancelTimer, platform.FrameResource:
		if !s.isMember {
			return errFrame(platform.CodeNotMember, "not a channel member")
		}
		if s.arm.isSealed() {
			return errFrame(platform.CodeStaleBinding, "binding sealed (detached / revoked)")
		}
		if f.Type == platform.FrameSubmit {
			routed, rerr := s.applyRouting(ctx, f)
			if rerr != nil {
				return *rerr
			}
			f = routed
		}
		res, derr := s.slot.Deliver(f)
		if derr != nil {
			// ErrNoOccupant (cell mid-re-mint / torn down) → retryable unavailable.
			return errFrame(platform.CodeUnavailable, "subject cell unavailable — retry")
		}
		return res.Frame
	default:
		return errFrame(platform.CodeBadPayload, "unknown frame_type: "+string(f.Type))
	}
}

// applyRouting resolves an empty-audience submit through the injected app routing
// 面 and rewrites the frame's payload with the concrete audience + kind (design
// §5.3: routing政策留 app, gateway 递话). An explicit audience is honoured as-is;
// a nil resolver leaves the frame untouched (the cell's own SubmitEnvelope
// validation catches an illegal empty-audience request). A per-request routing
// condition (no reachable brain) → an unavailable error frame (never写黑洞).
func (s *Session) applyRouting(ctx context.Context, f platform.Frame) (platform.Frame, *platform.Frame) {
	var p platform.SubmitPayload
	if err := f.DecodePayload(&p); err != nil {
		fr, _ := platform.NewFrame(platform.FrameError, s.BindingGen(), f.Ref, platform.ErrorPayload{
			Frame: string(f.Type), Code: platform.CodeBadPayload, Detail: err.Error(),
		})
		return f, &fr
	}
	if len(p.Audience) > 0 || s.gw.routing == nil {
		return f, nil
	}
	aud, kind, retryable, err := s.gw.routing(ctx, s.chID, nil, message.Kind(p.Kind))
	if retryable != "" || err != nil {
		detail := retryable
		if detail == "" {
			s.gw.logger.Error("gateway.routing", "channel", string(s.chID), "err", err)
			detail = "routing unavailable"
		}
		fr, _ := platform.NewFrame(platform.FrameError, s.BindingGen(), f.Ref, platform.ErrorPayload{
			Frame: string(f.Type), Code: platform.CodeUnavailable, Detail: detail,
		})
		return f, &fr
	}
	p.Audience = p.Audience[:0]
	for _, a := range aud {
		p.Audience = append(p.Audience, string(a))
	}
	p.Kind = string(kind)
	routed, _ := platform.NewFrame(platform.FrameSubmit, f.BindingGen, f.Ref, p)
	return routed, nil
}
