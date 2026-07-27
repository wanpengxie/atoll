package compute

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var (
	ErrOutboundClosed       = errors.New("compute: daemon outbound closed")
	ErrOutboundDisconnected = errors.New("compute: daemon outbound disconnected")
	ErrOutboundNotCurrent   = errors.New("compute: actor incarnation is not current")
)

const (
	defaultOutboundPoll  = 100 * time.Millisecond
	defaultOutboundRetry = 100 * time.Millisecond
)

// DaemonOutboundConfig configures the daemon-private physical-link organ.
type DaemonOutboundConfig struct {
	Parent       context.Context
	PollInterval time.Duration
	RetryDelay   time.Duration
}

// DaemonOutbound owns exact body slots and converges their future outbound
// calls onto the current authenticated physical session. It owns neither actor
// lifecycle truth nor session teardown.
type DaemonOutbound struct {
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	poll   time.Duration
	retry  time.Duration

	mu       sync.Mutex
	sealed   bool
	slots    map[*OutboundSlot]struct{}
	session  *link.AuthenticatedLinkSession
	watching map[*link.AuthenticatedLinkSession]struct{}

	workerWG sync.WaitGroup
	runnerWG sync.WaitGroup

	closeMu   sync.Mutex
	closing   bool
	closeDone chan struct{}
	closeErr  error
}

// OutboundArmsBundle is one immutable, atomically-published stream generation.
type OutboundArmsBundle struct {
	Session *link.AuthenticatedLinkSession
	Stream  *link.ActorStream

	Pen       harness.Pen
	Access    accessdoor.ResourceAccessHandle
	State     accessdoor.AccessHandle
	Schedule  schedule.ScheduleHandle
	Lifecycle actorcaps.LifecycleHandle
}

var disconnectedOutboundBundle = &OutboundArmsBundle{}

// OutboundSlot is an exact body-lifetime membrane. G1 and G2 slots for the
// same ActorID can coexist without overwriting one another.
type OutboundSlot struct {
	owner    *DaemonOutbound
	id       actor.ActorID
	key      actorhost.AttemptKey
	identity actorhost.IdentityCurrent
	attempt  actorhost.AttemptCurrent
	current  actorhost.ActualCurrent

	arms atomic.Pointer[OutboundArmsBundle]

	closed    atomic.Bool
	closeOnce sync.Once
	opening   bool // owner.mu
	retryAt   time.Time

	// pendingObs holds observations this slot could not put on the wire,
	// newest per kind. The slot is the body's one stable arm across a
	// replaceable stream, so carrying a value across the gap where no stream
	// exists is its job — a body publishes once and has no way to learn that a
	// stream later opened. Only FAILED publishes are held, so an edge that
	// already landed is never sent twice, while a level (device presence) still
	// converges to its current value once the stream is up.
	obsMu      sync.Mutex
	pendingObs map[actorrt.ObsKind]actorrt.ObsValue
}

// PreparedOutbound is the private Phase-B composition seam. It deliberately is
// not actorcaps.Caps while the production Caps lifecycle field still has the
// pre-cutover static type.
type PreparedOutbound struct {
	Slot *OutboundSlot
	Caps actorcaps.Caps
}

// NewDaemonOutbound starts an outbound level converger.
func NewDaemonOutbound(cfg DaemonOutboundConfig) *DaemonOutbound {
	parent := cfg.Parent
	if parent == nil {
		parent = context.Background()
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = defaultOutboundPoll
	}
	retry := cfg.RetryDelay
	if retry <= 0 {
		retry = defaultOutboundRetry
	}
	ctx, cancel := context.WithCancel(parent)
	outbound := &DaemonOutbound{
		ctx:       ctx,
		cancel:    cancel,
		wake:      make(chan struct{}, 1),
		poll:      poll,
		retry:     retry,
		slots:     make(map[*OutboundSlot]struct{}),
		watching:  make(map[*link.AuthenticatedLinkSession]struct{}),
		closeDone: make(chan struct{}),
	}
	outbound.runnerWG.Add(1)
	go outbound.run()
	return outbound
}

func (d *DaemonOutbound) run() {
	defer d.runnerWG.Done()
	ticker := time.NewTicker(d.poll)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-d.wake:
			d.convergePass()
		case <-ticker.C:
			d.convergePass()
		}
	}
}

// Wake coalesces a level-convergence hint.
func (d *DaemonOutbound) Wake() {
	if d == nil {
		return
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *DaemonOutbound) publishObs(
	id actor.ActorID,
	key actorhost.AttemptKey,
	kind actorrt.ObsKind,
	value actorrt.ObsValue,
) {
	if d == nil {
		return
	}
	d.mu.Lock()
	var target *OutboundSlot
	for slot := range d.slots {
		if slot.id == id && slot.key == key && !slot.closed.Load() {
			target = slot
			break
		}
	}
	d.mu.Unlock()
	if target != nil {
		// The error is the slot's business: a failed publish is held there and
		// flushed when a stream opens. No slot at all means this body has no
		// outbound arm (never prepared, or already gone) — nothing to carry it.
		_ = target.PublishObs(kind, value)
	}
}

// Prepare registers one exact disconnected slot before its Unit can be
// published.
func (d *DaemonOutbound) Prepare(
	id actor.ActorID,
	key actorhost.AttemptKey,
	identity actorhost.IdentityCurrent,
	attempt actorhost.AttemptCurrent,
	current actorhost.ActualCurrent,
) (PreparedOutbound, error) {
	if d == nil || id == "" {
		return PreparedOutbound{}, ErrOutboundClosed
	}
	// No system guard: a plan never carries a system coordinate (the kernel has
	// no record), so re-screening for it here would be redundant buckshot.
	if _, err := actorhost.ParseAttemptKey(string(key)); err != nil {
		return PreparedOutbound{}, err
	}
	slot := &OutboundSlot{
		owner: d, id: id, key: key,
		identity: identity, attempt: attempt, current: current,
	}
	slot.arms.Store(disconnectedOutboundBundle)
	d.mu.Lock()
	if d.sealed {
		d.mu.Unlock()
		return PreparedOutbound{}, ErrOutboundClosed
	}
	d.slots[slot] = struct{}{}
	d.mu.Unlock()
	d.Wake()
	return PreparedOutbound{
		Slot: slot,
		Caps: actorcaps.Caps{
			Pen:       outboundPen{slot: slot},
			Access:    outboundResourceAccess{slot: slot},
			State:     outboundState{slot: slot},
			Schedule:  outboundSchedule{slot: slot},
			Lifecycle: outboundLifecycle{slot: slot},
		},
	}, nil
}

// SetSession publishes an already-Plan-accepted exact physical session for
// future stream convergence. It never closes the predecessor session.
func (d *DaemonOutbound) SetSession(session *link.AuthenticatedLinkSession) error {
	if d == nil || session == nil || !session.IsCurrent() {
		return ErrOutboundDisconnected
	}
	d.mu.Lock()
	if d.sealed {
		d.mu.Unlock()
		return ErrOutboundClosed
	}
	if d.session == session {
		d.mu.Unlock()
		return nil
	}
	d.session = session
	var oldStreams []*link.ActorStream
	for slot := range d.slots {
		slot.retryAt = time.Time{}
		old := slot.arms.Swap(disconnectedOutboundBundle)
		if old != nil && old.Stream != nil {
			oldStreams = append(oldStreams, old.Stream)
		}
	}
	if _, exists := d.watching[session]; !exists {
		d.watching[session] = struct{}{}
		d.workerWG.Add(1)
		go d.watchSession(session)
	}
	d.mu.Unlock()
	for _, stream := range oldStreams {
		_ = stream.Close()
	}
	d.Wake()
	return nil
}

func (d *DaemonOutbound) watchSession(session *link.AuthenticatedLinkSession) {
	defer d.workerWG.Done()
	select {
	case <-session.Done():
		d.SessionDown(session)
	case <-d.ctx.Done():
	}
	d.mu.Lock()
	delete(d.watching, session)
	d.mu.Unlock()
}

// SessionDown clears only the exact current session.
func (d *DaemonOutbound) SessionDown(session *link.AuthenticatedLinkSession) {
	if d == nil || session == nil {
		return
	}
	var streams []*link.ActorStream
	d.mu.Lock()
	if d.session == session {
		d.session = nil
		for slot := range d.slots {
			old := slot.arms.Swap(disconnectedOutboundBundle)
			if old != nil && old.Stream != nil {
				streams = append(streams, old.Stream)
			}
		}
	}
	d.mu.Unlock()
	for _, stream := range streams {
		_ = stream.Close()
	}
	d.Wake()
}

func (d *DaemonOutbound) convergePass() {
	d.mu.Lock()
	if d.sealed {
		d.mu.Unlock()
		return
	}
	slots := make([]*OutboundSlot, 0, len(d.slots))
	for slot := range d.slots {
		slots = append(slots, slot)
	}
	d.mu.Unlock()
	for _, slot := range slots {
		d.convergeSlot(slot)
	}
}

func (d *DaemonOutbound) convergeSlot(slot *OutboundSlot) {
	isCurrent := slot.current.IsCurrent()
	var predecessor *link.ActorStream
	var session *link.AuthenticatedLinkSession

	d.mu.Lock()
	if d.sealed || slot.closed.Load() {
		d.mu.Unlock()
		return
	}
	if _, registered := d.slots[slot]; !registered {
		d.mu.Unlock()
		return
	}
	bundle := slot.arms.Load()
	session = d.session
	if !isCurrent || session == nil || !session.IsCurrent() {
		if bundle != disconnectedOutboundBundle {
			slot.arms.Store(disconnectedOutboundBundle)
			if bundle != nil {
				predecessor = bundle.Stream
			}
		}
		d.mu.Unlock()
		if predecessor != nil {
			_ = predecessor.Close()
		}
		return
	}
	if !slot.retryAt.IsZero() && time.Now().Before(slot.retryAt) {
		d.mu.Unlock()
		return
	}
	if bundle != nil && bundle.Session == session && bundle.Stream != nil && !channelClosed(bundle.Stream.Done()) {
		d.mu.Unlock()
		return
	}
	if bundle != nil && bundle.Stream != nil {
		predecessor = bundle.Stream
		slot.arms.Store(disconnectedOutboundBundle)
	}
	if slot.opening {
		d.mu.Unlock()
		if predecessor != nil {
			_ = predecessor.Close()
		}
		return
	}
	slot.opening = true
	d.workerWG.Add(1)
	d.mu.Unlock()
	if predecessor != nil {
		_ = predecessor.Close()
	}
	go d.openSlot(slot, session)
}

func (d *DaemonOutbound) openSlot(slot *OutboundSlot, session *link.AuthenticatedLinkSession) {
	defer d.workerWG.Done()
	if !session.IsCurrent() {
		d.mu.Lock()
		slot.opening = false
		slot.retryAt = time.Now().Add(d.retry)
		d.mu.Unlock()
		d.Wake()
		return
	}
	stream, err := session.OpenActorStream(d.ctx, slot.id, slot.key)
	stillCurrent := slot.current.IsCurrent()
	var loser *link.ActorStream
	var predecessor *link.ActorStream
	published := false

	d.mu.Lock()
	slot.opening = false
	_, registered := d.slots[slot]
	if err == nil && !d.sealed && registered && !slot.closed.Load() &&
		d.session == session && stillCurrent && session.IsCurrent() {
		raw := stream.Arms()
		next := &OutboundArmsBundle{
			Session:   session,
			Stream:    stream,
			Pen:       raw.Pen,
			Access:    raw.Access,
			State:     raw.State,
			Schedule:  raw.Schedule,
			Lifecycle: raw.Lifecycle,
		}
		old := slot.arms.Swap(next)
		if old != nil && old.Stream != nil && old.Stream != stream {
			predecessor = old.Stream
		}
		d.workerWG.Add(1)
		go d.watchStream(slot, session, stream)
		slot.retryAt = time.Time{}
		published = true
	} else {
		if registered && !slot.closed.Load() && !d.sealed &&
			d.session == session && stillCurrent {
			slot.retryAt = time.Now().Add(d.retry)
		}
		if stream != nil {
			loser = stream
		}
	}
	d.mu.Unlock()
	if published && !session.IsCurrent() {
		// The current pointer moved during the runtime publication call. Remove
		// only the exact bundle just installed; a successor is never touched.
		current := slot.arms.Load()
		if current != nil && current.Session == session && current.Stream == stream &&
			slot.arms.CompareAndSwap(current, disconnectedOutboundBundle) {
			loser = stream
			published = false
		}
	}
	if predecessor != nil {
		_ = predecessor.Close()
	}
	if loser != nil {
		_ = loser.Close()
	}
	if published {
		// The stream is live: hand it whatever could not go out while there was
		// none. Outside the lock — this puts frames on the wire.
		slot.flushPendingObs()
	}
	if !published {
		d.Wake()
	}
}

func (d *DaemonOutbound) watchStream(
	slot *OutboundSlot,
	session *link.AuthenticatedLinkSession,
	stream *link.ActorStream,
) {
	defer d.workerWG.Done()
	select {
	case <-stream.Done():
		// Exact completion is only a wake. convergeSlot rechecks session,
		// slot, current, and bundle before changing anything.
		d.mu.Lock()
		bundle := slot.arms.Load()
		if !d.sealed && !slot.closed.Load() && bundle != nil &&
			bundle.Session == session && bundle.Stream == stream {
			slot.retryAt = time.Now().Add(d.retry)
		}
		d.mu.Unlock()
		d.Wake()
	case <-d.ctx.Done():
	}
}

func channelClosed(done <-chan struct{}) bool {
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// Close unregisters this exact slot, publishes the closed sentinel, and
// signals its exact stream without joining it.
func (s *OutboundSlot) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		// A closed slot has no future stream to flush onto, and this body's
		// observations die with it — a successor mints its own.
		s.obsMu.Lock()
		s.pendingObs = nil
		s.obsMu.Unlock()
		owner := s.owner
		var stream *link.ActorStream
		owner.mu.Lock()
		delete(owner.slots, s)
		old := s.arms.Swap(disconnectedOutboundBundle)
		if old != nil {
			stream = old.Stream
		}
		owner.mu.Unlock()
		if stream != nil {
			_ = stream.Close()
		}
		owner.Wake()
	})
	return nil
}

// Coordinate reports the exact slot's welded actor coordinate.
func (s *OutboundSlot) Coordinate() (actor.ActorID, actorhost.AttemptKey) {
	if s == nil {
		return "", ""
	}
	return s.id, s.key
}

// CancelRequest and PublishObs are actorbase/actorrt host hooks, not actor
// capabilities. They use the same exact slot and one-load/one-call discipline
// as the five capability facades and never buffer or retry across disconnects.
func (s *OutboundSlot) CancelRequest(id message.ID) error {
	bundle, err := s.loadPhysical()
	if err != nil {
		return err
	}
	return bundle.Stream.SendCancelRequest(id)
}

func (s *OutboundSlot) PublishObs(kind actorrt.ObsKind, value actorrt.ObsValue) error {
	err := s.publishObsNow(kind, value)
	if err != nil && !s.closed.Load() {
		s.holdObs(kind, value)
	}
	return err
}

// publishObsNow is the one wire attempt, with no holding.
func (s *OutboundSlot) publishObsNow(kind actorrt.ObsKind, value actorrt.ObsValue) error {
	bundle, err := s.loadPhysical()
	if err != nil {
		return err
	}
	return bundle.Stream.PublishObs(string(kind), value)
}

// holdObs keeps the newest unsent value of one kind for the next flush.
func (s *OutboundSlot) holdObs(kind actorrt.ObsKind, value actorrt.ObsValue) {
	s.hold(kind, value, true)
}

// holdStaleObs puts a failed flush attempt back WITHOUT overwriting a newer
// value the body published while the flush was in flight. Re-arming an older
// value there would move the level backwards, which is the one thing a level
// must never do.
func (s *OutboundSlot) holdStaleObs(kind actorrt.ObsKind, value actorrt.ObsValue) {
	s.hold(kind, value, false)
}

func (s *OutboundSlot) hold(kind actorrt.ObsKind, value actorrt.ObsValue, overwrite bool) {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	if s.pendingObs == nil {
		s.pendingObs = map[actorrt.ObsKind]actorrt.ObsValue{}
	}
	if _, newer := s.pendingObs[kind]; newer && !overwrite {
		return
	}
	s.pendingObs[kind] = value
}

// flushPendingObs drains the held observations onto a stream that just opened.
// Anything that fails again is put back, so the next connect retries it; a
// value superseded meanwhile is not resurrected, because holdObs only ever
// writes the newest per kind.
func (s *OutboundSlot) flushPendingObs() {
	s.obsMu.Lock()
	held := s.pendingObs
	s.pendingObs = nil
	s.obsMu.Unlock()

	for kind, value := range held {
		if s.closed.Load() {
			return
		}
		if err := s.publishObsNow(kind, value); err != nil {
			s.holdStaleObs(kind, value)
		}
	}
}

// Seal stops future slot/session admission and joins DaemonOutbound workers
// without invalidating the arms held by still-running actor bodies.
func (d *DaemonOutbound) Seal(ctx context.Context) error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if !d.sealed {
		d.sealed = true
		d.cancel()
	}
	d.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if !waitOutboundGroup(ctx, &d.runnerWG) || !waitOutboundGroup(ctx, &d.workerWG) {
		return errors.New("compute: daemon outbound worker leak")
	}
	return nil
}

// CloseResidual exact-closes slots left after Host has stopped every body.
func (d *DaemonOutbound) CloseResidual() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	slots := make([]*OutboundSlot, 0, len(d.slots))
	for slot := range d.slots {
		slots = append(slots, slot)
	}
	d.mu.Unlock()
	for _, slot := range slots {
		_ = slot.Close()
	}
	d.mu.Lock()
	var closeErr error
	if len(d.slots) != 0 {
		closeErr = errors.Join(closeErr, fmt.Errorf("compute: daemon outbound slot leak: %d", len(d.slots)))
	}
	d.mu.Unlock()
	return closeErr
}

// Close is the standalone full close. The daemon composition root uses the
// finer Seal → Host.Close → CloseResidual sequence so bodies lose their arms
// only when their own Stop path runs.
func (d *DaemonOutbound) Close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	d.closeMu.Lock()
	if d.closing {
		done := d.closeDone
		d.closeMu.Unlock()
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case <-done:
			d.closeMu.Lock()
			err := d.closeErr
			d.closeMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	d.closing = true
	d.closeMu.Unlock()
	closeErr := errors.Join(d.Seal(ctx), d.CloseResidual())
	d.closeMu.Lock()
	d.closeErr = closeErr
	close(d.closeDone)
	d.closeMu.Unlock()
	return closeErr
}

func waitOutboundGroup(ctx context.Context, group *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	default:
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *OutboundSlot) loadConnected() (*OutboundArmsBundle, error) {
	if s == nil || s.closed.Load() {
		return nil, ErrOutboundClosed
	}
	bundle := s.arms.Load()
	if bundle == nil || bundle.Stream == nil || channelClosed(bundle.Stream.Done()) {
		return nil, ErrOutboundDisconnected
	}
	return bundle, nil
}

func (s *OutboundSlot) loadIdentity() (*OutboundArmsBundle, error) {
	if s == nil || !s.identity.IsCurrent() {
		return nil, ErrOutboundNotCurrent
	}
	return s.loadConnected()
}

func (s *OutboundSlot) loadAttempt() (*OutboundArmsBundle, error) {
	if s == nil || !s.attempt.IsCurrent() {
		return nil, ErrOutboundNotCurrent
	}
	return s.loadConnected()
}

func (s *OutboundSlot) loadPhysical() (*OutboundArmsBundle, error) {
	if s == nil || !s.current.IsCurrent() {
		return nil, ErrOutboundNotCurrent
	}
	return s.loadConnected()
}

type outboundPen struct{ slot *OutboundSlot }

func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	bundle, err := p.slot.loadAttempt()
	if err != nil {
		result := harness.WriteResult{}
		if env != nil {
			result.MessageID = env.ID
		}
		return result, err
	}
	return bundle.Pen.Write(ctx, env)
}

type outboundState struct{ slot *OutboundSlot }

func (a outboundState) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	bundle, err := a.slot.loadIdentity()
	if errors.Is(err, ErrOutboundDisconnected) {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	return bundle.State.Invoke(ctx, op, id, args, grant)
}

type outboundResourceAccess struct{ slot *OutboundSlot }

func (a outboundResourceAccess) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (accessdoor.Outcome, error) {
	bundle, err := a.slot.loadAttempt()
	if errors.Is(err, ErrOutboundDisconnected) {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	return bundle.Access.Invoke(ctx, op, id, args, grant)
}

func (a outboundResourceAccess) Create(ctx context.Context, id resource.ResourceID, spec accessdoor.CreateSpec, initial []byte) (accessdoor.Outcome, error) {
	bundle, err := a.slot.loadAttempt()
	if errors.Is(err, ErrOutboundDisconnected) {
		return accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if err != nil {
		return accessdoor.Outcome{}, err
	}
	return bundle.Access.Create(ctx, id, spec, initial)
}

func (a outboundResourceAccess) Stat(ctx context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	bundle, err := a.slot.loadAttempt()
	if err != nil {
		return accessdoor.StatResult{}, err
	}
	return bundle.Access.Stat(ctx, id)
}

func (a outboundResourceAccess) List(ctx context.Context, query accessdoor.ListQuery) (accessdoor.ListPage, error) {
	bundle, err := a.slot.loadAttempt()
	if err != nil {
		return accessdoor.ListPage{}, err
	}
	return bundle.Access.List(ctx, query)
}

func (a outboundResourceAccess) Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	bundle, err := a.slot.loadAttempt()
	if errors.Is(err, ErrOutboundDisconnected) {
		return accessdoor.FileAccess{}, accessdoor.Outcome{RejectReason: access.OutcomeUnknown}, nil
	}
	if err != nil {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, err
	}
	return bundle.Access.Open(ctx, id, mode)
}

func (a outboundResourceAccess) Redeem(ctx context.Context, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	bundle, err := a.slot.loadAttempt()
	if err != nil {
		return accessdoor.FileAccess{}, err
	}
	return bundle.Access.Redeem(ctx, route)
}

type outboundSchedule struct{ slot *OutboundSlot }

func (s outboundSchedule) Schedule(ctx context.Context, request schedule.ScheduleReq) (schedule.TimerID, error) {
	bundle, err := s.slot.loadIdentity()
	if err != nil {
		return "", err
	}
	return bundle.Schedule.Schedule(ctx, request)
}
func (s outboundSchedule) Cancel(ctx context.Context, id schedule.TimerID) error {
	bundle, err := s.slot.loadIdentity()
	if err != nil {
		return err
	}
	return bundle.Schedule.Cancel(ctx, id)
}
func (s outboundSchedule) Ack(ctx context.Context, id schedule.TimerID) error {
	bundle, err := s.slot.loadIdentity()
	if err != nil {
		return err
	}
	return bundle.Schedule.Ack(ctx, id)
}

type outboundLifecycle struct{ slot *OutboundSlot }

func (l outboundLifecycle) Fork(ctx context.Context, requestID message.ID, spec actorcaps.ForkSpec) (actor.ActorID, error) {
	bundle, err := l.slot.loadConnected()
	if err != nil {
		return "", err
	}
	return bundle.Lifecycle.Fork(ctx, requestID, spec)
}
func (l outboundLifecycle) EndSelf(ctx context.Context, request actorcaps.EndSelfRequest) error {
	bundle, err := l.slot.loadConnected()
	if err != nil {
		return err
	}
	return bundle.Lifecycle.EndSelf(ctx, request)
}

// Wrap makes slot cleanup the first Stop action even when the business actor
// has no Stop hook. It preserves the optional lifecycle/cancel/down behavior
// through no-op forwarding.
func (p PreparedOutbound) Wrap(impl actorrt.Actor) actorrt.Actor {
	if impl == nil {
		_ = p.Slot.Close()
		return nil
	}
	return actorrt.WithStopFirst(impl, func() { _ = p.Slot.Close() })
}

var (
	_ harness.Pen                     = outboundPen{}
	_ accessdoor.AccessHandle         = outboundState{}
	_ accessdoor.ResourceAccessHandle = outboundResourceAccess{}
	_ schedule.ScheduleHandle         = outboundSchedule{}
	_ actorcaps.LifecycleHandle       = outboundLifecycle{}
)
