package actorhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const (
	defaultPollInterval = 100 * time.Millisecond
	defaultRetryDelay   = 50 * time.Millisecond
	defaultCloseTimeout = 5 * time.Second
)

// Config constructs one execution-domain HostSupervisor.
type Config struct {
	Parent       context.Context
	Domain       ExecutionDomain
	Mailbox      int
	BodyBuilder  BodyBuilder
	Events       HostEventSink
	Logger       *slog.Logger
	PollInterval time.Duration
	RetryDelay   time.Duration
	CloseTimeout time.Duration
}

type buildJob struct {
	key  AttemptKey
	body BodyDesired
}

type bodyActual struct {
	key  AttemptKey
	unit *actorrt.Unit
}

type routeActual struct {
	key     AttemptKey
	binding Binding
	started time.Time
}

type retireEntry struct {
	unit *actorrt.Unit
}

type hostState struct {
	desired  *desiredValue
	build    *buildJob
	body     *bodyActual
	route    *routeActual
	retiring map[*actorrt.Unit]*retireEntry
	retryAt  time.Time
}

func (s *hostState) empty() bool {
	return s != nil &&
		s.desired == nil &&
		s.build == nil &&
		s.body == nil &&
		s.route == nil &&
		len(s.retiring) == 0 &&
		s.retryAt.IsZero()
}

// HostSupervisor is the sole desired/current/replacement owner for one
// execution domain.
type HostSupervisor struct {
	domain       ExecutionDomain
	mailbox      int
	builder      BodyBuilder
	events       HostEventSink
	logger       *slog.Logger
	pollInterval time.Duration
	retryDelay   time.Duration
	closeTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}

	spans spanRegistry

	acceptMu sync.Mutex
	mu       sync.RWMutex
	states   map[actor.ActorID]*hostState
	sealed   bool

	runnerWG  sync.WaitGroup
	builderWG sync.WaitGroup
	watcherWG sync.WaitGroup

	watcherCtx    context.Context
	watcherCancel context.CancelFunc

	closeMu   sync.Mutex
	closeDone chan struct{}
	closeErr  error
}

// New starts a HostSupervisor.
func New(cfg Config) (*HostSupervisor, error) {
	if !cfg.Domain.valid() {
		return nil, ErrInvalidDomain
	}
	if cfg.BodyBuilder == nil {
		return nil, fmt.Errorf("%w: nil body builder", ErrInvalidDesired)
	}
	parent := cfg.Parent
	if parent == nil {
		parent = context.Background()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	retry := cfg.RetryDelay
	if retry <= 0 {
		retry = defaultRetryDelay
	}
	closeTimeout := cfg.CloseTimeout
	if closeTimeout <= 0 {
		closeTimeout = defaultCloseTimeout
	}
	ctx, cancel := context.WithCancel(parent)
	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	h := &HostSupervisor{
		domain:        cfg.Domain,
		mailbox:       cfg.Mailbox,
		builder:       cfg.BodyBuilder,
		events:        cfg.Events,
		logger:        logger,
		pollInterval:  poll,
		retryDelay:    retry,
		closeTimeout:  closeTimeout,
		ctx:           ctx,
		cancel:        cancel,
		wake:          make(chan struct{}, 1),
		states:        make(map[actor.ActorID]*hostState),
		watcherCtx:    watcherCtx,
		watcherCancel: watcherCancel,
		closeDone:     make(chan struct{}),
	}
	h.runnerWG.Add(1)
	go h.run()
	return h, nil
}

// Domain reports the execution domain this Host owns.
func (h *HostSupervisor) Domain() ExecutionDomain {
	if h == nil {
		return ""
	}
	return h.domain
}

// Wake coalesces a level-reconciliation hint.
func (h *HostSupervisor) Wake() {
	if h == nil {
		return
	}
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *HostSupervisor) run() {
	defer h.runnerWG.Done()
	ticker := time.NewTicker(h.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.wake:
			h.reconcilePass()
		case <-ticker.C:
			h.reconcilePass()
		}
	}
}

// AcceptFullDesired validates a complete snapshot before changing any LKG
// entry. Application is per-ID coherent; no cross-ID revision is invented.
func (h *HostSupervisor) AcceptFullDesired(inputs []Desired) error {
	if h == nil {
		return ErrHostClosed
	}
	h.acceptMu.Lock()
	defer h.acceptMu.Unlock()
	next := make(map[actor.ActorID]desiredValue, len(inputs))
	for _, input := range inputs {
		value, err := normalizeDesired(input)
		if err != nil {
			return err
		}
		id := value.actorID()
		if _, exists := next[id]; exists {
			return fmt.Errorf("%w: duplicate actor %s", ErrInvalidDesired, id)
		}
		next[id] = value
	}

	// Validate immutable same-attempt payloads against the current LKG before
	// publishing any member of this snapshot.
	h.mu.RLock()
	if h.sealed {
		h.mu.RUnlock()
		return ErrHostClosed
	}
	for id, incoming := range next {
		state := h.states[id]
		if state == nil || state.desired == nil {
			continue
		}
		current := *state.desired
		if current.attemptKey() == incoming.attemptKey() && !current.equal(incoming) {
			h.mu.RUnlock()
			return fmt.Errorf("%w: actor %s", ErrSameAttemptDrift, id)
		}
	}
	currentIDs := make([]actor.ActorID, 0, len(h.states))
	for id, state := range h.states {
		if state.desired != nil {
			currentIDs = append(currentIDs, id)
		}
	}
	h.mu.RUnlock()

	idSet := make(map[actor.ActorID]struct{}, len(currentIDs)+len(next))
	for _, id := range currentIDs {
		idSet[id] = struct{}{}
	}
	for id := range next {
		idSet[id] = struct{}{}
	}
	ids := sortedActorIDs(idSet)
	for _, id := range ids {
		unlock := h.spans.lock(id)
		h.mu.Lock()
		if h.sealed {
			h.mu.Unlock()
			unlock()
			return ErrHostClosed
		}
		state := h.states[id]
		incoming, present := next[id]
		if !present {
			if state != nil {
				state.desired = nil
				state.retryAt = time.Time{}
				if state.build != nil {
					state.build = nil
				}
				h.deleteIfEmptyLocked(id, state)
			}
			h.mu.Unlock()
			unlock()
			continue
		}
		if state == nil {
			state = &hostState{}
			h.states[id] = state
		}
		copyValue := incoming
		state.desired = &copyValue
		state.retryAt = time.Time{}
		if state.build != nil &&
			(incoming.body == nil || state.build.key != incoming.attemptKey() ||
				!executionSpecEqual(state.build.body.ExecutionSpec, incoming.body.ExecutionSpec)) {
			state.build = nil
		}
		h.mu.Unlock()
		unlock()
	}
	h.Wake()
	return nil
}

func sortedActorIDs(set map[actor.ActorID]struct{}) []actor.ActorID {
	ids := make([]actor.ActorID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i]) < string(ids[j]) })
	return ids
}

func (h *HostSupervisor) reconcilePass() {
	h.mu.RLock()
	if h.sealed {
		h.mu.RUnlock()
		return
	}
	set := make(map[actor.ActorID]struct{}, len(h.states))
	for id := range h.states {
		set[id] = struct{}{}
	}
	h.mu.RUnlock()
	for _, id := range sortedActorIDs(set) {
		h.reconcileOne(id)
	}
}

type retireTask struct {
	id    actor.ActorID
	entry *retireEntry
}

func (h *HostSupervisor) reconcileOne(id actor.ActorID) {
	var retireTasks []retireTask
	var closeBindings []Binding
	var launch *buildJob

	unlock := h.spans.lock(id)
	h.mu.Lock()
	if h.sealed {
		h.mu.Unlock()
		unlock()
		return
	}
	state := h.states[id]
	if state == nil {
		h.mu.Unlock()
		unlock()
		return
	}
	desired := state.desired
	switch {
	case desired == nil:
		if state.body != nil {
			retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, state.body.unit))
			state.body = nil
		}
		if state.route != nil {
			closeBindings = append(closeBindings, state.route.binding)
			state.route = nil
		}
		state.build = nil
		state.retryAt = time.Time{}

	case desired.carrier != nil:
		if state.body != nil {
			retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, state.body.unit))
			state.body = nil
		}
		if state.route != nil && state.route.key != desired.carrier.AttemptKey {
			closeBindings = append(closeBindings, state.route.binding)
			state.route = nil
		}
		state.build = nil
		state.retryAt = time.Time{}

	case desired.body != nil:
		bodyDesired := *desired.body
		if state.route != nil {
			// Keep the old endpoint available while the replacement body builds.
		}
		acceptable := state.body != nil &&
			state.body.key == bodyDesired.AttemptKey &&
			state.body.unit.IsAlive()
		if state.body != nil && state.body.key == bodyDesired.AttemptKey && !state.body.unit.IsAlive() {
			retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, state.body.unit))
			state.body = nil
		}
		if !acceptable && state.build == nil &&
			(state.retryAt.IsZero() || !time.Now().Before(state.retryAt)) {
			job := &buildJob{key: bodyDesired.AttemptKey, body: bodyDesired}
			state.build = job
			state.retryAt = time.Time{}
			h.builderWG.Add(1)
			launch = job
		}
	}
	h.deleteIfEmptyLocked(id, state)
	h.mu.Unlock()
	unlock()

	h.executeRetireTasks(retireTasks)
	closeAll(closeBindings)
	if launch != nil {
		go h.build(id, launch)
	}
}

func appendRetire(tasks []retireTask, task *retireTask) []retireTask {
	if task != nil {
		return append(tasks, *task)
	}
	return tasks
}

func (h *HostSupervisor) build(id actor.ActorID, job *buildJob) {
	defer h.builderWG.Done()
	var current ActualCurrent
	unit, err := actorrt.Prepare(actorrt.UnitConfig{
		Parent:  h.ctx,
		ActorID: id,
		Kind:    job.body.ExecutionSpec.Kind,
		Mailbox: h.mailbox,
		Logger:  h.logger,
	}, func(self actorrt.Incarnation) actorrt.Actor {
		current = ActualCurrent{host: h, id: id, key: job.key, self: self}
		input := BodyBuildInput{
			ActorID:       id,
			AttemptKey:    job.key,
			ExecutionSpec: job.body.ExecutionSpec,
			Self:          self,
			Current:       current,
		}
		return h.builder(input)
	}, h)
	if err != nil {
		h.finishBuildFailure(id, job, err)
		return
	}

	var retireTasks []retireTask
	var closeBindings []Binding
	winner := false
	var startErr error
	unlock := h.spans.lock(id)
	h.mu.Lock()
	state := h.states[id]
	if !h.sealed && state != nil && state.build == job && state.desired != nil &&
		state.desired.body != nil && bodyDesiredEqual(*state.desired.body, job.body) {
		state.build = nil
		state.retryAt = time.Time{}
		if state.body != nil {
			retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, state.body.unit))
		}
		if state.route != nil {
			closeBindings = append(closeBindings, state.route.binding)
			state.route = nil
		}
		state.body = &bodyActual{key: job.key, unit: unit}
		// Publication and Start are one Host critical section. A concurrent
		// health pass must never mistake the deliberately Prepared publication
		// window for an exited body and retire the candidate before Start.
		// Start only flips Unit-local state and launches its goroutine; any
		// actor callback that checks Current waits on this span and observes the
		// fully-started publication after unlock.
		startErr = unit.Start()
		if startErr == nil {
			winner = true
		} else {
			state.body = nil
			retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, unit))
			state.retryAt = time.Now().Add(h.retryDelay)
		}
	} else {
		if state == nil {
			state = &hostState{}
			h.states[id] = state
		}
		if state.build == job {
			state.build = nil
		}
		retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, unit))
		h.deleteIfEmptyLocked(id, state)
	}
	h.mu.Unlock()
	unlock()

	h.executeRetireTasks(retireTasks)
	closeAll(closeBindings)
	if !winner {
		if startErr != nil {
			h.logger.Error("actorhost.unit_start_failed",
				"actor", string(id), "attempt", string(job.key), "err", startErr)
			h.Wake()
		}
		return
	}
	h.Wake()
}

func bodyDesiredEqual(left, right BodyDesired) bool {
	return left.ActorID == right.ActorID &&
		left.AttemptKey == right.AttemptKey &&
		executionSpecEqual(left.ExecutionSpec, right.ExecutionSpec)
}

func (h *HostSupervisor) finishBuildFailure(id actor.ActorID, job *buildJob, buildErr error) {
	h.logger.Warn("actorhost.build_failed", "actor", string(id), "attempt", string(job.key), "err", buildErr)
	unlock := h.spans.lock(id)
	h.mu.Lock()
	state := h.states[id]
	if state != nil && state.build == job {
		state.build = nil
		if !h.sealed && state.desired != nil && state.desired.body != nil &&
			bodyDesiredEqual(*state.desired.body, job.body) {
			state.retryAt = time.Now().Add(h.retryDelay)
		}
		h.deleteIfEmptyLocked(id, state)
	}
	h.mu.Unlock()
	unlock()
	h.Wake()
}

// Attach publishes one already-authorized exact Binding.
func (h *HostSupervisor) Attach(id actor.ActorID, key AttemptKey, binding Binding) error {
	if h == nil {
		return ErrHostClosed
	}
	if err := validateCoordinate(id, key); err != nil {
		return err
	}
	if !binding.Valid() {
		return fmt.Errorf("%w: nil binding", ErrInvalidDesired)
	}
	var predecessor Binding
	unlock := h.spans.lock(id)
	h.mu.Lock()
	if h.sealed {
		h.mu.Unlock()
		unlock()
		return ErrAttachRetryable
	}
	state := h.states[id]
	if state != nil && (state.body != nil || state.build != nil) {
		h.mu.Unlock()
		unlock()
		h.Wake()
		return ErrAttachRetryable
	}
	if state == nil || state.desired == nil || state.desired.carrier == nil ||
		state.desired.carrier.AttemptKey != key {
		h.mu.Unlock()
		unlock()
		return ErrStaleBinding
	}
	if state.route == nil {
		state.route = &routeActual{key: key, binding: binding, started: time.Now()}
		h.mu.Unlock()
		unlock()
		return nil
	}
	if state.route.binding == binding {
		h.mu.Unlock()
		unlock()
		return nil
	}
	order, err := CompareAttemptKeys(key, state.route.key)
	if err != nil {
		h.mu.Unlock()
		unlock()
		return err
	}
	if order < 0 {
		h.mu.Unlock()
		unlock()
		return ErrStaleBinding
	}
	predecessor = state.route.binding
	started := time.Now()
	if state.route.key == key {
		// A same-attempt rebind changes only the physical path; preserve the
		// remote body's L1 start coordinate across network reconnects.
		started = state.route.started
	}
	state.route = &routeActual{key: key, binding: binding, started: started}
	h.mu.Unlock()
	unlock()
	if predecessor.Valid() {
		_ = predecessor.Close()
	}
	return nil
}

// BindingDown removes only an exact current Binding. It never joins Done.
func (h *HostSupervisor) BindingDown(id actor.ActorID, binding Binding) {
	if h == nil || id == "" || !binding.Valid() {
		return
	}
	unlock := h.spans.lock(id)
	h.mu.Lock()
	state := h.states[id]
	if state != nil && state.route != nil && state.route.binding == binding {
		state.route = nil
		h.deleteIfEmptyLocked(id, state)
	}
	h.mu.Unlock()
	unlock()
	_ = binding.Close()
}

// Deliver snapshots the current endpoint and performs I/O outside host locks.
func (h *HostSupervisor) Deliver(id actor.ActorID, env *message.Envelope) error {
	endpoint, ok := h.endpoint(id)
	if !ok {
		return ErrNotHosted
	}
	return endpoint.Deliver(env)
}

// CancelRequest is a best-effort signal to the current endpoint.
func (h *HostSupervisor) CancelRequest(id actor.ActorID, requestID message.ID) {
	endpoint, ok := h.endpoint(id)
	if !ok {
		return
	}
	endpoint.CancelRequest(requestID)
}

func (h *HostSupervisor) endpoint(id actor.ActorID) (ActorEndpoint, bool) {
	if h == nil || id == "" || id == actor.SystemActorID {
		return nil, false
	}
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	var endpoint ActorEndpoint
	switch {
	case state == nil:
	case state.body != nil:
		endpoint = state.body.unit
	case state.route != nil:
		endpoint = state.route.binding
	}
	h.mu.RUnlock()
	unlock()
	return endpoint, endpoint != nil
}

func (h *HostSupervisor) isCurrent(id actor.ActorID, key AttemptKey, self actorrt.Incarnation) bool {
	if h == nil {
		return false
	}
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	ok := !h.sealed && state != nil && state.body != nil &&
		state.body.key == key &&
		state.body.unit.Self() == self &&
		state.body.unit.IsAlive()
	h.mu.RUnlock()
	unlock()
	return ok
}

// IdentityProbe binds an accepted Desired-level A probe.
func (h *HostSupervisor) IdentityProbe(id actor.ActorID) IdentityCurrent {
	return IdentityCurrent{host: h, id: id}
}

// AttemptProbe binds an accepted Desired-level A/G probe.
func (h *HostSupervisor) AttemptProbe(id actor.ActorID, key AttemptKey) AttemptCurrent {
	return AttemptCurrent{host: h, id: id, key: key}
}

func (h *HostSupervisor) identityCurrent(id actor.ActorID) bool {
	if h == nil || id == "" || id == actor.SystemActorID {
		return false
	}
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	ok := !h.sealed && state != nil && state.desired != nil
	h.mu.RUnlock()
	unlock()
	return ok
}

func (h *HostSupervisor) attemptCurrent(id actor.ActorID, key AttemptKey) bool {
	if h == nil || id == "" || id == actor.SystemActorID || !key.valid() {
		return false
	}
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	ok := !h.sealed && state != nil && state.desired != nil &&
		state.desired.attemptKey() == key
	h.mu.RUnlock()
	unlock()
	return ok
}

func (h *HostSupervisor) retireLocked(id actor.ActorID, state *hostState, unit *actorrt.Unit) *retireTask {
	if unit == nil {
		return nil
	}
	if state.retiring == nil {
		state.retiring = make(map[*actorrt.Unit]*retireEntry)
	}
	if _, exists := state.retiring[unit]; exists {
		return nil
	}
	entry := &retireEntry{unit: unit}
	state.retiring[unit] = entry
	h.watcherWG.Add(1)
	return &retireTask{id: id, entry: entry}
}

func (h *HostSupervisor) executeRetireTasks(tasks []retireTask) {
	for i := range tasks {
		task := tasks[i]
		go h.watchRetiring(task)
		task.entry.unit.Stop()
	}
}

func (h *HostSupervisor) watchRetiring(task retireTask) {
	defer h.watcherWG.Done()
	select {
	case <-task.entry.unit.Done():
		unlock := h.spans.lock(task.id)
		h.mu.Lock()
		state := h.states[task.id]
		if state != nil && state.retiring[task.entry.unit] == task.entry {
			delete(state.retiring, task.entry.unit)
			h.deleteIfEmptyLocked(task.id, state)
		}
		h.mu.Unlock()
		unlock()
	case <-h.watcherCtx.Done():
	}
}

func (h *HostSupervisor) deleteIfEmptyLocked(id actor.ActorID, state *hostState) {
	if state != nil && state.empty() {
		delete(h.states, id)
	}
}

func closeAll(bindings []Binding) {
	for _, binding := range bindings {
		if binding.Valid() {
			_ = binding.Close()
		}
	}
}

// OnExited implements actorrt.UnitEventSink and only forwards a current exact
// body's event. The event remains a wake hint; reconciliation rechecks level
// state.
func (h *HostSupervisor) OnExited(event actorrt.ExitedEvent) {
	id := event.Self.ID()
	var key AttemptKey
	current := false
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	if state != nil && state.body != nil &&
		state.body.unit == event.Unit &&
		state.body.unit.Self() == event.Self {
		key = state.body.key
		current = true
	}
	h.mu.RUnlock()
	unlock()
	if current && h.events != nil {
		h.events.OnBodyExited(id, key, event.Self, event.Cause)
	}
	h.Wake()
}

// OnObs implements actorrt.UnitEventSink and drops stale predecessor
// observations.
func (h *HostSupervisor) OnObs(event actorrt.UnitObsEvent) {
	id := event.Self.ID()
	var key AttemptKey
	current := false
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	if state != nil && state.body != nil &&
		state.body.unit == event.Unit &&
		state.body.unit.Self() == event.Self {
		key = state.body.key
		current = true
	}
	h.mu.RUnlock()
	unlock()
	if current && h.events != nil {
		h.events.OnBodyObs(id, key, event.Self, event.Kind, event.Value)
	}
}

// Inspect returns an immutable diagnostic snapshot for one actor.
func (h *HostSupervisor) Inspect(id actor.ActorID) (Snapshot, bool) {
	if h == nil {
		return Snapshot{}, false
	}
	unlock := h.spans.lock(id)
	h.mu.RLock()
	state := h.states[id]
	if state == nil {
		h.mu.RUnlock()
		unlock()
		return Snapshot{}, false
	}
	out := Snapshot{
		Building: state.build != nil,
		Retiring: len(state.retiring),
		Retrying: !state.retryAt.IsZero(),
	}
	if state.desired != nil {
		out.Desired = state.desired.clonePublic()
	}
	if state.body != nil {
		out.Actual = ActualBody
		out.Attempt = state.body.key
		out.Unit = state.body.unit
		out.StartedAt = state.body.unit.Stat().StartedAt
	} else if state.route != nil {
		out.Actual = ActualRoute
		out.Attempt = state.route.key
		out.Binding = state.route.binding
		out.StartedAt = state.route.started
	}
	h.mu.RUnlock()
	unlock()
	return out, true
}

// Close seals all mutation, joins producers, signals exact resources, waits
// boundedly for Units, and reports only resources that are still unfinished.
func (h *HostSupervisor) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.closeMu.Lock()
	select {
	case <-h.closeDone:
		err := h.closeErr
		h.closeMu.Unlock()
		return err
	default:
	}
	// Only one caller executes closure. Other callers wait below without
	// holding closeMu.
	done := h.closeDone
	h.closeMu.Unlock()

	h.mu.Lock()
	alreadySealed := h.sealed
	h.sealed = true
	h.mu.Unlock()
	if alreadySealed {
		select {
		case <-done:
			h.closeMu.Lock()
			err := h.closeErr
			h.closeMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := h.close(ctx)
	h.closeMu.Lock()
	h.closeErr = err
	close(h.closeDone)
	h.closeMu.Unlock()
	return err
}

func (h *HostSupervisor) close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.closeTimeout)
		defer cancel()
	}
	h.cancel()
	var faults []error
	if !waitGroup(ctx, &h.runnerWG) {
		faults = append(faults, errors.New("actorhost: reconcile worker leak"))
	}
	buildersJoined := waitGroup(ctx, &h.builderWG)
	if !buildersJoined {
		faults = append(faults, errors.New("actorhost: body builder leak"))
	}

	h.mu.RLock()
	set := make(map[actor.ActorID]struct{}, len(h.states))
	for id := range h.states {
		set[id] = struct{}{}
	}
	h.mu.RUnlock()
	var retireTasks []retireTask
	var bindings []Binding
	for _, id := range sortedActorIDs(set) {
		unlock := h.spans.lock(id)
		h.mu.Lock()
		state := h.states[id]
		if state != nil {
			state.desired = nil
			state.build = nil
			state.retryAt = time.Time{}
			if state.body != nil {
				retireTasks = appendRetire(retireTasks, h.retireLocked(id, state, state.body.unit))
				state.body = nil
			}
			if state.route != nil {
				bindings = append(bindings, state.route.binding)
				state.route = nil
			}
			h.deleteIfEmptyLocked(id, state)
		}
		h.mu.Unlock()
		unlock()
	}
	h.executeRetireTasks(retireTasks)
	closeAll(bindings)

	if !buildersJoined {
		// A late builder is still allowed to return and retire its prepared
		// loser. Do not race watcherWG.Wait/Cancel against that future Add.
		return errors.Join(faults...)
	}
	if !waitGroup(ctx, &h.watcherWG) {
		h.watcherCancel()
		h.watcherWG.Wait()
	}
	h.watcherCancel()

	h.mu.RLock()
	for id, state := range h.states {
		for unit := range state.retiring {
			select {
			case <-unit.Done():
			default:
				faults = append(faults, fmt.Errorf("actorhost: retiring unit leak: %s", id))
			}
		}
	}
	h.mu.RUnlock()
	return errors.Join(faults...)
}

func waitGroup(ctx context.Context, group *sync.WaitGroup) bool {
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
