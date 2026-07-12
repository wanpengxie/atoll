package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/channelkit"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// OperateExecutor / OperateRequest / OperateError re-export the sysactor operate
// face's injection-point contract at the platform boundary, so the app assembly
// implements it WITHOUT importing platform/internal/sysactor (the internal door
// stays sealed; the contract is the only thing that crosses). The gate does
// permission + routing; the app-supplied executor does the intent write + Home
// call (§2.7-后半 scope 判据: channel-internal control归门内政).
type (
	OperateExecutor = sysactor.OperateExecutor
	OperateRequest  = sysactor.OperateRequest
	OperateError    = sysactor.OperateError
)

// The four channel-operate message types re-exported at the platform boundary so
// the app's HTTP shims can Submit them through the door (Home.Human(u).Submit,
// audience=[system]) WITHOUT importing platform/internal/sysactor. They are the
// door's wire vocabulary — the shim must speak the exact strings the gate
// dispatches on, so a single home avoids drift (same posture as the contract
// type re-exports above; white-list ⑤).
const (
	TypeIntroduceActor  = sysactor.TypeIntroduceActor
	TypeRemoveActor     = sysactor.TypeRemoveActor
	TypeRestartActor    = sysactor.TypeRestartActor
	TypeSetDefaultAgent = sysactor.TypeSetDefaultAgent
)

// HomeConfig configures the channel-home assembly.
type HomeConfig struct {
	ChannelID channelpkg.ID
	DBPath    string
	Logger    *slog.Logger
	// ReconcileInterval tunes the closure reconciler's level safety-net sweep
	// period (the backstop for lost death edges). <=0 → the default. The death
	// edge closes the common case immediately; this sweep is a rare backstop.
	ReconcileInterval time.Duration
	// Desired is the eager-activation reconcile ring's read of intent (the
	// desired−actual diff's desired half). Injected by the app assembly root — the
	// substrate never knows the table behind it and must yield only confirmed
	// durable members. nil → no eager activation (the closure backstop still runs).
	Desired actorrt.DesiredSource
	// Builder is the platform class/id→factory table fork and activation resolve
	// against once the original admission closure is gone. nil → Fork and identity-
	// timer revival fail-fast (structural refusal, never a phantom actor).
	Builder CapsFactoryBuilder
	// Operate is the channel-operate executor injected into the system actor's
	// in-gate control plane (NP-1=c). nil → the four control types are inert (the
	// injection point is unfilled) — the app assembly fills it (executor = intent
	// write + Home-face call; the gate does permission + routing). White-list ⑤.
	Operate OperateExecutor
	// Clock is the schedule engine's time source, forwarded to
	// schedule.AssemblyDeps unchanged. nil → OpenScheduler's own default (the
	// real wall clock). Pulled forward from period 8 (G16) because a fake
	// clock is the only way to drive the engine's poll/backoff loop
	// deterministically in a test (host-attached revive retries pace at
	// schedule.backoffDuration, real wall-clock waits would make the test
	// slow and flaky).
	Clock schedule.Clock
	// ReservationTimeout overrides how long a create-outbox reservation may
	// sit unCommitted before ReconcilePull ages it out (期11 spec §1.7's
	// third reservation-deletion trigger, homeStorageHostControl's own nil-
	// safe-defaulted field). <=0 → defaultReservationTimeout (5m, production
	// default). Additive test-only knob (mirrors ComputeConfig.ScrubberInterval's
	// own shape): a crash-recovery walk driving the "abandoned reservation"
	// timeout path needs a fast, deterministic window rather than production's
	// multi-minute backstop.
	ReservationTimeout time.Duration
	// EventDropKinds is the producer-owned vocabulary of non-level diagnostic obs
	// kinds the presence fold buckets per name (queue overflow, closure fault,
	// checkpoint drop, …). The substrate names no such word: it stays blind to the
	// agent subsystem (archtest TestSubstrateBlindToAgent), so the assembly root
	// that CAN see every producer (app → lib/actorbase.ObsDropKinds ∪ agent/base.
	// ObsDropKinds) hands the union in. Empty → every drop lands in the "unknown"
	// bucket (honest, just uninformative).
	EventDropKinds []actorrt.ObsKind
}

// Home is the assembled channel-home. Its public surface is the capability set in
// the package doc — the bare Runtime/Deliverer/Membership/Registry never escape
// it; assembly only hands out capabilities. The app layer owns HTTP/transport;
// Home is pure Go.
type Home struct {
	channelID     channelpkg.ID
	minter        harness.Minter
	channel       *channelkit.Channel
	cs            *runtime.ChannelStores
	signal        *tap.Signal
	delivery      *tap.Pump
	links         *link.Acceptor
	presenceFold  *presence.Fold
	presenceSwept atomic.Int64
	logger        *slog.Logger
	nowMs         func() int64

	// (No per-user caller index (期12): a subject's own requests are closed
	// by the substrate expiry reaper — 义务归位 D3; the subject drives its
	// cell's caps through the OccupantDriver seam, see human.go.)

	// presenceSessions is the subjectgate door's L3 device-presence session
	// TOKEN SET per subject (channel, user) — 期12 S4's token form: the
	// gateway ws connect/disconnect are the ONLY producer of a human's device
	// presence (常驻 cell 不死, decay 挂 down-edge 对它失效, so the door must
	// explicitly feed — 根基档 §4.6/§6). A (channel,user) may hold several ws
	// (multi-tab/multi-device); online is fed on the FIRST token, offline
	// only when the LAST goes. Each ws holds its OWN token and a disconnect
	// removes only ITSELF — a stale disconnect from a pre-Remove session can
	// never extinguish a re-Admitted sibling's fresh session (the straddle
	// the plain refcount form couldn't rule out). Mutated and fed under
	// presenceMu together, so edges are totally ordered.
	presenceMu       sync.Mutex
	presenceSessions map[actor.ActorID]map[string]struct{}

	// systemPen is the welded system-authored write capability (minted once at
	// Open). Held on Home for the substrate's own enforcement writers — the
	// expiry reaper (sweepExpired) writes its unanswered_timeout terminals
	// through it (义务归位 D3: system-authored, never mint-as-caller).
	systemPen harness.Pen
	// expiryCursor is the reaper's keyset position across ticks (batch
	// fairness only — correctness is the level-scan's; restart-from-zero is
	// harmless). Touched only on the reconcile goroutine, no lock.
	expiryCursor storespec.ExpiryCursor

	// builder is the platform-layer class → caps-factory table (the fork
	// injection-point contract, CapsFactoryBuilder). nil until the domain's
	// factory table is injected — a nil builder makes SpawnHandle.Fork fail-fast
	// with ErrNoBuilder rather than fabricate a child. The table hands back the RAW
	// factory (func(Caps) Actor); the caps weld happens at the platform assembler
	// (buildCaps) when a fork child is born, so a child gets the identical membrane
	// set as a top-level admission (purity: the domain fills WHAT to build, the
	// platform seam owns HOW caps are welded — actorrt never touches harness/link).
	builder CapsFactoryBuilder

	// schedMinter mints a per-actor ScheduleHandle for the caps seam; engine is
	// the time-axis run loop assembled by OpenScheduler. The engine's Start/Close
	// is bound to Home's own open/close lifecycle (only minting a handle without
	// Start would be a cast-but-unwired half-piece).
	schedMinter schedule.Minter
	engine      *schedule.Engine

	// desired is the eager-activation reconcile ring's intent source (nil → no
	// eager activation). prevEagerDesired is the AlwaysOn set the LAST reconcile
	// tick managed — the deactivation diff is prevEagerDesired − currentDesired,
	// NEVER actual − desired (actual mixes in system/human/fork-child/daemon-attach
	// embodiments this ring must never evict). Touched only by the reconcile ticker
	// goroutine (and the one synchronous startup sweep before that goroutine
	// launches), so it needs no lock.
	desired          actorrt.DesiredSource
	prevEagerDesired map[actor.ActorID]bool

	// reconcileStop tears down the closure reconciler ticker (level backstop).
	reconcileStop   context.CancelFunc
	reconcileDone   chan struct{}
	reconcileLeaked atomic.Int64

	// pokeCh is the coalesced activation wake: Admit (a fresh membership write)
	// posts a non-blocking edge here so the reconcile ring runs its next sweep
	// immediately instead of waiting up to one tick (30s) to embody the new
	// member. Buffered size 1 = coalesced (many Admits between ticks fold into one
	// extra sweep). Same shape as tap.Signal's wake. nil-safe: a poke before the
	// ticker goroutine launches is dropped (the synchronous startup sweep already
	// covers genesis).
	pokeCh chan struct{}

	// reviveLogMu/reviveLogAt throttle the attached-host revive-skip log (see
	// reviveLogThrottle) to once per author per window — the schedule engine
	// backs off a transient EnsureLive failure at schedule.backoffDuration (1s)
	// pace, so an attached author's due identity timer would otherwise log
	// once a second for as long as it stays attached. Pure log hygiene, not a
	// correctness mechanism.
	reviveLogMu   sync.Mutex
	reviveLogAt   map[actor.ActorID]time.Time
	reviveBackoff map[actor.ActorID]reviveBackoffEntry

	// reviverStraddleHook is a test-only seam (nil in production): see its call
	// site in homeReviver.EnsureLive.
	reviverStraddleHook func()

	// reconcileBuildHook is a test-only seam (nil in production): fired in
	// reconcileActivation's 补臂 AFTER a real SpawnIfAbsent build lands but BEFORE
	// verifyPostBuild runs — the window a concurrent Home.Remove must be parked in
	// to prove the ring's own post-build straddle recheck self-undoes (mirror of
	// reviverStraddleHook for the reviver arm).
	reconcileBuildHook func(actor.ActorID)

	// closed is set true at the very START of Close (before any teardown step), the
	// authoritative "this home is shutting down" flag every subjectgate verb entry
	// checks. Close cannot JOIN the ws/垫片 goroutines that hold a HumanHandle (they
	// live in the app layer, outside Home's teardown), so the flag is what stops a
	// post-Close verb from touching a closing store: Human()/driverFor refuse from
	// this instant on (期12: verbs hold no capability of their own — the residual
	// in-flight write past the gate is fenced by the cell caps' own live membranes
	// once cells stop). atomic (lock-free read on the hot Submit path).
	closed    atomic.Bool
	state     atomic.Uint32
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
	faults    *homeFaults
}

type homeFaults struct {
	fail     map[string]error
	panicAt  map[string]any
	record   func(string)
	action   map[string]func()
	created  func(*Home)
	delivery func(storespec.StoredRow) error
	// wrapMembership, when set, decorates the membership control plane handed to
	// the link acceptor (only) — a test seam for injecting reconcileHost write
	// faults without touching the membership every other arm reads.
	wrapMembership func(storespec.MembershipControlPlane) storespec.MembershipControlPlane
}

func (f *homeFaults) checkpoint(name string) error {
	if f == nil {
		return nil
	}
	if f.record != nil {
		f.record(name)
	}
	if action := f.action[name]; action != nil {
		action()
	}
	if p, ok := f.panicAt[name]; ok {
		panic(p)
	}
	return f.fail[name]
}

type homeState uint32

const (
	homeConstructing homeState = iota
	homeActivating
	homePublished
	homeClosing
	homeClosed
)

// reviveLogThrottle bounds the attached-host revive-skip log (see
// Home.reviveLogAt).
const reviveLogThrottle = 30 * time.Second

// reconcileInterval is the closure reconciler's low-frequency safety-net sweep
// period. The death edge closes the common case immediately; this level sweep
// catches lost edges (clean despawn / ctx-cancel / open requests predating a
// restart). It is a backstop, not the primary path, so it runs rarely.
const reconcileInterval = 30 * time.Second

// Open assembles the channel home. Assembly is linearised by the tap seam (no
// construction cycle, no back-fill):
//
//	signal -> stores(OnCommit=signal.Notify) -> harness -> channelkit(registers
//	the sysactor factory; Start births it against the live runtime) ->
//	delivery tap -> link acceptor.
func Open(cfg HomeConfig) (*Home, error) { return openHome(cfg, nil) }

func openHome(cfg HomeConfig, faults *homeFaults) (_ *Home, retErr error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("platform: ChannelID required")
	}
	h := &Home{channelID: cfg.ChannelID, logger: logger, closeDone: make(chan struct{}), faults: faults}
	if faults != nil && faults.created != nil {
		faults.created(h)
	}
	h.state.Store(uint32(homeConstructing))
	_ = faults.checkpoint("state.constructing")
	defer func() {
		if p := recover(); p != nil {
			func() {
				defer func() {
					if q := recover(); q != nil {
						logger.Error("home.rollback.panic", "panic", q)
					}
				}()
				_ = h.closeInternal("panic")
			}()
			panic(p)
		}
		if retErr != nil {
			logger.Error("platform.home.rollback", "channel", cfg.ChannelID, "cause", retErr)
			retErr = errors.Join(retErr, h.closeInternal("rollback"))
		}
	}()
	ctx := context.Background()
	nowMs := func() int64 { return time.Now().UnixMilli() }
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	sweepEvery := cfg.ReconcileInterval
	if sweepEvery <= 0 {
		sweepEvery = reconcileInterval
	}

	// 1. Build the commit Signal (tap fan-out). It has NO dependencies, so it is
	//    built first and handed to the store as its post-commit source. The
	//    commit signal belongs to the log append chokepoint (Postgres WAL / Kafka
	//    offset), not to any one writer — so BOTH write paths (request-path Append
	//    and the control-plane membership mirror) fire it through the store,
	//    instead of only the harness path being wrapped.
	signal := tap.NewSignal()
	h.signal = signal
	if err := faults.checkpoint("construct.open_channel"); err != nil {
		return nil, fmt.Errorf("platform: open channel store: %w", err)
	}

	// 2. Open channel stores (substrate), wiring the commit signal as the store's
	//    OnCommit. Now any durable append — request or control-plane — wakes the
	//    tap identically. StorageMounts/StorageControl (期11 §4.3) are LATE-BOUND
	//    here (storagehost.go's lateAcceptor): the ONLY thing that can actually
	//    answer them — the link Acceptor's attach state — is not built until
	//    step 11 below. bindLateAcceptor closes that gap once it exists; any
	//    file-kind Create landing before then sees an honest empty mount list
	//    (§4.3's own "late-bound...延迟解析,调用时才读在线态" escape hatch).
	lateAcc := &lateAcceptor{}
	cs, err := runtime.OpenChannel(ctx, cfg.ChannelID, cfg.DBPath, runtime.OpenChannelOptions{
		OnCommit:       signal.Notify,
		StorageMounts:  lateStorageMounts{acc: lateAcc},
		StorageControl: lateStorageControl{acc: lateAcc},
		LaneControl:    lateLaneControl{acc: lateAcc},
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open channel store: %w", err)
	}
	h.cs = cs

	// 3. Build the harness Minter (the substrate mint machine). New returns a Minter, never a
	//    bare chain — the bare writer's visibility is compile-time capped inside the
	//    harness package. Every admission point (Spawn / attach / system closure)
	//    Mints a Pen welded to (actorID, chID); the welded identity is unforgeable
	//    by the holder. The post-commit Notify lives at the store append chokepoint,
	//    so there is no write-gate wrapper layer.
	minter, err := harness.New(harness.Deps{
		ChannelID: cfg.ChannelID,
		Log:       cs.Log,
		Logger:    logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	// The system Pen: every system-authored write (sysactor serve responses,
	// channelkit's author#3 closure terminals) commits through this welded pen, so
	// sender==SystemActorID rides each write by construction — no caller stamping
	// at the call sites.
	systemPen := minter.Mint(actor.SystemActorID, actor.KindSystem, cfg.ChannelID)

	// 4. Bootstrap: register the intrinsic system actor so its substrate-death
	//    terminals pass harness sender validation. Idempotent SEED: on a home
	//    restart over a persistent channel DB the row already exists, and a raw
	//    re-Insert would PK-conflict (actor_id is the table key) — failing Open
	//    before the restart-recovery reconciler below can even run. Insert itself
	//    stays strict (a duplicate is an error, locked by the store's
	//    coverage test); the idempotent seed lives here at the genesis call site
	//    (guard the idempotency at the platform bootstrap call site — do not relax substrate).
	if err := faults.checkpoint("construct.ensure_system"); err != nil {
		return nil, fmt.Errorf("platform: register system actor: %w", err)
	}
	if err := cs.Membership.EnsureSystemActor(ctx, nowMs()); err != nil {
		return nil, fmt.Errorf("platform: register system actor: %w", err)
	}

	// 5. Presence fold: mechanism-only latest-value cache. Both vocabularies are
	// an assembly concern: the level-kind set (folded testimony) and the injected
	// event-drop-kind set (producer-owned diagnostic buckets, see HomeConfig).
	presenceFold := presence.New(logger, clock,
		[]actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, cfg.EventDropKinds, sweepEvery)

	// 6. channelkit: actorrt runtime + sysactor + death-edge wiring. The system
	//    cell is built against the LIVE runtime (factory) — its liveness Stat seam
	//    reads the real runtime at construction, no back-filled pointer.
	//
	//    h is predeclared (nil) here and assigned below (step 9): sysactor is a
	//    ring0 special Proc (spec §3's out-generation matrix) that still enters
	//    through actorbase.New like every other actor, so its Hooks.Canceller
	//    wants Home.CancelRequest — but the system cell's factory is registered
	//    at channelkit.New (and the cell birthed at channel.Start), before Home
	//    is assigned. The closure captures the h VARIABLE (not its zero value);
	//    by the time a cancel actually fires (long after Open returns), h has
	//    been assigned.
	channel, err := channelkit.New(channelkit.Config{
		ChannelID: cfg.ChannelID,
		System: func(rt *actorrt.Runtime, inc actorrt.Incarnation) actorrt.Actor {
			// S6 Q5: the ring0 anchor's four caps arms装真 — all RAW (no
			// incarnation membrane), the anchor姿势 the system pen already wears
			// (权威自身不设 incarnation 门). Access/State are eager (the access
			// door is assembled by storeopen, before channelkit); Schedule/Spawn
			// are late-bound (their engines are assembled after this cell is born
			// — see sysanchorcaps.go), captured through the same h variable
			// Hooks.Canceller uses.
			homeOf := func() *Home { return h }
			caps := actorcaps.Caps{
				Pen:      systemPen,
				Access:   cs.Access.Mint(actor.SystemActorID),
				State:    cs.Access.MintState(actor.SystemActorID),
				Schedule: systemScheduleHandle{home: homeOf},
				Spawn:    systemSpawnHandle{inc: inc, home: homeOf},
			}
			hooks := actorbase.Hooks{
				Canceller: func(target actor.ActorID, requestID message.ID) {
					if h != nil {
						h.CancelRequest(target, requestID)
					}
				},
			}
			return actorbase.New(caps, hooks, sysactor.Def(sysactor.Deps{
				Registry: cs.Registry,
				Clock:    clock,
				Presence: presence.NewView(presenceFold, rt, cs.Registry),
				Logger:   logger,
				Operate:  cfg.Operate,
			}))
		},
		SystemPen:    systemPen,
		OpenRequests: cs.Query,
		// ClosedForever is closure's monotone predicate (拔根 #14/#15): a receiver's
		// callers are closed with receiver_unavailable ONLY when it is deregistered
		// or never a member — the irreversible facts (an actor id is minted once and
		// never reused; dereg never reverts). Same classification as the scheduler's
		// not_a_member reject. A crashed-but-still-registered receiver returns false
		// here and is left to the expiry reaper (its callers wait for the request
		// deadline) — no liveness snapshot is ever a terminal-write dependency.
		ClosedForever: func(ctx context.Context, id actor.ActorID) (bool, error) {
			rec, ok, err := cs.Registry.Lookup(ctx, id)
			if err != nil {
				return false, err // transient: skip this round, the reconciler retries next tick.
			}
			return !ok || !rec.IsActive(), nil
		},
		Clock:  clock,
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: construct channel: %w", err)
	}
	h.channel = channel

	// 7. Build the delivery tap: a Pump over the Signal-fed Deliverer. cursor start
	//    = current MaxSeq (mailbox semantics: only new commits). DeliverResult
	//    lands here as structured per-audience logs.
	if err := faults.checkpoint("construct.max_seq"); err != nil {
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	deliver := deliveryHandle(channel.Deliverer(), cfg.ChannelID, logger)
	if faults != nil && faults.delivery != nil {
		deliver = faults.delivery
	}

	// 8. Register the device-presence fold once for the runtime population. Every
	//    actor's obs wire naturally feeds this single fanout subscription; attach
	//    churn therefore needs no per-actor watcher bookkeeping.
	channel.Cells().WatchDown(presenceFold)
	channel.Cells().WatchObsAll(presenceFold)

	// 9. Assemble the Home shell now: the scheduler's Reviver and the eager
	//    reconcile arm both close over it (buildCaps, builder, cells), so it must
	//    exist before those are wired. schedMinter/engine/links are filled below —
	//    the link acceptor is built AFTER the scheduler because it welds a remote
	//    port's incarnation onto the schedule Minter (the time-axis wire arm), which
	//    only exists once OpenScheduler has run.
	h.channelID = cfg.ChannelID
	h.minter = minter
	h.channel = channel
	h.cs = cs
	h.signal = signal
	h.delivery = nil
	h.presenceFold = presenceFold
	h.logger = logger
	h.nowMs = nowMs
	h.builder = cfg.Builder
	h.desired = cfg.Desired
	h.prevEagerDesired = map[actor.ActorID]bool{}
	h.systemPen = systemPen
	h.reviveLogAt = map[actor.ActorID]time.Time{}
	h.reviveBackoff = map[actor.ActorID]reviveBackoffEntry{}
	h.pokeCh = make(chan struct{}, 1)

	// 10. Time axis (OpenScheduler). FireSink mints a pen per fire (author-welded);
	//     Reviver activates an absent identity-timer author via SpawnIfAbsent. The
	//     engine is Started here and Closed in Home.Close (minting a handle without
	//     Start would be a cast-but-unwired half-piece). BOOT-ORDER红线: the Reviver
	//     is wired and the engine is Started BEFORE the first reconcile sweep below,
	//     because an overdue fire on Start can precede the eager ring re-minting the
	//     always-on set — and append has no backfill, so the wake must be revivable
	//     from the first instant.
	rt := channel.Cells()
	if err := faults.checkpoint("construct.open_scheduler"); err != nil {
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}
	schedMinter, engine, err := runtime.OpenScheduler(cs, schedule.AssemblyDeps{
		Fire:   fireSink{minter: minter, registry: cs.Registry, rt: rt, chID: cfg.ChannelID},
		Host:   rt,
		Revive: homeReviver{h: h},
		Clock:  cfg.Clock,
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}
	h.schedMinter = schedMinter
	h.engine = engine

	// 11. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//      + lease judgement for attached computes). Still Construct — pure
	//      fallible preparation, zero goroutines: NewAcceptor only allocates its
	//      tables (Serve is what runs links, and nothing serves until the app
	//      binds an HTTP route after Open returns). It welds an attaching remote
	//      port's incarnation onto the same three minters a local cell's Caps draw
	//      from — the harness pen Minter, the access door (cs.Access), and the
	//      schedule engine Minter (which is why it must follow OpenScheduler) —
	//      so a daemon-hosted cell's message / off-log / time-axis capability is
	//      behaviourally identical to a local one (transport neutrality).
	//      Attached-port obs enters the runtime's one population subscription
	//      just like local-cell obs.
	acceptorMembership := storespec.MembershipControlPlane(cs.Membership)
	if faults != nil && faults.wrapMembership != nil {
		acceptorMembership = faults.wrapMembership(cs.Membership)
	}
	links := link.NewAcceptor(link.Config{
		Minter:             minter,
		Access:             cs.Access,
		Schedule:           schedMinter,
		Runtime:            rt,
		Membership:         acceptorMembership,
		Registry:           cs.Registry,
		ChannelID:          cfg.ChannelID,
		Logger:             logger,
		CancelRequest:      h.handleCancelUpstream,
		StorageHostControl: homeStorageHostControl{outbox: cs.Outbox, timeout: cfg.ReservationTimeout},
	})
	h.links = links

	// 12. Activate: construct is complete (every fallible preparation done, all
	//     ownership already in h) — start the components: channel cells, the
	//     schedule engine, then the delivery pump.
	h.state.Store(uint32(homeActivating))
	_ = faults.checkpoint("state.activating")
	if err := faults.checkpoint("activate.channel_start"); err != nil {
		return nil, fmt.Errorf("platform: start channel: %w", err)
	}
	if err := channel.Start(); err != nil {
		return nil, fmt.Errorf("platform: start channel: %w", err)
	}
	engine.Start()
	if err := faults.checkpoint("activate.before_pump"); err != nil {
		return nil, err
	}
	h.delivery = tap.OpenPump(signal, cs.Query, from, deliver, logger)

	// Close the late-binding window (see step 2's lateAcc note): every
	// file-kind placement decision from this instant on can actually route an
	// AllocRequest / see attached daemons as storage-mount candidates.
	if err := faults.checkpoint("publish.bind"); err != nil {
		return nil, err
	}
	lateAcc.bind(links)

	// 13. Reconcilers (level backstops). Run one sweep of EACH at startup —
	//     activation re-mints the always-on desired set; closure closes orphan open
	//     requests whose receiver is closed forever (deregistered / never a member)
	//     — a lost dereg edge, or a dereg that predates this process. Then a
	//     low-frequency ticker keeps both as the safety net for any lost death edge
	//     / intent change. The death edge (OnDown) remains the lossy fast-path for
	//     closure. Step order is fixed inside reconcileSweep (see its head).
	if err := faults.checkpoint("publish.sweep"); err != nil {
		return nil, err
	}
	h.reconcileSweep(ctx)
	reconcileCtx, reconcileStop := context.WithCancel(context.Background())
	reconcileDone := make(chan struct{})
	h.reconcileStop = reconcileStop
	h.reconcileDone = reconcileDone
	go func() {
		defer close(reconcileDone)
		t := time.NewTicker(sweepEvery)
		defer t.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-t.C:
				h.reconcileSweep(reconcileCtx)
			case <-h.pokeCh:
				// Admit poke: run the same sweep off-tick so a freshly-admitted member
				// embodies without the ≤30s wait.
				h.reconcileSweep(reconcileCtx)
			}
		}
	}()
	if err := faults.checkpoint("publish.goroutine_started"); err != nil {
		return nil, err
	}
	h.state.Store(uint32(homePublished))
	_ = faults.checkpoint("state.published")
	if err := faults.checkpoint("publish.published"); err != nil {
		return nil, err
	}
	logger.Info("platform.home.ready", "channel", string(cfg.ChannelID))
	return h, nil
}

// reconcileSweep runs the home's level-reconcile quartet in the fixed
// activation → closure → expiry → presence order; each step re-checks ctx so a
// cancel between steps stops the sweep before the next component.
//
// Closure (channel.Reconcile) authors a terminal ONLY on the monotone
// closed-forever fact (deregistered / never a member), never on liveness — so a
// receiver whose desired cell the ring has not yet (re)minted this sweep is still
// a registered member and is left untouched (its open requests wait for the
// deadline reaper), regardless of sweep order. Keying closure on the irreversible
// dereg fact instead of a reversible liveness dip dissolves the old
// "not-yet-minted cell mis-scanned as a corpse" hazard; activation is kept first
// as the natural order — re-mint the always-on desired set before the backstops.
func (h *Home) reconcileSweep(ctx context.Context) {
	h.reconcileActivation(ctx)
	if ctx.Err() != nil {
		return
	}
	h.channel.Reconcile(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepExpired(ctx)
	if ctx.Err() != nil {
		return
	}
	h.sweepPresence(ctx)
}

// sweepPresence enforces fold rows ⊆ (live embodiments ∪ active membership).
// A failed registry read skips the whole pass: treating failure as an empty set
// would erase every member's last testimony.
func (h *Home) sweepPresence(ctx context.Context) {
	rows, err := h.cs.Registry.ListActive(ctx)
	if err != nil {
		h.logger.Warn("platform.presence.sweep_registry_failed", "error", err)
		return
	}
	keep := make(map[actor.ActorID]struct{}, len(rows))
	for _, row := range rows {
		keep[row.ID] = struct{}{}
	}
	for _, id := range h.channel.Cells().LiveIDs() {
		keep[id] = struct{}{}
	}
	removed := h.presenceFold.Sweep(func(id actor.ActorID) bool {
		_, ok := keep[id]
		return ok
	})
	if removed > 0 {
		h.presenceSwept.Add(int64(removed))
		h.logger.Debug("platform.presence.swept", "rows", removed)
	}
}

// PresenceSweptCount reports how many testimony rows the reconciliation
// backstop has cleared over this Home's lifetime.
func (h *Home) PresenceSweptCount() int64 {
	return h.presenceSwept.Load()
}

// logReviveAttached logs, throttled per author (reviveLogThrottle), that an
// identity-timer wake was skipped because its author is currently placed on a
// daemon (§10.13 推导2/3) rather than home.
func (h *Home) logReviveAttached(id actor.ActorID, host string) {
	now := time.Now()
	h.reviveLogMu.Lock()
	if last, ok := h.reviveLogAt[id]; ok && now.Sub(last) < reviveLogThrottle {
		h.reviveLogMu.Unlock()
		return
	}
	h.reviveLogAt[id] = now
	h.reviveLogMu.Unlock()
	h.logger.Warn("platform.revive.attached", "channel", string(h.channelID), "actor", string(id), "host", host)
}

// reconcileActivation is the eager-activation half of the reconcile ring: it
// mints the desired−actual difference and deactivates the members it previously
// managed that are no longer desired. It is a substrate assembly-layer mechanism
// (the reconcile ring骨架 lives here in platform, not the actorrt kernel), driven
// by the same ticker as the closure backstop.
//
// desired = 两源之并 (union of two intent sources), read to completion BEFORE any
// mint/despawn: 组合域 — the app-injected DesiredSource (channel_actors intent
// rows); user域 — platform-internal, derived from THIS channel's own registry (the
// per-channel human members, Kind==human && Host==""). The user域 authority lives
// only inside the channel truth born within Open, so the app cannot enumerate it
// (chicken-egg) — the ring derives it itself. Union 原子性: either source read
// failing aborts the WHOLE tick — no arm runs and prevEagerDesired is NOT updated,
// so 削臂 can never diff against a truncated current and evict live cells wholesale.
//
// 补 (revive): for every desired member absent from the live set, resolve its
// factory through factoryFor (human → the platform's own built-in cell factory;
// others → the app-injected组合域 builder), weld caps at the platform seam, and
// SpawnIfAbsent it (the CAS discards the shell if some other path won the race —
// admission or a concurrent Reviver fire). Kind comes from the MEMBERSHIP record
// (rec.Kind, the authority), never re-answered by the builder.
//
// 交集红线: desired = intent ∩ durable membership. A组合域 intent row whose Admit
// never landed (crash between the two non-atomic writes) is not in the membership
// snapshot, so it is skipped BEFORE current[id]=true — a非成员 must never enter the
// 削臂 management set.
//
// 削 (deactivate, 反误杀): the diff is prevEagerDesired − currentDesired, NEVER
// actual − desired. LiveIDs() mixes in system / fork-child / daemon-attach
// embodiments this ring must never evict — the protected categories that never
// enter the desired set (human is NO LONGER protected: it is a managed member now
// — its Admit puts it in desired, its removal takes it out and the 削臂 evicts it,
// membership-gone = true death). The 削 set is every id that was desired in the
// prior tick and is no longer desired.
//
// placement filter (§10.13 推导2/7, S6): both arms consult the membership Host
// column BEFORE embodying/evicting — an id currently placed on a daemon (Host != "")
// is this ring's business in neither direction: 补 must not double-embody an
// already-attached identity locally, and 削 must not evict an embodiment that is
// legitimately live elsewhere.
func (h *Home) reconcileActivation(ctx context.Context) {
	var composed []actorrt.DesiredMember
	if h.desired != nil {
		m, err := h.desired.Members(ctx)
		if err != nil {
			h.logger.Error("platform.reconcile.desired_failed", "channel", string(h.channelID), "err", err)
			return
		}
		composed = m
		if ctx.Err() != nil {
			return
		}
	}
	actives, err := h.cs.Registry.ListActive(ctx)
	if err != nil {
		h.logger.Error("platform.reconcile.registry_failed", "channel", string(h.channelID), "err", err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	// The active-membership snapshot serves BOTH roles, read once for atomicity: it
	// is the user域 source AND the 交集红线 membership filter (+ the placement Host
	// read the two arms consult).
	member := make(map[actor.ActorID]storespec.Record, len(actives))
	for _, rec := range actives {
		member[rec.ID] = rec
	}
	desiredIDs := make(map[actor.ActorID]bool, len(composed)+len(actives))
	for _, m := range composed {
		desiredIDs[m.ID] = true
	}
	for _, rec := range actives {
		if rec.Kind == actor.KindHuman && rec.Host == "" {
			desiredIDs[rec.ID] = true
		}
	}

	rt := h.channel.Cells()
	now := time.UnixMilli(h.nowMs())
	actual := make(map[actor.ActorID]bool)
	for _, id := range rt.LiveIDs() {
		actual[id] = true
	}
	current := make(map[actor.ActorID]bool)
	for id := range desiredIDs {
		if ctx.Err() != nil {
			return
		}
		rec, ok := member[id]
		if !ok || !rec.IsActive() {
			continue // 交集红线: desired-but-not-a-durable-member — skip BEFORE current
		}
		current[id] = true
		if actual[id] {
			h.clearReviveBackoff(id)
			continue
		}
		if rec.Host != "" {
			continue // attached elsewhere — not this ring's authority to embody (反误杀)
		}
		if _, held := h.backoffGate(id, now); held {
			// Build backoff active (a prior BuildFailure has not elapsed): skip the
			// build this tick — the SAME account EnsureLive maintains, so the ring and
			// the reviver back a failing actor off in lockstep instead of the ring
			// re-hammering it every tick/poke while the reviver waits. Drop id from
			// current exactly as the build-failure arm below does: a member that never
			// embodied is not carried as managed into prevEagerDesired.
			delete(current, id)
			continue
		}
		factory, ok := h.factoryFor(rec)
		if !ok {
			h.logger.Warn("platform.reconcile.no_factory", "channel", string(h.channelID), "actor", string(id))
			continue
		}
		kind := rec.Kind
		mid := id
		inc, built, buildErr := rt.SpawnIfAbsent(mid, kind, func(inc actorrt.Incarnation) actorrt.Actor {
			return build(h.buildCaps(mid, kind, inc), h.hooks(), factory)
		})
		if ctx.Err() != nil {
			if built {
				rt.Despawn(inc)
			}
			return
		}
		if buildErr != nil {
			if errors.Is(buildErr, actorrt.ErrRuntimeSealed) {
				h.logger.Info("platform.reconcile.runtime_sealed", "channel", string(h.channelID), "actor", string(mid))
				return
			}
			h.logger.Error("platform.reconcile.build_failed", "channel", string(h.channelID), "actor", string(mid), "error", buildErr)
			var failure *actorrt.BuildFailure
			if errors.As(buildErr, &failure) {
				h.recordBuildFailure(mid, now)
			}
			delete(current, mid)
			continue
		}
		// Post-build straddle recheck (shared verifyPostBuild, mirror of the reviver
		// arm's S-P20 recheck): a concurrent Home.Remove (dereg) or daemon attach (Host
		// stamp) can land BETWEEN the Lookup that admitted mid into current above and
		// this build. On a real build (CAS winner), re-read under inc — on any non-OK
		// outcome verifyPostBuild undoes it (pointer-guarded Despawn) and we drop mid
		// from current so it is neither counted managed nor carried into
		// prevEagerDesired: resurrecting a dead-write cell past its dereg (into an
		// unhoused cell, death-后写 window) is the笔 this closes. A lost CAS (!built) is
		// a no-op — some other path already owns the embodiment.
		if built {
			if h.reconcileBuildHook != nil {
				h.reconcileBuildHook(mid)
			}
			if _, res, _ := h.verifyPostBuild(ctx, mid, inc); res != recheckOK {
				delete(current, mid)
			}
		}
	}
	for id := range h.prevEagerDesired {
		if ctx.Err() != nil {
			return
		}
		if current[id] {
			continue
		}
		if rec, ok := member[id]; ok && rec.Host != "" {
			continue // attached elsewhere — not gone, not this ring's to evict (反误杀)
		}
		rt.DespawnID(id)
	}
	h.prevEagerDesired = current
}

// factoryFor is the single activation-dispatch point shared by the reconcile
// ring's 补臂 and homeReviver.EnsureLive: it maps a durable member record to the
// ActorFactory that embodies it. A human member (Kind==KindHuman) resolves to the
// platform's OWN built-in human cell factory — user域 supply is platform internal
// 政 (the per-channel human member's authority lives only in this channel's
// registry, unreachable by the app), so the app-injected builder is never asked
// for it. Every other kind resolves through the组合域 builder table (nil builder →
// not-found). Kind is caller-held (rec.Kind), never re-answered.
func (h *Home) factoryFor(rec storespec.Record) (ActorFactory, bool) {
	if rec.Kind == actor.KindHuman {
		return humanCellFactory(h, rec.ID), true
	}
	if h.builder == nil {
		return ActorFactory{}, false
	}
	return h.builder.Lookup(rec.ID)
}

// recheckResult classifies a post-build straddle recheck (verifyPostBuild).
type recheckResult int

const (
	recheckOK       recheckResult = iota // still an active home-placed member — keep the build
	recheckGone                          // Remove'd / not a durable member — build undone
	recheckAttached                      // daemon-attached mid-build — build undone
	recheckFault                         // registry lookup fault — build undone
)

// verifyPostBuild is the shared straddle-window closure for every eager LOCAL birth
// (the reconcile ring's 补臂 and homeReviver.EnsureLive): after SpawnIfAbsent mints
// inc, re-read the registry NOW and confirm id is still an active, HOME-placed
// member. The window it closes: a concurrent Home.Remove (dereg) or a daemon attach
// (Host stamp) landing BETWEEN the pre-build Lookup and this build's landing. On any
// non-OK outcome it UNDOES the build — Despawn is pointer-guarded (evicts IFF the
// runtime map still points to THIS inc, so a legitimate successor that already
// re-admitted the id is never evicted) — and returns the classification so each
// caller maps it to its own contract (EnsureLive → transient err / poison verdict;
// the ring → drop from current/managed). rec carries the fresh Host for the
// attached case's log; err is set only for recheckFault (the caller wraps it).
func (h *Home) verifyPostBuild(ctx context.Context, id actor.ActorID, inc actorrt.Incarnation) (rec storespec.Record, res recheckResult, err error) {
	rec2, ok2, lerr := h.cs.Registry.Lookup(ctx, id)
	if ctx.Err() != nil {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckFault, ctx.Err()
	}
	if lerr != nil {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckFault, lerr
	}
	if !ok2 || !rec2.IsActive() {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckGone, nil
	}
	return rec2, recheckOK, nil
}

// View returns the read-only observation set (ReadAfterSeq / MaxSeq /
// ListActors / daemon attachment). It carries no write capability — observation
// only. The host (app) reads these projections OUT-OF-BAND (no message, no
// truth-log write) — UI status polling must not pollute the log; in-universe
// actors instead ask the system actor by message (that path is logged).
func (h *Home) View() View {
	return View{
		query:    h.cs.Query,
		registry: h.cs.Registry,
		links:    h.links,
		presence: presence.NewView(h.presenceFold, h.channel.Cells(), h.cs.Registry),
		rt:       h.channel.Cells(),
		nowMs:    h.nowMs,
	}
}

// Admit registers one actor as durable channel membership truth and nothing more
// — the pure-membership primitive (the not→member edge of §4.6). It writes a
// NEUTRAL row (Binding="" / Host=""): membership precedes embodiment, and the
// host path (daemon attach / activation ring) owns Binding/Host stamping — Admit
// never guesses placement. It does not Mint a pen or place a cell; the desired
// member is embodied by the reconcile ring's SpawnIfAbsent (activation) or a
// daemon attach, never by Admit itself. After the write it pokes the ring so the
// embodiment lands on the next immediate sweep rather than waiting a full tick.
// Idempotent for an already-active (kind, principal): the registry returns the
// existing minted instance id and the extra reconcile poke is harmless.
func (h *Home) Admit(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, error) {
	if h.closed.Load() {
		return "", ErrClosed
	}
	id, err := h.cs.Membership.Admit(ctx, kind, principal, h.nowMs())
	if err != nil {
		return "", fmt.Errorf("platform: Admit membership: %w", err)
	}
	h.pokeReconcile()
	h.logger.Info("platform.member.admitted", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind), "principal", principal)
	return id, nil
}

// PrincipalOf returns the opaque principal recorded for an actor instance.
func (h *Home) PrincipalOf(ctx context.Context, id actor.ActorID) (string, bool, error) {
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil || !ok {
		return "", ok, err
	}
	return rec.Principal, true, nil
}

func (h *Home) ResolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	reg, ok := h.cs.Registry.(storespec.PrincipalRegistry)
	if !ok {
		return "", false, errors.New("platform: principal registry unavailable")
	}
	rec, found, err := reg.LookupActivePrincipal(ctx, kind, principal)
	return rec.ID, found, err
}

// pokeReconcile posts a coalesced wake to the reconcile ring (non-blocking: a
// full buffer already carries the pending edge). No-op if the ticker goroutine
// has not launched yet (genesis is covered by the synchronous startup sweep).
func (h *Home) pokeReconcile() {
	if h.pokeCh == nil {
		return
	}
	select {
	case h.pokeCh <- struct{}{}:
	default:
	}
}

// hooks is the actorbase engine's per-host wiring for every Proc-shaped def
// this Home builds (spec §3's out-generation matrix, row 1): a cell host always
// has a live CancelRequest reach, so Hooks.Canceller is never nil here (the
// daemon-side gap — no caller-side cancel upstream frame — is a DIFFERENT
// host, wired in compute.go instead).
func (h *Home) hooks() actorbase.Hooks {
	return actorbase.Hooks{Canceller: h.CancelRequest}
}

// CancelRequest reaches the request-scope of cancel(scope) for one in-flight
// request `target` is running under `requestID`. Home holds the runtime
// directly (cell/port hosting is transport-neutral inside it — CancelRequest
// reaches a daemon-hosted port the same way it reaches a local cell) so this
// is a direct call, no Acceptor indirection needed. No-op if the request
// already closed or `target` has no live embodiment — cancel is a
// best-effort hint, the caller's closure owns the terminal.
func (h *Home) CancelRequest(target actor.ActorID, requestID message.ID) {
	h.channel.Cells().CancelRequest(target, requestID)
}

// handleCancelUpstream is the home's disposition for one KindCancelRequest frame
// (the caller-side upstream twin of CancelRequest): a daemon-hosted caller,
// identified by the connection's authenticated bound id, abandons one of ITS OWN
// outbound requests named by requestID. The caller self-reports NEITHER the
// request's target NOR its own identity — the home takes both from truth: it
// reverse-resolves the request envelope from the log by id, reads the target from
// its audience, and validates that the request's sender == the connection's bound
// id (a half-trusted daemon may only revoke a request it actually authored). Four
// failure branches — not found / non-request kind / empty audience / sender
// mismatch — all silently drop + log (best-effort no-ack semantics: an upstream
// cancel is a hint, never a verdict; the caller's own closure already owns the
// terminal and the request's deadline still collapses its reqCtx). On the happy
// path it fires Home.CancelRequest(target, requestID) — the exact same reach a
// local cell's Hooks.Canceller takes.
func (h *Home) handleCancelUpstream(boundID actor.ActorID, requestID message.ID) {
	req, ok, err := h.cs.Requests.FindByID(context.Background(), requestID)
	if err != nil || !ok {
		h.logger.Info("platform.home.cancel_upstream.not_found", "request", string(requestID), "sender", string(boundID), "err", err)
		return
	}
	if req.Kind != message.KindRequest {
		h.logger.Info("platform.home.cancel_upstream.not_a_request", "request", string(requestID), "kind", string(req.Kind), "sender", string(boundID))
		return
	}
	if len(req.Audience) == 0 {
		h.logger.Info("platform.home.cancel_upstream.empty_audience", "request", string(requestID), "sender", string(boundID))
		return
	}
	if req.Sender.ID != boundID {
		h.logger.Info("platform.home.cancel_upstream.sender_mismatch", "request", string(requestID), "sender", string(boundID), "authored_by", string(req.Sender.ID))
		return
	}
	h.CancelRequest(req.Audience[0], requestID)
}

// KickDaemon closes every link this compute currently holds (the substrate
// half of a revocation, §8.3) and returns the count closed. It is a write
// handle (unlike View, a read-only face) — revoking access is a write. The
// authority to decide WHEN to kick (a daemon's credential was just revoked)
// lives entirely in the app layer; this method only executes the mechanical
// teardown. Kicked ports fall silent (quiet-stop, no receiver_unavailable) —
// a kick is a voluntary revocation, not an observed death.
func (h *Home) KickDaemon(computeID string) int {
	return h.links.KickDaemon(computeID)
}

// buildCaps assembles the caps bundle — the five-capability bundle welded to (id,
// inc). Handing out the handle and wrapping the live membrane happen in the same
// step (invariant: no bare handle escapes). It is the SINGLE
// caps assembler, shared by activation and by fork (spawnHandle.Fork
// holds this method value as its capsAssembler and re-runs it against each child's
// incarnation) — so a fork child is born with the IDENTICAL membrane set as a
// top-level admission (recursive assembly), never a raw un-membraned closure.
//
// Wired this period: Pen (livePen over the harness pen), Access + State
// (liveAccess over the channel-scoped Mint and actor-scoped MintState handles —
// cs.Access is already assembled by storeopen, drawn directly), Spawn (the
// by-incarnation fork/despawn handle; builder may be nil → Fork fail-fasts).
//
// Schedule is welded over the schedule engine's per-author ScheduleHandle
// (h.schedMinter, assembled by OpenScheduler at Open step 10) inside the same
// liveSchedule membrane the other caps wear — self-targeted timers gated on this
// incarnation still being live. schedMinter is always set before any participant
// admission (the system cell does not pass through buildCaps).
func (h *Home) buildCaps(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps {
	rt := h.channel.Cells()
	return actorcaps.Caps{
		Pen:      link.NewLivePen(h.minter.Mint(id, kind, h.channelID), inc, rt),
		Access:   link.NewLiveResourceAccess(h.cs.Access.Mint(id), inc, rt),
		State:    link.NewLiveAccess(h.cs.Access.MintState(id), inc, rt),
		Schedule: link.NewLiveSchedule(h.schedMinter.Mint(id), inc, rt),
		// The child assembler is buildChildCaps, NOT buildCaps: every fork
		// descendant is an incarnation-level citizen (spec §4.1), so its private
		// state must be per-incarnation memory, not this durable MintState arm. Any
		// actor's fork children — top-level or itself a child — take that path.
		Spawn: newSpawnHandle(inc, rt, h.builder, h.buildChildCaps, h.hooks()),
	}
}

// buildChildCaps is the FORK-CHILD caps assembler (spec §4.1 户籍轴): identical to
// buildCaps except the State arm is a per-incarnation in-memory backend instead of
// the durable actor_state MintState. substrate-本质: a fork child holds no durable
// name分, so it holds no durable state — a fresh empty memStateStore is minted per
// Fork and welded to THIS incarnation, so it evaporates with the incarnation and a
// same-named reincarnation inherits nothing (EH2 root-cure, spec P1-2). The other
// four arms (Pen / Access / Schedule / Spawn) are byte-for-byte buildCaps's — a
// child writes truth, reads/writes父授 workspace resources, arms incarnation timers,
// and forks its own (memory-state) children exactly like any actor.
func (h *Home) buildChildCaps(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps {
	caps := h.buildCaps(id, kind, inc)
	caps.State = link.NewLiveAccess(accessdoor.NewMemoryStateHandle(id), inc, h.channel.Cells())
	return caps
}

// ServeAttach is the attach admission surface: the app hands an upgraded WS request here so a
// daemon can attach its actor streams. Home keeps the internal link acceptor and
// only exposes this capability — the acceptor object never escapes.
func (h *Home) ServeAttach(w http.ResponseWriter, r *http.Request, daemonID string) {
	if h.closed.Load() || h.links == nil {
		http.Error(w, "home closed", http.StatusServiceUnavailable)
		return
	}
	h.links.Serve(w, r, daemonID)
}

// Subscribe is the subscription registration surface (client push): a client stream subscribes to
// the commit Signal and reads forward from its own seq cursor. It returns the
// wake channel and the unsubscribe func — the internal Signal never escapes.
func (h *Home) Subscribe() (<-chan struct{}, func()) {
	if h.closed.Load() {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	return h.signal.Subscribe()
}

// Close tears down the channel home by quiescing PRODUCERS before the CONSUMERS
// they feed, so no still-live producer can enqueue work into an already-dead sink:
//
//  1. reconcile ticker — stops the SpawnIfAbsent/activation producer.
//  2. link acceptor    — stops port producers (their schedule/access wire arms)
//     and all external compute traffic.
//  3. delivery tap      — stops delivering messages that would drive a cell to
//     schedule/emit anew.
//  4. cells             — stops every in-proc cell (the Schedule/emit producers);
//     their goroutines are joined.
//  5. schedule engine   — only NOW, with every schedule PRODUCER gone, is the time
//     engine's run loop stopped. Stopping it earlier (the old "engine first" order)
//     left a window where a still-live cell/port could Schedule() an in-memory
//     (incarnation-bind) timer into a dead run loop, silently losing it.
//  6. channel stores    — the DB the engine fired into (FireSink→pen→log) and cells
//     persisted to, torn down last.
//
// No deadlock: cell shutdown never blocks on the engine (Schedule/Cancel only
// insert into mem + post a wake, never join the run loop), and engine.Close only
// joins its own run goroutine (fireDue's Append/Revive never block on the
// already-stopped cells).
func (h *Home) Close() error { return h.closeInternal("normal") }

func (h *Home) closeInternal(reason string) error {
	return h.closeInternalWithin(reason, 5*time.Second)
}

func (h *Home) closeInternalWithin(reason string, reconcileTimeout time.Duration) error {
	h.closeOnce.Do(func() {
		started := time.Now()
		defer close(h.closeDone)
		var errs []error
		addErr := func(err error) {
			if err != nil {
				errs = append(errs, err)
			}
		}
		var teardownPanic any
		guard := func(name string, fn func()) {
			defer func() {
				if p := recover(); p != nil {
					if teardownPanic == nil {
						teardownPanic = p
					}
					h.logger.Error("home.teardown.panic", "step", name, "panic", p)
				}
			}()
			fn()
		}
		step := func(name string, fn func()) {
			guard(name+".checkpoint", func() {
				if err := h.faults.checkpoint(name); err != nil {
					errs = append(errs, err)
				}
			})
			guard(name, fn)
		}
		// Step ZERO seals the construction authority — before even the closing
		// state flip, so there is no instant at which the Home is observably
		// "entering close" while the runtime still admits new embodiments
		// (公理 7: 进入关闭即封门, and close entry IS this line).
		step("close.seal", func() {
			if h.channel != nil && h.channel.Cells() != nil {
				h.channel.Cells().Seal()
			}
		})
		step("close.begin", func() {
			h.state.Store(uint32(homeClosing))
			_ = h.faults.checkpoint("state.closing")
			h.closed.Store(true)
		})
		step("close.reconcile", func() {
			if h.reconcileStop == nil {
				return
			}
			h.reconcileStop()
			if h.reconcileDone == nil {
				return
			}
			select {
			case <-h.reconcileDone:
			case <-time.After(reconcileTimeout):
				h.reconcileLeaked.Add(1)
				h.logger.Error("home.reconcile.join_timeout", "timeout", reconcileTimeout,
					"safety", "runtime admission sealed; late writes hit closed stores")
			}
		})
		step("close.links", func() {
			if h.links != nil {
				addErr(h.links.Close())
			}
		})
		step("close.delivery", func() {
			if h.delivery != nil {
				h.delivery.Close()
			}
		})
		step("close.cells", func() {
			if h.channel == nil {
				return
			}
			rt := h.channel.Cells()
			if rt != nil {
				rt.StopAll()
				if leaked := rt.DrainZombies(0); len(leaked) > 0 {
					h.logger.Warn("home.close.zombies_leaked", "channel", h.channelID, "count", len(leaked), "actors", leaked)
				}
			}
			h.channel.Close()
		})
		step("close.engine", func() {
			if h.engine != nil {
				h.engine.Close()
			}
		})
		step("close.stores", func() {
			if h.cs != nil {
				addErr(h.cs.Close())
			}
		})
		h.closeErr = errors.Join(errs...)
		leaked := h.reconcileLeaked.Load()
		if h.links != nil {
			leaked += h.links.Leaked()
		}
		if h.delivery != nil {
			leaked += h.delivery.Leaked()
		}
		// zombieTotal is the runtime's LIFETIME zombie count — a cumulative
		// account, not this close's delta — so it is reported as its own field
		// rather than folded into leaked (which sums per-instance close deltas).
		var zombieTotal int64
		if h.channel != nil {
			leaked += h.channel.Leaked()
			if h.channel.Cells() != nil {
				zombieTotal = h.channel.Cells().LeakedTotal()
			}
		}
		if h.engine != nil {
			leaked += h.engine.Leaked()
		}
		h.state.Store(uint32(homeClosed))
		_ = h.faults.checkpoint("state.closed")
		step("close.end", func() {})
		h.logger.Info("platform.home.closed", "channel", h.channelID, "reason", reason,
			"cleanup_errors", len(errs), "leaked", leaked, "zombie_total", zombieTotal,
			"duration", time.Since(started))
		if teardownPanic != nil {
			panic(teardownPanic)
		}
	})
	<-h.closeDone
	return h.closeErr
}

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is the channel-home's read-only observation set: committed message tail
// (ReadAfterSeq), head cursor (MaxSeq), and active actor roster (ListActors). It
// holds only read interfaces — there is no write path through a View.
type View struct {
	query    storespec.MessageQuery
	registry storespec.Registry
	links    *link.Acceptor
	presence presence.View
	rt       *actorrt.Runtime
	nowMs    func() int64
}

// Snapshot composes membership, embodiment and testimony at read time. The
// fields are advisory and intentionally not a linearizable transaction.
func (v View) Snapshot(ctx context.Context, id actor.ActorID) (presence.Snapshot, error) {
	return v.presence.Snapshot(ctx, id)
}

// TestimonyAgeMs projects a fold receipt timestamp through the same clock used
// to stamp it. Clock skew is represented as age zero, never a negative age.
func (v View) TestimonyAgeMs(receivedAt int64) int64 {
	age := v.nowMs() - receivedAt
	if age < 0 {
		return 0
	}
	return age
}

// Stat reads the authoritative embodiment presence for id: live=true means id
// has a live embodiment on THIS home right now (cell or attached port — the
// `kill -0` read, actorrt.Runtime.Stat, transport-neutral). This is NOT the
// device/L3 advisory axis (Snapshot above): that is a self-reported,
// three-state, decays-to-unknown push signal from the actor's own client;
// this is the substrate's own authoritative self-read of embodiment, never
// asked of the actor, never advisory. The two axes answer different
// questions and must not be conflated.
func (v View) Stat(id actor.ActorID) (startedAt time.Time, live bool) {
	if v.rt == nil {
		return time.Time{}, false
	}
	stat, ok := v.rt.Stat(id)
	if !ok {
		return time.Time{}, false
	}
	return stat.StartedAt, true
}

// IsAttached reports whether daemon (compute) id has a live attach right now
// (L0 attachment) — read-time, derived from the link acceptor, never stored.
func (v View) IsAttached(daemonID string) bool {
	if v.links == nil {
		return false
	}
	return v.links.IsAttached(daemonID)
}

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (v View) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	return v.query.ReadAfterSeq(ctx, afterSeq, limit)
}

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (v View) MaxSeq(ctx context.Context) (int64, error) {
	return v.query.MaxSeq(ctx)
}

// ListActors returns all active actors from the membership registry.
func (v View) ListActors(ctx context.Context) ([]storespec.Record, error) {
	return v.registry.ListActive(ctx)
}

// ---------------------------------------------------------------------------
// delivery tap handle -- the cell-delivery Pump's per-row work
// ---------------------------------------------------------------------------

// deliveryHandle is the delivery tap's per-row work: deliver the committed
// envelope to its audience cells and OBSERVE the per-audience Outcome (the
// substrate's structured DeliverResult lands here — NotHosted / MailboxFull /
// Stopped are logged, never silently dropped). It is best-effort (push-mailbox
// semantics): a not-hosted / full mailbox is observed, not retried, so the
// handle always returns nil and the pump cursor always advances.
func deliveryHandle(d actorrt.Deliverer, chID channelpkg.ID, logger *slog.Logger) func(storespec.StoredRow) error {
	return func(row storespec.StoredRow) error {
		env := row.Envelope
		res, err := d.Deliver(env.Audience, &env)
		if err != nil {
			logger.Error("platform.delivery.error",
				"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID), "err", err)
			return nil
		}
		for id, outcome := range res.Per {
			if outcome == actorrt.Delivered {
				continue
			}
			logger.Warn("platform.delivery.outcome",
				"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID),
				"audience", string(id), "outcome", outcomeString(outcome))
		}
		return nil
	}
}

// outcomeString names an actorrt.Outcome for structured logging (an observation
// label, not a semantic branch — the handle does not act differently per kind).
func outcomeString(o actorrt.Outcome) string {
	switch o {
	case actorrt.Delivered:
		return "delivered"
	case actorrt.NotHosted:
		return "not_hosted"
	case actorrt.MailboxFull:
		return "mailbox_full"
	case actorrt.Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}
