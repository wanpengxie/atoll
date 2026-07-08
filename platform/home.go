package platform

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/channelkit"
	"github.com/wanpengxie/atoll/platform/internal/devicepresence"
	"github.com/wanpengxie/atoll/platform/internal/link"
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
}

// Home is the assembled channel-home. Its public surface is the capability set in
// the package doc — the bare Runtime/Deliverer/Membership/Registry never escape
// it; assembly only hands out capabilities. The app layer owns HTTP/transport;
// Home is pure Go.
type Home struct {
	channelID  channelpkg.ID
	minter     harness.Minter
	channel    *channelkit.Channel
	cs         *runtime.ChannelStores
	signal     *tap.Signal
	delivery   *tap.Pump
	links      *link.Acceptor
	deviceFold *devicepresence.Fold
	logger     *slog.Logger
	nowMs      func() int64

	// (humanCallers author#2 index detached — platform/human.go 旧形整删
	// 2026-07-08，重建见 TODO(human-canonical)。)

	// presenceSessions is the subjectgate door's L3 device-presence session
	// refcount per subject (channel, user): the gateway ws connect/disconnect are
	// the ONLY producer of a human's device presence (常驻 cell 不死, decay 挂
	// down-edge 对它失效, so the door must explicitly feed — 根基档 §4.6/§6). A
	// (channel,user) may hold several ws (multi-tab/multi-device); online is fed on
	// the FIRST session, offline only when the LAST drops. The count is mutated and
	// the fold fed under presenceMu together, so edges are totally ordered — a late
	// disconnect from an old session can only decrement (never overwrite a still-live
	// sibling's online), which is the generation guard the refcount subsumes.
	presenceMu       sync.Mutex
	presenceSessions map[actor.ActorID]int

	// placement decides which host a new activity (fork/activation) starts on.
	// Single fixed-home identity this period (SinglePlacement); shaped now so
	// fork/activation route through Place() and multi-home swaps additively.
	placement actorrt.Placement
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
	reconcileStop context.CancelFunc
	reconcileDone chan struct{}

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
	reviveLogMu sync.Mutex
	reviveLogAt map[actor.ActorID]time.Time

	// obsReg dedups this home's own per-actor WatchObs registration (the local-cell
	// arm of the actor-source obs axis, mirroring Acceptor.obsReg's dedup for the
	// daemon-attach arm) — WatchObs itself is append-only with no built-in dedup
	// (runtime.go), so a losing SpawnIfAbsent/Fork build (CAS discarded) must not
	// leave a duplicate registration behind. Registered once, at buildCaps (the
	// single convergence point for every local birth path).
	obsMu  sync.Mutex
	obsReg map[actor.ActorID]bool

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
	// live in the app layer, outside Home's teardown), so "cells stopped ⇒ no one can
	// Arm" does NOT hold: a gateway goroutine could Submit → mint a fresh caller →
	// Arm AFTER stopAllHumanCallers cleared the index, stranding a死后 timer against
	// a closing store. The flag closes that window structurally — every verb (and
	// humanCaller's index write) refuses once it is set. atomic (lock-free read on
	// the hot Submit path).
	closed atomic.Bool
}

// watchObs registers the device-presence fold for id's obs PUSH, deduped so a
// discarded build (SpawnIfAbsent/Fork CAS loser) never leaves the runtime's
// append-only obsWatch registry double-appended for the same id.
func (h *Home) watchObs(id actor.ActorID) {
	h.obsMu.Lock()
	defer h.obsMu.Unlock()
	if h.obsReg[id] {
		return
	}
	h.channel.Cells().WatchObs(id, h.deviceFold)
	h.obsReg[id] = true
}

// unwatchObs is watchObs's symmetric un-registration (H5): obsReg is a name
// registry that must not outlive the membership it tracks (WatchObs is
// append-only, runtime.go), so a dereg without the matching UnwatchObs would
// leak the fold's registration across a future re-admission of the same id
// (mirrors Acceptor.obsReg's own dereg-site cleanup for the daemon-attach arm).
// A no-op if id was never registered here (e.g. it only ever lived attached).
func (h *Home) unwatchObs(id actor.ActorID) {
	h.obsMu.Lock()
	defer h.obsMu.Unlock()
	if !h.obsReg[id] {
		return
	}
	h.channel.Cells().UnwatchObs(id, h.deviceFold)
	delete(h.obsReg, id)
}

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
//	signal -> stores(OnCommit=signal.Notify) -> harness -> channelkit(spawns
//	sysactor against the live runtime) -> delivery tap -> link acceptor.
func Open(cfg HomeConfig) (*Home, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("platform: ChannelID required")
	}
	ctx := context.Background()
	nowMs := func() int64 { return time.Now().UnixMilli() }

	// 1. Build the commit Signal (tap fan-out). It has NO dependencies, so it is
	//    built first and handed to the store as its post-commit source. The
	//    commit signal belongs to the log append chokepoint (Postgres WAL / Kafka
	//    offset), not to any one writer — so BOTH write paths (request-path Append
	//    and the control-plane membership mirror) fire it through the store,
	//    instead of only the harness path being wrapped.
	signal := tap.NewSignal()

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
		_ = cs.Close()
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
	if exists, err := cs.Registry.Exists(ctx, actor.SystemActorID); err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("platform: check system actor: %w", err)
	} else if !exists {
		if err := cs.Membership.Insert(ctx, storespec.Record{
			ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: nowMs(),
		}); err != nil {
			_ = cs.Close()
			return nil, fmt.Errorf("platform: register system actor: %w", err)
		}
	}

	// 5. Device-presence fold (L3): folds actor-source obs PUSH edges
	//     into a volatile per-actor level (in-memory; never persisted), decays to
	//     unknown on the actor's death edge (link-down cascade). Built BEFORE
	//     channelkit so the system cell can read it (sysactor observes L1/L2/L3);
	//     registered as a down watcher + per-actor obs watcher below.
	deviceFold := devicepresence.New(logger)

	// 6. channelkit: actorrt runtime + sysactor + death-edge wiring. The system
	//    cell is built against the LIVE runtime (factory) — its liveness Stat seam
	//    reads the real runtime at construction, no back-filled pointer.
	//
	//    h is predeclared (nil) here and assigned below (step 9): sysactor is a
	//    ring0 special Proc (spec §3's out-generation matrix) that still enters
	//    through actorbase.New like every other actor, so its Hooks.Canceller
	//    wants Home.CancelRequest — but the system cell is spawned INSIDE
	//    channelkit.New, before Home exists. The closure captures the h
	//    VARIABLE (not its zero value); by the time a cancel actually fires
	//    (long after Open returns), h has been assigned.
	var h *Home
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	channel := channelkit.New(channelkit.Config{
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
				Stat:     &runtimeLivenessAdapter{rt: rt},
				Device:   deviceFold,
				Operate:  cfg.Operate,
			}))
		},
		SystemPen:    systemPen,
		OpenRequests: cs.Query,
		Clock:        clock,
		Logger:       logger,
	})

	// 7. Build the delivery tap: a Pump over the Signal-fed Deliverer. cursor start
	//    = current MaxSeq (mailbox semantics: only new commits). DeliverResult
	//    lands here as structured per-audience logs.
	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		channel.Cells().StopAll()
		channel.Cells().DrainZombies(0)
		channel.Stop()
		_ = cs.Close()
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	deliver := deliveryHandle(channel.Deliverer(), cfg.ChannelID, logger)
	delivery := tap.NewPump(signal, cs.Query, from, deliver, logger)
	delivery.Start()

	// 8. Register the device-presence fold as a global down watcher (so an actor's
	//     death edge decays its L3 to unknown — the link-down cascade). Per-actor
	//     obs registration happens at attach (Acceptor below).
	channel.Cells().WatchDown(deviceFold)

	// 9. Assemble the Home shell now: the scheduler's Reviver and the eager
	//    reconcile arm both close over it (buildCaps, builder, cells), so it must
	//    exist before those are wired. schedMinter/engine/links are filled below —
	//    the link acceptor is built AFTER the scheduler because it welds a remote
	//    port's incarnation onto the schedule Minter (the time-axis wire arm), which
	//    only exists once OpenScheduler has run.
	h = &Home{
		channelID:        cfg.ChannelID,
		minter:           minter,
		channel:          channel,
		cs:               cs,
		signal:           signal,
		delivery:         delivery,
		deviceFold:       deviceFold,
		logger:           logger,
		nowMs:            nowMs,
		placement:        SinglePlacement{},
		builder:          cfg.Builder,
		desired:          cfg.Desired,
		prevEagerDesired: map[actor.ActorID]bool{},
		reviveLogAt:      map[actor.ActorID]time.Time{},
		obsReg:           map[actor.ActorID]bool{},
		pokeCh:           make(chan struct{}, 1),
	}

	// 10. Time axis (OpenScheduler). FireSink mints a pen per fire (author-welded);
	//     Reviver activates an absent identity-timer author via SpawnIfAbsent. The
	//     engine is Started here and Closed in Home.Close (minting a handle without
	//     Start would be a cast-but-unwired half-piece). BOOT-ORDER红线: the Reviver
	//     is wired and the engine is Started BEFORE the first reconcile sweep below,
	//     because an overdue fire on Start can precede the eager ring re-minting the
	//     always-on set — and append has no backfill, so the wake must be revivable
	//     from the first instant.
	rt := channel.Cells()
	schedMinter, engine, err := runtime.OpenScheduler(cs, schedule.AssemblyDeps{
		Fire:   fireSink{minter: minter, registry: cs.Registry, rt: rt, chID: cfg.ChannelID},
		Host:   rt,
		Revive: homeReviver{h: h},
		Clock:  cfg.Clock,
		Logger: logger,
	})
	if err != nil {
		delivery.Close()
		channel.Cells().StopAll()
		channel.Cells().DrainZombies(0)
		channel.Stop()
		_ = cs.Close()
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}
	h.schedMinter = schedMinter
	h.engine = engine
	engine.Start()

	// 11. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//      + lease judgement for attached computes). It welds an attaching remote
	//      port's incarnation onto the same three minters a local cell's Caps draw
	//      from — the harness pen Minter, the access door (cs.Access), and the
	//      schedule engine Minter — so a daemon-hosted cell's message / off-log /
	//      time-axis capability is behaviourally identical to a local one (transport
	//      neutrality). It also folds each attached actor's obs PUSH into the
	//      device-presence fold (per-actor WatchObs at attach).
	links := link.NewAcceptor(link.Config{
		Minter:             minter,
		Access:             cs.Access,
		Schedule:           schedMinter,
		Runtime:            rt,
		Membership:         cs.Membership,
		Registry:           cs.Registry,
		ChannelID:          cfg.ChannelID,
		Logger:             logger,
		ObsWatcher:         deviceFold,
		CancelRequest:      h.handleCancelUpstream,
		StorageHostControl: homeStorageHostControl{outbox: cs.Outbox, timeout: cfg.ReservationTimeout},
	})
	h.links = links
	// Close the late-binding window (see step 2's lateAcc note): every
	// file-kind placement decision from this instant on can actually route an
	// AllocRequest / see attached daemons as storage-mount candidates.
	lateAcc.bind(links)

	// 12. Reconcilers (level backstops). Run one sweep of EACH at startup —
	//     activation re-mints the always-on desired set; closure is the home-restart
	//     recovery path (#4, close orphan open requests whose receiver's embodiment
	//     predates this process). Then a low-frequency ticker keeps both as the
	//     safety net for any lost death edge / intent change. The death edge (OnDown)
	//     remains the lossy fast-path for closure.
	//
	//     BOOT-ORDER红线 (红线12): activation MUST precede closure, at the first sweep
	//     AND every tick. closure's verdict is "absent right now" (livenessProbe),
	//     not "predates this process" — so a receiver whose desired cell the ring has
	//     not yet (re)minted this sweep would be scanned as receiver_unavailable and
	//     its deferred open requests wrongly closed. Minting first, then scanning,
	//     keeps a restart / mid-life-crash cell's open requests alive across the sweep.
	h.reconcileActivation(ctx)
	channel.Reconcile(ctx)
	sweepEvery := cfg.ReconcileInterval
	if sweepEvery <= 0 {
		sweepEvery = reconcileInterval
	}
	reconcileCtx, reconcileStop := context.WithCancel(context.Background())
	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		t := time.NewTicker(sweepEvery)
		defer t.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-t.C:
				h.reconcileActivation(reconcileCtx)
				channel.Reconcile(reconcileCtx)
			case <-h.pokeCh:
				// Admit poke: run the same activation-then-closure sweep off-tick so
				// a freshly-admitted member embodies without the ≤30s wait. Boot-order
				// 红线 holds (activation precedes closure) on this path too.
				h.reconcileActivation(reconcileCtx)
				channel.Reconcile(reconcileCtx)
			}
		}
	}()
	h.reconcileStop = reconcileStop
	h.reconcileDone = reconcileDone

	logger.Info("platform.home.ready", "channel", string(cfg.ChannelID))
	return h, nil
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
	}
	actives, err := h.cs.Registry.ListActive(ctx)
	if err != nil {
		h.logger.Error("platform.reconcile.registry_failed", "channel", string(h.channelID), "err", err)
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
	actual := make(map[actor.ActorID]bool)
	for _, id := range rt.LiveIDs() {
		actual[id] = true
	}
	current := make(map[actor.ActorID]bool)
	for id := range desiredIDs {
		rec, ok := member[id]
		if !ok || !rec.IsActive() {
			continue // 交集红线: desired-but-not-a-durable-member — skip BEFORE current
		}
		current[id] = true
		if actual[id] {
			continue
		}
		if rec.Host != "" {
			continue // attached elsewhere — not this ring's authority to embody (反误杀)
		}
		factory, ok := h.factoryFor(rec)
		if !ok {
			h.logger.Warn("platform.reconcile.no_factory", "channel", string(h.channelID), "actor", string(id))
			continue
		}
		kind := rec.Kind
		mid := id
		inc, built := rt.SpawnIfAbsent(mid, kind, func(inc actorrt.Incarnation) actorrt.Actor {
			return build(h.buildCaps(mid, kind, inc), h.hooks(), factory)
		})
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
		if current[id] {
			continue
		}
		if rec, ok := member[id]; ok && rec.Host != "" {
			continue // attached elsewhere — not gone, not this ring's to evict (反误杀)
		}
		// 登记 (latent, H5-对称): DespawnID here does NOT unwatchObs — a no-longer-desired
		// member may be re-desired later, so its obs registration legitimately survives
		// a mere deactivation. The only leak is a member whose户籍 row is deleted by RAW
		// SQL (bypassing Home.Remove, which DOES unwatchObs); that path is not reachable
		// through any supported verb, so the symmetric un-registration stays with Remove.
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
	if lerr != nil {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckFault, lerr
	}
	if !ok2 || !rec2.IsActive() {
		h.channel.Cells().Despawn(inc)
		return storespec.Record{}, recheckGone, nil
	}
	if rec2.Host != "" {
		h.channel.Cells().Despawn(inc)
		return rec2, recheckAttached, nil
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
		query:      h.cs.Query,
		registry:   h.cs.Registry,
		links:      h.links,
		deviceFold: h.deviceFold,
		stat:       &runtimeLivenessAdapter{rt: h.channel.Cells()},
	}
}

// Spawn admits one actor into the channel as durable membership truth and, when
// def is non-empty, places it as a live in-process cell (binding=embedded) with
// a Pen welded to its own (id, channelID).
//
// Identity weld at the admission membrane: the app supplies the id (domain authority —
// "this id may be admitted") and a def (spec §4 S3's ActorFactory); the substrate
// Mints the welded Pen and hands the caps to def's build in ONE step, so the cell
// is born with a pen welded to its own id (the substrate's "actorID and write
// capability are welded inseparably" invariant). The app never sees a bare writer,
// a Minter, or actorcaps.Caps itself — it only chooses WHAT to place (an
// ActorFactory over harness.Pen or an actorbase.Def); Home decides HOW.
//
// Membership is VERIFIED, not applied (膜律 — the not→member edge belongs to Admit
// alone; Spawn never mints membership as a side effect). Spawn-replace embodies an
// EXISTING member (A-P14: restart's single real-factory caller); a Spawn of a
// non-member id is an error — restarting an orphan row would be a membrane bypass.
// The verify precedes the cell so the order invariant still holds: durable
// membership BEFORE the live embodiment (the birth mirror of the death-side
// "despawn before deregister"). The sender gate does not query the registry (kind
// is welded into the pen at Mint); membership ≠ embodiment (the live cell layers on
// top of the durable identity-level truth), and a construction-time plane-2 access
// invoke needing a member-grant resolves against that durable membership.
//
// Two-phase Spawn: the build closure runs inside rt.Spawn. It welds the whole caps
// bundle bound to THIS incarnation (livePen + liveAccess membranes, spawn handle)
// and hands the gated bundle to def's build in ONE step — no bare handle escapes. A
// participant is a gated cap holder; substrate anchors are not (see channelkit/compute).
func (h *Home) Spawn(ctx context.Context, id actor.ActorID, kind actor.Kind, def ActorFactory) error {
	if id == "" {
		return fmt.Errorf("platform: Spawn id required")
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return fmt.Errorf("platform: Spawn membership lookup: %w", err)
	}
	if !ok || !rec.IsActive() {
		return fmt.Errorf("platform: Spawn requires an active member: %s", id)
	}
	rt := h.channel.Cells()
	rt.Spawn(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return build(h.buildCaps(id, kind, inc), h.hooks(), def)
	})
	h.logger.Info("platform.member.spawned", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind))
	return nil
}

// Admit registers one actor as durable channel membership truth and nothing more
// — the pure-membership primitive (the not→member edge of §4.6). It writes a
// NEUTRAL row (Binding="" / Host=""): membership precedes embodiment, and the
// host path (daemon attach / activation ring) owns Binding/Host stamping — Admit
// never guesses placement. It does not Mint a pen or place a cell; the desired
// member is embodied by the reconcile ring's SpawnIfAbsent (activation) or a
// daemon attach, never by Admit itself. After the write it pokes the ring so the
// embodiment lands on the next immediate sweep rather than waiting a full tick.
// Idempotent: ApplyMemberTransitions upserts a live row (a re-Admit of an existing
// member is a no-op-shaped write + a harmless extra poke).
func (h *Home) Admit(ctx context.Context, id actor.ActorID, kind actor.Kind) error {
	if id == "" {
		return fmt.Errorf("platform: Admit id required")
	}
	// An Admit of an ALREADY-active member is a pure no-op (poke only): it must NOT
	// re-apply the neutral Host="" row, because applyMemberAddTx UPDATEs host on any
	// host-diff — a re-Admit of a daemon-hosted member (an idempotent introduce
	// retry) would else clobber its live Host back to "" (placement authority is the
	// attach/plan path's, never Admit's). Only an inactive/absent id takes the apply
	// (reactivate/insert), where Host="" is the correct genesis-neutral state the
	// host path stamps later.
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil {
		return fmt.Errorf("platform: Admit membership lookup: %w", err)
	}
	if ok && rec.IsActive() {
		h.pokeReconcile()
		return nil
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{
		ID: id, Kind: kind, Binding: actor.Binding(""), Host: "", At: h.nowMs(),
	}}, nil); err != nil {
		return fmt.Errorf("platform: Admit membership: %w", err)
	}
	h.pokeReconcile()
	h.logger.Info("platform.member.admitted", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind))
	return nil
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
// caps assembler, shared by admission (Home.Spawn) and by fork (spawnHandle.Fork
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
//
// buildCaps is also the obs-registration convergence point (G8): every local
// birth path — Home.Spawn, the reconcile ring's eager 补臂, homeReviver, and
// spawnHandle.Fork — calls this method to weld a child's caps, so registering
// h.watchObs(id) here once covers all four without a separate call at each
// birth site (and its dedup makes a build a losing SpawnIfAbsent/Fork CAS
// discards a no-op double-registration, not a double fanout).
func (h *Home) buildCaps(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps {
	rt := h.channel.Cells()
	h.watchObs(id)
	return actorcaps.Caps{
		Pen:      link.NewLivePen(h.minter.Mint(id, kind, h.channelID), inc, rt),
		Access:   link.NewLiveResourceAccess(h.cs.Access.Mint(id), inc, rt),
		State:    link.NewLiveAccess(h.cs.Access.MintState(id), inc, rt),
		Schedule: link.NewLiveSchedule(h.schedMinter.Mint(id), inc, rt),
		// The child assembler is buildChildCaps, NOT buildCaps: every fork
		// descendant is an incarnation-level citizen (spec §4.1), so its private
		// state must be per-incarnation memory, not this durable MintState arm. Any
		// actor's fork children — top-level or itself a child — take that path.
		Spawn: newSpawnHandle(inc, rt, h.builder, h.buildChildCaps, h.placement, h.hooks()),
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
	h.links.Serve(w, r, daemonID)
}

// Subscribe is the subscription registration surface (client push): a client stream subscribes to
// the commit Signal and reads forward from its own seq cursor. It returns the
// wake channel and the unsubscribe func — the internal Signal never escapes.
func (h *Home) Subscribe() (<-chan struct{}, func()) {
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
func (h *Home) Close() error {
	// 0. Mark closed FIRST: the subjectgate verbs run on ws/垫片 goroutines Close
	//    never joins (they are the app's, not Home's), so quiescing cells below is
	//    NOT enough to stop a fresh Submit→Arm. The flag makes every verb entry (and
	//    humanCaller's index write) refuse from this instant on, so stopAllHumanCallers
	//    at step 4.5 clears the index for good — no goroutine can re-mint into it.
	h.closed.Store(true)
	// 1. Reconciler ticker: stop the level sweep and join it, so no Reconcile runs
	//    against the writer/runtime/stores being torn down below.
	if h.reconcileStop != nil {
		h.reconcileStop()
		<-h.reconcileDone
	}
	// 2. Link acceptor: close all WS links, tear down every actor stream, wait for
	//    Serve goroutines. Stops external compute traffic (and its port-side
	//    schedule/access producers) before the runtime/stores underneath shut down.
	linkErr := h.links.Close()
	// 3. Delivery tap: stop the pump so no fresh delivery drives a cell to produce.
	h.delivery.Close()
	// 4. Cells: judge every actor cell dead (system actors included) — the last
	//    schedule/emit producers. StopAll no longer joins (G0): it enrols each as a
	//    zombie and returns; DrainZombies is the sole aggregate join.
	h.channel.Cells().StopAll()
	// 4.1. Drain the zombie cohort under one aggregate grace; a卡死 cell that never
	//    exits is reported as leaked (into the shutdown log) instead of hanging Close
	//    forever — the whole point of the zombie ledger.
	if leaked := h.channel.Cells().DrainZombies(0); len(leaked) > 0 {
		h.logger.Warn("home.close.zombies_leaked", "channel", h.channelID, "count", len(leaked), "actors", leaked)
	}
	// 4.2. Closure consumer: join channelkit's resident down-edge goroutine now —
	//    after cells stop (no more edges produced), before the stores close (a late
	//    materialise must not write into a closing store). G0-3 teardown序.
	h.channel.Stop()
	// 4.5. (human caller teardown detached — platform/human.go 旧形整删
	//    2026-07-08，重建见 TODO(human-canonical)。)
	// 5. Schedule engine: every producer is gone, so stopping the run loop now can
	//    no longer strand a Schedule() into a dead engine.
	if h.engine != nil {
		h.engine.Close()
	}
	// 6. Channel stores (DB) last.
	csErr := h.cs.Close()

	if linkErr != nil {
		return linkErr
	}
	return csErr
}

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is the channel-home's read-only observation set: committed message tail
// (ReadAfterSeq), head cursor (MaxSeq), and active actor roster (ListActors). It
// holds only read interfaces — there is no write path through a View.
type View struct {
	query      storespec.MessageQuery
	registry   storespec.Registry
	links      *link.Acceptor
	deviceFold *devicepresence.Fold
	stat       *runtimeLivenessAdapter
}

// DevicePresence returns the latest opaque L3 device-presence snapshot an actor
// pushed (via the obs axis), folded read-time. known=false = UNKNOWN (the actor
// never reported, or its link dropped and the fold decayed it) — NOT offline.
// The caller decodes the bytes via introspect.ParseDevicePresence. Advisory only;
// authoritative reachability is send→terminal.
func (v View) DevicePresence(id actor.ActorID) (snapshot []byte, known bool) {
	if v.deviceFold == nil {
		return nil, false
	}
	return v.deviceFold.Device(id)
}

// Stat reads the authoritative embodiment presence for id: live=true means id
// has a live embodiment on THIS home right now (cell or attached port — the
// `kill -0` read, actorrt.Runtime.Stat, transport-neutral). This is NOT the
// device/L3 advisory axis (DevicePresence above): that is a self-reported,
// three-state, decays-to-unknown push signal from the actor's own client;
// this is the substrate's own authoritative self-read of embodiment, never
// asked of the actor, never advisory. The two axes answer different
// questions and must not be conflated.
func (v View) Stat(id actor.ActorID) (startedAt time.Time, live bool) {
	if v.stat == nil {
		return time.Time{}, false
	}
	return v.stat.Stat(id)
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
// runtimeLivenessAdapter -- bridges actorrt.Runtime.Stat -> sysactor.LivenessStat
// ---------------------------------------------------------------------------

type runtimeLivenessAdapter struct {
	rt *actorrt.Runtime
}

func (a *runtimeLivenessAdapter) Stat(id actor.ActorID) (startedAt time.Time, present bool) {
	if a.rt == nil {
		return time.Time{}, false
	}
	stat, ok := a.rt.Stat(id)
	if !ok {
		return time.Time{}, false
	}
	return stat.StartedAt, true
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
