package platform

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/channelkit"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime"
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
// the app's HTTP shims can submit them through the subjectgate frame path
// (audience=[system]) WITHOUT importing platform/internal/sysactor. They are the
// door's wire vocabulary — the shim must speak the exact strings the gate
// dispatches on, so a single home avoids drift (same posture as the contract
// type re-exports above; white-list ⑤).
const (
	TypeIntroduceActor  = sysactor.TypeIntroduceActor
	TypeRemoveActor     = sysactor.TypeRemoveActor
	TypeRestartActor    = sysactor.TypeRestartActor
	TypeSetDefaultAgent = sysactor.TypeSetDefaultAgent
)

// ErrClosed is the refusal every mutating Home entry point (Admit / Remove /
// Restart / subjectgate slot verbs) returns once Home.Close has begun. Checked
// BEFORE any store read so a verb racing teardown never touches a closing
// store. The app maps it to 503.
var ErrClosed = errors.New("platform: channel home is closed")

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
	// OnRevoke, when set, is fired by Remove AFTER the dereg cascade commits (the
	// membership撤销 emit point, gateway 期 S3 表② remove.go). The assembly root
	// bridges it into the gateway's RevocationSource so the subject's频道臂 seals
	// (read-side revocation). nil → no bridge (the fold Sweep + read-side reader
	// resource recheck remain the level backstop). Home passes its own channelID at
	// the call site, so the sink is (channel, subject)-addressed.
	OnRevoke func(subject actor.ActorID)
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
	// own cell's caps through the subjectgate frame protocol → the cell's
	// identity-dimension Sys verbs, see humancell.go.)

	// onRevoke is HomeConfig.OnRevoke (the membership撤销 emit point) — fired by
	// Remove after the dereg cascade so the gateway can seal the subject's频道臂.
	onRevoke func(subject actor.ActorID)

	// subjectgate is the human接入轴 binding registry (gateway 期 S2): the
	// per-identity slot store (four-tuple {绑定世代, gateway epoch, 帧递交端,
	// presence level}). Built once at Open (装配链 step①, before any cell
	// construction path). A human cell's factory consults it at Proc start
	// (step③④) for its frame delivery端 + presence self-report槽; a nil-slot
	// (no gateway attach yet) cell is mailbox-only. The gateway (S3) ensures
	// slots at attach (step②) and drives frames through them.
	subjectgate *subjectgate.Registry

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
	// authoritative "this home is shutting down" flag every mutating entry point
	// checks. Close cannot JOIN the gateway session goroutines in the app layer
	// (outside Home's teardown), so the flag is what stops a post-Close Admit/
	// Remove/Restart from touching a closing store; a residual in-flight subject
	// write past the gate is fenced by the cell caps' own live membranes once
	// cells stop. atomic (lock-free read on the hot path).
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
