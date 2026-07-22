package home

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
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ErrClosed is the refusal every mutating Home entry point returns once
// Home.Close has begun. Checked
// BEFORE any store read so a verb racing teardown never touches a closing
// store. The app maps it to 503.
var ErrClosed = errors.New("platform: channel home is closed")

// Config configures the channel-home assembly.
type Config struct {
	ChannelID channelpkg.ID
	DBPath    string
	// Genesis is written exactly once during a bootstrap open. ExpectedGenesis
	// is the strict self-identity asserted by ChannelHost on a normal open.
	Genesis         *storespec.ChannelGenesis
	ExpectedGenesis *storespec.ChannelGenesis
	// MustExistDB is set when reopening a channel already present in the app
	// directory. The store then refuses to create a missing file or repair an
	// incomplete schema. New-channel creation leaves it false.
	MustExistDB bool
	// Bootstrap permits the one new-channel 0→1 owner transition. It is
	// mutually exclusive with MustExistDB; every normal reopen requires exactly
	// one active owner before any authority/effect consumer is bound.
	Bootstrap bool
	Logger    *slog.Logger
	// ReconcileInterval tunes the closure reconciler's level safety-net sweep
	// period (the backstop for lost death edges). <=0 → the default. The death
	// edge closes the common case immediately; this sweep is a rare backstop.
	ReconcileInterval time.Duration
	// CompositionResolver supplies the world-declaration half for the app-owned
	// assembly. Home then reads channel composition from its own channel store
	// and derives both Desired and Builder from that authoritative source.
	CompositionResolver  CompositionResolver
	IntroductionResolver IntroductionResolver
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
	// default). Additive test-only knob (mirrors compute.Config.ScrubberInterval's
	// own shape): a crash-recovery walk driving the "abandoned reservation"
	// timeout path needs a fast, deterministic window rather than production's
	// multi-minute backstop.
	ReservationTimeout time.Duration
	// OnMembershipChange, when set, is fired by Admit (after the membership write)
	// and by Remove (with the principal captured BEFORE the dereg cascade, since the
	// registry row is gone after). It is the membership-change poke emit point
	// (连接模型勘误期 §3.2 表② 逐符号迁移: the old (channel, subject) OnRevoke is reborn
	// as a per-principal poke — 性质 changed from 撤销执行 to pure及时性). The assembly
	// root bridges it directly into Gateway.Poke(principal); the gateway
	// re-resolves that principal's WHOLE channel set (subscriptions + presence). nil →
	// no poke (the resolver's每批 recheck + sweep remain the correctness正门).
	OnMembershipChange func(principal string)
}

// Home is the assembled channel-home. Its public surface is the capability set in
// the package doc — the bare Runtime/Deliverer/Membership/Registry never escape
// it; assembly only hands out capabilities. The app layer owns HTTP/transport;
// Home is pure Go.
type Home struct {
	channelID    channelpkg.ID
	minter       harness.Minter
	channel      *channelkit.Channel
	cs           *runtime.ChannelStores
	controlIndex *actorControlIndex
	liveness     *livenessLedger
	stateHandles accessdoor.StateHandleResolver
	grantOverlay *actorGrantOverlay
	forkMu       sync.Mutex
	forkReceipts map[forkReceiptKey]forkReceipt
	usedForkIDs  map[actor.ActorID]struct{}
	signal       *tap.Signal
	delivery     *tap.Pump
	links        *link.Acceptor
	presenceFold *presence.Fold
	logger       *slog.Logger
	nowMs        func() int64
	opEntry      *opEntry

	// (No per-user caller index (期12): a subject's own requests are closed
	// by the substrate expiry reaper — 义务归位 D3; the subject drives its
	// own cell's caps through the subjectgate frame protocol → the cell's
	// identity-dimension Sys verbs, see humancell.go.)

	// onMembershipChange is Config.OnMembershipChange (the membership-change poke emit
	// point) — fired by Admit (after the write) and Remove (principal captured before
	// the dereg cascade) so the gateway re-resolves that principal's channel set.
	// 连接模型勘误期 §3.2.
	onMembershipChange func(principal string)

	// subjectgate is the human接入轴 slot registry (gateway 期 S2): the
	// per-identity slot store — each slot is the在场与递交接头盒 (the gateway
	// epoch presence register + the帧递交端). Built once at Open (装配链 step①, before any cell
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
	firedCursor  runtime.FiredTimerCursor

	// factories is the required platform-layer class/id → actor definition
	// resolver used by fork and activation. The caps weld happens at the platform assembler
	// (buildCaps) when a fork child is born, so a child gets the identical membrane
	// set as a top-level admission (purity: the domain fills WHAT to build, the
	// platform seam owns HOW caps are welded — actorrt never touches harness/link).
	factories ActorFactoryResolver

	// schedMinter mints a per-actor ScheduleHandle for the caps seam; engine is
	// the time-axis run loop assembled by OpenScheduler. The engine's Start/Close
	// is bound to Home's own open/close lifecycle (only minting a handle without
	// Start would be a cast-but-unwired half-piece).
	schedMinter schedule.Minter
	engine      *schedule.Engine

	// noFactoryWarned is this Home's edge-only log state: one warning per
	// continuously unresolved actor, cleared when resolution succeeds or the
	// actor leaves the reconcile set. It is not shared across channel homes.
	noFactoryWarned map[actor.ActorID]struct{}
	actorGates      actorLifecycleGate
	indexMu         sync.Mutex
	portIndex       map[actor.ActorID]homePortEntry

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
	// Test seams turn off edge accelerators while leaving the natural level
	// tick running; correctness must not depend on either switch.
	disablePoke                 atomic.Bool
	disableForkInlineActivation atomic.Bool

	// reviveMu guards BOTH per-author revive accounts below: reviveLogAt (the
	// attached-host revive-skip log throttle) and reviveBackoff (the transient
	// EnsureLive failure backoff entries). reviveLogAt throttles the log (see
	// reviveLogThrottle) to once per author per window — the schedule engine
	// backs off a transient EnsureLive failure at schedule.backoffDuration (1s)
	// pace, so an attached author's due identity timer would otherwise log
	// once a second for as long as it stays attached. Pure log hygiene, not a
	// correctness mechanism.
	reviveMu      sync.Mutex
	reviveLogAt   map[actor.ActorID]time.Time
	reviveBackoff map[actor.ActorID]reviveBackoffEntry

	// closed is set true at the very START of Close (before any teardown step), the
	// authoritative "this home is shutting down" flag every mutating entry point
	// checks. Close cannot JOIN the gateway session goroutines in the app layer
	// (outside Home's teardown), so the flag is what stops a post-Close Admit/
	// Remove/Restart from touching a closing store; a residual in-flight subject
	// write past the gate is fenced by the cell caps' own live membranes once
	// cells stop. atomic (lock-free read on the hot path).
	closed    atomic.Bool
	closeOnce sync.Once
	// storeCloseDone/storeCloseMu make the store half of close retryable: the
	// runtime teardown is one-shot, but a failed store close is re-attempted on
	// every close call (a sealed Destroy retries through here to completion).
	storeCloseDone atomic.Bool
	storeCloseMu   sync.Mutex
	closeDone      chan struct{}
	closeErr       error
}
