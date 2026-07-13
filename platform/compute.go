package platform

// compute.go is the daemon (attached-compute) assembly root: link.Dial (connect
// to the channel home, no actors declared yet) → actorrt.Runtime (business
// cells, built once and outlives any single link) → the daemon's own reconcile
// ring (computeRing), which diffs ComputeConfig.Desired against the locally-live
// set and drives Reattach → OpenStream → Spawn → StartStream per actor.
// Cloud daemon and user-proxy daemon are the same binary; cmd selects concrete
// actors.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// defaultComputePoll is the reconcile ring's desired-source poll period when
// ComputeConfig.Poll is unset (S-P10).
const defaultComputePoll = 30 * time.Second

// defaultResyncInterval is the ring's slow-cadence periodic full-set re-
// declaration period (kubelet periodic resync) when ComputeConfig.Resync is
// unset — minute-level, a level-triggered convergence backstop rather than a
// liveness signal (the poll cadence above is the fast edge-driven half).
const defaultResyncInterval = 5 * time.Minute

// ComputeConfig configures the attached compute. ServerWS carries any auth
// credential in its query string (the ?key= the app layer resolves on WS
// upgrade) — the link layer is auth-agnostic, so there is no separate key field.
//
// Desired and Builder are the two halves of the daemon's OWN reconcile ring
// (§10.13 推导2: the reconcile paradigm is host-neutral — every host that carries
// live embodiments runs the same diff loop over its own desired source). Neither
// may be nil: a compute with no desired source or no builder never turns the
// ring at all, so RunCompute fails fast rather than silently running an empty
// daemon (same nil discipline as HomeConfig.Builder — a structural refusal, never
// a phantom no-op).
type ComputeConfig struct {
	ServerWS  string
	ComputeID string
	Logger    *slog.Logger
	// Desired is the reconcile ring's read of intent — the same DesiredSource
	// contract the home's eager-activation ring reads (AlwaysOn members only;
	// lazy activation does not apply to a daemon, which has no delivery seam of
	// its own to activate on demand against).
	Desired actorrt.DesiredSource
	// Builder resolves each desired member's factory. nil is a fail-fast
	// misconfiguration, not "no actors" (an intentionally-empty daemon should
	// still supply a Builder that finds nothing).
	Builder ComputeBuilder
	// Poll is the ring's desired-source poll period. <=0 → defaultComputePoll.
	Poll time.Duration
	// Resync is the ring's slow-cadence periodic full-set re-declaration period
	// (kubelet periodic resync 半, #7): every Resync the ring unconditionally
	// Reattaches its full current declared set — even with no missing/削 diff —
	// so the home's reconcileHost re-runs and absorbs any account-vs-reality
	// drift into bounded convergence (a削章 dereg-write that failed once, a late
	// migration, any future divergence source). Level-triggered backstop; the
	// Poll cadence is the fast edge-driven half. <=0 → defaultResyncInterval.
	Resync time.Duration
	// StorageHost (optional, 期11 §4) is the daemon's file-kind storage host
	// — cmd/daemon/internal/storagehost.Host, injected by cmd/daemon/main.go
	// (this package cannot construct one itself: the concrete os.Root-touching
	// implementation lives in a cmd/daemon-confined package, §8.2's
	// server-zero-storage red line — RunCompute only ROUTES already-built
	// control-plane frames to it). nil → every AllocRequest this daemon
	// receives is answered honestly OK:false (no storage host wired), and no
	// Scrubber pass ever runs (a daemon that never hosts file-kind resources
	// needs neither).
	StorageHost StorageHost
	// ScrubberInterval overrides the storage-host reconcile bridge's periodic
	// ReconcilePull cadence (storageHostForwarder.pump). <=0 →
	// scrubberPumpInterval (60s, production default). A startup pass ALWAYS
	// runs immediately regardless of this value — only the PERIODIC ticker
	// after it is affected. Additive test-only knob (mirrors Poll's own
	// nil-safe-default shape): a walk test driving delete→tombstone→
	// ReclaimAck end-to-end needs a fast, deterministic cadence rather than
	// production's 60s backstop interval.
	ScrubberInterval time.Duration
	// LocalFileOpener (optional, 期11 §5) is the daemon-side same-machine
	// byte-access capability the resource lane's Local route and this
	// daemon's own lane-inbound (target) handler both redeem through —
	// SAME underlying storagehost.Host as StorageHost, a DIFFERENT facet of
	// it (Streamer's OpenRead/OpenWrite rather than Alloc/Reconcile). No
	// mirror type is needed here (unlike StorageHost's Reconcile shapes):
	// its signature is expressed purely in io.ReadSeekCloser +
	// accessdoor.LocalWriteHandle, both already reachable from cmd/daemon's
	// adapter without importing platform/internal/link. nil → every file
	// byte redemption on this compute answers an honest "no storage host
	// wired" error.
	LocalFileOpener LocalFileOpener
}

// RunCompute connects to the channel home and runs the daemon's own reconcile
// ring against cfg.Desired: it hosts every AlwaysOn desired member as a cell,
// declaring it to the home (Reattach) before opening its stream. The home
// dispatches envelopes down each actor's link stream into the cell's mailbox;
// the cell's emits flow UP that same stream as native ipc (blocking on the
// home's authoritative EmitAck). No local truth.
//
// The link is NOT the unit of life here — rt is (§10.13 推导3: wire-session
// death ≠ hosted-work death). RunCompute dials in a redial loop with an
// exponential backoff (1s→30s cap, reset on success); every hosted cell
// survives any number of link deaths, resuming under the SAME identity once
// the wire returns (reopen, never respawn — F6). The ONLY path that ever
// StopAll's rt is ctx cancellation (graceful shutdown, each stream KindDetach
// first) — a link death alone only re-enters the redial loop.
//
// RunCompute blocks until ctx is cancelled.
func RunCompute(ctx context.Context, cfg ComputeConfig) error { return runCompute(ctx, cfg, nil) }

func runCompute(ctx context.Context, cfg ComputeConfig, hooks *computeLifecycleHooks) (retErr error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Desired == nil {
		return fmt.Errorf("platform: RunCompute requires a Desired source (nil never turns the ring — fail-fast, not a silent no-op)")
	}
	if cfg.Builder == nil {
		return fmt.Errorf("platform: RunCompute requires a Builder (nil never resolves a factory — fail-fast, not a silent no-op)")
	}
	computeID := cfg.ComputeID
	if computeID == "" {
		computeID = uuid.NewString()
	}
	poll := cfg.Poll
	if poll <= 0 {
		poll = defaultComputePoll
	}
	resync := cfg.Resync
	if resync <= 0 {
		resync = defaultResyncInterval
	}

	// Cell running is the kernel: the daemon owns an actorrt.Runtime directly. It
	// is built HERE, once — OUTSIDE the redial loop below (F14) — because a
	// hosted cell's identity and in-memory state must outlive a single wire
	// session.
	rt, del := actorrt.New(actorrt.Config{})
	defer func() {
		// StopAll no longer joins (G0): it enrols every hosted cell as a zombie and
		// returns. DrainZombies is the aggregate join — a卡死 cell is reported leaked
		// into the shutdown log instead of hanging RunCompute's teardown forever.
		rt.StopAll()
		if leaked := rt.DrainZombies(0); len(leaked) > 0 {
			logger.Warn("compute.close.zombies_leaked", "compute", computeID, "count", len(leaked), "actors", leaked)
		}
	}()
	watcher := &cellDownWatcher{down: map[actor.ActorID]func(cause string){}}
	rt.WatchDown(watcher)
	// The obs arm outlives every individual link the same way rt does — built
	// once, Rebind'd per connection, pumped for the daemon's whole life.
	obsFwd := newCellObsForwarder(logger)
	rt.WatchObsAll(obsFwd)
	var forwarders sync.WaitGroup
	forwarders.Add(1)
	go func() {
		defer forwarders.Done()
		defer func() {
			if hooks != nil && hooks.obsExited != nil {
				hooks.obsExited()
			}
		}()
		obsFwd.pump(ctx)
	}()
	// The caller-side cancel arm, like the obs arm, outlives every individual link:
	// built once, Rebind'd per connection, so a hosted caller's Canceller always
	// sends up whichever Dialer is currently connected.
	cancelFwd := newCellCancelForwarder()
	// The storage host bridge (期11 §4), same daemon-lifetime shape as
	// obsFwd/cancelFwd — built once (nil-host-safe: cfg.StorageHost may be
	// nil), Rebind'd per connection, pumped for the daemon's whole life.
	storageFwd := newStorageHostForwarder(cfg.StorageHost, logger, cfg.ScrubberInterval)
	forwarders.Add(1)
	go func() {
		defer forwarders.Done()
		defer func() {
			if hooks != nil && hooks.storageExited != nil {
				hooks.storageExited()
			}
		}()
		if hooks != nil && hooks.storagePump != nil {
			hooks.storagePump(ctx, storageFwd)
			return
		}
		storageFwd.pump(ctx)
	}()
	// forwarderLeaked is this RunCompute invocation's forwarder leak account —
	// the same per-instance atomic every other lifecycle component keeps. The
	// single deferred close arbitration below is its only writer, so one
	// timeout incident counts exactly once.
	forwarderLeaked := &atomic.Int64{}
	if hooks != nil && hooks.forwarderLeaked != nil {
		forwarderLeaked = hooks.forwarderLeaked
	}
	defer func() {
		timeout := 5 * time.Second
		if hooks != nil && hooks.forwarderTimeout > 0 {
			timeout = hooks.forwarderTimeout
		}
		done := make(chan struct{})
		go func() { forwarders.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(timeout):
			// Bounded abandon proof: neither pump produces actors. A storage pump
			// may still hold the pure os.Root handle, so ownership is transferred
			// intact to process exit and daemon main must skip sh.Close.
			forwarderLeaked.Add(1)
			logger.Error("platform.compute.forwarder_join_timeout", "timeout", timeout,
				"leaked", forwarderLeaked.Load(),
				"safety", "pure os.Root handle ownership transferred to process exit; no actor production")
			retErr = errors.Join(retErr, ErrComputeForwardersLeaked)
		}
	}()

	ring := &computeRing{
		rt:          rt,
		del:         del,
		watcher:     watcher,
		obsFwd:      obsFwd,
		cancelFwd:   cancelFwd,
		builder:     cfg.Builder,
		logger:      logger,
		prevCurrent: map[actor.ActorID]actor.Kind{},
		arms:        map[actor.ActorID]*link.RebindableArms{},
	}

	backoff := redialInitialBackoff
	for {
		// Dial declares NOTHING yet: every actor this compute hosts is declared by
		// the ring's own Reattach (the full-set declaration idiom, §S-P8) inside
		// the first reconcile pass below.
		dialCfg := link.DialConfig{DespawnLocal: func(id actor.ActorID) { rt.DespawnID(id) }, LocalFileOpener: cfg.LocalFileOpener}
		if cfg.StorageHost != nil {
			dialCfg.AllocHandler = storageFwd.handleAlloc
		}
		d, err := link.Dial(ctx, cfg.ServerWS, computeID, nil, dialCfg, logger)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("platform.compute.dial_failed", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(jitterBackoff(backoff)):
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}

		// Host→remote despawn hook: a KindDespawn frame from the home ends that
		// actor's arm here (§10.5) — despawn the local cell, the stream loop
		// replies KindDetach. Re-installed on every connection (it closes over
		// THIS d's stream read loops).
		obsFwd.Rebind(d)
		cancelFwd.Rebind(d)
		storageFwd.Rebind(d)
		// §5's resource lane needs no per-connection carrier setup any more (片③
		// flattened it): the Dialer accepts home-relayed inbound lane substreams
		// via its always-running accept loop, and opens its own redeem substreams
		// on demand — both live for the connection's whole life without a
		// dedicated open step here.

		linkStart := time.Now()
		graceful := ring.runLink(ctx, d, cfg.Desired, poll, resync)
		_ = d.Close() // idempotent: a no-op if the link already tore itself down.
		if graceful {
			return nil
		}
		backoff = redialBackoffAfterLink(backoff, time.Since(linkStart))

		logger.Warn("platform.compute.link_down", "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jitterBackoff(backoff)):
		}
		backoff = nextRedialBackoff(backoff)
	}
}
