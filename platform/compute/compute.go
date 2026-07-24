package compute

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const (
	defaultComputePoll   = 30 * time.Second
	redialInitialBackoff = time.Second
	redialMaxBackoff     = 30 * time.Second
)

// Config configures one daemon execution domain. PlanSource is the sole
// authenticated plan/factory snapshot; actor bodies and the physical link are
// deliberately owned by different organs.
type Config struct {
	ServerWS         string
	Logger           *slog.Logger
	PlanSource       PlanSource
	Poll             time.Duration
	StorageHost      StorageHost
	ScrubberInterval time.Duration
	LocalFileOpener  LocalFileOpener
}

type PlanSource interface {
	ApplyPlan([]platform.PlanActor) error
	ActorFactorySource
}

func Run(ctx context.Context, cfg Config) error { return runCompute(ctx, cfg, nil) }

// computeLifecycleHooks contains only test observation seams. It owns no
// runtime state and cannot alter actor/link convergence.
type computeLifecycleHooks struct {
	forwarderTimeout time.Duration
	forwarderLeaked  *atomic.Int64
	storageExited    func()
	storagePump      func(context.Context, *storageHostForwarder)
}

type daemonHostEvents struct{ outbound *DaemonOutbound }

func (*daemonHostEvents) OnBodyExited(actor.ActorID, actorhost.AttemptKey, actorrt.Incarnation, error) {
}

func (e *daemonHostEvents) OnBodyObs(
	id actor.ActorID,
	key actorhost.AttemptKey,
	_ actorrt.Incarnation,
	kind actorrt.ObsKind,
	value actorrt.ObsValue,
) {
	e.outbound.publishObs(id, key, kind, value)
}

func daemonBodyBuilder(outbound *DaemonOutbound, source PlanSource) actorhost.BodyBuilder {
	return func(input actorhost.BodyBuildInput) actorrt.Actor {
		prepared, prepareErr := outbound.Prepare(
			input.ActorID,
			input.AttemptKey,
			input.Current,
		)
		if prepareErr != nil {
			return nil
		}
		transferred := false
		defer func() {
			if !transferred {
				_ = prepared.Slot.Close()
			}
		}()
		factory, ok := source.Lookup(input.ActorID)
		if !ok {
			return nil
		}
		caps := actorcaps.Caps{
			Pen: prepared.Pen, Access: prepared.Access, State: prepared.State,
			Schedule: prepared.Schedule, Lifecycle: prepared.Lifecycle,
		}
		hooks := actorbase.Hooks{Canceller: func(_ actor.ActorID, requestID message.ID) {
			_ = prepared.Slot.CancelRequest(requestID)
		}}
		body := hostcommon.Build(caps, hooks, factory)
		wrapped := prepared.Wrap(body)
		if wrapped != nil {
			transferred = true
		}
		return wrapped
	}
}

func runCompute(ctx context.Context, cfg Config, hooks *computeLifecycleHooks) (retErr error) {
	if cfg.PlanSource == nil {
		return errors.New("compute: PlanSource required")
	}
	if cfg.ServerWS == "" {
		return errors.New("compute: ServerWS required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	poll := cfg.Poll
	if poll <= 0 {
		poll = defaultComputePoll
	}

	// Runtime organs use an explicit composition lifetime. The caller context
	// stops link admission, but must not asynchronously tear down sessions or
	// bodies ahead of the ordered close DAG below.
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	outbound := NewDaemonOutbound(DaemonOutboundConfig{Parent: runtimeCtx})
	storage := newStorageHostForwarder(cfg.StorageHost, logger, cfg.ScrubberInterval)
	var storageWG sync.WaitGroup
	storageWG.Add(1)
	go func() {
		defer storageWG.Done()
		defer func() {
			if hooks != nil && hooks.storageExited != nil {
				hooks.storageExited()
			}
		}()
		if hooks != nil && hooks.storagePump != nil {
			hooks.storagePump(ctx, storage)
			return
		}
		storage.pump(ctx)
	}()

	var host *actorhost.HostSupervisor
	var currentSession *link.AuthenticatedLinkSession
	closeRuntime := func() {
		defer runtimeCancel()
		sealCtx, sealCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := outbound.Seal(sealCtx); err != nil {
			retErr = errors.Join(retErr, err)
		}
		sealCancel()
		if host != nil {
			hostCtx, hostCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := host.Close(hostCtx); err != nil {
				retErr = errors.Join(retErr, err)
			}
			hostCancel()
		}
		if err := outbound.CloseResidual(); err != nil {
			retErr = errors.Join(retErr, err)
		}
		if currentSession != nil {
			outbound.SessionDown(currentSession)
			if err := currentSession.Close(); err != nil {
				retErr = errors.Join(retErr, err)
			}
			sessionCtx, sessionCancel := context.WithTimeout(context.Background(), 10*time.Second)
			select {
			case <-currentSession.Done():
			case <-sessionCtx.Done():
				retErr = errors.Join(retErr, errors.New("compute: link session leak"))
			}
			sessionCancel()
			currentSession = nil
		}
		timeout := 5 * time.Second
		if hooks != nil && hooks.forwarderTimeout > 0 {
			timeout = hooks.forwarderTimeout
		}
		joined := make(chan struct{})
		go func() {
			storageWG.Wait()
			close(joined)
		}()
		select {
		case <-joined:
		case <-time.After(timeout):
			if hooks != nil && hooks.forwarderLeaked != nil {
				hooks.forwarderLeaked.Add(1)
			}
			retErr = errors.Join(retErr, ErrForwardersLeaked)
		}
	}
	defer closeRuntime()

	planWake := make(chan struct{}, 1)
	backoff := redialInitialBackoff
	for {
		dialCfg := link.DialConfig{
			PlanChanged: func() {
				select {
				case planWake <- struct{}{}:
				default:
				}
			},
			LocalFileOpener: cfg.LocalFileOpener,
		}
		if cfg.StorageHost != nil {
			dialCfg.AllocHandler = storage.handleAlloc
		}
		dialer, err := link.Dial(ctx, cfg.ServerWS, dialCfg, logger)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("platform.compute.dial_failed", "err", err, "retry_in", backoff)
			if !waitBackoff(ctx, jitterBackoff(backoff)) {
				return nil
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}

		if host == nil {
			domain := actorhost.ExecutionDomain(dialer.DaemonID())
			host, err = actorhost.New(actorhost.Config{
				Parent:      runtimeCtx,
				Domain:      domain,
				Logger:      logger,
				Events:      &daemonHostEvents{outbound: outbound},
				BodyBuilder: daemonBodyBuilder(outbound, cfg.PlanSource),
			})
			if err != nil {
				_ = dialer.Close()
				return err
			}
		}

		session, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
			Peer: actorhost.ExecutionDomain("server"),
			OpenActorStream: func(
				openCtx context.Context,
				id actor.ActorID,
				key actorhost.AttemptKey,
			) (link.ActorStreamResource, error) {
				return dialer.OpenExactActorStream(openCtx, id, key, host)
			},
			CloseTransport: dialer.Close,
			TransportDone:  dialer.Done(),
		})
		if err != nil {
			_ = dialer.Close()
			return err
		}
		storage.Rebind(dialer)

		if err := acceptDaemonPlan(ctx, dialer, cfg.PlanSource, host); err != nil {
			logger.Warn("platform.compute.plan_rejected", "err", err)
			_ = session.Close()
			<-session.Done()
			if ctx.Err() != nil {
				return nil
			}
			if !waitBackoff(ctx, jitterBackoff(backoff)) {
				return nil
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}
		if err := outbound.SetSession(session); err != nil {
			_ = session.Close()
			<-session.Done()
			return err
		}
		currentSession = session
		host.Wake()
		outbound.Wake()
		backoff = redialInitialBackoff

		ticker := time.NewTicker(poll)
		connected := true
		for connected {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return nil
			case <-session.Done():
				connected = false
			case <-planWake:
				if err := acceptDaemonPlan(ctx, dialer, cfg.PlanSource, host); err != nil {
					logger.Warn("platform.compute.plan_refresh_failed", "err", err)
				}
			case <-ticker.C:
				if err := acceptDaemonPlan(ctx, dialer, cfg.PlanSource, host); err != nil {
					logger.Warn("platform.compute.plan_refresh_failed", "err", err)
				}
			}
		}
		ticker.Stop()
		currentSession = nil
		outbound.SessionDown(session)
		_ = session.Close()
		logger.Warn("platform.compute.link_down", "retry_in", backoff)
		if !waitBackoff(ctx, jitterBackoff(backoff)) {
			return nil
		}
		backoff = nextRedialBackoff(backoff)
	}
}

func acceptDaemonPlan(
	ctx context.Context,
	dialer *link.Dialer,
	source PlanSource,
	host *actorhost.HostSupervisor,
) error {
	plan, err := dialer.PullPlan(ctx)
	if err != nil {
		return err
	}
	if err := source.ApplyPlan(plan); err != nil {
		return err
	}
	desired := make([]actorhost.Desired, 0, len(plan))
	for _, row := range plan {
		desired = append(desired, actorhost.BodyDesired{
			ActorID: row.ActorID, AttemptKey: row.AttemptKey,
			ExecutionSpec: actorhost.ExecutionSpec{
				Kind: row.Kind, Class: row.Class,
				Config: append([]byte(nil), row.Config...),
			},
		})
	}
	return host.AcceptFullDesired(desired)
}

func nextRedialBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > redialMaxBackoff {
		return redialMaxBackoff
	}
	return next
}

func jitterBackoff(value time.Duration) time.Duration {
	half := value / 2
	if half <= 0 {
		return value
	}
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

func waitBackoff(ctx context.Context, value time.Duration) bool {
	timer := time.NewTimer(value)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var ErrForwardersLeaked = errors.New("compute: forwarders leaked")

var _ actorhost.HostEventSink = (*daemonHostEvents)(nil)
