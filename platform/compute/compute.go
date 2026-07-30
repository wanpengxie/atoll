package compute

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
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

// Config configures one daemon execution domain. Factories resolves a class
// into this daemon's factory at body-build time — the desired snapshot pulled
// over the link is the ONE plan ledger, exactly as on the server host; actor
// bodies and the physical link are deliberately owned by different organs.
type Config struct {
	ServerWS         string
	Logger           *slog.Logger
	Factories        ActorFactorySource
	Poll             time.Duration
	StorageHost      StorageHost
	ScrubberInterval time.Duration
	LocalFileOpener  LocalFileOpener
}

type daemonHostEvents struct{ outbound *DaemonOutbound }

func (*daemonHostEvents) OnBodyExited(actor.ActorID, actorhost.AttemptKey, actorrt.Incarnation, error) {
}

// OnBodyObs forwards one observation the Host already ruled current. The
// Incarnation is what selects the slot — the coordinate cannot, since an
// abandoned build shares it with the body that replaced it.
func (e *daemonHostEvents) OnBodyObs(
	_ actor.ActorID,
	_ actorhost.AttemptKey,
	self actorrt.Incarnation,
	kind actorrt.ObsKind,
	value actorrt.ObsValue,
) {
	e.outbound.publishObs(self, kind, value)
}

func daemonBodyBuilder(
	outbound *DaemonOutbound,
	factories ActorFactorySource,
	logger *slog.Logger,
) actorhost.BodyBuilder {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(input actorhost.BodyBuildInput) actorrt.Actor {
		prepared, prepareErr := outbound.Prepare(
			input.ActorID,
			input.AttemptKey,
			input.Self,
			input.Identity,
			input.Attempt,
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
		// The factory is derived here, from the spec this build claim carries —
		// the exact generation by construction. A class this daemon cannot build
		// fails this body alone, loudly; the Host retries it on its own backoff
		// while every other row converges.
		factory, ok := factories.BuildClass(
			input.ActorID,
			input.ExecutionSpec.Class,
			input.ExecutionSpec.Config,
		)
		if !ok {
			logger.Error("platform.compute.actor_factory_missing",
				"actor", input.ActorID, "class", input.ExecutionSpec.Class,
				"reason", "class_not_registered")
			return nil
		}
		hooks := actorbase.Hooks{Canceller: func(_ actor.ActorID, requestID message.ID) {
			_ = prepared.Slot.CancelRequest(requestID)
		}}
		body := hostcommon.Build(prepared.Caps, hooks, factory)
		wrapped := prepared.Wrap(body)
		if wrapped != nil {
			transferred = true
		}
		return wrapped
	}
}

func Run(ctx context.Context, cfg Config) (retErr error) {
	if cfg.Factories == nil {
		return errors.New("compute: Factories required")
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
		retErr = errors.Join(retErr, awaitForwarders(&storageWG, 5*time.Second))
	}
	defer closeRuntime()

	planWake := make(chan struct{}, 1)
	sessionLedger := link.NewRemoteSessionLedger(logger)
	backoff := redialInitialBackoff
	for {
		dialCfg := link.DialConfig{
			SessionLedger: sessionLedger,
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
				BodyBuilder: daemonBodyBuilder(outbound, cfg.Factories, logger),
			})
			if err != nil {
				_ = dialer.Close()
				return err
			}
		}

		session, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
			Peer:      actorhost.ExecutionDomain("server"),
			Authority: dialer.Authority(),
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

		if err := acceptDaemonPlan(ctx, dialer, host); err != nil {
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
				if err := acceptDaemonPlan(ctx, dialer, host); err != nil {
					logger.Warn("platform.compute.plan_refresh_failed", "err", err)
				}
			case <-ticker.C:
				if err := acceptDaemonPlan(ctx, dialer, host); err != nil {
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

// acceptDaemonPlan pulls the authenticated plan and hands it to the Host as
// one desired snapshot. That is the whole of it: there is no second ledger to
// publish into, so nothing can sit on a different generation than the desired
// the Host serves. A row this daemon cannot build is discovered at that row's
// own body build, logged there, and retried on the Host's backoff — it does
// not hold the rest of the plan hostage the way whole-plan eager rejection
// did (which kept truth-dead bodies running and healthy rows waiting for as
// long as one bad row stayed bad).
func acceptDaemonPlan(
	ctx context.Context,
	dialer *link.Dialer,
	host *actorhost.HostSupervisor,
) error {
	plan, err := dialer.PullPlan(ctx)
	if err != nil {
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

// awaitForwarders joins the forwarder waitgroup within timeout. A timeout
// means a forwarder goroutine outlived the ordered close DAG: root ownership
// still transfers (the caller returns), and the incident surfaces as
// ErrForwardersLeaked instead of a silent hang.
func awaitForwarders(wg *sync.WaitGroup, timeout time.Duration) error {
	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
		return nil
	case <-time.After(timeout):
		return ErrForwardersLeaked
	}
}

var _ actorhost.HostEventSink = (*daemonHostEvents)(nil)
