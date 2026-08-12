package compute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const (
	redialInitialBackoff = time.Second
	redialMaxBackoff     = 30 * time.Second
	carrierAcceptTimeout = 10 * time.Second

	// compartmentPlanInterval is the floor on how stale this device's
	// compartment set can be with no poke at all. Pokes only buy latency; this
	// tick is what makes the loop self-healing when one is lost.
	compartmentPlanInterval = 30 * time.Second
	compartmentPlanTimeout  = 20 * time.Second
)

type Config struct {
	ServerWS         string
	Credential       string
	AtollHome        string
	Logger           *slog.Logger
	BuildCompartment CompartmentBuilder
	// OnAttached, when set, is called with the server-assigned daemon id after
	// every ACCEPTED carrier attach (initial and reconnect alike, same id).
	// This is the one moment the daemon learns which daemons row it IS — the
	// assembly root uses it to complete the device home's persisted identity
	// triple {daemon_id, api_key, server_ws}. Called from the carrier
	// goroutine; implementations must be safe for that.
	OnAttached func(daemonID string)
}

type daemonHostEvents struct{ outbound *DaemonOutbound }

func (*daemonHostEvents) OnBodyExited(actor.ActorID, actorhost.AttemptKey, actorrt.Incarnation, error) {
}
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
		prepared, err := outbound.Prepare(
			input.ActorID, input.AttemptKey, input.Self,
			input.Identity, input.Attempt, input.Current,
		)
		if err != nil {
			return nil
		}
		transferred := false
		defer func() {
			if !transferred {
				_ = prepared.Slot.Close()
			}
		}()
		factory, ok := factories.BuildClass(
			input.ActorID, input.ExecutionSpec.Class, input.ExecutionSpec.Config)
		if !ok {
			logger.Error("platform.compute.actor_factory_missing",
				"actor", input.ActorID, "class", input.ExecutionSpec.Class)
			return nil
		}
		hooks := actorbase.Hooks{Canceller: func(_ actor.ActorID, requestID message.ID) {
			_ = prepared.Slot.CancelRequest(requestID)
		}}
		body := prepared.Wrap(hostcommon.Build(prepared.Caps, hooks, factory))
		if body != nil {
			transferred = true
		}
		return body
	}
}

type terminalCarrierError struct{ err error }

func (e terminalCarrierError) Error() string { return e.err.Error() }
func (e terminalCarrierError) Unwrap() error { return e.err }

func Run(ctx context.Context, cfg Config) (retErr error) {
	if cfg.ServerWS == "" {
		return errors.New("compute: ServerWS required")
	}
	if cfg.Credential == "" {
		return errors.New("compute: Credential required")
	}
	if cfg.AtollHome == "" {
		return errors.New("compute: AtollHome required")
	}
	if cfg.BuildCompartment == nil {
		return errors.New("compute: BuildCompartment required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	manager := newCompartmentManager(ctx, cfg, logger)
	var daemonLock *os.File
	var daemonID string
	defer func() {
		// Teardown's verdict joins Run's own: a close that had to abandon
		// workers surfaces its account to the caller instead of vanishing
		// behind the defer.
		retErr = errors.Join(retErr, manager.close())
		if daemonLock != nil {
			_ = daemonLock.Close()
		}
	}()
	backoff := redialInitialBackoff
	for {
		wire, response, err := link.DialDeviceCarrier(ctx, cfg.ServerWS, cfg.Credential, logger)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if terminalHTTP(response) {
				if response.Body != nil {
					_ = response.Body.Close()
				}
				return terminalCarrierError{fmt.Errorf("compute: carrier rejected: %s", response.Status)}
			}
			delay := retryAfter(response, jitterBackoff(backoff), time.Now())
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			logger.Warn("platform.compute.carrier_dial_failed",
				"err", err, "retry_in", delay.Round(time.Millisecond).String())
			if !waitBackoff(ctx, delay) {
				return nil
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}
		accepted, err := awaitCarrierVerdict(wire)
		if err != nil {
			_ = wire.Close()
			if terminal, ok := err.(terminalCarrierError); ok {
				return terminal
			}
			delay := jitterBackoff(backoff)
			logger.Warn("platform.compute.carrier_verdict_retryable",
				"err", err, "retry_in", delay.Round(time.Millisecond).String())
			if !waitBackoff(ctx, delay) {
				return nil
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}
		if cfg.OnAttached != nil {
			cfg.OnAttached(accepted.DaemonID)
		}
		root, err := coordinatePath(filepath.Join(cfg.AtollHome, "daemons"), accepted.DaemonID)
		if err != nil {
			_ = wire.Close()
			return terminalCarrierError{fmt.Errorf("compute: invalid daemon root: %w", err)}
		}
		if daemonLock == nil {
			if err := os.MkdirAll(root, 0o700); err != nil {
				_ = wire.Close()
				return fmt.Errorf("compute: create daemon root: %w", err)
			}
			daemonLock, err = lockDaemonRoot(root)
			if err != nil {
				_ = wire.Close()
				return err
			}
			daemonID = accepted.DaemonID
		} else if accepted.DaemonID != daemonID {
			_ = wire.Close()
			return terminalCarrierError{fmt.Errorf(
				"compute: authenticated daemon identity changed from %q to %q",
				daemonID, accepted.DaemonID)}
		}
		manager.bindCarrier(wire, accepted, root)
		backoff = redialInitialBackoff
		if err, retry := awaitCarrierCycle(ctx, manager.terminal, wire.Done()); !retry {
			_ = wire.Close()
			return err
		}
		manager.carrierDown(wire)
		if ctx.Err() == nil {
			logger.Warn("platform.compute.carrier_down", "daemon", daemonID)
		}
		if !waitBackoff(ctx, jitterBackoff(backoff)) {
			return nil
		}
		backoff = nextRedialBackoff(backoff)
	}
}

// awaitCarrierCycle gives an already-buffered terminal verdict precedence over
// the physical done edge it causes. A tombstone reject and transport closure
// can become readable together; choosing the latter as retryable would redial a
// daemon the authority has just permanently rejected.
func awaitCarrierCycle(
	ctx context.Context,
	terminal <-chan error,
	done <-chan struct{},
) (err error, retry bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case err := <-terminal:
		return err, false
	case <-done:
		select {
		case err := <-terminal:
			return err, false
		default:
			return nil, true
		}
	}
}

func awaitCarrierVerdict(wire *link.ClientCarrier) (link.SpineFrame, error) {
	type result struct {
		frame link.SpineFrame
		err   error
	}
	done := make(chan result, 1)
	go func() {
		var frame link.SpineFrame
		err := wire.ReadSpine(&frame)
		done <- result{frame: frame, err: err}
	}()
	timer := time.NewTimer(carrierAcceptTimeout)
	defer timer.Stop()
	select {
	case result := <-done:
		if result.err != nil {
			return link.SpineFrame{}, result.err
		}
		if err := result.frame.Validate(); err != nil {
			return link.SpineFrame{}, err
		}
		switch result.frame.Kind {
		case link.SpineCarrierAccept:
			if result.frame.DaemonID == "" || result.frame.CarrierGen == "" {
				return link.SpineFrame{}, errors.New("compute: malformed carrier_accept")
			}
			return result.frame, nil
		case link.SpineCarrierReject:
			err := fmt.Errorf("compute: carrier rejected: %s", result.frame.Reason)
			if result.frame.Class == link.CarrierTerminal {
				return link.SpineFrame{}, terminalCarrierError{err}
			}
			return link.SpineFrame{}, err
		default:
			return link.SpineFrame{}, errors.New("compute: carrier verdict required")
		}
	case <-timer.C:
		return link.SpineFrame{}, errors.New("compute: carrier_accept timeout")
	}
}

func terminalHTTP(response *http.Response) bool {
	if response == nil {
		return false
	}
	return response.StatusCode >= 400 && response.StatusCode < 500 &&
		response.StatusCode != http.StatusTooManyRequests
}

func retryAfter(response *http.Response, fallback time.Duration, now time.Time) time.Duration {
	if response == nil ||
		(response.StatusCode != http.StatusTooManyRequests && response.StatusCode < 500) {
		return fallback
	}
	value := strings.TrimSpace(response.Header.Get("Retry-After"))
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return fallback
}

func lockDaemonRoot(root string) (*os.File, error) {
	path := filepath.Join(root, ".atoll.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("compute: open daemon lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("compute: daemon root is already in use: %w", err)
	}
	return file, nil
}

func coordinatePath(base, coordinate string) (string, error) {
	if coordinate == "" || filepath.IsAbs(coordinate) ||
		filepath.Clean(coordinate) != coordinate || coordinate == "." ||
		strings.ContainsAny(coordinate, `/\`) {
		return "", errors.New("coordinate must be one relative path segment")
	}
	path := filepath.Join(base, coordinate)
	relative, err := filepath.Rel(base, path)
	if err != nil || relative == "." || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("coordinate escapes its root")
	}
	return path, nil
}

func nextRedialBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > redialMaxBackoff {
		return redialMaxBackoff
	}
	return next
}

func jitterBackoff(value time.Duration) time.Duration {
	span := value / 5
	if span <= 0 {
		return value
	}
	return value - span + time.Duration(rand.Int63n(int64(2*span)+1))
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

var _ actorhost.HostEventSink = (*daemonHostEvents)(nil)
