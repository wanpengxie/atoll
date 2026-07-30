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
	redialInitialBackoff   = time.Second
	redialMaxBackoff       = 30 * time.Second
	carrierAcceptTimeout   = 10 * time.Second
	compartmentJoinTimeout = 30 * time.Second
)

type Config struct {
	ServerWS         string
	Credential       string
	AtollHome        string
	Logger           *slog.Logger
	BuildCompartment CompartmentBuilder
	ScrubberInterval time.Duration
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

func Run(ctx context.Context, cfg Config) error {
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
		manager.close()
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
			if !waitBackoff(ctx, jitterBackoff(backoff)) {
				return nil
			}
			backoff = nextRedialBackoff(backoff)
			continue
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
		select {
		case <-ctx.Done():
			_ = wire.Close()
			return nil
		case err := <-manager.terminal:
			_ = wire.Close()
			return err
		case <-wire.Done():
			select {
			case err := <-manager.terminal:
				return err
			default:
			}
			manager.carrierDown(wire)
		}
		if !waitBackoff(ctx, jitterBackoff(backoff)) {
			return nil
		}
		backoff = nextRedialBackoff(backoff)
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

var ErrForwardersLeaked = errors.New("compute: forwarders leaked")
var _ actorhost.HostEventSink = (*daemonHostEvents)(nil)
