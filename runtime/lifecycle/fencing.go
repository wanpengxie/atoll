package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/runtime/store"
)

// FencingChecker validates a (fencing_token, daemon_epoch) tuple against
// a channel's local channel_lock row.
//
// It is the gate every channel-local write must clear: lifecycle/create
// validates it before bootstrapping; workerhost/fence validates every
// inbound IPC mutation; scheduler validates before timer-driven writes.
type FencingChecker struct {
	lock *store.ChannelLock
}

// NewFencingChecker builds a checker bound to one channel's sqlite.
func NewFencingChecker(lock *store.ChannelLock) (*FencingChecker, error) {
	if lock == nil {
		return nil, errors.New("lifecycle: NewFencingChecker lock nil")
	}
	return &FencingChecker{lock: lock}, nil
}

// Validate verifies the supplied tuple matches the row's stored
// fencing_token + daemon_epoch.
//
// Returns one of:
//
//   - nil — write is allowed.
//   - ErrFenceMismatch — channel exists but the tuple is wrong (caller
//     should refuse the mutation; worker IPC paths translate this to a
//     fence_invalid IPC reply so the stale worker exits).
//   - ErrChannelUnbound — channel_lock row is missing entirely (channel
//     not yet bootstrapped).
func (c *FencingChecker) Validate(
	ctx context.Context,
	fencing placement.FencingToken,
	daemonEpoch placement.DaemonEpoch,
) error {
	row, ok, err := c.lock.Get(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle: fencing get: %w", err)
	}
	if !ok {
		return ErrChannelUnbound
	}
	if row.FencingToken != fencing || row.DaemonEpoch != daemonEpoch {
		return &FenceMismatchError{
			HaveToken: row.FencingToken,
			GotToken:  fencing,
			HaveEpoch: row.DaemonEpoch,
			GotEpoch:  daemonEpoch,
		}
	}
	return nil
}

// Snapshot returns the current fencing snapshot. Used by workerhost to
// stamp every spawned worker with the right tuple.
func (c *FencingChecker) Snapshot(ctx context.Context) (placement.FencingToken, placement.DaemonEpoch, error) {
	row, ok, err := c.lock.Get(ctx)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, ErrChannelUnbound
	}
	return row.FencingToken, row.DaemonEpoch, nil
}

// ErrChannelUnbound is returned when the channel_lock row is absent.
var ErrChannelUnbound = errors.New("lifecycle: channel_lock row missing (channel unbound)")

// FenceMismatchError carries the actual vs expected fencing tuple.
type FenceMismatchError struct {
	HaveToken placement.FencingToken
	GotToken  placement.FencingToken
	HaveEpoch placement.DaemonEpoch
	GotEpoch  placement.DaemonEpoch
}

// Error implements error.
func (e *FenceMismatchError) Error() string {
	return fmt.Sprintf(
		"lifecycle: fence mismatch (have token=%d epoch=%d, got token=%d epoch=%d)",
		e.HaveToken, e.HaveEpoch, e.GotToken, e.GotEpoch,
	)
}
