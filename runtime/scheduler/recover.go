package scheduler

import (
	"context"
	"errors"
)

// RecoverFn is invoked at daemon startup (phase 3 of T1.6) to rescan
// the messages table for any pending requests whose not_before has
// already passed, arming the in-memory timers.
type RecoverFn func(ctx context.Context, nowMs int64) error

// Recoverer runs the startup recovery step exactly once.
type Recoverer struct {
	recover RecoverFn
	nowFn   func() int64
}

// NewRecoverer builds a Recoverer.
func NewRecoverer(recover RecoverFn, nowFn func() int64) (*Recoverer, error) {
	if recover == nil {
		return nil, errors.New("scheduler: NewRecoverer recover nil")
	}
	if nowFn == nil {
		return nil, errors.New("scheduler: NewRecoverer nowFn nil")
	}
	return &Recoverer{recover: recover, nowFn: nowFn}, nil
}

// Run executes the recovery scan.
func (r *Recoverer) Run(ctx context.Context) error {
	return r.recover(ctx, r.nowFn())
}
