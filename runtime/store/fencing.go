package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/runtime/fence"
)

// FencingTuple bundles the (fencing_token, daemon_epoch) pair that
// every channel-local mutation must present to the channel_lock fencing
// gate. Ledger operations still receive this tuple through context.Context;
// kernel/harness.MessageLog.Append receives its tuple explicitly.
//
// Production wiring (runtime/workerhost) stamps the tuple onto the
// per-request context before calling Ledger.Reserve / Ledger.Commit.
// Pure-store unit tests that do not exercise fencing pass an unstamped
// context and use NewLedger without a lock so the validate step is skipped.
type FencingTuple struct {
	Token fence.FencingToken
	Epoch fence.DaemonEpoch
}

// WriteFence is the pure store-side fencing contract. Concrete channel
// ownership schemes live outside runtime/store and inject an implementation.
type WriteFence interface {
	ValidateWriteTx(ctx context.Context, tx *sql.Tx, token fence.FencingToken, epoch fence.DaemonEpoch) error
}

// WriteFenceFunc adapts a function into WriteFence.
type WriteFenceFunc func(ctx context.Context, tx *sql.Tx, token fence.FencingToken, epoch fence.DaemonEpoch) error

// ValidateWriteTx implements WriteFence.
func (f WriteFenceFunc) ValidateWriteTx(ctx context.Context, tx *sql.Tx, token fence.FencingToken, epoch fence.DaemonEpoch) error {
	return f(ctx, tx, token, epoch)
}

// FencingStaleError is the typed error returned by WriteFence
// implementations when the caller's (token, epoch) tuple does not match
// the current channel owner. Callers map this to
// message.HarnessWorkerFencingStale per L1 §10.3.1.
type FencingStaleError struct {
	HaveToken fence.FencingToken
	GotToken  fence.FencingToken
	HaveEpoch fence.DaemonEpoch
	GotEpoch  fence.DaemonEpoch
	Reason    string
}

// Error implements error.
func (e *FencingStaleError) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason != "" {
		return "store: fencing stale: " + e.Reason
	}
	return fmt.Sprintf(
		"store: fencing stale (have token=%q epoch=%d, got token=%q epoch=%d)",
		e.HaveToken, e.HaveEpoch, e.GotToken, e.GotEpoch,
	)
}

// IsFencingStale reports whether err is (or wraps) a FencingStaleError.
func IsFencingStale(err error) bool {
	var fse *FencingStaleError
	return errors.As(err, &fse)
}

type ctxKeyFencing struct{}

// CtxWithFencing returns a child ctx carrying the fencing tuple. Call
// this at the edge (workerhost.handle*, scheduler tick, lifecycle
// channel-bound write) before invoking any channel-local mutation.
func CtxWithFencing(ctx context.Context, token fence.FencingToken, epoch fence.DaemonEpoch) context.Context {
	return context.WithValue(ctx, ctxKeyFencing{}, FencingTuple{Token: token, Epoch: epoch})
}

// FencingFromCtx returns the tuple stamped by CtxWithFencing. ok=false
// when no tuple has been stamped — concrete fenced stores treat the
// missing tuple as a stale write (HarnessWorkerFencingStale) when they
// were constructed with a non-nil ChannelLock.
func FencingFromCtx(ctx context.Context) (FencingTuple, bool) {
	v, ok := ctx.Value(ctxKeyFencing{}).(FencingTuple)
	return v, ok
}
