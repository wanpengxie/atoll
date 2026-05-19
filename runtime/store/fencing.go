package store

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/placement"
)

// FencingTuple bundles the (fencing_token, daemon_epoch) pair that
// every channel-local mutation must present to the channel_lock fencing
// gate. Ledger operations still receive this tuple through context.Context;
// kernel/log.MessageLog.Append receives its tuple explicitly.
//
// Production wiring (runtime/workerhost) stamps the tuple onto the
// per-request context before calling Ledger.Reserve / Ledger.Commit.
// Pure-store unit tests that do not exercise fencing pass an unstamped
// context and use NewLedger without a lock so the validate step is skipped.
type FencingTuple struct {
	Token placement.FencingToken
	Epoch placement.DaemonEpoch
}

type ctxKeyFencing struct{}

// CtxWithFencing returns a child ctx carrying the fencing tuple. Call
// this at the edge (workerhost.handle*, scheduler tick, lifecycle
// channel-bound write) before invoking any channel-local mutation.
func CtxWithFencing(ctx context.Context, token placement.FencingToken, epoch placement.DaemonEpoch) context.Context {
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
