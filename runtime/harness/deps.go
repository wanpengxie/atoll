package harness

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// (The harness holds NO type-registry seam: the substrate is type-agnostic —
// business-type vocabulary / handler routing / discovery is a domain concern,
// validated by the receiving actor + resolved by the caller's catalog, never a
// substrate write-time check. The harness validates STRUCTURE: kind, addressing,
// closure.)

// (Logging is *log/slog* — the Go-std structured-logging facade, the same role
// K8s gives logr / Erlang gives the kernel logger: one facade the whole project
// funnels through, backend chosen via slog.Handler, injected from the edge. The
// substrate does not define its own Logger vocabulary — that was a reinvention
// of slog. nil → caller defaults to slog.New(slog.DiscardHandler).)

// Metrics is the minimal counter seam used for harness reject accounting.
type Metrics interface {
	IncCounter(name string, tags ...string)
}

// NoopMetrics drops every metric call.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, ...string) {}

// CallerContext carries the principal + transport metadata the harness
// needs to verify a write. It is plumbed through context.Context (see
// CtxWithCaller / CallerFromCtx) so step implementations do not need a
// per-step parameter and concrete bindings can populate it once at the
// edge.
type CallerContext struct {
	// ActorID is the authenticated principal that issued the write.
	// step 1 / step 3 compare it against envelope.sender.id.
	ActorID actor.ActorID

	// ChannelID is the channel binding the caller is authenticated for.
	// Step 0/1 rejects (harness_engine_acl_denied) when it differs from the
	// harness-bound channel.
	ChannelID channel.ID
}

// Deps bundles every collaborator a runtime harness Chain needs. One
// Deps instance is shared across all step implementations.
type Deps struct {
	// ChannelID identifies which channel this harness is bound to. The
	// chain enforces envelope.channel_id matches. Caller-context channel
	// mismatches reject as harness_engine_acl_denied at the entry gate;
	// envelope.channel_id mismatches reject as harness_channel_mismatch in
	// the envelope-shape step.
	ChannelID channel.ID

	// ActorRegistry resolves sender.id / audience entries to storespec.Record
	// (kind / binding / deregistration timestamp). Required. (The substrate is
	// type-agnostic — there is no TypeRegistry dep: business-type vocabulary is
	// a domain concern, not a substrate write-time check.)
	ActorRegistry storespec.Registry

	// Log is the channel-local messages-table sink. Required — step 9
	// engine append calls Log.Append. (v2: no fencing — single writer.)
	Log storespec.MessageLog

	// NowMs returns unix-ms (engine ts_received write source). Defaults
	// to time.Now when nil.
	NowMs func() int64

	// Logger receives per-step pass/reject diagnostics. nil → discard.
	Logger *slog.Logger

	// Metrics receives per-reject counters. nil → NoopMetrics.
	Metrics Metrics
}

// Validate returns nil when Deps is wired enough to assemble a Chain.
func (d Deps) Validate() error {
	if d.ChannelID == "" {
		return errors.New("harness: Deps.ChannelID required")
	}
	if d.ActorRegistry == nil {
		return errors.New("harness: Deps.ActorRegistry required")
	}
	if d.Log == nil {
		return errors.New("harness: Deps.Log required")
	}
	return nil
}

// ---------------------------------------------------------------------
// CallerContext plumbing via context.Context
// ---------------------------------------------------------------------

type ctxKeyCaller struct{}

// CtxWithCaller returns a child ctx carrying the caller identity. Must be set
// by the binding edge before invoking Chain.Write; absence is rejected as
// harness_engine_acl_denied.
func CtxWithCaller(ctx context.Context, c CallerContext) context.Context {
	return context.WithValue(ctx, ctxKeyCaller{}, c)
}

// CallerFromCtx pulls the CallerContext set by CtxWithCaller. Returns
// the zero value when absent.
func callerFromCtx(ctx context.Context) CallerContext {
	if v, ok := ctx.Value(ctxKeyCaller{}).(CallerContext); ok {
		return v
	}
	return CallerContext{}
}

// ---------------------------------------------------------------------
// Raw envelope JSON plumbing — needed by Step 2 envelope-shape unknown
// field fail-closed check (proto-layer0 §7.3).
// ---------------------------------------------------------------------

type ctxKeyRawEnvelope struct{}

// CtxWithRawEnvelope returns a child ctx carrying the original envelope
// JSON bytes the caller decoded into *message.Envelope. This is a
// binding→harness INJECTION SEAM (like CtxWithCaller): wire-level bindings
// (a connect-in port, the HTTP API) MUST plumb the raw wire bytes through it so Step 2's
// unknown-top-level-field fail-closed check (proto-layer0 §7.3) has the
// original JSON — without this setter exported, that check is
// dead-by-construction. In-process Go callers MAY omit it (the struct-typed
// Envelope already pins the field set). The getter is harness-internal
// plumbing and stays unexported.
func CtxWithRawEnvelope(ctx context.Context, raw []byte) context.Context {
	return context.WithValue(ctx, ctxKeyRawEnvelope{}, raw)
}

// rawEnvelopeFromCtx pulls the raw envelope JSON bytes set by
// CtxWithRawEnvelope. Returns nil when absent. Internal harness read.
func rawEnvelopeFromCtx(ctx context.Context) []byte {
	if v, ok := ctx.Value(ctxKeyRawEnvelope{}).([]byte); ok {
		return v
	}
	return nil
}
