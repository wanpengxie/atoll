package harness

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khlog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TypeView is the read-only projection of one type_registry row the
// harness needs at write time. It mirrors the install-time fields the
// adapter framework writes (allowed_kinds, handler_actor_id,
// max_pending_ms) — runtime/harness deliberately keeps the contract
// minimal so multiple registry implementations (in-memory, sqlite,
// federation) can fulfill it.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload is opaque
// to the protocol layer; the harness does NOT validate payload schemas
// and the type_registry does NOT store payload schemas. Payload
// consistency between caller and handler is a product-layer concern.
type TypeView struct {
	// Type is the envelope.type value, e.g. "feishu.chat.send".
	Type string

	// AllowedKinds is the closed set of envelope.kind the harness will
	// accept for this type (step 5 reject reason: kind_not_allowed).
	AllowedKinds []message.Kind

	// MaxPendingMs is the per-type request timeout used when a request's
	// receiver is a tool and the envelope omitted expires_at.
	MaxPendingMs int64

	// HandlerActorID is the type's default concrete receiver (L2
	// §1.4.2). When non-empty AND envelope.kind==request, step 5
	// asserts the explicit audience equals this id
	// (audience_handler_mismatch).
	HandlerActorID actor.ActorID
}

// TypeRegistry is the read seam the harness uses to fetch one type_view
// row at write time. Implementations live in adapters/framework (memory
// + sqlite) — runtime/harness consumes the interface only.
type TypeRegistry interface {
	// Lookup returns the registered view for typeName, or ok=false when
	// the type is not registered. The harness step 4 / step 5 fan out
	// from this — a not-found triggers unknown_type reject.
	Lookup(ctx context.Context, typeName string) (TypeView, bool, error)
}

// Logger is the minimal structured logger used by the harness hot path.
// It mirrors the adapter framework's Logger shape without importing the
// framework package into runtime/harness.
type Logger interface {
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NoopLogger drops every log call.
type NoopLogger struct{}

func (NoopLogger) Debug(string, ...any) {}
func (NoopLogger) Warn(string, ...any)  {}
func (NoopLogger) Error(string, ...any) {}

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

	// AllowProvidedSenderKind reports whether the caller transport is
	// allowed to ship a non-empty envelope.sender.kind value. Trusted
	// in-process callers (e.g. adapter framework) set this true; daemon
	// RPC / worker IPC set it false because the daemon writes the kind
	// from actor_registry. Step 3 ENFORCES the registry value either
	// way; the flag controls how mismatch is reported (silent overwrite
	// vs sender_kind_mismatch).
	AllowProvidedSenderKind bool
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

	// ActorRegistry resolves sender.id / audience entries / handler_actor_id
	// to actor.Record (kind / binding / deregistration timestamp).
	// Required.
	ActorRegistry actor.Registry

	// TypeRegistry resolves business types declared by adapters /
	// channel template. Optional; when nil the chain assumes only core
	// types are allowed (every business type fails step 4 unknown_type).
	TypeRegistry TypeRegistry

	// Log is the channel-local messages-table sink. Required — step 9
	// engine append calls Log.Append; step 3 (and step 8 catch) calls
	// Log.FindByID for dedupe / parent existence checks.
	Log khlog.MessageLog

	// Fencing is the explicit ownership tuple passed to Log.Append at
	// step 9. Unfenced test logs ignore the zero value; production daemon
	// wiring must populate it from the channel_lock row.
	Fencing khlog.FencingTuple

	// NowMs returns unix-ms (engine ts_received write source). Defaults
	// to time.Now when nil.
	NowMs func() int64

	// Logger receives per-step pass/reject diagnostics. nil → NoopLogger.
	Logger Logger

	// Metrics receives per-reject counters. nil → NoopMetrics.
	Metrics Metrics

	// DefaultAudience is the resolve-half seam for StepAudienceResolve: it
	// returns the channel's declared default routing rule for the given
	// channel — the concrete actor(s) a human's empty-audience write
	// resolves to. Today the daemon wires this to the channel template's
	// HumanCallerDefaultAudience (per-channel seed); it can later be
	// swapped for an event-sourced topology projection without touching
	// the step. nil (or returning empty) means "no default" — the empty
	// audience then falls through to StepKindAndAudience which rejects it
	// (harness_audience_empty), unchanged. Only consulted when
	// sender.kind==human and audience is empty.
	DefaultAudience func(channel.ID) []actor.ActorID
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

// CtxWithCaller returns a child ctx carrying caller. Use at the edge
// (workerhost / control handler / adapter framework respond) before
// invoking Chain.Write.
func CtxWithCaller(ctx context.Context, c CallerContext) context.Context {
	return context.WithValue(ctx, ctxKeyCaller{}, c)
}

// CallerFromCtx pulls the CallerContext set by CtxWithCaller. Returns
// the zero value when absent.
func CallerFromCtx(ctx context.Context) CallerContext {
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
// JSON bytes the caller decoded into *message.Envelope. Wire-level
// callers (worker IPC, HTTP API) SHOULD plumb this so Step 2 can
// fail-closed on unknown top-level fields. In-process Go callers MAY
// omit it — the struct-typed Envelope already pins the field set.
func CtxWithRawEnvelope(ctx context.Context, raw []byte) context.Context {
	return context.WithValue(ctx, ctxKeyRawEnvelope{}, raw)
}

// RawEnvelopeFromCtx pulls the raw envelope JSON bytes set by
// CtxWithRawEnvelope. Returns nil when absent.
func RawEnvelopeFromCtx(ctx context.Context) []byte {
	if v, ok := ctx.Value(ctxKeyRawEnvelope{}).([]byte); ok {
		return v
	}
	return nil
}
