package harness

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
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

// caller carries the principal + transport metadata the harness needs to
// verify a write. It is plumbed through context.Context (see ctxWithCaller /
// callerFromCtx) so step implementations do not need a per-step parameter; the
// boundPen populates it once from the welded (actorID, chID) before driving the
// chain. It is harness-internal (unexported): the substrate's pen-minting surface is Mint,
// which takes the raw (actorID, chID) — there is no caller-constructible
// identity context outside the package.
type caller struct {
	// actorID is the authenticated principal that issued the write.
	// step 1 / step 3 compare it against envelope.sender.id.
	actorID actor.ActorID

	// kind is the WELDED kind — set once at Mint time, the pen-authoritative
	// counterpart of actorID. stepSenderConsistent reads it (via
	// callerFromCtx) instead of querying the registry: kind is welded truth,
	// not a name-list lookup.
	kind actor.Kind

	// chID is the channel binding the caller is authenticated for.
	// Step 0/1 rejects (harness_engine_acl_denied) when it differs from the
	// harness-bound channel.
	chID channel.ID
}

// Deps bundles every collaborator the runtime write engine needs. One
// Deps instance is shared across all step implementations.
type Deps struct {
	// ChannelID identifies which channel this harness is bound to. The
	// chain enforces envelope.channel_id matches. Caller-context channel
	// mismatches reject as harness_engine_acl_denied at the entry gate;
	// envelope.channel_id mismatches reject as harness_channel_mismatch in
	// the envelope-shape step.
	ChannelID channel.ID

	// (There is NO ActorRegistry dep: the sender door trusts the pen weld —
	// identity + kind are welded at Mint, liveness is gated one layer up by
	// livePen.IsLive() — so no step reads the membership registry at write
	// time (the receiver/audience half was evicted earlier). The substrate is
	// likewise type-agnostic — no TypeRegistry dep either: business-type
	// vocabulary is a domain concern, not a substrate write-time check.)

	// Log is the channel-local messages-table sink. Required — step 9
	// engine append calls Log.Append. (v2: no fencing — single writer.)
	Log storespec.MessageLog

	// NowMs returns unix-ms (engine ts_received write source). Defaults
	// to time.Now when nil.
	NowMs func() int64

	// Logger receives per-step pass/reject diagnostics. nil → discard.
	Logger *slog.Logger
}

// Validate returns nil when Deps is wired enough to assemble the engine.
func (d Deps) Validate() error {
	if d.ChannelID == "" {
		return errors.New("harness: Deps.ChannelID required")
	}
	if d.Log == nil {
		return errors.New("harness: Deps.Log required")
	}
	return nil
}

// ---------------------------------------------------------------------
// caller plumbing via context.Context (harness-internal)
// ---------------------------------------------------------------------

type ctxKeyCaller struct{}

// ctxWithCaller returns a child ctx carrying the caller identity. The boundPen
// sets it from the welded principal before driving the chain; there is no
// exported setter — identity is never caller-plumbed, it is Mint-welded.
func ctxWithCaller(ctx context.Context, c caller) context.Context {
	return context.WithValue(ctx, ctxKeyCaller{}, c)
}

// callerFromCtx pulls the caller set by ctxWithCaller. Returns the zero value
// when absent.
func callerFromCtx(ctx context.Context) caller {
	if v, ok := ctx.Value(ctxKeyCaller{}).(caller); ok {
		return v
	}
	return caller{}
}

// (The former CtxWithRawEnvelope raw-JSON injection seam is gone: the
// unknown-top-level-field fail-closed check now rides the Envelope TYPE
// itself — message.Envelope.UnmarshalJSON rejects out-of-set keys at every
// wire decode, so there is no per-binding plumbing obligation left to
// enforce, and no binding can forget it.)
