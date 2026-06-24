package harness

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// chain is the write engine's concrete step pipeline (it has no separate
// contract interface — the former pure-contract harness package was deleted;
// the engine's own seams live here in runtime/harness).
// It assembles the 9 numbered steps declared in proto-layer1 §2.0
// (Step 0 entry gate + Step 1-9 main pipeline) and runs them in stable
// ascending-ID order, short-circuiting on the first reject (or on the
// first idempotent dedupe hit).
//
// chain is the BARE writer (no identity). It is封死在包内 (unexported): the
// substrate never hands out a bare writer. New藏 it inside a minter and returns
// the minter; Mint welds an identity onto it via boundPen. write is safe for
// concurrent use as long as Deps implementations are concurrent-safe (the
// standard sqlite-backed store / actor registry satisfy this).
type chain struct {
	deps  Deps
	steps []step
}

// New assembles the write engine from Deps and returns a Minter — the铸笔机.
// Returns an error when Deps is incomplete or any step refuses to construct.
//
// New constructs the bare chain internally, hides it inside a minter, and
// returns the minter — the platform receives a铸笔机 (Minter), never the bare
// chain. The bare writer's visibility is compile-time封顶 inside the harness
// package.
//
// Construction-confined to platform: a write engine built outside the platform
// assembly has no commit signal (store OnCommit), closure reconciler, or
// presence-watcher wiring — a half-wired write门. New/Deps may therefore only
// be referenced by the platform tree (enforced by
// archtest.TestHarnessConstructionConfinedToPlatform); downstream speaks the
// harness.Pen / WriteResult seam, never builds the engine itself.
func New(deps Deps) (Minter, error) {
	if err := deps.Validate(); err != nil {
		return nil, err
	}
	if deps.NowMs == nil {
		deps.NowMs = func() int64 { return time.Now().UnixMilli() }
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Metrics == nil {
		deps.Metrics = NoopMetrics{}
	}

	steps := []step{
		newStepCallerAuth(deps),
		newStepEnvelopeShape(deps),
		newStepNormalize(deps),
		newStepSenderConsistent(deps),
		newStepTypeRegistered(deps),
		newStepKindAndAudience(deps),
		newStepResponsePairing(deps),
		// StepEngineAppend (step 9) is fused into chain.write so the Step
		// interface can stay pure (no side-effects beyond envelope
		// mutation). Keeping engine append out of the Step slice also
		// lets unit tests run steps 0..8 in isolation with a stub Log.
	}
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].ID() < steps[j].ID() })

	return &minter{chain: &chain{deps: deps, steps: steps}}, nil
}

// write runs the chain against env per proto-layer1 §2.0. The envelope
// is mutated in place during StepNormalize so the caller observes
// default-filled fields when the call returns.
func (c *chain) write(ctx context.Context, env *message.Envelope) (res WriteResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("harness: panic: %v\n%s", r, debug.Stack())
		}
	}()
	if env == nil {
		return WriteResult{}, errors.New("harness: nil envelope")
	}

	// Single linear loop: steps run in strict ascending-ID order.
	//
	// The store-derived is_terminal no longer lives on the envelope (kernel
	// purified it); it is captured here from the step that owns it
	// (StepResponsePairing) and handed to Append.
	var isTerminal bool
	for _, s := range c.steps {
		out, err := s.Run(ctx, env)
		if err != nil {
			c.observeError(ctx, env, s.ID(), err)
			return WriteResult{}, err
		}
		if !out.Continue() {
			c.observeReject(ctx, env, s.ID(), out.RejectReason, out.Detail)
			return rejectFromOutcome(out, env), nil
		}
		if out.IsTerminal {
			isTerminal = true
		}
		c.observePass(ctx, env, s.ID())
	}

	// StepEngineAppend — canonical sink. is_terminal (StepResponsePairing)
	// was captured above; the store allocates seq per L2 §1.4.1.
	env.TSReceived = c.deps.NowMs()
	appendRes, err := c.deps.Log.Append(ctx, env, isTerminal)
	if err != nil {
		// Map the typed AppendError to a closed-set reject when possible.
		// storespec.AppendError.Reason is the wire string (storespec must
		// not import harness); convert back to the typed reject here.
		var appErr *storespec.AppendError
		if errors.As(err, &appErr) {
			reason := HarnessRejectReason(appErr.Reason)
			c.observeReject(ctx, env, StepEngineAppend, reason, appErr.Detail)
			return WriteResult{
				MessageID:    appErr.PartialMessageID,
				RejectReason: reason,
				RejectDetail: appErr.Detail,
			}, nil
		}
		c.observeError(ctx, env, StepEngineAppend, err)
		return WriteResult{}, fmt.Errorf("harness: engine append: %w", err)
	}

	c.observePass(ctx, env, StepEngineAppend)
	return WriteResult{
		MessageID: env.ID,
		Seq:       int64(appendRes.Seq),
	}, nil
}

func (c *chain) observePass(ctx context.Context, env *message.Envelope, step stepID) {
	c.deps.Logger.Debug("harness.write.step_ok",
		"step", int(step),
		"step_name", stepName(step),
		"channel_id", string(env.ChannelID),
		"message_id", string(env.ID),
		"correlation_id", string(env.CorrelationID),
		"request_id", requestIDFromCtx(ctx),
		"type", env.Type,
		"kind", string(env.Kind),
	)
}

func (c *chain) observeReject(ctx context.Context, env *message.Envelope, step stepID, reason HarnessRejectReason, detail string) {
	if reason == "" {
		return
	}
	c.deps.Metrics.IncCounter("harness.reject", "reason", string(reason), "step", stepName(step))
	c.deps.Logger.Warn("harness.write.reject",
		"step", int(step),
		"step_name", stepName(step),
		"reason", string(reason),
		"detail", detail,
		"channel_id", string(env.ChannelID),
		"message_id", string(env.ID),
		"correlation_id", string(env.CorrelationID),
		"request_id", requestIDFromCtx(ctx),
		"type", env.Type,
		"kind", string(env.Kind),
	)
}

func (c *chain) observeError(ctx context.Context, env *message.Envelope, step stepID, err error) {
	c.deps.Metrics.IncCounter("harness.error", "step", stepName(step))
	c.deps.Logger.Error("harness.write.error",
		"step", int(step),
		"step_name", stepName(step),
		"error", err.Error(),
		"channel_id", string(env.ChannelID),
		"message_id", string(env.ID),
		"correlation_id", string(env.CorrelationID),
		"request_id", requestIDFromCtx(ctx),
		"type", env.Type,
		"kind", string(env.Kind),
	)
}

func stepName(step stepID) string {
	switch step {
	case StepCallerAuth:
		return "caller_auth"
	case StepEnvelopeShape:
		return "envelope_shape"
	case StepNormalize:
		return "normalize"
	case StepSenderConsistent:
		return "sender_consistent"
	case StepTypeRegistered:
		return "type_registered"
	case StepKindAndAudience:
		return "kind_and_audience"
	case StepResponsePairing:
		return "response_pairing"
	case StepEngineAppend:
		return "engine_append"
	default:
		return fmt.Sprintf("step_%d", step)
	}
}

// rejectFromOutcome packages a step outcome into a WriteResult.
func rejectFromOutcome(out outcome, env *message.Envelope) WriteResult {
	r := WriteResult{
		RejectReason: out.RejectReason,
		RejectDetail: out.Detail,
	}
	if env != nil && env.ID != "" {
		r.MessageID = env.ID
	}
	return r
}

// ctxKeyRequestID carries an optional request-id for per-step log correlation.
// Replaces the deleted pkg/requestctx — the engine owns its own minimal ctx key
// rather than importing an observability util (topology §1 法则).
type ctxKeyRequestID struct{}

// WithRequestID attaches a request-id to ctx for harness log correlation.
// Binding→harness INJECTION SEAM (like CtxWithRawEnvelope): wire-level bindings
// carry a request-id at the edge and plumb it in so the harness's per-step
// diagnostics correlate to the originating request. The getter
// (requestIDFromCtx) is harness-internal plumbing and stays unexported.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID{}, id)
}

func requestIDFromCtx(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyRequestID{}).(string)
	return v
}
