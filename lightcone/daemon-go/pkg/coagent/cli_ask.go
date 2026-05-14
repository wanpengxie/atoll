package coagent

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// askFlags lists the flags `coagent ask` accepts. ask is the only
// subcommand that REQUIRES --type (per L2 §3.3 "ask 必填") and
// audience handling is special — see resolveAskAudience.
var askFlags = map[string]bool{
	"type": true, "payload": true, "payload-file": true, "parent": true,
	"correlation-id": true, "audience": true,
	"doc-refs": true, "expires-at": true,
	"visibility": true, "sender-id": true, "channel-id": true,
	// Intentionally omitted: --private, --system, --not-before. Per
	// L2 §3.3 these only apply to emit (kind=event semantics).
}

// runAsk implements `coagent ask` (L2 §3.2.2). The audience handling
// is the L2 §3.2.2 three-branch rule:
//
//  1. Explicit --audience: must be exactly one concrete actor
//     (length != 1 || contains '*' → client-side
//     request_audience_invalid). Harness then checks actor exists
//     and matches handler_actor_id.
//  2. Missing --audience: try Binding.ResolveHandlerActorID(type).
//     Success → fill audience automatically. Failure → client-side
//     request_audience_invalid (caller must pass --audience).
//  3. Any case: audience MUST NOT default to ['*'] (harness Step 5
//     rejects but we surface a friendlier message client-side first).
func runAsk(ctx context.Context, cfg Config, argv []string) int {
	fs := flag.NewFlagSet("coagent ask", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	cf := bindCommonFlags(fs, askFlags)
	flagArgs, positional := splitFlagArgs(argv, boolFlagNames())
	if err := fs.Parse(flagArgs); err != nil {
		return exitUsage
	}
	cf.markSet(fs)
	cf.MessageContent = strings.Join(positional, " ")

	turnCtx, terr := LoadTurnCtx(cfg.Env, cfg.HomeDir)
	if terr != nil {
		fmt.Fprintf(cfg.Stderr, "coagent: ask: %v (continuing with env only)\n", terr)
	}

	binding, code, msg := resolveBinding(cfg, turnCtx)
	if code != 0 {
		fmt.Fprintln(cfg.Stderr, "coagent: ask: "+msg)
		return code
	}

	env, opts, err := buildAskEnvelope(ctx, cfg, cf, turnCtx, binding)
	if err != nil {
		if re, ok := AsReject(err); ok {
			writeReject(cfg.Stderr, "ask", re)
			return exitReject
		}
		fmt.Fprintf(cfg.Stderr, "coagent: ask: %v\n", err)
		return exitFlagFormat
	}

	res, serr := binding.Send(ctx, env, opts)
	if serr != nil {
		if re, ok := AsReject(serr); ok {
			writeReject(cfg.Stderr, "ask", re)
			return exitReject
		}
		fmt.Fprintf(cfg.Stderr, "coagent: ask failed: %v\n", serr)
		return exitInfra
	}
	writeSuccess(cfg.Stdout, res)
	return 0
}

// buildAskEnvelope assembles the kind=request envelope. The audience
// handling is split out into resolveAskAudience for testability.
func buildAskEnvelope(
	ctx context.Context,
	cfg Config,
	cf *commonFlags,
	tc TurnCtx,
	binding Binding,
) (*v4types.Envelope, SendOptions, error) {
	if cf.Type == "" {
		return nil, SendOptions{}, fmt.Errorf("ask requires --type (L2 §3.3 'ask 必填')")
	}

	env, opts, err := buildBaseEnvelope(cfg, cf, tc, v4types.KindRequest)
	if err != nil {
		return nil, opts, err
	}

	// ask positional content → payload.text when --payload not given.
	// Same convention as emit so `coagent ask --type agent.text "hi"` works.
	if len(env.Payload) == 0 && cf.MessageContent != "" {
		raw, merr := json.Marshal(map[string]string{"text": cf.MessageContent})
		if merr != nil {
			return nil, opts, fmt.Errorf("encode message: %w", merr)
		}
		env.Payload = raw
	}

	// Audience resolution — the three-branch rule.
	audience, aerr := resolveAskAudience(ctx, cf, env.Type, binding)
	if aerr != nil {
		return nil, opts, aerr
	}
	env.Audience = audience

	return env, opts, nil
}

// resolveAskAudience implements the L2 §3.2.2 three-branch validation
// from the CLI side. It returns either:
//
//   - audience slice ready for the envelope (always length 1)
//   - a *RejectError(request_audience_invalid) when caller's input or
//     the type_registry lookup makes the audience impossible to
//     resolve client-side
//   - a non-reject error for unrecoverable lookup failure (e.g. sql
//     transport error from the in_worker_bus binding's TypeLookup)
//
// The harness-side branches (actor_not_registered / handler_mismatch)
// surface later when Binding.Send returns its own *RejectError — we
// don't pre-empt them here so the CLI stays decoupled from the
// registry / actor_registry state.
func resolveAskAudience(
	ctx context.Context,
	cf *commonFlags,
	typeName string,
	binding Binding,
) ([]string, error) {
	if explicit := parseAudience(cf.Audience); len(explicit) > 0 {
		// length != 1 → multi-receiver, not allowed for request.
		if len(explicit) != 1 {
			return nil, &RejectError{
				Reason: v4types.HarnessRequestAudienceInvalid,
				Detail: fmt.Sprintf("ask requires exactly one concrete receiver; got %d (%v)", len(explicit), explicit),
			}
		}
		// '*' in the single slot → broadcast, not allowed.
		if explicit[0] == "*" {
			return nil, &RejectError{
				Reason: v4types.HarnessRequestAudienceInvalid,
				Detail: "ask audience cannot be '*' (would broadcast)",
			}
		}
		return explicit, nil
	}

	// No explicit audience → try the binding's type_registry lookup.
	if binding == nil {
		return nil, &RejectError{
			Reason: v4types.HarnessRequestAudienceInvalid,
			Detail: "no binding available to resolve handler_actor_id from type_registry",
		}
	}
	actorID, ok, lerr := binding.ResolveHandlerActorID(ctx, typeName)
	if lerr != nil {
		return nil, fmt.Errorf("resolve handler_actor_id for type %q: %w", typeName, lerr)
	}
	if !ok || actorID == "" {
		return nil, &RejectError{
			Reason: v4types.HarnessRequestAudienceInvalid,
			Detail: fmt.Sprintf("type %q has no handler_actor_id in type_registry; pass --audience explicitly", typeName),
		}
	}
	return []string{actorID}, nil
}
