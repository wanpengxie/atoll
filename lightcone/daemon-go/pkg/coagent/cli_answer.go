package coagent

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// answerFlags lists the flags `coagent answer` accepts. parent /
// correlation-id / audience are NOT in the set — answer derives them
// from the looked-up request per L2 §3.4.3.
var answerFlags = map[string]bool{
	"type": true, "payload": true,
	"doc-refs": true, "expires-at": true,
	"visibility": true, "sender-id": true, "channel-id": true,
}

// runAnswer implements `coagent answer <request_id>` (L2 §3.2.3 +
// §3.4.3). The first positional arg is the request id; remaining
// positional text becomes payload.text (when --payload absent).
//
// Per §3.4.3 the CLI auto-fills:
//
//   - parent_id      = <request_id>
//   - correlation_id = request.correlation_id
//   - audience       = [request.sender.id]
//   - type           = request.type (unless --type overrides)
//
// Binding.LookupRequest provides those fields. When the binding has
// no lookup capability (daemon_rpc with no sqlite handle), the CLI
// requires the caller to pass --type AND --audience explicitly; the
// harness then rejects sufficiently-malformed answers (e.g. missing
// parent linkage) via response_parent_invalid.
func runAnswer(ctx context.Context, cfg Config, argv []string) int {
	fs := flag.NewFlagSet("coagent answer", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	cf := bindCommonFlags(fs, answerFlags)
	flagArgs, positional := splitFlagArgs(argv, boolFlagNames())
	if err := fs.Parse(flagArgs); err != nil {
		return exitUsage
	}
	cf.markSet(fs)
	if len(positional) == 0 {
		fmt.Fprintln(cfg.Stderr, "coagent: answer requires a <request_id> argument")
		return exitUsage
	}
	requestID := positional[0]
	cf.MessageContent = strings.Join(positional[1:], " ")

	turnCtx, terr := LoadTurnCtx(cfg.Env, cfg.HomeDir)
	if terr != nil {
		fmt.Fprintf(cfg.Stderr, "coagent: answer: %v (continuing with env only)\n", terr)
	}

	binding, code, msg := resolveBinding(cfg, turnCtx)
	if code != 0 {
		fmt.Fprintln(cfg.Stderr, "coagent: answer: "+msg)
		return code
	}

	env, opts, err := buildAnswerEnvelope(ctx, cfg, cf, turnCtx, binding, requestID)
	if err != nil {
		if re, ok := AsReject(err); ok {
			writeReject(cfg.Stderr, "answer", re)
			return exitReject
		}
		fmt.Fprintf(cfg.Stderr, "coagent: answer: %v\n", err)
		return exitFlagFormat
	}

	res, serr := binding.Send(ctx, env, opts)
	if serr != nil {
		if re, ok := AsReject(serr); ok {
			writeReject(cfg.Stderr, "answer", re)
			return exitReject
		}
		fmt.Fprintf(cfg.Stderr, "coagent: answer failed: %v\n", serr)
		return exitInfra
	}
	writeSuccess(cfg.Stdout, res)
	return 0
}

// buildAnswerEnvelope assembles the kind=response envelope. The
// request lookup feeds parent_id / correlation_id / audience / type
// defaults; explicit --type / --audience flags still win.
func buildAnswerEnvelope(
	ctx context.Context,
	cfg Config,
	cf *commonFlags,
	tc TurnCtx,
	binding Binding,
	requestID string,
) (*v4types.Envelope, SendOptions, error) {
	if requestID == "" {
		return nil, SendOptions{}, fmt.Errorf("answer: request_id is empty")
	}

	// Look up the prior request when the binding supports it. Failures
	// are surfaced; absence (ok=false) falls back to caller-supplied
	// flags only.
	var (
		req       *v4types.Envelope
		lookupOK  bool
		lookupErr error
	)
	req, lookupOK, lookupErr = binding.LookupRequest(ctx, requestID)
	if lookupErr != nil {
		return nil, SendOptions{}, fmt.Errorf("lookup request %q: %w", requestID, lookupErr)
	}

	env, opts, err := buildBaseEnvelope(cfg, cf, tc, v4types.KindResponse)
	if err != nil {
		return nil, opts, err
	}

	// parent_id is mandatory for kind=response — comes from the
	// positional argument always. Harness Step 2 + Step 8 enforce.
	env.ParentID = requestID

	// audience / correlation_id / type default from the request when
	// lookup succeeded AND caller didn't override.
	if lookupOK && req != nil {
		if !cf.Set["correlation-id"] {
			env.CorrelationID = req.CorrelationID
		}
		if !cf.Set["audience"] {
			env.Audience = []string{req.Sender.ID}
		}
		if !cf.Set["type"] {
			env.Type = req.Type
		}
	}

	// If the binding has no LookupRequest, the caller MUST pass
	// --type at minimum (audience for response can be inferred from
	// the request sender, which is exactly what lookup gives us; without
	// it the harness has nothing to anchor to). Surface a friendly
	// client-side error so caller knows to add --type.
	if env.Type == "" {
		return nil, opts, fmt.Errorf("answer: --type required when binding has no LookupRequest capability")
	}

	// Default audience when neither lookup nor flag provided one:
	// kind=response requires a concrete audience just like request.
	// (L1 §10.2 step 5 audience narrowing rule is symmetric across
	// kind=request and kind=response after L2 §3.4.3 normalization —
	// the response goes to the requester.) When caller passed
	// neither --audience nor a lookup hit, we have no anchor and
	// must reject early so the harness doesn't see audience=['*'].
	if len(env.Audience) == 0 {
		return nil, opts, fmt.Errorf("answer: --audience required when binding has no LookupRequest capability")
	}

	// Same payload-text shortcut as emit / ask.
	if len(env.Payload) == 0 && cf.MessageContent != "" {
		raw, merr := json.Marshal(map[string]string{"text": cf.MessageContent})
		if merr != nil {
			return nil, opts, fmt.Errorf("encode message: %w", merr)
		}
		env.Payload = raw
	}

	return env, opts, nil
}
