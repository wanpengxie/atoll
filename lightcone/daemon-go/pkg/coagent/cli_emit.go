package coagent

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// emitFlags lists the flags `coagent emit` accepts. The L2 §3.3 table:
//
//	emit:  --type --payload --parent --correlation-id --audience
//	       --private --system --doc-refs --not-before --expires-at
//	       (+ hidden: --visibility / --sender-id / --channel-id)
var emitFlags = map[string]bool{
	"type": true, "payload": true, "payload-file": true, "parent": true,
	"correlation-id": true, "audience": true,
	"private": true, "system": true,
	"doc-refs": true, "not-before": true, "expires-at": true,
	"visibility": true, "sender-id": true, "channel-id": true,
}

// runEmit implements `coagent emit` (L2 §3.2.1). Default type is
// `agent.text` (a core type) — caller's `--type` overrides.
//
// The positional tail (everything after the recognized flags) is
// joined with a single space and used as the message text payload:
//
//	coagent emit "调研做完了，结论是..."
//
// → envelope.payload = {"text": "调研做完了，结论是..."} when --payload
// was not provided. Explicit --payload always wins over the
// positional form (caller intent: structured payload).
func runEmit(ctx context.Context, cfg Config, argv []string) int {
	fs := flag.NewFlagSet("coagent emit", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	cf := bindCommonFlags(fs, emitFlags)
	flagArgs, positional := splitFlagArgs(argv, boolFlagNames())
	if err := fs.Parse(flagArgs); err != nil {
		return exitUsage
	}
	cf.markSet(fs)
	cf.MessageContent = strings.Join(positional, " ")

	turnCtx, terr := LoadTurnCtx(cfg.Env, cfg.HomeDir)
	if terr != nil {
		fmt.Fprintf(cfg.Stderr, "coagent: emit: %v (continuing with env only)\n", terr)
	}

	binding, code, msg := resolveBinding(cfg, turnCtx)
	if code != 0 {
		fmt.Fprintln(cfg.Stderr, "coagent: emit: "+msg)
		return code
	}

	env, opts, err := buildEmitEnvelope(cfg, cf, turnCtx)
	if err != nil {
		fmt.Fprintf(cfg.Stderr, "coagent: emit: %v\n", err)
		return exitFlagFormat
	}

	res, serr := binding.Send(ctx, env, opts)
	if serr != nil {
		if re, ok := AsReject(serr); ok {
			writeReject(cfg.Stderr, "emit", re)
			return exitReject
		}
		fmt.Fprintf(cfg.Stderr, "coagent: emit failed: %v\n", serr)
		return exitInfra
	}
	writeSuccess(cfg.Stdout, res)
	return 0
}

// buildEmitEnvelope builds the kind=event envelope from parsed flags
// + the turn context. We pre-fill enough fields that the harness 9-step
// chain has everything it needs (the harness normalize will still
// double-default correlation_id / payload / etc. — we lean on it).
func buildEmitEnvelope(cfg Config, cf *commonFlags, tc TurnCtx) (*v4types.Envelope, SendOptions, error) {
	env, opts, err := buildBaseEnvelope(cfg, cf, tc, v4types.KindEvent)
	if err != nil {
		return nil, opts, err
	}

	// emit-specific defaults:
	//   - type default is agent.text (core type) when caller omitted
	//     --type AND there is no positional content (then the message
	//     is purely "I want to do something" — rare; still default to
	//     agent.text so the harness accepts it).
	if env.Type == "" {
		env.Type = "agent.text"
	}

	// emit positional content → payload.text. The L1 §1.1 core types
	// accept any JSON object so {"text": "..."} validates.
	if len(env.Payload) == 0 && cf.MessageContent != "" {
		raw, merr := json.Marshal(map[string]string{"text": cf.MessageContent})
		if merr != nil {
			return nil, opts, fmt.Errorf("encode message: %w", merr)
		}
		env.Payload = raw
	}

	// --private shorthand expands to audience=<self> + visibility=private
	// (L2 §3.3). Caller --audience overrides --private's audience hint.
	if cf.Private && len(env.Audience) == 0 {
		if env.Sender.ID == "" {
			return nil, opts, fmt.Errorf("--private requires sender id (set COAGENT_SELF_ID or --sender-id)")
		}
		env.Audience = []string{env.Sender.ID}
	}

	return env, opts, nil
}
