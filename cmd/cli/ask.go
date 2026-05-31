package main

// ask.go implements the `coagent ask | emit | answer` subcommands.
//
// These three commands are the domain-CLI wrapper layer described in
// L4 §2.3.2 — they translate agent-friendly invocations
//
//	coagent ask --type xhs.publish --audience tool:xhs --payload-file p.json
//
// into a single POST to the server gateway
//
//	POST /api/channels/<COAGENT_CHANNEL_ID>/messages
//	{type, kind, audience, parent_id?, visibility?, payload}
//
// They are the dual-purpose binding M1.6-T5 phase-4 calls for:
//   - Domain CLIs (adapters/device/xhs/cli) spawn `coagent ask` to
//     dispatch a v4 request without re-implementing HTTP / auth.
//   - Workers / agents that prefer shell over Go can invoke them
//     interactively for debugging.
//
// Auth / scope env (precedence — explicit flag → env → default):
//
//	--server-url  COAGENT_SERVER_URL  http://localhost:8832
//	--token       COAGENT_SESSION_TOKEN
//	--channel     COAGENT_CHANNEL_ID
//
// Stdout (success):
//
//	{"id":"<envelope.id>", "correlation_id":"<envelope.id>", "kind":"request",
//	 "seq": <int?>, "dedupe": <bool?>}
//
// Exit codes (mirror archived daemon-go cmd/coagent + L4 §2.3.2):
//
//	0 success
//	2 usage error (missing flag, bad shape, unknown subcommand)
//	3 harness reject (409 with reject_reason) — code echoed via stderr JSON
//	4 infra error (transport / unmarshal / 5xx)
//	5 no binding (404 / channel_unbound)
//	6 flag format (--payload not valid JSON)
//
// The exit code mapping is consumed by adapters/device/xhs/cli's
// real_provider.go (classifyExit). KEEP IN SYNC when changing codes.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/pkg/coagentsdk"
)

// askExitCodes mirror the archived daemon-go cmd/coagent contract.
// Internal constants — external callers should never hard-code these
// numbers; instead use the named exit code in stderr triage.
//
// Codes 7-9 are the response-multitype-refactor §3.7 G additions for
// the `--watch` surface. They sit above the legacy 0-6 set so existing
// xhs-cli classifyExit logic keeps working unchanged for non-watch
// invocations.
const (
	askExitOK            = 0
	askExitUsage         = 2
	askExitHarnessReject = 3
	askExitInfra         = 4
	askExitNoBinding     = 5
	askExitFlagFormat    = 6
	// askExitWatchTimeout — `--watch` deadline elapsed before a final
	// response (payload.status ∈ {completed, failed}) arrived. The
	// substrate may still emit final later; the CLI just stopped waiting.
	askExitWatchTimeout = 7
	// askExitWatchFailed — final response arrived with
	// payload.status="failed". The reason (proto-layer0 §2.6 terminal
	// failure closed set) is echoed in the stderr reject blob.
	askExitWatchFailed = 8
	// askExitWatchInfra — watch transport / decode failure before any
	// final response was observed (websocket drop, malformed envelope).
	askExitWatchInfra = 9
)

// runAsk dispatches `coagent ask --type T --audience A [...] --payload-file P`
// to POST .../messages with kind=request.
func runAsk(args []string) {
	os.Exit(runWriteMessageCmd("ask", "request", args))
}

// runEmit dispatches `coagent emit --type T [--audience A,B] [...] --payload-file P`
// to POST .../messages with kind=event.
func runEmit(args []string) {
	os.Exit(runWriteMessageCmd("emit", "event", args))
}

// runAnswer dispatches `coagent answer --type T --parent-id P [...] --payload-file P`
// to POST .../messages with kind=response.
func runAnswer(args []string) {
	os.Exit(runWriteMessageCmd("answer", "response", args))
}

// runWriteMessageCmd is the shared implementation. `name` is the
// subcommand label (used in flag.FlagSet for `coagent ask -h`); `kind`
// is the v4 message kind to stamp on the request body.
func runWriteMessageCmd(name, kind string, args []string) int {
	fs := flag.NewFlagSet("coagent "+name, flag.ContinueOnError)
	// ContinueOnError so we can map parse failure → askExitUsage instead
	// of bare os.Exit(2) — matches archived cmd/coagent contract.

	var (
		typeName    = fs.String("type", "", "v4 envelope.type (required)")
		audienceCSV = fs.String("audience", "", "audience actor ids (comma-separated; required)")
		parentID    = fs.String("parent-id", "", "parent message id (required when kind=response)")
		channel     = fs.String("channel", "", "channel id (env COAGENT_CHANNEL_ID; required)")
		payload     = fs.String("payload", "", "inline JSON payload (mutually exclusive with --payload-file)")
		payloadFile = fs.String("payload-file", "", "path to a JSON file containing the payload (preferred for large / sensitive payloads)")
		visibility  = fs.String("visibility", "", "envelope visibility: public|private|system (default=public)")
		// R4-3: caller MUST supply envelope.id per L3 §1.8.1. Default
		// is a fresh uuid; agents that want L1 §2.3 idempotent retries
		// (same id + same content) supply --message-id explicitly so
		// re-runs collapse to one daemon append.
		messageID = fs.String("message-id", "", "sender-provided envelope.id (default: fresh uuid; reuse to make a retry idempotent)")
		// `--watch` / `--timeout` are the response-multitype-refactor
		// §3.7 G surface: after the request is accepted, subscribe to
		// the channel log and stream provisional responses to stderr
		// until a final response (payload.status ∈ {completed, failed})
		// arrives or the timeout elapses. Defaults preserve the legacy
		// emit-and-return behaviour so existing callers are unaffected.
		watch       = fs.Bool("watch", false, "after emit, stream provisional + final responses (kind=request only; default: emit-and-return)")
		watchTimout = fs.Duration("timeout", 30*time.Second, "max time to wait for a final response when --watch is set")
	)
	serverURL, token := bindGlobalFlags(fs)
	// Use a Buffer for parse errors so we can emit them as flat JSON to
	// stderr instead of cobra-style multi-line usage blobs.
	fs.SetOutput(io.Discard)

	if err := fs.Parse(args); err != nil {
		writeAskReject(askErrUsage, fmt.Sprintf("parse flags: %s", err))
		return askExitUsage
	}

	// Required-flag validation.
	if strings.TrimSpace(*typeName) == "" {
		writeAskReject(askErrUsage, "--type is required")
		return askExitUsage
	}
	chID := strings.TrimSpace(*channel)
	if chID == "" {
		chID = strings.TrimSpace(os.Getenv("COAGENT_CHANNEL_ID"))
	}
	if chID == "" {
		writeAskReject(askErrUsage, "--channel (or COAGENT_CHANNEL_ID env) is required")
		return askExitUsage
	}

	audience := splitCSV(*audienceCSV)
	if len(audience) == 0 {
		writeAskReject(askErrUsage, "--audience is required")
		return askExitUsage
	}
	for _, id := range audience {
		if id == "" || id == "*" {
			writeAskReject(askErrUsage, "--audience must contain only concrete actor ids")
			return askExitUsage
		}
	}
	if kind == "request" || kind == "response" {
		// L1 §10.2: requests/responses must address exactly one actor.
		if len(audience) != 1 {
			writeAskReject(askErrUsage, "--audience must be exactly one concrete actor id for kind="+kind)
			return askExitUsage
		}
	}
	if kind == "response" && strings.TrimSpace(*parentID) == "" {
		writeAskReject(askErrUsage, "--parent-id is required for kind=response")
		return askExitUsage
	}

	// Resolve payload from --payload or --payload-file (mutually exclusive).
	rawPayload, derr := readPayload(*payload, *payloadFile)
	if derr != nil {
		writeAskReject(askErrFlagFormat, derr.Error())
		return askExitFlagFormat
	}

	// Resolve token: --token > COAGENT_SESSION_TOKEN.
	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = strings.TrimSpace(os.Getenv("COAGENT_SESSION_TOKEN"))
	}
	// Resolve server URL: --server-url > COAGENT_SERVER_URL.
	resolvedURL := *serverURL
	if resolvedURL == "" {
		resolvedURL = strings.TrimSpace(os.Getenv("COAGENT_SERVER_URL"))
	}

	c, err := newHTTPClient(resolvedURL, resolvedToken)
	if err != nil {
		writeAskReject(askErrInfra, err.Error())
		return askExitInfra
	}

	// R4-3: caller MUST supply envelope.id per L3 §1.8.1. Use the
	// caller-provided --message-id when set so retries collapse via L1
	// §2.3 dedupe; otherwise fall back to a fresh uuid for a one-shot
	// invocation.
	resolvedMessageID := strings.TrimSpace(*messageID)
	if resolvedMessageID == "" {
		resolvedMessageID = uuid.NewString()
	}

	// Compose the request body for the gateway POST.
	body := map[string]any{
		"id":      resolvedMessageID,
		"type":    *typeName,
		"kind":    kind,
		"payload": rawPayload,
	}
	if len(audience) > 0 {
		body["audience"] = audience
	}
	if strings.TrimSpace(*parentID) != "" {
		body["parent_id"] = *parentID
	}
	if strings.TrimSpace(*visibility) != "" {
		body["visibility"] = *visibility
	}

	// `--watch` race fix (D18 / F27): the CLI subscribes AFTER the POST
	// (see runAskWatch), so we must capture the channel cursor BEFORE
	// the emit. Otherwise a fast actor that emits its final response
	// before the WS subscribe completes lands in viewcache at seq>cursor
	// when the WS subscribes — and the subscribe's since_seq=cursor
	// replay would miss it. We pre-fetch the cursor here so the watch
	// later replays from this exact seq, covering the race window.
	var watchSinceSeq int64
	if *watch && kind == "request" {
		sdkClient := &coagentsdk.Client{BaseURL: resolvedURL, SessionToken: resolvedToken}
		cur, cerr := sdkClient.Cursor(context.Background(), chID)
		if cerr != nil {
			writeAskReject(askErrInfra, "watch: fetch cursor: "+cerr.Error())
			return askExitWatchInfra
		}
		watchSinceSeq = cur
	}

	var ack map[string]any
	if err := c.do("POST", "/api/channels/"+chID+"/messages", body, &ack); err != nil {
		return classifyAskError(err)
	}

	// Stdout success envelope — keep the keys flat and stable; consumers
	// (xhs-cli RealProvider, agent shells) parse them positionally.
	out := map[string]any{
		"kind": kind,
	}
	// Real envelope.id from the daemon ack.
	if mid, ok := ack["message_id"].(string); ok && mid != "" {
		out["id"] = mid
	}
	if corr, ok := ack["correlation_id"].(string); ok && corr != "" {
		out["correlation_id"] = corr
	}
	if seq, ok := ack["seq"]; ok {
		out["seq"] = seq
	}
	if d, ok := ack["deduped"].(bool); ok && d {
		out["dedupe"] = true
	}

	// `--watch` (kind=request only) keeps the existing ack on stderr as
	// a structured trace line (so wrapper CLIs that read stdout JSON
	// still only see the final response there), then streams provisional
	// responses to stderr and exits when the final response arrives.
	if *watch && kind == "request" {
		ackLine, _ := json.Marshal(out)
		fmt.Fprintf(os.Stderr, "coagent: emitted %s\n", string(ackLine))
		requestID, _ := out["id"].(string)
		if strings.TrimSpace(requestID) == "" {
			writeAskReject(askErrInfra, "watch: missing request id from gateway ack")
			return askExitWatchInfra
		}
		return runAskWatch(askWatchParams{
			ServerURL: resolvedURL,
			Token:     resolvedToken,
			ChannelID: chID,
			RequestID: requestID,
			Timeout:   *watchTimout,
			SinceSeq:  watchSinceSeq,
		})
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		writeAskReject(askErrInfra, "encode stdout: "+err.Error())
		return askExitInfra
	}
	return askExitOK
}

// askWatchParams carries the resolved transport / correlation values
// runAskWatch needs to drive the SDK Watch loop.
type askWatchParams struct {
	ServerURL string
	Token     string
	ChannelID string
	RequestID string
	Timeout   time.Duration
	// SinceSeq is the channel cursor captured BEFORE the emit POST. Watch
	// subscribes with since_seq=SinceSeq so any envelope appended to the
	// channel between the cursor capture and the WS subscribe is replayed
	// — covers the subscribe-after-submit race (D18 / F27).
	SinceSeq int64
}

// runAskWatch subscribes via the SDK's Watch handle and dispatches each
// envelope by (kind, payload.status):
//
//   - kind=event           → ignored (one-line stderr trace, no dispatch)
//   - kind=response final  → write final payload JSON to stdout, exit per status
//   - kind=response prov.  → write `⏳ <status>: <detail>` to stderr, keep waiting
//
// On timeout the function emits a reject blob to stderr and exits with
// askExitWatchTimeout. The substrate may still emit the final response
// later — this is the caller-side deadline, not a substrate cancel.
func runAskWatch(p askWatchParams) int {
	if strings.TrimSpace(p.ServerURL) == "" {
		writeAskReject(askErrInfra, "watch: server URL is required (set --server-url or COAGENT_SERVER_URL)")
		return askExitWatchInfra
	}
	if p.Timeout <= 0 {
		p.Timeout = 30 * time.Second
	}
	client := &coagentsdk.Client{
		BaseURL:      p.ServerURL,
		SessionToken: p.Token,
	}
	// Two distinct contexts:
	//   - watchCtx is the SDK lifetime; we cancel it explicitly (via
	//     watch.Close()) on every exit path so the SDK read goroutine
	//     unwinds cleanly without racing on a tiny read deadline.
	//   - timer drives the user-facing deadline. We deliberately do NOT
	//     wire the timeout into the SDK context, because the SDK's
	//     nextReadWindow shrinks the read deadline to ~1ns once ctx is
	//     past — that races with gorilla's "no repeated reads after
	//     error" invariant. Closing via watch.Close() is the clean exit.
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch, err := client.Watch(watchCtx, p.ChannelID, p.RequestID, coagentsdk.WithSinceSeq(p.SinceSeq))
	if err != nil {
		writeAskReject(askErrInfra, "watch: subscribe: "+err.Error())
		return askExitWatchInfra
	}
	defer watch.Close()

	timer := time.NewTimer(p.Timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			writeAskReject("watch_timeout", fmt.Sprintf("no final response within %s for request %s", p.Timeout, p.RequestID))
			return askExitWatchTimeout
		case ev, ok := <-watch.Events():
			if !ok {
				writeAskReject("watch_timeout", fmt.Sprintf("watch closed without final response for request %s", p.RequestID))
				return askExitWatchTimeout
			}
			if ev.Err != nil {
				writeAskReject(askErrInfra, "watch: "+ev.Err.Error())
				return askExitWatchInfra
			}
			if ev.Envelope == nil {
				continue
			}
			env := ev.Envelope
			switch string(env.Kind) {
			case "event":
				// Side-channel observation; trace one line so debug
				// shows it but don't drive any state transition.
				fmt.Fprintf(os.Stderr, "coagent: watch: event type=%s id=%s (ignored)\n", env.Type, string(env.ID))
				continue
			case "response":
				status, detail := decodeResponsePayload(env.Payload)
				if !ev.IsFinal {
					// Provisional — stream to stderr, keep waiting.
					fmt.Fprintf(os.Stderr, "coagent: watch: ⏳ %s: %s\n", status, detail)
					continue
				}
				// Final response — emit the payload to stdout so
				// downstream pipelines can JSON-parse it, then exit per
				// completed / failed split.
				if _, err := os.Stdout.Write(append([]byte(env.Payload), '\n')); err != nil {
					writeAskReject(askErrInfra, "watch: write stdout: "+err.Error())
					return askExitWatchInfra
				}
				if status == "completed" {
					return askExitOK
				}
				// status="failed" → emit reject blob + non-zero exit.
				// proto-layer0 §2.6 terminal failure reason closed set
				// (unanswered_timeout / receiver_internal_error /
				// receiver_unavailable) is echoed verbatim in detail.
				writeAskReject("response_failed", detail)
				return askExitWatchFailed
			default:
				// kind=request: shouldn't happen (parent_id filter
				// excludes non-replies). Log and keep waiting.
				fmt.Fprintf(os.Stderr, "coagent: watch: unexpected kind=%s (ignored)\n", env.Kind)
			}
		}
	}
}

// decodeResponsePayload extracts (status, detail) from a response
// envelope payload. detail prefers payload.reason (terminal failure
// closed set), then payload.detail, then payload.message, then a
// truncated raw fallback so the human-readable line is never empty.
func decodeResponsePayload(raw json.RawMessage) (status, detail string) {
	if len(raw) == 0 {
		return "", ""
	}
	var obj struct {
		Status  string `json:"status"`
		Reason  string `json:"reason"`
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", truncate(string(raw), 80)
	}
	status = obj.Status
	detail = firstNonEmpty(obj.Reason, obj.Detail, obj.Message)
	if detail == "" {
		detail = truncate(string(raw), 80)
	}
	return status, detail
}

// askErrCode-style sentinels — these are the strings emitted in the
// stderr reject JSON's "reason" field. classifyExit in
// adapters/device/xhs/cli/internal/xhs/real_provider.go reads this
// blob, so KEEP THE STRINGS STABLE.
const (
	askErrUsage      = "usage_error"
	askErrFlagFormat = "flag_format_error"
	askErrInfra      = "coagent_infra"
	askErrNoBinding  = "coagent_no_binding"
)

// classifyAskError maps an httpClient.do error to (exit code, reject
// blob) and writes the blob to stderr.
func classifyAskError(err error) int {
	var he *httpError
	if !errors.As(err, &he) || he == nil {
		writeAskReject(askErrInfra, err.Error())
		return askExitInfra
	}
	// Try to parse the server's JSON body for harness reject metadata.
	var bodyParsed struct {
		RejectReason string `json:"reject_reason"`
		RejectDetail string `json:"reject_detail"`
		Error        string `json:"error"`
	}
	_ = json.Unmarshal([]byte(he.Body), &bodyParsed)

	switch {
	case he.Status == 409 && bodyParsed.RejectReason != "":
		// Harness reject — pass the reason verbatim so xhs-cli's
		// classifyExit can use it (e.g. "harness_kind_not_allowed_for_type",
		// "harness_id_duplicate_conflict", "harness_engine_acl_denied", ...).
		writeAskReject(bodyParsed.RejectReason, bodyParsed.RejectDetail)
		return askExitHarnessReject
	case he.Status == 404 || strings.Contains(strings.ToLower(bodyParsed.Error), "channel_unbound"):
		writeAskReject(askErrNoBinding, fallback(bodyParsed.Error, he.Body))
		return askExitNoBinding
	case he.Status >= 500:
		writeAskReject(askErrInfra, fmt.Sprintf("http %d: %s", he.Status, fallback(bodyParsed.Error, he.Body)))
		return askExitInfra
	default:
		writeAskReject(askErrUsage, fmt.Sprintf("http %d: %s", he.Status, fallback(bodyParsed.Error, he.Body)))
		return askExitUsage
	}
}

// writeAskReject emits the flat reject JSON the xhs-cli RealProvider
// parses via rejectFromStderr. The shape is:
//
//	{"error":"reject","reason":"<r>","detail":"<d>"}
func writeAskReject(reason, detail string) {
	blob := map[string]string{
		"error":  "reject",
		"reason": reason,
		"detail": detail,
	}
	raw, _ := json.Marshal(blob)
	// Prefix line lets a human eyeball the reject; the trailing JSON
	// is what programmatic consumers parse.
	fmt.Fprintf(os.Stderr, "coagent: rejected: %s %s\n%s\n", reason, detail, string(raw))
}

// readPayload reads the payload from --payload or --payload-file, with
// mutually-exclusive validation. An empty payload is allowed and maps
// to JSON `null` (some xhs.* request schemas have no required fields).
func readPayload(inline, path string) (json.RawMessage, error) {
	inline = strings.TrimSpace(inline)
	path = strings.TrimSpace(path)
	if inline != "" && path != "" {
		return nil, errors.New("--payload and --payload-file are mutually exclusive")
	}
	switch {
	case inline != "":
		if !json.Valid([]byte(inline)) {
			return nil, fmt.Errorf("--payload is not valid JSON: %q", truncate(inline, 80))
		}
		return json.RawMessage(inline), nil
	case path != "":
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read --payload-file %q: %w", path, err)
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("--payload-file %q does not contain valid JSON", path)
		}
		return json.RawMessage(raw), nil
	default:
		return json.RawMessage("null"), nil
	}
}

// splitCSV splits "a, b , c" into ["a","b","c"]; empty fragments
// dropped. Used for the --audience flag.
func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// firstNonEmpty returns the first trimmed-non-empty element from xs.
func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if s := strings.TrimSpace(x); s != "" {
			return s
		}
	}
	return ""
}

// fallback returns a when non-empty, else b.
func fallback(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
