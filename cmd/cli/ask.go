package main

// ask.go implements the `coagent ask | emit | answer` subcommands.
//
// These three commands are the domain-CLI wrapper layer described in
// L4 §2.3.2 — they translate agent-friendly invocations
//
//	coagent ask --type xhs.publish --audience tool:xhs-adapter --payload-file p.json
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
//	--token       COAGENT_SESSION_TOKEN | COAGENT_AUTH_TOKEN | DAEMON_URL-paired token
//	--channel     COAGENT_CHANNEL_ID
//
// Stdout (success):
//
//	{"id":"<envelope.id>", "correlation_id":"<envelope.id>", "kind":"request",
//	 "frame_id":"<gateway frame_id>", "seq": <int?>, "dedupe": <bool?>}
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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// askExitCodes mirror the archived daemon-go cmd/coagent contract.
// Internal constants — external callers should never hard-code these
// numbers; instead use the named exit code in stderr triage.
const (
	askExitOK            = 0
	askExitUsage         = 2
	askExitHarnessReject = 3
	askExitInfra         = 4
	askExitNoBinding     = 5
	askExitFlagFormat    = 6
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
		audienceCSV = fs.String("audience", "", "audience actor ids (comma-separated; required when kind=request)")
		parentID    = fs.String("parent-id", "", "parent message id (required when kind=response)")
		channel     = fs.String("channel", "", "channel id (env COAGENT_CHANNEL_ID; required)")
		payload     = fs.String("payload", "", "inline JSON payload (mutually exclusive with --payload-file)")
		payloadFile = fs.String("payload-file", "", "path to a JSON file containing the payload (preferred for large / sensitive payloads)")
		visibility  = fs.String("visibility", "", "envelope visibility: public|private|system (default=public)")
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
	if kind == "request" {
		// L1 §10.2 step 5: requests must have exactly one concrete audience.
		if len(audience) != 1 || audience[0] == "" || audience[0] == "*" {
			writeAskReject(askErrUsage, "--audience must be exactly one concrete actor id for kind=request")
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

	// Resolve token: --token > COAGENT_SESSION_TOKEN > COAGENT_AUTH_TOKEN > DAEMON_URL-paired fallback (legacy).
	resolvedToken := *token
	if resolvedToken == "" {
		resolvedToken = firstNonEmpty(
			os.Getenv("COAGENT_SESSION_TOKEN"),
			os.Getenv("COAGENT_AUTH_TOKEN"),
		)
	}
	// Resolve server URL: --server-url > COAGENT_SERVER_URL > DAEMON_URL.
	resolvedURL := *serverURL
	if resolvedURL == "" {
		resolvedURL = firstNonEmpty(
			os.Getenv("COAGENT_SERVER_URL"),
			os.Getenv("DAEMON_URL"),
		)
	}

	c, err := newHTTPClient(resolvedURL, resolvedToken)
	if err != nil {
		writeAskReject(askErrInfra, err.Error())
		return askExitInfra
	}

	// Compose the request body for the gateway POST.
	body := map[string]any{
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

	var ack map[string]any
	if err := c.do("POST", "/api/channels/"+chID+"/messages", body, &ack); err != nil {
		return classifyAskError(err)
	}

	// Stdout success envelope — keep the keys flat and stable; consumers
	// (xhs-cli RealProvider, agent shells) parse them positionally.
	out := map[string]any{
		"kind": kind,
	}
	// Preferred: real envelope.id from daemon ack. Fall back to
	// frame_id when the gateway hasn't yet been upgraded (older daemons
	// still in the field during rolling upgrades).
	if mid, ok := ack["message_id"].(string); ok && mid != "" {
		out["id"] = mid
	} else if fid, ok := ack["frame_id"].(string); ok && fid != "" {
		out["id"] = fid
	}
	if corr, ok := ack["correlation_id"].(string); ok && corr != "" {
		out["correlation_id"] = corr
	} else if id, ok := out["id"].(string); ok {
		// Legacy fallback: for kind=request/event the correlation_id
		// equals envelope.id per L1 §1.5. Echo it so wrapper CLIs that
		// expect a non-empty correlation_id don't panic.
		if kind == "request" || kind == "event" {
			out["correlation_id"] = id
		}
	}
	if fid, ok := ack["frame_id"].(string); ok {
		out["frame_id"] = fid
	}
	if seq, ok := ack["seq"]; ok {
		out["seq"] = seq
	}
	if d, ok := ack["deduped"].(bool); ok && d {
		out["dedupe"] = true
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		writeAskReject(askErrInfra, "encode stdout: "+err.Error())
		return askExitInfra
	}
	return askExitOK
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
		// classifyExit can use it (e.g. "kind_not_allowed",
		// "dedupe_match", "missing_caller_auth", ...).
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
