package coagent

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// commonFlags holds the union of L2 §3.3 horizontal flags. Each
// subcommand instantiates one of these, binds the flags it accepts
// onto a *flag.FlagSet, parses argv, then translates the raw values
// into envelope fields.
//
// Why a single struct: the L2 §3.3 table is small and stable — six
// flags share semantics across two or three subcommands. A union
// struct keeps the parsing / normalize logic centralised and lets us
// add cross-cutting validation (e.g. --private + --visibility public
// mutual exclusion) in one place.
type commonFlags struct {
	Type           string
	Payload        string
	Parent         string
	CorrelationID  string
	Audience       string // raw comma-separated; parseAudience splits
	Private        bool
	System         bool
	DocRefs        string // raw comma-separated; parseDocRefs splits
	NotBefore      string
	ExpiresAt      string
	Visibility     string // hidden flag for tests / advanced users
	SenderID       string // hidden override; production reads COAGENT_SELF_ID
	ChannelID      string // hidden override; production reads COAGENT_CHANNEL_ID
	MessageContent string // positional argument for the "free-form text" form

	// Set tracks which flags the caller explicitly provided so we can
	// distinguish "absent" from "explicit empty value". Indexed by the
	// flag name as registered with the FlagSet.
	Set map[string]bool
}

// bindCommonFlags registers every L2 §3.3 flag (plus a few hidden
// helpers) on fs and returns a *commonFlags wired to the FlagSet's
// pointer storage. The `accept` set lets per-subcommand callers
// restrict which flags they expose — flags not in accept are simply
// skipped (the FlagSet then rejects them as unknown).
func bindCommonFlags(fs *flag.FlagSet, accept map[string]bool) *commonFlags {
	cf := &commonFlags{Set: map[string]bool{}}
	register := func(name string, target *string, def string, usage string) {
		if accept != nil && !accept[name] {
			return
		}
		fs.StringVar(target, name, def, usage)
	}
	registerBool := func(name string, target *bool, def bool, usage string) {
		if accept != nil && !accept[name] {
			return
		}
		fs.BoolVar(target, name, def, usage)
	}
	register("type", &cf.Type, "", "business / core type (L2 §3.3 --type)")
	register("payload", &cf.Payload, "", "JSON payload string (L2 §3.3 --payload)")
	register("parent", &cf.Parent, "", "parent_id of a prior message (L2 §3.3 --parent)")
	register("correlation-id", &cf.CorrelationID, "", "explicit correlation_id; 'new' generates a UUID (L2 §3.3.1)")
	register("audience", &cf.Audience, "", "comma-separated audience actor ids (L2 §3.3 --audience)")
	registerBool("private", &cf.Private, false, "shorthand for --visibility private --audience <self> (L2 §3.3 --private)")
	registerBool("system", &cf.System, false, "shorthand for --visibility system (L2 §3.3 --system)")
	register("doc-refs", &cf.DocRefs, "", "comma-separated workspace doc paths (L2 §3.3 --doc-refs)")
	register("not-before", &cf.NotBefore, "", "delay before delivery; absolute ms timestamp or relative '+30m'")
	register("expires-at", &cf.ExpiresAt, "", "expiration; absolute ms timestamp or relative '+30m'")
	register("visibility", &cf.Visibility, "", "override visibility (public|private|system); rarely needed")
	register("sender-id", &cf.SenderID, "", "override envelope.sender.id; production reads COAGENT_SELF_ID")
	register("channel-id", &cf.ChannelID, "", "override envelope.channel_id; production reads COAGENT_CHANNEL_ID")
	return cf
}

// markSet walks the FlagSet after Parse and populates cf.Set so
// downstream normalize code can distinguish "caller did not pass
// this flag" from "caller passed --x=" (empty explicit value).
func (cf *commonFlags) markSet(fs *flag.FlagSet) {
	fs.Visit(func(f *flag.Flag) {
		cf.Set[f.Name] = true
	})
}

// parseAudience splits the raw comma-separated value into a slice.
// Trims whitespace per entry and drops empty entries. Returns nil
// when the raw value is empty — callers branch on `len(...) == 0`
// rather than nil-checking explicitly.
func parseAudience(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// parseDocRefs is the doc_refs counterpart of parseAudience. Returns
// (*[]string, error): nil pointer when raw is empty (envelope.doc_refs
// stays NULL tri-state), pointer to non-empty slice when caller
// supplied at least one path. We never return an empty `[]` — L0 §2.1
// allows it as tri-state but the flag form has no syntax to express
// it intentionally.
func parseDocRefs(raw string) (*[]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return &out, nil
}

// parsePayload validates raw is a JSON object string and returns the
// raw bytes ready to drop into envelope.payload. The harness
// expects a JSON object body (L0 §2.2). Empty raw → ("", nil) so the
// caller can decide whether to leave Payload empty (harness then
// rejects missing_required_field) or fill `{}`.
func parsePayload(raw string) (json.RawMessage, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var probe any
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		return nil, fmt.Errorf("--payload is not valid JSON: %w", err)
	}
	if _, ok := probe.(map[string]any); !ok {
		return nil, errors.New("--payload must be a JSON object")
	}
	return json.RawMessage(raw), nil
}

// parseTime resolves the dual-form time flag. The two accepted forms
// per L2 §3.3 are:
//
//   - absolute ms timestamp: a decimal integer >= 0 (e.g. "1700000000000")
//   - relative duration:     a Go-style duration prefixed with '+' or '-'
//     (e.g. "+30m", "-5s", "+1h30m"). The resulting time is `now + dur`.
//
// `now` is the wall-clock the CLI uses for envelope.ts (so tests can
// inject a deterministic value). Returns a pointer to int64 ms (tri-state
// nil = "not provided"); error when the raw value cannot be parsed.
func parseTime(raw string, now time.Time) (*int64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	if strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		dur, err := time.ParseDuration(raw[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q: %w", raw, err)
		}
		var t time.Time
		if raw[0] == '+' {
			t = now.Add(dur)
		} else {
			t = now.Add(-dur)
		}
		ms := t.UnixMilli()
		return &ms, nil
	}
	ts, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp %q: %w", raw, err)
	}
	if ts < 0 {
		return nil, fmt.Errorf("timestamp %d is negative", ts)
	}
	return &ts, nil
}

// resolveVisibility resolves the visibility-related flags per L2 §3.3.
// --private and --system are shorthands; --visibility is the long form.
// We reject mutually exclusive combinations to keep ambiguity out of the
// wire shape (the harness has no "preferred" tie-break).
//
// Returns ("", error) when the flags conflict. Returns ("", nil) when
// the caller supplied nothing — the harness normalize then defaults to
// "public".
func resolveVisibility(cf *commonFlags) (v4types.Visibility, error) {
	count := 0
	if cf.Private {
		count++
	}
	if cf.System {
		count++
	}
	if cf.Visibility != "" {
		count++
	}
	if count > 1 {
		return "", errors.New("--private / --system / --visibility are mutually exclusive")
	}
	if cf.Private {
		return v4types.VisibilityPrivate, nil
	}
	if cf.System {
		return v4types.VisibilitySystem, nil
	}
	if cf.Visibility == "" {
		return "", nil
	}
	v := v4types.Visibility(cf.Visibility)
	switch v {
	case v4types.VisibilityPublic, v4types.VisibilityPrivate, v4types.VisibilitySystem:
		return v, nil
	}
	return "", fmt.Errorf("--visibility %q must be one of {public, private, system}", cf.Visibility)
}
