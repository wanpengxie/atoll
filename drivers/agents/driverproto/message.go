package driverproto

import (
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"

	"github.com/wanpengxie/atoll/runtime/harness"
)

type Attachment struct {
	Address   string
	Name      string
	LocalPath string
}

type DriverMessage struct {
	SourceID    string
	Type        string
	Sender      string
	Caller      harness.Caller
	Origin      *Origin
	Payload     json.RawMessage
	Text        string
	Attachments []Attachment
}

// Origin is which of a person's screens a message came from.
type Origin struct {
	Session string `json:"session"`
	Label   string `json:"label,omitempty"`
}

// OriginLine renders it beside the caller line. A person holds several screens
// at once, so knowing who spoke is not knowing where they spoke — and "open
// this on my phone" cannot be acted on without the second.
//
// The session id is reproduced exactly because an agent sends it back: it is
// what ui.* words take to say which screen to operate. The label is for reading.
func OriginLine(o *Origin) string {
	if o == nil || o.Session == "" {
		return ""
	}
	if o.Label != "" {
		return "[origin session=" + o.Session + " " + o.Label + "]"
	}
	return "[origin session=" + o.Session + "]"
}

type ContextMessage struct {
	Seq     int64
	Sender  string
	Kind    string
	Type    string
	Payload json.RawMessage
	Text    string
}

type AttemptToken uint64
type ActionToken uint64
type WorkerTurnRef string

type TurnKind uint8

const (
	TurnChat TurnKind = iota
	TurnCompact
	TurnSelect
	// TurnNew replaces the provider-native conversation behind the current
	// actor. The actor identity, declaration, workspace and selected options
	// stay put; only the resumable provider session becomes a fresh one.
	TurnNew
)

type TurnOptions struct {
	Model  string `json:"model"`
	Effort string `json:"effort"`
}

type WorkerTurnTarget struct {
	Attempt AttemptToken
	Native  WorkerTurnRef
}

func (t WorkerTurnTarget) Valid() bool { return t.Attempt != 0 && t.Native != "" }

type StartRequest struct {
	Attempt    AttemptToken
	Life       context.Context
	Messages   []DriverMessage
	Background []ContextMessage
	Kind       TurnKind
	Options    TurnOptions
}

// ActorSegments reads the three segments of an inserted member id. The
// substrate treats an ActorID as one opaque identifier and never takes it
// apart; reading the segments is an agent-layer need — a model has to be told
// who is speaking in words it can reason about — so the reading lives here,
// with the agents that need it, and no id-shaped helper is pushed upstream.
//
// The fixed system actor is not an inserted member and has no segments.
func ActorSegments(id actor.ActorID) (kind actor.Kind, seed string, ok bool) {
	parts := strings.Split(string(id), ":")
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	parsed, valid := actor.ParseKind(parts[0])
	if !valid {
		return "", "", false
	}
	return parsed, parts[1], true
}

// SeedLabel names what an id's middle segment means for that kind. It is a
// presentation choice, not a protocol fact: the registry stores one column,
// and it is a login account for a human and a declaration for anything a
// declaration mints. Calling either by the other's name would have the model
// reasoning about the sender from a false premise.
func SeedLabel(kind actor.Kind) string {
	if kind == actor.KindHuman {
		return "principal"
	}
	return "declaration"
}

// CallerLine renders the header prepended to every request text handed to a
// model. It names the sender in the vocabulary the system prompt defines,
// because the id alone reads as an opaque string: an agent shown only
// "human:root:1787128257816" has to infer that the first segment is a kind and
// the second a principal, and the observed failure was an agent that did not —
// it took the sender for itself and answered accordingly.
func CallerLine(caller harness.Caller) string {
	fields := make([]string, 0, 4)
	if caller.Channel != "" {
		fields = append(fields, "channel="+string(caller.Channel))
	}
	if kind, seed, ok := ActorSegments(caller.Actor); ok {
		fields = append(fields, "kind="+string(kind), SeedLabel(kind)+"="+seed)
	}
	fields = append(fields, "actor="+string(caller.Actor))
	return "[from " + strings.Join(fields, " ") + "]"
}

// ResolveAttachment strips daemon://<device>/<channel>/<path> into a path
// relative to the agent cwd. A device or channel mismatch leaves LocalPath
// empty. Multi-device routing can later return a non-local result here without
// changing callers.
//
// This cannot reuse runtime/resourcespec.ParseFileAddress: archtest confines
// that kernel contract leaf to runtime and platform/home. Keep this parser to
// the minimum provider-facing projection needed here.
func ResolveAttachment(a Attachment, self Situation) Attachment {
	a.LocalPath = ""
	u, err := url.Parse(a.Address)
	if err != nil || strings.Contains(strings.ToLower(a.Address), "%2f") ||
		u.Scheme != "daemon" || u.Opaque != "" || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		u.Host != self.DeviceName || !strings.HasPrefix(u.Path, "/") {
		return a
	}
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[0] != filepath.Base(filepath.Clean(self.WorkspaceDir)) || !validAttachmentPath(parts[1]) {
		return a
	}
	a.LocalPath = parts[1]
	return a
}

func validAttachmentPath(value string) bool {
	if value == "" || strings.HasPrefix(value, "/") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

// AttachmentLines renders one model-visible line per attachment, without a
// trailing newline. It returns an empty string when there are no attachments.
func AttachmentLines(atts []Attachment, self Situation) string {
	lines := make([]string, 0, len(atts))
	for _, attachment := range atts {
		resolved := ResolveAttachment(attachment, self)
		var line strings.Builder
		line.WriteString("[附件")
		if resolved.Name != "" {
			line.WriteString(" name=")
			line.WriteString(strconv.Quote(resolved.Name))
		}
		line.WriteString(" path=")
		if resolved.LocalPath != "" {
			line.WriteString(strconv.Quote(resolved.LocalPath))
		} else {
			line.WriteString(strconv.Quote(resolved.Address))
			line.WriteString(` note="not on this device"`)
		}
		line.WriteByte(']')
		lines = append(lines, line.String())
	}
	return strings.Join(lines, "\n")
}

// maxFieldsLine caps the rendered remainder. A body is a word's contract and is
// normally small, but nothing structurally prevents a large one, and a prompt is
// not the place to discover that. Truncation is announced rather than silent:
// an agent that sees the marker knows to read the payload another way instead of
// trusting a sentence that stops mid-object.
const maxFieldsLine = 2000

// FieldsLine renders the parts of a request body that are NOT the text, so a
// word's structured contract reaches the agent instead of being dropped.
//
// Agents used to see only body.text. That was not a decision — for a long time
// text was all a body held — but it silently discarded every other field, which
// meant a word could not grow a parameter an agent was supposed to act on. The
// first thing that needs one is the origin of a person's message: which of their
// screens they typed it on, and therefore which one to operate when asked.
//
// The remainder is rendered as JSON rather than flattened to key=value pairs:
// these fields are contracts an agent will send back (a session id, a target),
// and reproducing them exactly matters more than reading prettily. Returns ""
// when the body is only text, or is not an object at all.
func FieldsLine(payload json.RawMessage) string {
	if len(payload) == 0 {
		return ""
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return "" // not an object: Text already carried whatever this is
	}
	delete(fields, "text")
	// origin has its own line beside the caller's. Leaving it here too would
	// print the same fact twice — once as prose an agent reads, once as raw
	// JSON — on every single message a person sends.
	delete(fields, "origin")
	if len(fields) == 0 {
		return ""
	}
	// Sorted so the same body always renders identically; an agent re-reading
	// its own context must not see two spellings of one message.
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("[fields")
	for _, k := range keys {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.Write(fields[k])
	}
	b.WriteByte(']')
	line := b.String()
	if len(line) > maxFieldsLine {
		return line[:maxFieldsLine] + " …truncated]"
	}
	return line
}
