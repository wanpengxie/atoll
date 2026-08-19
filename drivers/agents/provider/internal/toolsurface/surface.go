// Package toolsurface owns the single provider-facing projection of atoll's
// canonical meta-tool surface. Provider adapters supply a naming dialect; all
// catalog, prompt, result, and error projection stays here.
package toolsurface

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

const (
	ClaudeServer        = "atoll"
	ClaudeExposedPrefix = "mcp__" + ClaudeServer + "__"
	MaxCatalogTools     = 64
	MaxDescriptionBytes = 32 << 10
	MaxSchemaBytes      = 256 << 10
	MaxResultBytes      = 1 << 20
)

type Dialect uint8

const (
	Codex Dialect = iota
	Claude
)

type Entry struct {
	Canonical string
	Wire      string
	Exposed   string
	Spec      driverproto.ToolSpec
}

type Surface struct {
	entries []Entry
	byWire  map[string]Entry
	digest  string
	self    driverproto.Situation
}

func Assemble(catalog []driverproto.ToolSpec, dialect Dialect, self driverproto.Situation) (Surface, error) {
	if len(catalog) > MaxCatalogTools {
		return Surface{}, fmt.Errorf("tool catalog has %d entries; limit is %d", len(catalog), MaxCatalogTools)
	}
	s := Surface{entries: make([]Entry, 0, len(catalog)), byWire: make(map[string]Entry, len(catalog)), self: self}
	seenCanonical := make(map[string]bool, len(catalog))
	for _, spec := range catalog {
		name := strings.TrimSpace(spec.Name)
		if name == "" || seenCanonical[name] {
			return Surface{}, fmt.Errorf("tool catalog contains empty or duplicate name %q", name)
		}
		if len(spec.Description) > MaxDescriptionBytes || len(spec.Schema) > MaxSchemaBytes {
			return Surface{}, fmt.Errorf("tool %q exceeds catalog field limits", name)
		}
		if len(spec.Schema) == 0 || !json.Valid(spec.Schema) {
			return Surface{}, fmt.Errorf("tool %q has invalid JSON schema", name)
		}
		seenCanonical[name] = true
		exposed := name
		if dialect == Claude {
			exposed = ClaudeExposedPrefix + name
		}
		entry := Entry{Canonical: name, Wire: name, Exposed: exposed, Spec: driverproto.ToolSpec{Name: name, Description: spec.Description, Schema: append(json.RawMessage(nil), spec.Schema...)}}
		if dialect == Codex {
			entry.Wire = exposed
			if strings.HasPrefix(entry.Wire, "mcp") {
				return Surface{}, fmt.Errorf("codex tool name %q uses reserved mcp prefix", entry.Wire)
			}
		}
		s.entries = append(s.entries, entry)
		s.byWire[entry.Wire] = entry
	}
	s.digest = digest(s.entries)
	return s, nil
}

func (s Surface) Entries() []Entry { return append([]Entry(nil), s.entries...) }
func (s Surface) Digest() string   { return s.digest }

func (s Surface) Canonical(wireName string) (string, bool) {
	entry, ok := s.byWire[wireName]
	return entry.Canonical, ok
}

// Guide is the tool-usage half of the system prompt. It is rendered per home
// because reach differs by home: only c0 can act on another channel's
// membership, and only a non-c0 channel needs its space words forwarded. The
// shared half states the tools; the reach paragraph states what is true where
// this agent sits, and says nothing about doors it does not have.
func (s Surface) Guide() string {
	name := func(canonical string) string {
		for _, entry := range s.entries {
			if entry.Canonical == canonical {
				return entry.Exposed
			}
		}
		return canonical
	}
	shared := fmt.Sprintf(`
## Atoll channel tools

Use %s for the live member roster. The system door is a fixed channel facility,
not a roster member: use %s to discover its words and %s to invoke one. For
members, use %s, then %s when one word needs detail, then %s. Long calls return
an acknowledgement: collect it with %s, inspect %s, or stop it with %s. Read
structured error codes and recovery hints before retrying. Atoll ResourceID
read/write/stat is not exposed by these tools.
`, name("list_actors"), name("system_describe"), name("system_call"),
		name("describe_actor"), name("describe_type"), name("call_actor"),
		name("await_result"), name("list_pending"), name("cancel"))

	var reach string
	switch {
	case s.self.Channel == "":
		reach = fmt.Sprintf(`
%s serves this channel only. Anything about another channel has to go through a
member that is a door onto it; %s tells you whether such a member exists here
and what it accepts.
`, name("system_call"), name("describe_actor"))
	case s.self.IsCore:
		reach = fmt.Sprintf(`
You are in %s, this node's registry channel. %s serves both this channel's own
membership and the space registry — channels, templates, principals, devices.

Some members here are peers: doors onto other channels. A peer always takes
agent.ask, which hands the request to that channel's service agent. Because you
are in the registry channel, a peer also takes the channel-membership words
(system.member.*, system.log.recent) and applies them to ITS channel, so you can
read and change another channel's membership yourself instead of asking its
agent to do it. Send those with %s, addressed to the peer. Run %s on the peer
first — the words it lists are exactly the words it accepts. A peer whose id is
"peer:svcactor:..." is this channel's own service door, not a route anywhere.
`, s.self.Channel, name("system_call"), name("call_actor"), name("describe_actor"))
	default:
		reach = fmt.Sprintf(`
You are in channel %s. %s serves this channel's own membership, and it also
forwards space words — channels, templates, principals, devices — to the
registry channel on your behalf, so you can send those from here directly.

Acting on ANOTHER channel's membership is the registry channel's privilege, not
yours. The peer in your roster is this channel's own service door, not a route
to another channel.
`, s.self.Channel, name("system_call"))
	}
	return strings.TrimSpace(shared) + "\n\n" + strings.TrimSpace(reach)
}

// Identity states who the agent is. Nothing else in its context does: the
// roster, describe and the [from ...] line on every request all name OTHER
// actors, so an agent without this block has no way to tell its own facts from
// the sender's. The closing sentence is the one that matters — the observed
// failure was an agent reading a word documented as returning "the effective
// caller's" facts as returning the facts of whoever had just written to it,
// and answering that the sender was already a member when it was itself.
func (s Surface) Identity() string {
	if s.self.ActorID == "" {
		return ""
	}
	facts := []string{}
	if s.self.Kind != "" {
		facts = append(facts, "kind "+s.self.Kind)
	}
	if s.self.Class != "" {
		facts = append(facts, "class "+s.self.Class)
	}
	if s.self.Seed != "" {
		facts = append(facts, "declaration "+s.self.Seed)
	}
	where := ""
	if s.self.Channel != "" {
		where = ", in channel " + s.self.Channel
	}
	detail := ""
	if len(facts) > 0 {
		detail = " — " + strings.Join(facts, ", ")
	}
	return strings.TrimSpace(fmt.Sprintf(`
## Who you are

Atoll identifies an actor by three facts, spelled out in its id
"<kind>:<seed>:<incarnation>":

- **actor id** — the whole thing. It addresses one live member of one channel,
  and it is what every tool here takes as a target. Ids are channel-local: the
  same person in two channels is two different actor ids.
- **kind** — the first segment, from the closed set human / agent / tool / peer
  / system. A human is a person; an agent is model-driven like you; a tool is
  an adapter; a peer is a door onto another channel; system is a channel's
  fixed door.
- **principal or declaration** — the middle segment. For a human it is the
  principal: the login account that person signs in as, stable across every
  channel they are in. For an agent or tool it is the declaration it was minted
  from. This is provenance, not permission — it never authorises anything by
  itself.
- **incarnation** — the trailing timestamp. Restarting a member gives it a new
  incarnation under the same seed, so ids change while the principal or
  declaration does not.

You are the actor %s%s%s.

Every request you receive comes from a DIFFERENT actor, and its "from" line
gives you that actor's channel, kind, principal-or-declaration and full id. You
are never that actor. A word documented as answering about "the caller", "the
effective caller" or "you" answers about %s — not about whoever wrote to you.
`, s.self.ActorID, detail, where, s.self.ActorID))
}

// AppendGuide assembles the full system-prompt tail: who the agent is, the
// declaration-authored instructions, then the tools and their reach.
func (s Surface) AppendGuide(prompt string) string {
	sections := []string{}
	if identity := s.Identity(); identity != "" {
		sections = append(sections, identity)
	}
	if trimmed := strings.TrimSpace(prompt); trimmed != "" {
		sections = append(sections, trimmed)
	}
	sections = append(sections, s.Guide())
	return strings.Join(sections, "\n\n")
}

func (s Surface) MapResult(result driverproto.ToolResult) driverproto.ToolResult {
	text := strings.TrimSpace(result.Text)
	if len(text) > MaxResultBytes {
		return driverproto.ToolResult{Text: ErrorText("internal_error", "tool result exceeded the 1 MiB transport limit", "Narrow the request or ask the target for a smaller result"), IsError: true}
	}
	var value any
	if json.Unmarshal([]byte(text), &value) != nil {
		if result.IsError {
			return driverproto.ToolResult{Text: ErrorText("internal_error", text, "Inspect the failure and retry only when safe"), IsError: true}
		}
		return driverproto.ToolResult{Text: text, IsError: false}
	}
	value = s.mapValue(value)
	raw, err := json.Marshal(value)
	if err != nil {
		return driverproto.ToolResult{Text: ErrorText("internal_error", err.Error(), "Inspect adapter logs"), IsError: true}
	}
	return driverproto.ToolResult{Text: string(raw), IsError: result.IsError}
}

func (s Surface) mapValue(v any) any {
	switch value := v.(type) {
	case map[string]any:
		for key, child := range value {
			value[key] = s.mapValue(child)
		}
		return value
	case []any:
		for i := range value {
			value[i] = s.mapValue(value[i])
		}
		return value
	case string:
		entries := append([]Entry(nil), s.entries...)
		sort.Slice(entries, func(i, j int) bool { return len(entries[i].Canonical) > len(entries[j].Canonical) })
		for _, entry := range entries {
			if entry.Canonical != entry.Exposed {
				value = replaceIdentifier(value, entry.Canonical, entry.Exposed)
			}
		}
		return value
	default:
		return v
	}
}

func replaceIdentifier(value, old, replacement string) string {
	var out strings.Builder
	for {
		i := strings.Index(value, old)
		if i < 0 {
			out.WriteString(value)
			return out.String()
		}
		end := i + len(old)
		leftOK := i == 0 || !identifierByte(value[i-1])
		rightOK := end == len(value) || !identifierByte(value[end])
		if leftOK && rightOK {
			out.WriteString(value[:i])
			out.WriteString(replacement)
			value = value[end:]
			continue
		}
		out.WriteString(value[:end])
		value = value[end:]
	}
}

func identifierByte(b byte) bool {
	return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

func ErrorText(code, message, recovery string) string {
	errValue := map[string]any{"code": code, "message": strings.TrimSpace(message)}
	if strings.TrimSpace(recovery) != "" {
		errValue["recovery_hint"] = strings.TrimSpace(recovery)
	}
	raw, _ := json.Marshal(map[string]any{"ok": false, "error": errValue})
	return string(raw)
}

func digest(entries []Entry) string {
	h := sha256.New()
	for _, entry := range entries {
		_, _ = h.Write([]byte(entry.Canonical + "\x00" + entry.Wire + "\x00" + entry.Exposed + "\x00" + entry.Spec.Description + "\x00"))
		_, _ = h.Write(entry.Spec.Schema)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
