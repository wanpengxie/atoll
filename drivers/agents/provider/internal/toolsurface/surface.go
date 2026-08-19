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
}

func Assemble(catalog []driverproto.ToolSpec, dialect Dialect) (Surface, error) {
	if len(catalog) > MaxCatalogTools {
		return Surface{}, fmt.Errorf("tool catalog has %d entries; limit is %d", len(catalog), MaxCatalogTools)
	}
	s := Surface{entries: make([]Entry, 0, len(catalog)), byWire: make(map[string]Entry, len(catalog))}
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

func (s Surface) Guide() string {
	name := func(canonical string) string {
		for _, entry := range s.entries {
			if entry.Canonical == canonical {
				return entry.Exposed
			}
		}
		return canonical
	}
	return strings.TrimSpace(fmt.Sprintf(`
## Atoll channel tools

Use %s for the live member roster. The system door is a fixed channel facility,
not a roster member: use %s to discover its words and %s to invoke one; do not
send system words through %s. For members, use %s, then %s when one word needs
detail, then %s. Long calls return an acknowledgement: collect it with %s, inspect
%s, or stop it with %s. Read structured error codes and recovery hints before
retrying. Atoll ResourceID read/write/stat is not exposed by these tools.
`, name("list_actors"), name("system_describe"), name("system_call"), name("call_actor"),
		name("describe_actor"), name("describe_type"), name("call_actor"), name("await_result"),
		name("list_pending"), name("cancel")))
}

func (s Surface) AppendGuide(prompt string) string {
	guide := s.Guide()
	if strings.TrimSpace(prompt) == "" {
		return guide
	}
	return strings.TrimSpace(prompt) + "\n\n" + guide
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
