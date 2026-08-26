package coderunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// TypeValidate is the pre-flight half of code.run: resolve a requires list
// against the channel exactly as code.run would, and hand back what each
// requirement resolves to together with that actor's manifest — the words,
// input and output schemas a program author needs before writing run().
// Pure Go: it never starts Node.
//
// The requires list is CONFIG, never code: a mode-one call names it in the
// payload, a fixed-program member carries it in its template config, and it
// is never derived from program text.
const TypeValidate = "code.validate"

type validatePayload struct {
	Requires []string `json:"requires,omitempty"`
}

// validateResult is the code.validate answer. ok is false iff something is
// missing; an ambiguous requirement is still resolved (to the first sorted
// candidate, as code.run would) and reported so the author can narrow it.
type validateResult struct {
	OK        bool                     `json:"ok"`
	Resolved  map[string]resolvedActor `json:"resolved"`
	Missing   []string                 `json:"missing"`
	Ambiguous map[string][]string      `json:"ambiguous,omitempty"`
	Errors    map[string]string        `json:"errors,omitempty"`
}

type resolvedActor struct {
	Actor actor.ActorID                  `json:"actor"`
	Class string                         `json:"class,omitempty"`
	Words map[string]introspect.WordSpec `json:"words,omitempty"`
}

// decodeValidate mirrors decodeRun's mode rule: a mode-one member takes the
// list from the payload; a fixed-program member refuses one and answers for
// its own config.
func (a *coderunnerActor) decodeValidate(raw json.RawMessage) ([]string, error) {
	var fields map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) == 0 {
		fields = map[string]json.RawMessage{}
	} else if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("code.validate input must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var payload validatePayload
	if len(fields) > 0 {
		if err := dec.Decode(&payload); err != nil {
			return nil, fmt.Errorf("invalid code.validate input: %w", err)
		}
		var trailing any
		if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, errors.New("invalid code.validate input: multiple JSON values")
		}
	}
	requiresRaw, hasRequires := fields["requires"]
	if hasRequires {
		var value []string
		if json.Unmarshal(requiresRaw, &value) != nil || value == nil {
			return nil, errors.New("requires must be an array of strings")
		}
	}
	if a.cfg.Program == "" {
		if !hasRequires {
			return nil, errors.New("mode-one coderunner requires a requires array to validate")
		}
		if err := validateRequires(payload.Requires); err != nil {
			return nil, err
		}
		return payload.Requires, nil
	}
	if hasRequires {
		return nil, errors.New("fixed-program coderunner validates its own config; requires is not accepted in code.validate")
	}
	return a.cfg.Requires, nil
}

func (a *coderunnerActor) handleValidate(sys actorbase.Sys, msg actorbase.Msg) {
	requires, err := a.decodeValidate(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_input", err.Error())
		return
	}
	res, err := resolveRequirementsDetailed(sys, msg, requires, a.logger())
	if err != nil {
		_, _ = sys.Fail(msg, "dependency_missing", err.Error())
		return
	}
	out := validateResult{
		OK:        len(res.missing) == 0,
		Resolved:  make(map[string]resolvedActor, len(res.resolved)),
		Missing:   append([]string{}, res.missing...),
		Ambiguous: res.ambiguous(),
	}
	// Describe each resolved actor so the author gets its words and schemas.
	// A describe that fails does not fail the validation — the requirement
	// resolved; the manifest is simply reported as unavailable.
	names := make([]string, 0, len(res.resolved))
	for name := range res.resolved {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		id := res.resolved[name]
		entry := resolvedActor{Actor: id}
		describeMsg, derr := callAndWait(sys, msg, id, introspect.QueryDescribe, struct{}{})
		if derr != nil {
			if out.Errors == nil {
				out.Errors = map[string]string{}
			}
			out.Errors[name] = derr.Error()
		} else {
			var describe introspect.Describe
			if uerr := json.Unmarshal(describeMsg.Payload, &describe); uerr != nil {
				if out.Errors == nil {
					out.Errors = map[string]string{}
				}
				out.Errors[name] = "decode actor.describe: " + uerr.Error()
			} else {
				entry.Class = describe.Class
				entry.Words = describe.Words
			}
		}
		out.Resolved[name] = entry
	}
	_, _ = sys.Reply(msg, out)
}
