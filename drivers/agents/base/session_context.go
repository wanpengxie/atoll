package base

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// ContextObject is the disposable, channel-shared model transcript for one
// session. Version is the last ledger message incorporated by the assembler.
type ContextObject struct {
	Messages []json.RawMessage `json:"messages"`
	Version  message.ID        `json:"version,omitempty"`
}

type ContextRef struct {
	Resource resource.ResourceID `json:"resource"`
	Version  message.ID          `json:"version"`
}

func ContextResource(session string) (resource.ResourceID, error) {
	if strings.TrimSpace(session) == "" || strings.TrimSpace(session) != session || strings.ContainsAny(session, "\r\n\x00") {
		return "", errors.New("session must be a non-blank trimmed id")
	}
	return resource.ResourceID("ctx/" + session), nil
}

func ReadContext(sys actorbase.Sys, ref ContextRef) (ContextObject, error) {
	out, err := sys.Resource().Read(ref.Resource)
	if err != nil {
		return ContextObject{}, err
	}
	if !out.Accepted() {
		return ContextObject{}, fmt.Errorf("context read rejected: %s", out.RejectReason)
	}
	if !out.Found {
		return ContextObject{}, errors.New("context_missing")
	}
	var object ContextObject
	if err := json.Unmarshal(out.Value, &object); err != nil {
		return ContextObject{}, fmt.Errorf("context_invalid: %w", err)
	}
	if ref.Version != "" && object.Version != ref.Version {
		return ContextObject{}, fmt.Errorf("context_stale: have %s want %s", object.Version, ref.Version)
	}
	if object.Messages == nil {
		object.Messages = []json.RawMessage{}
	}
	return object, nil
}

func LoadContext(sys actorbase.Sys, session string) (ContextObject, bool, error) {
	id, err := ContextResource(session)
	if err != nil {
		return ContextObject{}, false, err
	}
	out, err := sys.Resource().Read(id)
	if err != nil {
		return ContextObject{}, false, err
	}
	if !out.Accepted() || !out.Found {
		return ContextObject{}, false, nil
	}
	var object ContextObject
	if err := json.Unmarshal(out.Value, &object); err != nil {
		return ContextObject{}, false, fmt.Errorf("context_invalid: %w", err)
	}
	if object.Messages == nil {
		object.Messages = []json.RawMessage{}
	}
	return object, true, nil
}

func WriteContext(sys actorbase.Sys, session string, object ContextObject) (ContextRef, error) {
	id, err := ContextResource(session)
	if err != nil {
		return ContextRef{}, err
	}
	raw, err := json.Marshal(object)
	if err != nil {
		return ContextRef{}, err
	}
	out, err := sys.Resource().Write(id, raw)
	if err != nil {
		return ContextRef{}, err
	}
	if !out.Accepted() {
		out, err = sys.Resource().Create(id, raw)
		if err != nil {
			return ContextRef{}, err
		}
	}
	if !out.Accepted() {
		return ContextRef{}, fmt.Errorf("context write rejected: %s", out.RejectReason)
	}
	return ContextRef{Resource: id, Version: object.Version}, nil
}

func DeleteContext(sys actorbase.Sys, session string) error {
	id, err := ContextResource(session)
	if err != nil {
		return err
	}
	out, err := sys.Resource().Delete(id)
	if err != nil {
		return err
	}
	if !out.Accepted() {
		return fmt.Errorf("context delete rejected: %s", out.RejectReason)
	}
	return nil
}

// ContextTokens is the one transcript-size rule used by session assembly and
// compaction. A provider-reported prompt size anchors the last normal
// assistant call; only content appended after it is estimated.
func ContextTokens(messages []json.RawMessage) int {
	estimate := func(items []json.RawMessage) int {
		total := 0
		for _, item := range items {
			total += len(item)
		}
		return (total + 3) / 4
	}
	for i := len(messages) - 1; i >= 0; i-- {
		var assistant struct {
			Role       string `json:"role"`
			StopReason string `json:"stopReason"`
			Usage      struct {
				ContextTokens      int `json:"context_tokens"`
				ContextTokensCamel int `json:"contextTokens"`
				Input              int `json:"input"`
				CacheRead          int `json:"cacheRead"`
				CacheWrite         int `json:"cacheWrite"`
				CacheReadSnake     int `json:"cache_read"`
				CacheWriteSnake    int `json:"cache_write"`
				Output             int `json:"output"`
				OutputTokens       int `json:"outputTokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(messages[i], &assistant) != nil || assistant.Role != "assistant" || assistant.StopReason == "error" || assistant.StopReason == "aborted" {
			continue
		}
		contextTokens := assistant.Usage.ContextTokens
		if contextTokens == 0 {
			contextTokens = assistant.Usage.ContextTokensCamel
		}
		if contextTokens == 0 {
			cacheRead, cacheWrite := assistant.Usage.CacheRead, assistant.Usage.CacheWrite
			if cacheRead == 0 {
				cacheRead = assistant.Usage.CacheReadSnake
			}
			if cacheWrite == 0 {
				cacheWrite = assistant.Usage.CacheWriteSnake
			}
			contextTokens = assistant.Usage.Input + cacheRead + cacheWrite
		}
		if contextTokens == 0 {
			continue
		}
		output := assistant.Usage.Output
		if output == 0 {
			output = assistant.Usage.OutputTokens
		}
		return contextTokens + output + estimate(messages[i+1:])
	}
	return estimate(messages)
}
