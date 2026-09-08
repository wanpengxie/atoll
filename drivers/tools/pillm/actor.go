package pillm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

const Class = "pi-llm"

type Config struct {
	Node            string `json:"node,omitempty"`
	DefaultProvider string `json:"default_provider,omitempty"`
	DefaultModel    string `json:"default_model,omitempty"`
	APIKey          string `json:"api_key,omitempty"`
	MaxConcurrency  int    `json:"max_concurrency,omitempty"`
}

func DefaultConfig() json.RawMessage {
	return json.RawMessage(`{"node":"node","default_provider":"openai","default_model":"gpt-5.6-sol","max_concurrency":4}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Node: "node", DefaultProvider: "openai", DefaultModel: "gpt-5.6-sol", MaxConcurrency: 4}
	if len(raw) == 0 {
		return cfg, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	var x any
	if err := dec.Decode(&x); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("pi-llm config: multiple JSON values")
	}
	if strings.TrimSpace(cfg.Node) == "" || strings.TrimSpace(cfg.DefaultProvider) == "" || strings.TrimSpace(cfg.DefaultModel) == "" {
		return Config{}, errors.New("pi-llm config: node and default model are required")
	}
	if cfg.APIKey != "" && strings.TrimSpace(cfg.APIKey) == "" {
		return Config{}, errors.New("pi-llm config: api_key must not be blank")
	}
	if cfg.MaxConcurrency < 1 || cfg.MaxConcurrency > 64 {
		return Config{}, errors.New("pi-llm config: max_concurrency must be 1..64")
	}
	return cfg, nil
}

func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: DefaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"node":{"type":"string","minLength":1},"default_provider":{"type":"string","minLength":1},"default_model":{"type":"string","minLength":1},"api_key":{"type":"string","minLength":1,"writeOnly":true},"max_concurrency":{"type":"integer","minimum":1,"maximum":64}}}`)})
}
func construct(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) { return proc(cfg, deps), nil }}}}, nil
}

func manifest() introspect.Manifest {
	return introspect.Manifest{Class: Class, Interfaces: []string{"actor", "llm"}, Capabilities: map[string]bool{"pi_provider": true, "streaming": true, "request_cancel": true}, Words: map[string]introspect.WordSpec{
		llmproto.TypeGenerate: {Description: "Run one stateless Pi LLM provider request. Context and tools are explicit; this endpoint does not run an Agent loop or execute tools. api_key optionally overrides the Provider Actor's configured default for this call.", InputSchema: json.RawMessage(`{"type":"object","required":["messages"],"properties":{"provider":{"type":"string"},"model":{"type":"string"},"api_key":{"type":"string","minLength":1,"writeOnly":true},"system_prompt":{"type":"string"},"messages":{"type":"array","minItems":1},"tools":{"type":"array"},"options":{"type":"object"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["provider","model","message"],"properties":{"provider":{"type":"string"},"model":{"type":"string"},"message":{"type":"object"},"events":{"type":"array"}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "capacity", "model_not_found", "provider_error", "cancelled", "runtime_unavailable"}},
		llmproto.TypeModels:   {Description: "List the model catalog embedded from the pinned Pi provider package; querying never occupies Agent control state.", InputSchema: json.RawMessage(`{"type":"object","properties":{"provider":{"type":"string"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["models"],"properties":{"models":{"type":"array"}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "runtime_unavailable"}},
	}}
}

func proc(cfg Config, deps registry.Deps) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		emit(sys, "actor.initializing", map[string]any{"component": Class, "runtime": pibridge.Version})
		b, err := pibridge.Start(sys.Life(), cfg.Node, deps.WorkspaceDir, deps.Logger, pibridge.ProviderEnvironment)
		if err != nil {
			emit(sys, "actor.failed", map[string]any{"component": Class, "detail": err.Error()})
			return err
		}
		emit(sys, "actor.ready", map[string]any{"component": Class, "runtime": pibridge.Version})
		sem := make(chan struct{}, cfg.MaxConcurrency)
		var wg sync.WaitGroup
		// Stop the child before waiting for active calls. A mailbox failure is not
		// required to imply that every request context was already cancelled.
		defer func() {
			b.Close()
			wg.Wait()
		}()
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			if msg.Kind != message.KindRequest {
				continue
			}
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go func(msg actorbase.Msg) {
					defer wg.Done()
					defer func() { <-sem }()
					handle(sys, b, cfg, deps.WorkspaceDir, msg)
				}(msg)
			default:
				_, _ = sys.Fail(msg, "capacity", "Pi LLM endpoint is at max_concurrency")
			}
		}
	}
}

func handle(sys actorbase.Sys, b *pibridge.Bridge, cfg Config, cwd string, msg actorbase.Msg) {
	var op string
	var args any
	switch msg.Type {
	case llmproto.TypeGenerate:
		var req llmproto.GenerateRequest
		if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		if len(req.Messages) == 0 {
			_, _ = sys.Fail(msg, "invalid_args", "messages must not be empty")
			return
		}
		if req.Provider == "" {
			req.Provider = cfg.DefaultProvider
		}
		if req.Model == "" {
			req.Model = cfg.DefaultModel
		}
		// A call-specific credential overrides the Provider Actor's default.
		// The configured default is added only after decoding the Atoll request;
		// it therefore crosses the private Go-to-Node bridge but never appears in
		// the caller's llm.generate message or the response.
		if req.APIKey == "" {
			req.APIKey = cfg.APIKey
		}
		op, args = llmproto.TypeGenerate, req
	case llmproto.TypeModels:
		var req llmproto.ModelsRequest
		if err := actorbase.DecodeStrictEmpty(msg.Payload, &req); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		op, args = llmproto.TypeModels, req
	default:
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("Pi LLM actor does not answer %q", msg.Type))
		return
	}
	raw, err := b.Call(msg.Ctx(), op, args, cwd, func(event json.RawMessage) { _, _ = sys.Progress(msg, "processing", map[string]any{"event": event}) })
	if err != nil {
		code := "provider_error"
		var pe *pibridge.Error
		if errors.As(err, &pe) && pe.Code != "" {
			code = pe.Code
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			code = "cancelled"
		}
		_, _ = sys.Fail(msg, code, err.Error())
		return
	}
	_, _ = sys.Reply(msg, json.RawMessage(raw))
}
func emit(sys actorbase.Sys, typ string, value any) {
	spec, err := behavior.EventSpecJSON(message.Root(), typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}
