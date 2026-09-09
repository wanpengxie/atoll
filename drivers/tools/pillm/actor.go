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
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
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
	MaxRetries       int    `json:"max_retries,omitempty"`
	BaseDelayMS      int64  `json:"base_delay_ms,omitempty"`
	MaxDelayMS       int64  `json:"max_delay_ms,omitempty"`
	RequestTimeoutMS int64  `json:"request_timeout_ms,omitempty"`
	Node             string `json:"node,omitempty"`
	APIKey           string `json:"api_key,omitempty"`
	MaxConcurrency   int    `json:"max_concurrency,omitempty"`
}

func DefaultConfig() json.RawMessage {
	return json.RawMessage(`{"node":"node","max_concurrency":4,"max_retries":2,"base_delay_ms":500,"max_delay_ms":5000,"request_timeout_ms":300000}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Node: "node", MaxConcurrency: 4}
	cfg.MaxRetries = 2
	cfg.BaseDelayMS = 500
	cfg.MaxDelayMS = 5000
	cfg.RequestTimeoutMS = 300000
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
	if strings.TrimSpace(cfg.Node) == "" {
		return Config{}, errors.New("pi-llm config: node is required")
	}
	if cfg.APIKey != "" && strings.TrimSpace(cfg.APIKey) == "" {
		return Config{}, errors.New("pi-llm config: api_key must not be blank")
	}
	if cfg.MaxConcurrency < 1 || cfg.MaxConcurrency > 64 {
		return Config{}, errors.New("pi-llm config: max_concurrency must be 1..64")
	}
	if cfg.MaxRetries < 0 || cfg.MaxRetries > 5 || cfg.BaseDelayMS < 1 || cfg.MaxDelayMS < cfg.BaseDelayMS || cfg.MaxDelayMS > 60000 || cfg.RequestTimeoutMS < 1 || cfg.RequestTimeoutMS > 1800000 {
		return Config{}, errors.New("pi-llm retry/timeout configuration out of range")
	}
	return cfg, nil
}

func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: DefaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"node":{"type":"string","minLength":1},"api_key":{"type":"string","minLength":1,"writeOnly":true},"max_concurrency":{"type":"integer","minimum":1,"maximum":64},"max_retries":{"type":"integer","minimum":0,"maximum":5},"base_delay_ms":{"type":"integer","minimum":1},"max_delay_ms":{"type":"integer","minimum":1,"maximum":60000},"request_timeout_ms":{"type":"integer","minimum":1,"maximum":1800000}}}`)})
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
		llmproto.TypeGenerate: {Description: "Read one versioned session context object and run one bounded Pi LLM request.", InputSchema: json.RawMessage(`{"type":"object","required":["context"],"properties":{"provider":{"type":"string"},"model":{"type":"string","minLength":1},"purpose":{"type":"string"},"api_key":{"type":"string","minLength":1,"writeOnly":true},"system_prompt":{"type":"string"},"context":{"type":"object","required":["resource","version"],"properties":{"resource":{"type":"string","minLength":1},"version":{"type":"string","minLength":1}},"additionalProperties":false},"tools":{"type":"array"},"options":{"type":"object"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["provider","model","message","attempts"],"properties":{"provider":{"type":"string"},"model":{"type":"string"},"message":{"type":"object"},"attempts":{"type":"integer","minimum":1},"usage":{"type":"object"},"error_code":{"type":"string"},"retryable":{"type":"boolean"}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "context_missing", "context_invalid", "context_stale", "capacity", "model_not_found", "auth", "permission", "rate_limited", "transient_provider", "transport_error", "unknown_provider_error", "cancelled", "deadline_exceeded", "runtime_unavailable"}},
		llmproto.TypeModels:   {Description: "List the model catalog embedded from the pinned Pi provider package; querying never occupies Agent control state.", InputSchema: json.RawMessage(`{"type":"object","properties":{"provider":{"type":"string"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["models"],"properties":{"models":{"type":"array"}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "runtime_unavailable"}},
		llmproto.TypeCount:    {Description: "Count the current version of a session context object.", InputSchema: json.RawMessage(`{"type":"object","required":["context"],"properties":{"context":{"type":"object","required":["resource","version"],"properties":{"resource":{"type":"string","minLength":1},"version":{"type":"string","minLength":1}},"additionalProperties":false}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["context_tokens"],"properties":{"context_tokens":{"type":"integer","minimum":0}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "context_missing", "context_invalid", "context_stale"}},
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
	var generateProvider, generateModel string
	switch msg.Type {
	case llmproto.TypeGenerate:
		var req llmproto.GenerateRequest
		if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		if req.Context.Resource == "" || req.Context.Version == "" {
			_, _ = sys.Fail(msg, "invalid_args", "context.resource and context.version are required")
			return
		}
		object, err := agentbase.ReadContext(sys, req.Context)
		if err != nil {
			code := "context_invalid"
			if strings.HasPrefix(err.Error(), "context_missing") {
				code = "context_missing"
			}
			if strings.HasPrefix(err.Error(), "context_stale") {
				code = "context_stale"
			}
			_, _ = sys.Fail(msg, code, err.Error())
			return
		}
		if len(object.Messages) == 0 {
			_, _ = sys.Fail(msg, "context_invalid", "context messages must not be empty")
			return
		}
		type bridgeGenerate struct {
			llmproto.ModelRef
			Purpose      string            `json:"purpose,omitempty"`
			SystemPrompt string            `json:"system_prompt,omitempty"`
			Messages     []json.RawMessage `json:"messages"`
			Tools        []json.RawMessage `json:"tools,omitempty"`
			Options      json.RawMessage   `json:"options,omitempty"`
			APIKey       string            `json:"api_key,omitempty"`
		}
		if req.Model == "" {
			_, _ = sys.Fail(msg, "invalid_args", "model is required")
			return
		}
		// A call-specific credential overrides the Provider Actor's default.
		// The configured default is added only after decoding the Atoll request;
		// it therefore crosses the private Go-to-Node bridge but never appears in
		// the caller's llm.generate message or the response.
		if req.APIKey == "" {
			req.APIKey = cfg.APIKey
		}
		op, args = llmproto.TypeGenerate, bridgeGenerate{ModelRef: req.ModelRef, Purpose: req.Purpose, SystemPrompt: req.SystemPrompt, Messages: object.Messages, Tools: req.Tools, Options: req.Options, APIKey: req.APIKey}
		generateProvider, generateModel = req.Provider, req.Model
	case llmproto.TypeModels:
		var req llmproto.ModelsRequest
		if err := actorbase.DecodeStrictEmpty(msg.Payload, &req); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		op, args = llmproto.TypeModels, req
	case llmproto.TypeCount:
		var req llmproto.CountRequest
		if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil || req.Context.Resource == "" || req.Context.Version == "" {
			_, _ = sys.Fail(msg, "invalid_args", "context.resource and context.version are required")
			return
		}
		object, err := agentbase.ReadContext(sys, req.Context)
		if err != nil {
			code := "context_invalid"
			if strings.HasPrefix(err.Error(), "context_missing") {
				code = "context_missing"
			}
			if strings.HasPrefix(err.Error(), "context_stale") {
				code = "context_stale"
			}
			_, _ = sys.Fail(msg, code, err.Error())
			return
		}
		tokens := agentbase.ContextTokens(object.Messages)
		_, _ = sys.Reply(msg, llmproto.CountResponse{ContextTokens: tokens})
		return
	default:
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("Pi LLM actor does not answer %q", msg.Type))
		return
	}
	if op == llmproto.TypeGenerate {
		ctx, cancel := context.WithCancel(msg.Ctx())
		defer cancel()
		done := heartbeat(ctx, 10*time.Second, func() { _, _ = sys.Progress(msg, "processing", map[string]any{"phase": "provider_wait"}) })
		raw, attempts, err := retryGenerate(ctx, cfg, func(ctx context.Context) (json.RawMessage, error) { return b.Call(ctx, op, args, cwd, nil) }, func(attempt int, delay time.Duration) {
			_, _ = sys.Progress(msg, "processing", map[string]any{"phase": "provider_attempt", "attempt": attempt, "retry_delay_ms": delay.Milliseconds()})
		})
		cancel()
		<-done
		if err != nil {
			failure := classify(err)
			var actual *providerFailure
			if errors.As(err, &actual) {
				failure = actual
			}
			assistant, _ := json.Marshal(map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Provider error: " + failure.Error()}}, "stopReason": "error", "errorMessage": failure.Error()})
			_, _ = sys.Reply(msg, llmproto.GenerateResponse{Attempts: attempts, Message: assistant, Provider: generateProvider, Model: generateModel, ErrorCode: failure.Code, Retryable: retryable(failure)})
			return
		}
		var result map[string]json.RawMessage
		_ = json.Unmarshal(raw, &result)
		result["attempts"], _ = json.Marshal(attempts)
		if _, present := result["error_code"]; !present {
			code, retryable := assistantError(result["message"])
			if code != "" {
				result["error_code"], _ = json.Marshal(code)
				result["retryable"], _ = json.Marshal(retryable)
			}
		}
		_, _ = sys.Reply(msg, result)
		return
	}
	raw, err := b.Call(msg.Ctx(), op, args, cwd, nil)
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

func assistantError(raw json.RawMessage) (string, bool) {
	var assistant struct {
		StopReason   string `json:"stopReason"`
		ErrorMessage string `json:"errorMessage"`
	}
	if json.Unmarshal(raw, &assistant) != nil {
		return "", false
	}
	text := strings.ToLower(assistant.ErrorMessage)
	if strings.Contains(text, "context") && (strings.Contains(text, "length") || strings.Contains(text, "token") || strings.Contains(text, "large")) {
		return "context_overflow", true
	}
	if assistant.StopReason == "length" {
		return "length_recoverable", true
	}
	if assistant.StopReason == "error" {
		return "unknown_provider_error", strings.Contains(text, "timeout") || strings.Contains(text, "temporar") || strings.Contains(text, "unavailable")
	}
	return "", false
}
func emit(sys actorbase.Sys, typ string, value any) {
	spec, err := behavior.EventSpecJSON(message.Root(), typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}
