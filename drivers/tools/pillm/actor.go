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
	DefaultProvider  string `json:"default_provider,omitempty"`
	DefaultModel     string `json:"default_model,omitempty"`
	APIKey           string `json:"api_key,omitempty"`
	MaxConcurrency   int    `json:"max_concurrency,omitempty"`
}

func DefaultConfig() json.RawMessage {
	return json.RawMessage(`{"node":"node","default_provider":"openai","default_model":"gpt-5.6-sol","max_concurrency":4,"max_retries":2,"base_delay_ms":500,"max_delay_ms":5000,"request_timeout_ms":300000}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Node: "node", DefaultProvider: "openai", DefaultModel: "gpt-5.6-sol", MaxConcurrency: 4}
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
	if strings.TrimSpace(cfg.Node) == "" || strings.TrimSpace(cfg.DefaultProvider) == "" || strings.TrimSpace(cfg.DefaultModel) == "" {
		return Config{}, errors.New("pi-llm config: node and default model are required")
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
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: DefaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"node":{"type":"string","minLength":1},"default_provider":{"type":"string","minLength":1},"default_model":{"type":"string","minLength":1},"api_key":{"type":"string","minLength":1,"writeOnly":true},"max_concurrency":{"type":"integer","minimum":1,"maximum":64},"max_retries":{"type":"integer","minimum":0,"maximum":5},"base_delay_ms":{"type":"integer","minimum":1},"max_delay_ms":{"type":"integer","minimum":1,"maximum":60000},"request_timeout_ms":{"type":"integer","minimum":1,"maximum":1800000}}}`)})
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
		llmproto.TypeGenerate: {Description: "Run one stateless Pi LLM request with bounded provider retries and one total deadline. No Agent loop or tools are executed. Progress contains lightweight liveness only; the final assistant preserves provider signatures.", InputSchema: json.RawMessage(`{"type":"object","required":["messages"],"properties":{"provider":{"type":"string"},"model":{"type":"string"},"api_key":{"type":"string","minLength":1,"writeOnly":true},"system_prompt":{"type":"string"},"messages":{"type":"array","minItems":1},"tools":{"type":"array"},"options":{"type":"object"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["provider","model","message","attempts"],"properties":{"provider":{"type":"string"},"model":{"type":"string"},"message":{"type":"object"},"attempts":{"type":"integer","minimum":1}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "context_invalid", "capacity", "model_not_found", "auth", "permission", "rate_limited", "transient_provider", "transport_error", "unknown_provider_error", "cancelled", "deadline_exceeded", "runtime_unavailable"}},
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
			_, _ = sys.Fail(msg, failure.Code, failure.Error(), map[string]any{"attempts": attempts, "http_status": failure.Status, "provider_code": failure.ProviderCode})
			return
		}
		var result map[string]json.RawMessage
		_ = json.Unmarshal(raw, &result)
		result["attempts"], _ = json.Marshal(attempts)
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
func emit(sys actorbase.Sys, typ string, value any) {
	spec, err := behavior.EventSpecJSON(message.Root(), typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}
