package piworkspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const Class = "pi-workspace"

type Config struct {
	Node           string `json:"node,omitempty"`
	MaxConcurrency int    `json:"max_concurrency,omitempty"`
	QueueCapacity  int    `json:"queue_capacity,omitempty"`
	IdleMS         int64  `json:"idle_ms,omitempty"`
}

func defaultConfig() json.RawMessage {
	return json.RawMessage(`{"node":"node","max_concurrency":4,"queue_capacity":32,"idle_ms":300000}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Node: "node", MaxConcurrency: 4, QueueCapacity: 32, IdleMS: 300000}
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
		return Config{}, errors.New("pi-workspace config: multiple JSON values")
	}
	if strings.TrimSpace(cfg.Node) == "" {
		return Config{}, errors.New("pi-workspace config: node required")
	}
	if cfg.MaxConcurrency < 1 || cfg.MaxConcurrency > 64 {
		return Config{}, errors.New("pi-workspace config: max_concurrency must be 1..64")
	}
	if cfg.QueueCapacity < 1 || cfg.QueueCapacity > 1024 {
		return Config{}, errors.New("pi-workspace config: queue_capacity must be 1..1024")
	}
	if cfg.IdleMS < 0 || cfg.IdleMS > int64((24*time.Hour)/time.Millisecond) {
		return Config{}, errors.New("pi-workspace config: idle_ms must be 0..86400000")
	}
	return cfg, nil
}

func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: defaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"node":{"type":"string","minLength":1},"max_concurrency":{"type":"integer","minimum":1,"maximum":64},"queue_capacity":{"type":"integer","minimum":1,"maximum":1024},"idle_ms":{"type":"integer","minimum":0,"maximum":86400000}}}`)})
}
func construct(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) { return proc(cfg, deps), nil }}}}, nil
}

func manifest() introspect.Manifest {
	words := map[string]introspect.WordSpec{
		workspaceproto.TypeRead:  {Description: "Execute Pi read using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.ReadInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeWrite: {Description: "Execute Pi write using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.WriteInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeEdit:  {Description: "Execute Pi edit using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.EditInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeBash:  {Description: "Execute Pi bash using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.BashInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeGrep:  {Description: "Execute Pi grep using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.GrepInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeFind:  {Description: "Execute Pi find using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.FindInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeLS:    {Description: "Execute Pi ls using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.LSInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
	}
	if powerShellAvailable() {
		words[workspaceproto.TypePowerShell] = introspect.WordSpec{Description: "Execute PowerShell using the branch execution snapshot supplied by its Looper.", InputSchema: workspaceproto.TransportInputSchema(workspaceproto.PowerShellInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()}
	}
	return introspect.Manifest{Class: Class, Interfaces: []string{"actor", "workspace"}, Capabilities: map[string]bool{"pi_base_tools": true, "search_tools": true, "request_cancel": true}, Words: words}
}

func powerShellAvailable() bool {
	for _, name := range []string{"pwsh", "powershell", "powershell.exe"} {
		if _, err := exec.LookPath(name); err == nil {
			return true
		}
	}
	return false
}
func toolOutputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","required":["content"],"properties":{"content":{"type":"array"},"details":{}},"additionalProperties":true}`)
}
func toolErrors() []string {
	return []string{"invalid_args", "tool_failed", "cancelled", "capacity", "runtime_unavailable"}
}

func proc(cfg Config, deps registry.Deps) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		emit(sys, "actor.initializing", map[string]any{"component": Class, "runtime": pibridge.Version})
		bridges := newBridgePool(sys.Life(), cfg, deps)
		emit(sys, "actor.ready", map[string]any{"component": Class, "runtime": pibridge.Version})
		return runScheduler(sys, cfg, func(msg actorbase.Msg) { handle(sys, bridges, msg) }, bridges.Close)
	}
}

func runScheduler(sys actorbase.Sys, cfg Config, execute func(actorbase.Msg), shutdown func()) error {
	jobs := make(chan actorbase.Msg, cfg.QueueCapacity)
	admitted := make(chan struct{}, cfg.QueueCapacity)
	var wg sync.WaitGroup
	worker := func(jobs <-chan actorbase.Msg) {
		defer wg.Done()
		for msg := range jobs {
			<-admitted
			if err := msg.Ctx().Err(); err != nil {
				_, _ = sys.Fail(msg, "cancelled", err.Error())
				continue
			}
			execute(msg)
		}
	}
	for range cfg.MaxConcurrency {
		wg.Add(1)
		go worker(jobs)
	}
	defer func() {
		shutdown()
		close(jobs)
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
		if !isWord(msg.Type) {
			_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("Pi workspace does not answer %q", msg.Type))
			continue
		}
		if msg.Ctx().Err() != nil {
			_, _ = sys.Fail(msg, "cancelled", msg.Ctx().Err().Error())
			continue
		}
		select {
		case admitted <- struct{}{}:
		default:
			_, _ = sys.Fail(msg, "capacity", "Pi workspace queue is full")
			continue
		}
		jobs <- msg
	}
}

func isWord(s string) bool {
	if s == workspaceproto.TypePowerShell && !powerShellAvailable() {
		return false
	}
	return isReadWord(s) || s == workspaceproto.TypeWrite || s == workspaceproto.TypeEdit || s == workspaceproto.TypeBash || s == workspaceproto.TypePowerShell
}
func isReadWord(s string) bool {
	return s == workspaceproto.TypeRead || s == workspaceproto.TypeGrep || s == workspaceproto.TypeFind || s == workspaceproto.TypeLS
}
func handle(sys actorbase.Sys, bridges *bridgePool, msg actorbase.Msg) {
	session := msg.Context().Session
	if session == "" {
		_, _ = sys.Fail(msg, "invalid_args", "branch session is required in message context")
		return
	}
	var req workspaceproto.ExecuteRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if strings.TrimSpace(req.Execution.CWD) == "" || !filepath.IsAbs(req.Execution.CWD) {
		_, _ = sys.Fail(msg, "invalid_args", "execution.cwd must be an absolute path")
		return
	}
	for key, value := range req.Execution.Env {
		if key == "" || key == "PWD" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') {
			_, _ = sys.Fail(msg, "invalid_args", "execution.env contains an invalid entry")
			return
		}
	}
	_, args, err := decodeArgs(msg.Type, req.Input)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	b, err := bridges.Acquire(session)
	if err != nil {
		_, _ = sys.Fail(msg, "runtime_unavailable", err.Error())
		return
	}
	defer bridges.Release(session, b)
	raw, err := b.CallWithEnvironment(msg.Ctx(), msg.Type, args, req.Execution.CWD, req.Execution.Env, func(update json.RawMessage) { _, _ = sys.Progress(msg, "processing", map[string]any{"update": update}) })
	if err != nil {
		code := "tool_failed"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			code = "cancelled"
		}
		var toolErr *pibridge.Error
		if !errors.As(err, &toolErr) && code != "cancelled" {
			bridges.Invalidate(session, b)
		}
		_, _ = sys.Fail(msg, code, err.Error())
		return
	}
	_, _ = sys.Reply(msg, json.RawMessage(raw))
}

type bridgeEntry struct {
	bridge     *pibridge.Bridge
	inflight   int
	generation uint64
	timer      *time.Timer
}

type bridgePool struct {
	mu       sync.Mutex
	ctx      context.Context
	node     string
	runtime  string
	logger   *slog.Logger
	idle     time.Duration
	closed   bool
	sessions map[string]*bridgeEntry
}

func newBridgePool(ctx context.Context, cfg Config, deps registry.Deps) *bridgePool {
	return &bridgePool{ctx: ctx, node: cfg.Node, runtime: deps.WorkspaceDir, logger: deps.Logger, idle: time.Duration(cfg.IdleMS) * time.Millisecond, sessions: make(map[string]*bridgeEntry)}
}

func (p *bridgePool) Acquire(session string) (*pibridge.Bridge, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("Pi workspace bridge pool is closed")
	}
	if entry := p.sessions[session]; entry != nil {
		if entry.bridge.Alive() {
			if entry.timer != nil {
				entry.timer.Stop()
				entry.timer = nil
			}
			entry.inflight++
			return entry.bridge, nil
		}
		if entry.timer != nil {
			entry.timer.Stop()
		}
		delete(p.sessions, session)
	}
	b, err := pibridge.Start(p.ctx, p.node, p.runtime, p.logger)
	if err != nil {
		return nil, err
	}
	p.sessions[session] = &bridgeEntry{bridge: b, inflight: 1, generation: 1}
	return b, nil
}

func (p *bridgePool) Release(session string, b *pibridge.Bridge) {
	p.mu.Lock()
	entry := p.sessions[session]
	if entry == nil || entry.bridge != b {
		p.mu.Unlock()
		return
	}
	entry.inflight--
	if entry.inflight > 0 {
		p.mu.Unlock()
		return
	}
	if p.idle <= 0 {
		delete(p.sessions, session)
		p.mu.Unlock()
		b.Close()
		return
	}
	entry.generation++
	generation := entry.generation
	entry.timer = time.AfterFunc(p.idle, func() { p.expire(session, b, generation) })
	p.mu.Unlock()
}

func (p *bridgePool) expire(session string, b *pibridge.Bridge, generation uint64) {
	p.mu.Lock()
	entry := p.sessions[session]
	if entry == nil || entry.bridge != b || entry.generation != generation || entry.inflight != 0 {
		p.mu.Unlock()
		return
	}
	delete(p.sessions, session)
	p.mu.Unlock()
	b.Close()
}

func (p *bridgePool) Invalidate(session string, b *pibridge.Bridge) {
	p.mu.Lock()
	entry := p.sessions[session]
	if entry == nil || entry.bridge != b {
		p.mu.Unlock()
		return
	}
	delete(p.sessions, session)
	if entry.timer != nil {
		entry.timer.Stop()
	}
	p.mu.Unlock()
	b.Close()
}

func (p *bridgePool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	entries := make([]*bridgeEntry, 0, len(p.sessions))
	for _, entry := range p.sessions {
		if entry.timer != nil {
			entry.timer.Stop()
		}
		entries = append(entries, entry)
	}
	p.sessions = make(map[string]*bridgeEntry)
	p.mu.Unlock()
	for _, entry := range entries {
		entry.bridge.Close()
	}
}

func decodeArgs(word string, raw json.RawMessage) (string, json.RawMessage, error) {
	var path string
	switch word {
	case workspaceproto.TypeRead:
		var req workspaceproto.ReadRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		if err := requirePositiveWhenPresent(raw, "offset", req.Offset); err != nil {
			return "", nil, err
		}
		if err := requirePositiveWhenPresent(raw, "limit", req.Limit); err != nil {
			return "", nil, err
		}
		path = req.Path
	case workspaceproto.TypeWrite:
		var req workspaceproto.WriteRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		path = req.Path
	case workspaceproto.TypeEdit:
		var req workspaceproto.EditRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		path = req.Path
	case workspaceproto.TypeBash:
		var req workspaceproto.BashRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		if strings.TrimSpace(req.Command) == "" {
			return "", nil, errors.New("command is required")
		}
	case workspaceproto.TypePowerShell:
		var req workspaceproto.BashRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		if strings.TrimSpace(req.Command) == "" {
			return "", nil, errors.New("command is required")
		}
	case workspaceproto.TypeGrep:
		var req workspaceproto.GrepRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		if req.Pattern == "" || req.Context < 0 {
			return "", nil, errors.New("pattern is required and context must be non-negative")
		}
		if err := requirePositiveWhenPresent(raw, "limit", req.Limit); err != nil {
			return "", nil, err
		}
		path = req.Path
	case workspaceproto.TypeFind:
		var req workspaceproto.FindRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		if req.Pattern == "" {
			return "", nil, errors.New("pattern is required")
		}
		if err := requirePositiveWhenPresent(raw, "limit", req.Limit); err != nil {
			return "", nil, err
		}
		path = req.Path
	case workspaceproto.TypeLS:
		var req workspaceproto.LSRequest
		if err := actorbase.DecodeStrict(raw, &req); err != nil {
			return "", nil, err
		}
		if err := requirePositiveWhenPresent(raw, "limit", req.Limit); err != nil {
			return "", nil, err
		}
		path = req.Path
	default:
		return "", nil, errors.New("unsupported workspace word")
	}
	if (word == workspaceproto.TypeRead || word == workspaceproto.TypeWrite || word == workspaceproto.TypeEdit) && strings.TrimSpace(path) == "" {
		return "", nil, errors.New("path is required")
	}
	if isReadWord(word) && path == "" {
		path = "."
	}
	return path, append(json.RawMessage(nil), raw...), nil
}

func requirePositiveWhenPresent(raw json.RawMessage, field string, value int) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	if _, present := fields[field]; present && value < 1 {
		return fmt.Errorf("%s must be positive", field)
	}
	return nil
}
func emit(sys actorbase.Sys, typ string, value any) {
	spec, err := behavior.EventSpecJSON(message.Root(), harness.Context{}, typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}
