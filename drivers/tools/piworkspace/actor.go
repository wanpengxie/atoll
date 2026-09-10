package piworkspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

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
}

func defaultConfig() json.RawMessage {
	return json.RawMessage(`{"node":"node","max_concurrency":1,"queue_capacity":32}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{Node: "node", MaxConcurrency: 1, QueueCapacity: 32}
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
	return cfg, nil
}

func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: defaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"node":{"type":"string","minLength":1},"max_concurrency":{"type":"integer","minimum":1,"maximum":64},"queue_capacity":{"type":"integer","minimum":1,"maximum":1024}}}`)})
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
		workspaceproto.TypeRead:  {Description: "Pi read tool, rooted at this Channel's workspace. Text and image content plus truncation details are preserved.", InputSchema: json.RawMessage(workspaceproto.ReadInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeWrite: {Description: "Pi write tool, rooted at this Channel's workspace. The default single execution lane serializes shared-workspace effects.", InputSchema: json.RawMessage(workspaceproto.WriteInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeEdit:  {Description: "Pi edit tool. Each oldText must identify one unique, non-overlapping region of the original file.", InputSchema: json.RawMessage(workspaceproto.EditInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeBash:  {Description: "Pi bash tool in this Channel's workspace. Cancellation, timeout, tail truncation and spill-path details are preserved; cwd is not an OS sandbox.", InputSchema: json.RawMessage(workspaceproto.BashInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeGrep:  {Description: "Search file contents with ripgrep, rooted at this Channel's workspace. Patterns and globs are search expressions, not paths.", InputSchema: json.RawMessage(workspaceproto.GrepInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeFind:  {Description: "Find files by glob with fd, rooted at this Channel's workspace.", InputSchema: json.RawMessage(workspaceproto.FindInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
		workspaceproto.TypeLS:    {Description: "List a directory rooted at this Channel's workspace.", InputSchema: json.RawMessage(workspaceproto.LSInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()},
	}
	if powerShellAvailable() {
		words[workspaceproto.TypePowerShell] = introspect.WordSpec{Description: "Execute PowerShell in this Channel's workspace. Commands may have write side effects; cwd is not an OS sandbox.", InputSchema: json.RawMessage(workspaceproto.PowerShellInputSchema), OutputSchema: toolOutputSchema(), ErrorCodes: toolErrors()}
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
	return []string{"invalid_args", "path_outside_workspace", "tool_failed", "cancelled", "capacity", "runtime_unavailable"}
}

func proc(cfg Config, deps registry.Deps) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		emit(sys, "actor.initializing", map[string]any{"component": Class, "runtime": pibridge.Version})
		b, err := pibridge.Start(sys.Life(), cfg.Node, deps.WorkspaceDir, deps.Logger)
		if err != nil {
			emit(sys, "actor.failed", map[string]any{"component": Class, "detail": err.Error()})
			return err
		}
		emit(sys, "actor.ready", map[string]any{"component": Class, "runtime": pibridge.Version})
		return runScheduler(sys, cfg, func(msg actorbase.Msg) { handle(sys, b, deps.WorkspaceDir, msg) }, b.Close)
	}
}

func runScheduler(sys actorbase.Sys, cfg Config, execute func(actorbase.Msg), shutdown func()) error {
	reads := make(chan actorbase.Msg, cfg.QueueCapacity)
	writes := make(chan actorbase.Msg, cfg.QueueCapacity)
	all := make(chan actorbase.Msg, cfg.QueueCapacity)
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
	if cfg.MaxConcurrency == 1 {
		wg.Add(1)
		go worker(all)
	} else {
		wg.Add(1)
		go worker(writes)
		for range cfg.MaxConcurrency - 1 {
			wg.Add(1)
			go worker(reads)
		}
	}
	defer func() {
		// Terminate active tool calls before waiting for workers; queued calls
		// then fail promptly against the closed bridge while the mailbox is no
		// longer admitting new jobs.
		shutdown()
		if cfg.MaxConcurrency == 1 {
			close(all)
		} else {
			close(reads)
			close(writes)
		}
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
		if cfg.MaxConcurrency == 1 {
			all <- msg
		} else if isReadWord(msg.Type) {
			reads <- msg
		} else {
			writes <- msg
		}
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
func handle(sys actorbase.Sys, b *pibridge.Bridge, cwd string, msg actorbase.Msg) {
	path, args, err := decodeArgs(msg.Type, msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if msg.Type != workspaceproto.TypeBash && msg.Type != workspaceproto.TypePowerShell {
		if err := safeWorkspacePath(cwd, path); err != nil {
			_, _ = sys.Fail(msg, "path_outside_workspace", "path must remain inside the Channel workspace")
			return
		}
	}
	raw, err := b.Call(msg.Ctx(), msg.Type, args, cwd, func(update json.RawMessage) { _, _ = sys.Progress(msg, "processing", map[string]any{"update": update}) })
	if err != nil {
		code := "tool_failed"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			code = "cancelled"
		}
		_, _ = sys.Fail(msg, code, err.Error())
		return
	}
	_, _ = sys.Reply(msg, json.RawMessage(raw))
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
func safeWorkspacePath(root, path string) error {
	if filepath.IsAbs(path) {
		return errors.New("absolute path")
	}
	clean := filepath.Clean(path)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return errors.New("lexical escape")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return err
	}
	// Resolve the deepest existing prefix. This catches both an existing target
	// symlink and a symlinked parent of a not-yet-created write target.
	probe := filepath.Join(rootAbs, clean)
	for {
		_, statErr := os.Lstat(probe)
		if statErr == nil {
			break
		}
		if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return statErr
		}
		probe = parent
	}
	probeReal, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootReal, probeReal)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return errors.New("symlink escape")
	}
	return nil
}
func emit(sys actorbase.Sys, typ string, value any) {
	spec, err := behavior.EventSpecJSON(message.Root(), harness.Context{}, typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}
