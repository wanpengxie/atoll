package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strings"
	"sync"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	workspaceproto "github.com/wanpengxie/atoll/drivers/tools/piworkspace/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

type toolExecutorKind uint8

const (
	executeWorkspace toolExecutorKind = iota + 1
	executeEnvironment
	executeHostMeta
)

type resolvedTool struct {
	Name       string
	Actor      string
	Word       string
	Definition json.RawMessage
	Kind       toolExecutorKind
	Meta       *metatool.MetaTool
	LedgerCall bool
}

type modelPrefix struct {
	SystemPrompt string
	ToolsJSON    string
	Tools        []resolvedTool
	ByName       map[string]resolvedTool
}

type modelDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

const (
	environmentGetName    = "environment_get"
	environmentUpdateName = "environment_update"
	environmentHintPrefix = "[current branch environment; runtime hint, not conversation]\n"
)

var environmentGetSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{}}`)
var environmentUpdateSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"cwd":{"type":"string"},"set":{"type":"object","additionalProperties":{"type":"string"}},"unset":{"type":"array","items":{"type":"string"},"uniqueItems":true}},"anyOf":[{"required":["cwd"]},{"required":["set"]},{"required":["unset"]}]}`)

func buildModelPrefix(cfg Config) (modelPrefix, error) {
	tools := make([]resolvedTool, 0, len(workspaceproto.ModelToolSpecs())+2+len(metatool.MetaTools()))
	for _, spec := range workspaceproto.ModelToolSpecs() {
		definition, err := encodeModelDefinition(spec.Name, spec.Description, json.RawMessage(spec.Schema))
		if err != nil {
			return modelPrefix{}, err
		}
		tools = append(tools, resolvedTool{Name: spec.Name, Actor: workspaceActor, Word: spec.Word, Definition: definition, Kind: executeWorkspace})
	}
	for _, spec := range []struct {
		name, description string
		schema            json.RawMessage
	}{
		{environmentGetName, "Read the current branch CWD and environment variables held by this Looper.", environmentGetSchema},
		{environmentUpdateName, "Update the current branch CWD and environment variables for subsequent workspace calls.", environmentUpdateSchema},
	} {
		definition, err := encodeModelDefinition(spec.name, spec.description, spec.schema)
		if err != nil {
			return modelPrefix{}, err
		}
		tools = append(tools, resolvedTool{Name: spec.name, Word: spec.name, Definition: definition, Kind: executeEnvironment})
	}
	catalog := metatool.MetaTools()
	for i := range catalog {
		meta := catalog[i]
		definition, err := encodeModelDefinition(meta.Spec.Name, meta.Spec.Description, meta.Spec.Schema)
		if err != nil {
			return modelPrefix{}, err
		}
		tools = append(tools, resolvedTool{Name: meta.Spec.Name, Actor: hostActor, Word: meta.Spec.Name, Definition: definition, Kind: executeHostMeta, Meta: &meta, LedgerCall: hostMetaWritesRequest(meta.Spec.Name)})
	}
	byName := make(map[string]resolvedTool, len(tools))
	definitions := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		if !validToolName.MatchString(tool.Name) {
			return modelPrefix{}, fmt.Errorf("invalid fixed model tool name %q", tool.Name)
		}
		if _, exists := byName[tool.Name]; exists {
			return modelPrefix{}, fmt.Errorf("duplicate fixed model tool name %q", tool.Name)
		}
		byName[tool.Name] = tool
		definitions = append(definitions, tool.Definition)
	}
	raw, err := json.Marshal(definitions)
	if err != nil {
		return modelPrefix{}, err
	}
	return modelPrefix{SystemPrompt: cfg.Prompt, ToolsJSON: string(raw), Tools: tools, ByName: byName}, nil
}

func encodeModelDefinition(name, description string, schema json.RawMessage) (json.RawMessage, error) {
	if err := validInputSchema(schema); err != nil {
		return nil, fmt.Errorf("fixed tool %q has invalid input schema: %w", name, err)
	}
	return json.Marshal(modelDefinition{Name: name, Description: description, Parameters: append(json.RawMessage(nil), schema...)})
}

func fixedMaterializedTool(name string) (resolvedTool, bool) {
	for _, spec := range workspaceproto.ModelToolSpecs() {
		if spec.Name == name {
			return resolvedTool{Name: name, Word: spec.Word, Kind: executeWorkspace}, true
		}
	}
	if name == environmentGetName || name == environmentUpdateName {
		return resolvedTool{Name: name, Word: name, Kind: executeEnvironment}, true
	}
	for _, meta := range metatool.MetaTools() {
		if meta.Spec.Name == name {
			return resolvedTool{Name: name, Word: hostHandleCall, Kind: executeHostMeta, LedgerCall: hostMetaWritesRequest(name)}, true
		}
	}
	return resolvedTool{}, false
}

func hostMetaWritesRequest(name string) bool {
	switch name {
	case "await_result", "cancel", "list_pending":
		return false
	default:
		return true
	}
}

type branchEnvironment struct {
	CWD  string
	Vars map[string]string
}

func (e branchEnvironment) clone() branchEnvironment {
	return branchEnvironment{CWD: e.CWD, Vars: cloneStringMap(e.Vars)}
}

type branchRuntime struct {
	mu             sync.Mutex
	Environment    branchEnvironment
	Context        agentbase.ContextObject
	HintAdded      bool
	hostRequestIDs map[message.ID]struct{}
}

func (r *branchRuntime) environmentSnapshot() branchEnvironment {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Environment.clone()
}

func (r *branchRuntime) addHostRequest(id message.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hostRequestIDs == nil {
		r.hostRequestIDs = make(map[message.ID]struct{})
	}
	r.hostRequestIDs[id] = struct{}{}
}

func (r *branchRuntime) hasHostRequest(id message.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.hostRequestIDs[id]
	return ok
}

func (r *branchRuntime) removeHostRequest(id message.ID) {
	r.mu.Lock()
	delete(r.hostRequestIDs, id)
	r.mu.Unlock()
}

func (r *branchRuntime) filterHostRequests(ids []message.ID) []message.ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]message.ID, 0, len(ids))
	for _, id := range ids {
		if _, ok := r.hostRequestIDs[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

type environmentDefaults struct {
	host      channel.ID
	lookup    func(channel.ID) (string, bool)
	variables map[string]string
}

func newEnvironmentDefaults(cfg Config, deps registry.Deps) environmentDefaults {
	return environmentDefaults{host: cfg.HostChannel, lookup: deps.WorkspaceFor, variables: processEnvironment()}
}

func (d environmentDefaults) snapshot() (branchEnvironment, error) {
	if d.host == "" {
		return branchEnvironment{}, errors.New("host Channel is unavailable")
	}
	if d.lookup == nil {
		return branchEnvironment{}, errors.New("host Channel workspace resolver is unavailable")
	}
	cwd, ok := d.lookup(d.host)
	if !ok || cwd == "" {
		return branchEnvironment{}, fmt.Errorf("host Channel workspace %q is not active on this device", d.host)
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return branchEnvironment{}, err
	}
	return branchEnvironment{CWD: filepath.Clean(abs), Vars: cloneStringMap(d.variables)}, nil
}

func processEnvironment() map[string]string {
	out := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok && validEnvironmentKeyForOS(key, goruntime.GOOS) {
			setEnvironmentEntryForOS(out, key, value, goruntime.GOOS)
		}
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func environmentHint(env branchEnvironment) json.RawMessage {
	keys := make([]string, 0, len(env.Vars))
	for key := range env.Vars {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var body strings.Builder
	body.WriteString(environmentHintPrefix)
	body.WriteString("cwd=")
	body.WriteString(env.CWD)
	body.WriteString("\nenv:\n")
	for _, key := range keys {
		body.WriteString(key)
		body.WriteByte('=')
		body.WriteString(env.Vars[key])
		body.WriteByte('\n')
	}
	raw, _ := json.Marshal(map[string]any{"role": "user", "content": body.String()})
	return raw
}

func isEnvironmentHint(raw json.RawMessage) bool {
	var msg struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	return json.Unmarshal(raw, &msg) == nil && msg.Role == "user" && strings.HasPrefix(msg.Content, environmentHintPrefix)
}

func replaceEnvironmentHint(messages []json.RawMessage, env branchEnvironment) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(messages)+1)
	for _, raw := range messages {
		if !isEnvironmentHint(raw) {
			out = append(out, raw)
		}
	}
	return append(out, environmentHint(env))
}

func removeEnvironmentHint(messages []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(messages))
	for _, raw := range messages {
		if !isEnvironmentHint(raw) {
			out = append(out, raw)
		}
	}
	return out
}

func (r *branchRuntime) appendInitialEnvironmentHint(messages []json.RawMessage) []json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.HintAdded {
		return messages
	}
	r.HintAdded = true
	return replaceEnvironmentHint(messages, r.Environment)
}

func (l *looper) branchForStart(req agentloop.StartRequest, object agentbase.ContextObject) (*branchRuntime, agentbase.ContextObject, error) {
	if l.branches == nil {
		l.branches = make(map[string]*branchRuntime)
	}
	if runtime := l.branches[req.SessionID]; runtime != nil {
		runtime.mu.Lock()
		if runtime.Context.Messages != nil && runtime.Context.Version == object.Version {
			object = cloneContextObject(runtime.Context)
		}
		runtime.Context = cloneContextObject(object)
		runtime.mu.Unlock()
		return runtime, object, nil
	}
	env, err := l.initialEnv.snapshot()
	if err != nil {
		return nil, agentbase.ContextObject{}, err
	}
	if req.Open != nil && req.Open.Base != nil {
		if parent := l.branches[req.Open.Base.Session]; parent != nil {
			env = parent.environmentSnapshot()
		}
	}
	// A context restored from another Holder may carry that runtime's marker.
	// Remove it now; drive appends this Holder's current hint only after the
	// first input batch, when the initial model context is complete.
	object.Messages = removeEnvironmentHint(object.Messages)
	runtime := &branchRuntime{Environment: env, Context: cloneContextObject(object)}
	l.branches[req.SessionID] = runtime
	return runtime, object, nil
}

func cloneContextObject(in agentbase.ContextObject) agentbase.ContextObject {
	return agentbase.ContextObject{Messages: cloneMessages(in.Messages), Version: in.Version}
}

type environmentUpdate struct {
	CWD   *string           `json:"cwd,omitempty"`
	Set   map[string]string `json:"set,omitempty"`
	Unset []string          `json:"unset,omitempty"`
}

func executeEnvironmentTool(tool resolvedTool, params json.RawMessage, runtime *branchRuntime) (json.RawMessage, bool) {
	if runtime == nil {
		return mustJSON(map[string]any{"error": "branch environment is unavailable"}), true
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	switch tool.Name {
	case environmentGetName:
		var empty struct{}
		if err := actorbase.DecodeStrictEmpty(params, &empty); err != nil {
			return mustJSON(map[string]any{"error": err.Error()}), true
		}
		return mustJSON(map[string]any{"cwd": runtime.Environment.CWD, "env": cloneStringMap(runtime.Environment.Vars)}), false
	case environmentUpdateName:
		var update environmentUpdate
		if err := actorbase.DecodeStrict(params, &update); err != nil {
			return mustJSON(map[string]any{"error": err.Error()}), true
		}
		if update.CWD == nil && len(update.Set) == 0 && len(update.Unset) == 0 {
			return mustJSON(map[string]any{"error": "at least one of cwd, set, or unset is required"}), true
		}
		next := runtime.Environment.clone()
		if update.CWD != nil {
			candidate := strings.TrimSpace(*update.CWD)
			if candidate == "" {
				return mustJSON(map[string]any{"error": "cwd must not be blank"}), true
			}
			if !filepath.IsAbs(candidate) {
				candidate = filepath.Join(next.CWD, candidate)
			}
			candidate = filepath.Clean(candidate)
			info, err := os.Stat(candidate)
			if err != nil || !info.IsDir() {
				if err == nil {
					err = errors.New("not a directory")
				}
				return mustJSON(map[string]any{"error": "cwd: " + err.Error()}), true
			}
			next.CWD = candidate
		}
		if duplicate := duplicateEnvironmentKeyForOS(update.Set, goruntime.GOOS); duplicate != "" {
			return mustJSON(map[string]any{"error": fmt.Sprintf("duplicate environment name %q", duplicate)}), true
		}
		for key, value := range update.Set {
			if !validEnvironmentEntry(key, value) {
				return mustJSON(map[string]any{"error": fmt.Sprintf("invalid environment entry %q", key)}), true
			}
			setEnvironmentEntryForOS(next.Vars, key, value, goruntime.GOOS)
		}
		for _, key := range update.Unset {
			if !validEnvironmentKey(key) {
				return mustJSON(map[string]any{"error": fmt.Sprintf("invalid environment name %q", key)}), true
			}
			deleteEnvironmentEntryForOS(next.Vars, key, goruntime.GOOS)
		}
		runtime.Environment = next
		return mustJSON(map[string]any{"cwd": next.CWD, "env": cloneStringMap(next.Vars)}), false
	default:
		return mustJSON(map[string]any{"error": "unknown environment tool"}), true
	}
}

func validEnvironmentKey(key string) bool {
	return validEnvironmentKeyForOS(key, goruntime.GOOS)
}

func validEnvironmentEntry(key, value string) bool {
	return validEnvironmentKey(key) && !strings.ContainsRune(value, '\x00')
}

func validEnvironmentKeyForOS(key, goos string) bool {
	if key == "" || strings.ContainsAny(key, "=\x00") {
		return false
	}
	if goos == "windows" {
		return !strings.EqualFold(key, "PWD")
	}
	return key != "PWD"
}

func environmentKeysEqualForOS(left, right, goos string) bool {
	if goos == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func deleteEnvironmentEntryForOS(variables map[string]string, key, goos string) {
	for existing := range variables {
		if environmentKeysEqualForOS(existing, key, goos) {
			delete(variables, existing)
		}
	}
}

func setEnvironmentEntryForOS(variables map[string]string, key, value, goos string) {
	deleteEnvironmentEntryForOS(variables, key, goos)
	variables[key] = value
}

func duplicateEnvironmentKeyForOS(variables map[string]string, goos string) string {
	keys := make([]string, 0, len(variables))
	for key := range variables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		for _, prior := range keys[:i] {
			if environmentKeysEqualForOS(prior, key, goos) {
				return key
			}
		}
	}
	return ""
}

// hostJobTable makes the standard metatool Exec operate through the one body
// Handle. Its ids remain the outer Handle request ids, so await/list/cancel all
// address the same engine JobTable rows. branch records only which of those
// rows were opened by this branch's Host meta tools; the engine JobTable remains
// the sole owner of request progress, deadlines, terminals, and cancellation.
type hostJobTable struct {
	base   actorbase.JobTable
	host   actor.ActorID
	branch *branchRuntime
}

func (h hostJobTable) Submit(spec behavior.RequestSpec) (message.ID, error) {
	if h.branch == nil {
		return "", errors.New("branch runtime is unavailable")
	}
	wrapped, err := wrapHostRequest(spec, h.host)
	if err != nil {
		return "", err
	}
	id, err := h.base.Submit(wrapped)
	if err == nil {
		h.branch.addHostRequest(id)
	}
	return id, err
}
func (h hostJobTable) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	if h.branch == nil || !h.branch.hasHostRequest(id) {
		return nil, false, errors.New("request does not belong to the Host meta-tool account")
	}
	env, found, err := h.base.Await(ctx, id, window)
	if found || errors.Is(err, actorbase.ErrCallClosed) {
		h.branch.removeHostRequest(id)
	}
	return env, found, err
}
func (h hostJobTable) ProgressEvents(id message.ID) <-chan *message.Envelope {
	if h.branch == nil || !h.branch.hasHostRequest(id) {
		closed := make(chan *message.Envelope)
		close(closed)
		return closed
	}
	return h.base.ProgressEvents(id)
}
func (h hostJobTable) List() []message.ID {
	if h.branch == nil {
		return nil
	}
	return h.branch.filterHostRequests(h.base.List())
}
func (h hostJobTable) Cancel(id message.ID) error {
	if h.branch == nil || !h.branch.hasHostRequest(id) {
		return errors.New("request does not belong to the Host meta-tool account")
	}
	// Do not detach the branch reference here. JobTable.Cancel is deliberately
	// idempotent and returns nil when a real final already won the race and is
	// buffered for Await. Keeping the reference preserves that final; a later
	// Await removes it after either collecting the final or observing that the
	// account really closed. List still hides a cancelled/closed engine row.
	return h.base.Cancel(id)
}

func wrapHostRequest(spec behavior.RequestSpec, host actor.ActorID) (behavior.RequestSpec, error) {
	if host == "" || len(spec.Audience) != 1 {
		return behavior.RequestSpec{}, errors.New("host request requires one target and one Handle")
	}
	payload, err := json.Marshal(map[string]any{
		"type": spec.Type, "payload": spec.Payload, "audience": spec.Audience, "visibility": spec.Visibility,
	})
	if err != nil {
		return behavior.RequestSpec{}, err
	}
	return behavior.RequestSpec{
		Cause: spec.Cause, Context: spec.Context, Type: hostHandleCall, Payload: payload,
		Audience: message.Audience{host}, Visibility: spec.Visibility, ExpiresAt: spec.ExpiresAt,
	}, nil
}

func (l *looper) hostMetaExec(sys actorbase.Sys, host actor.ActorID, branch *branchRuntime) (*metatool.Exec, error) {
	base, ok := sys.(actorbase.JobTable)
	if !ok {
		return nil, errors.New("looper system has no JobTable")
	}
	if branch == nil {
		return nil, errors.New("branch runtime is unavailable")
	}
	jobs := hostJobTable{base: base, host: host, branch: branch}
	call := func(ctx context.Context, spec behavior.RequestSpec, window time.Duration) (*message.Envelope, bool, error) {
		// Introspection calls are synchronous and never become await_result jobs.
		// Submit them directly to the same engine account without attaching their
		// ids to the branch's async Host-request set.
		wrapped, err := wrapHostRequest(spec, host)
		if err != nil {
			return nil, false, err
		}
		id, err := base.Submit(wrapped)
		if err != nil {
			return nil, false, err
		}
		env, found, err := base.Await(ctx, id, window)
		if err != nil || !found {
			_ = base.Cancel(id)
		}
		return env, found, err
	}
	return &metatool.Exec{Jobs: jobs, Call: call, Clock: time.Now, FastPathWindow: 15 * time.Second}, nil
}
