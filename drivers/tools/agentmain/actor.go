package agentmain

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const Class = "agent-main"
const pulse = "agent.main.internal.pulse"

type Config struct {
	LLMActor         string `json:"llm_actor,omitempty"`
	Model            string `json:"model,omitempty"`
	Session          string `json:"session,omitempty"`
	ContextWindow    int    `json:"context_window,omitempty"`
	ReserveTokens    int    `json:"reserve_tokens,omitempty"`
	KeepRecentTokens int    `json:"keep_recent_tokens,omitempty"`
}

func defaults() Config {
	return Config{LLMActor: "pi-llm", Session: "main", ContextWindow: 128000, ReserveTokens: 16384, KeepRecentTokens: 20000}
}
func defaultConfig() json.RawMessage {
	return json.RawMessage(`{"llm_actor":"pi-llm","session":"main","context_window":128000,"reserve_tokens":16384,"keep_recent_tokens":20000}`)
}
func parse(raw json.RawMessage) (Config, error) {
	c := defaults()
	if len(raw) > 0 {
		if err := actorbase.DecodeStrict(raw, &c); err != nil {
			return c, err
		}
	}
	if strings.TrimSpace(c.LLMActor) == "" || strings.TrimSpace(c.Session) == "" || c.ContextWindow < 4096 || c.ReserveTokens < 0 || c.KeepRecentTokens < 1 || c.ReserveTokens+c.KeepRecentTokens >= c.ContextWindow {
		return c, errors.New("agent-main: llm_actor and session required")
	}
	return c, nil
}
func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: defaultConfig, ValidateConfig: func(r json.RawMessage) error { _, e := parse(r); return e }, ConfigSchema: json.RawMessage(`{"type":"object","properties":{"llm_actor":{"type":"string"},"model":{"type":"string"},"session":{"type":"string"},"context_window":{"type":"integer","minimum":4096},"reserve_tokens":{"type":"integer","minimum":0},"keep_recent_tokens":{"type":"integer","minimum":1}},"additionalProperties":false}`)})
}
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	cfg, e := parse(spec.Config)
	if e != nil {
		return platform.ActorDecl{}, e
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) { return proc(cfg), nil }}}}, nil
}
func manifest() introspect.Manifest {
	return introspect.Manifest{Class: Class, Interfaces: []string{"actor", "agent-main"}, Words: map[string]introspect.WordSpec{
		"agent.main.merge":     {Description: "Merge one branch's unmerged closed boundaries into main.", InputSchema: json.RawMessage(`{"type":"object","required":["from_session"],"properties":{"from_session":{"type":"string"}},"additionalProperties":false}`)},
		"agent.main.merge_all": {Description: "Merge every pending manual branch.", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`)},
		"agent.main.track":     {Description: "Set main's merge policy for one branch.", InputSchema: json.RawMessage(`{"type":"object","required":["from_session","merge"],"properties":{"from_session":{"type":"string"},"merge":{"enum":["auto","manual","never"]}},"additionalProperties":false}`)},
	}}
}

func proc(cfg Config) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		if err := ensureMain(sys, cfg); err != nil {
			if !errors.Is(err, errMainContextWrite) {
				return fmt.Errorf("initialize main %s: %w", cfg.Session, err)
			}
			slog.Warn("agent-main context initialization failed", "actor", sys.Self(), "session", cfg.Session, "error", err)
		}
		if _, err := sys.After(10*time.Second, pulse, map[string]any{}, schedule.TimerHomeMemory); err != nil {
			return err
		}
		for {
			msg, e := sys.Recv()
			if e != nil {
				return e
			}
			if msg.Kind == message.KindEvent && msg.Type == pulse && msg.Sender.ID == sys.Self() {
				if err := autoMerge(sys, msg, cfg); err != nil {
					slog.Warn("agent-main auto merge failed", "actor", sys.Self(), "session", cfg.Session, "error", err)
				}
				if _, err := sys.After(10*time.Second, pulse, map[string]any{}, schedule.TimerHomeMemory); err != nil {
					return err
				}
				continue
			}
			if msg.Kind != message.KindRequest {
				continue
			}
			switch msg.Type {
			case "agent.main.track":
				track(sys, msg, cfg)
			case "agent.main.merge":
				mergeOne(sys, msg, cfg)
			case "agent.main.merge_all":
				mergeAll(sys, msg, cfg)
			default:
				_, _ = sys.Fail(msg, "type_unsupported", "unknown agent-main word")
			}
		}
	}
}

func ensureMain(sys actorbase.Sys, cfg Config) error {
	if _, found, err := agentbase.LoadContext(sys, cfg.Session); err != nil {
		return err
	} else if found {
		return nil
	}
	ledger, err := rows(sys, cfg.Session)
	if err != nil {
		return err
	}
	object, opened, err := materializeMain(ledger)
	if err != nil {
		return err
	}
	if !opened {
		id, err := emitMain(sys, "session.opened", map[string]any{"session_id": cfg.Session, "base": nil}, cfg)
		if err != nil {
			return err
		}
		object.Version = id
	}
	return writeMainContext(sys, cfg, object)
}

// materializeMain uses the ledger already read through View. A failed KV delete
// cannot make the old cache authoritative for the next merge or compaction.
func materializeMain(ledger []row) (agentbase.ContextObject, bool, error) {
	object := agentbase.ContextObject{Messages: []json.RawMessage{}}
	opened := false
	for _, item := range ledger {
		switch item.Type {
		case "session.opened":
			opened = true
		case "session.compact":
			var compact struct {
				Context []json.RawMessage `json:"context"`
			}
			if err := json.Unmarshal(item.Body, &compact); err != nil {
				return object, opened, fmt.Errorf("invalid main compact %s: %w", item.ID, err)
			}
			object.Messages = compact.Context
			object.Version = item.ID
		case "session.reset":
			object.Messages = []json.RawMessage{}
			object.Version = item.ID
		case "session.merge":
			var merge struct {
				Decision string          `json:"decision"`
				Summary  json.RawMessage `json:"summary"`
			}
			if err := json.Unmarshal(item.Body, &merge); err != nil {
				return object, opened, fmt.Errorf("invalid main merge %s: %w", item.ID, err)
			}
			if merge.Decision == "merged" && len(merge.Summary) > 0 {
				object.Messages = append(object.Messages, merge.Summary)
				object.Version = item.ID
			}
		}
	}
	return object, opened, nil
}

var errMainContextWrite = errors.New("main context write failed")

// Retrying this exact KV value does not repeat the committed merge/compact.
func writeMainContext(sys actorbase.Sys, cfg Config, object agentbase.ContextObject) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if _, err = agentbase.WriteContext(sys, cfg.Session, object); err == nil {
			return nil
		}
		slog.Warn("agent-main context write failed", "actor", sys.Self(), "session", cfg.Session, "attempt", attempt, "error", err)
	}
	if deleteErr := agentbase.DeleteContext(sys, cfg.Session); deleteErr != nil {
		slog.Error("agent-main context deletion failed", "actor", sys.Self(), "session", cfg.Session, "error", deleteErr)
		return fmt.Errorf("%w after 3 attempts: %v; delete failed: %v", errMainContextWrite, err, deleteErr)
	}
	return fmt.Errorf("%w after 3 attempts: %v; cache deleted", errMainContextWrite, err)
}

func emitMain(sys actorbase.Sys, typ string, value map[string]any, cfg Config) (message.ID, error) {
	value["session"] = cfg.Session
	spec, e := behavior.EventSpecJSON(message.Root(), typ, value)
	if e != nil {
		return "", e
	}
	return sys.Emit(spec)
}
func track(sys actorbase.Sys, msg actorbase.Msg, cfg Config) {
	var req struct {
		Session string `json:"from_session"`
		Merge   string `json:"merge"`
	}
	if actorbase.DecodeStrict(msg.Payload, &req) != nil || strings.TrimSpace(req.Session) == "" || (req.Merge != "auto" && req.Merge != "manual" && req.Merge != "never") {
		_, _ = sys.Fail(msg, "invalid_args", "session and merge policy required")
		return
	}
	_, e := emitMain(sys, "session.track", map[string]any{"session_id": cfg.Session, "from_session": req.Session, "merge": req.Merge}, cfg)
	if e != nil {
		_, _ = sys.Fail(msg, "ledger_unavailable", e.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "tracked", "session": req.Session, "merge": req.Merge})
}
func mergeOne(sys actorbase.Sys, msg actorbase.Msg, cfg Config) {
	var req struct {
		Session string `json:"from_session"`
	}
	if actorbase.DecodeStrict(msg.Payload, &req) != nil || req.Session == "" {
		_, _ = sys.Fail(msg, "invalid_args", "session required")
		return
	}
	merged, e := mergeBranch(sys, cfg, req.Session, "manual")
	if e != nil {
		_, _ = sys.Fail(msg, "merge_failed", e.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": map[bool]string{true: "merged", false: "already_up_to_date"}[merged], "session": req.Session})
}
func mergeAll(sys actorbase.Sys, msg actorbase.Msg, cfg Config) {
	branches, archived, err := relations(sys, cfg)
	if err != nil {
		_, _ = sys.Fail(msg, "merge_failed", err.Error(), map[string]any{"count": 0})
		return
	}
	n := 0
	var pending []string
	for b, p := range branches {
		if p == "manual" && archived[b] {
			pending = append(pending, b)
		}
	}
	sort.Strings(pending)
	for _, b := range pending {
		ok, err := mergeBranch(sys, cfg, b, "manual")
		if err != nil {
			_, _ = sys.Fail(msg, "merge_failed", err.Error(), map[string]any{"count": n, "from_session": b})
			return
		}
		if ok {
			n++
		}
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "merged", "count": n})
}
func autoMerge(sys actorbase.Sys, _ actorbase.Msg, cfg Config) error {
	branches, archived, err := relations(sys, cfg)
	if err != nil {
		return err
	}
	for b := range archived {
		if branches[b] == "auto" || branches[b] == "" {
			if _, err := mergeBranch(sys, cfg, b, "auto"); err != nil {
				return fmt.Errorf("merge branch %s: %w", b, err)
			}
		}
	}
	return nil
}

type row struct {
	Seq    int64
	ID     message.ID
	Parent message.ID
	Kind   message.Kind
	Type   string
	Body   json.RawMessage
	Sender actor.ActorID
	To     message.Audience
}

func rows(sys actorbase.Sys, session string) ([]row, error) {
	return queryRows(sys, session)
}

func queryRows(sys actorbase.Sys, session string) ([]row, error) {
	snapshot, err := sys.View().Read(sys.Life(), actorcaps.LedgerRead{Session: session})
	if err != nil {
		return nil, err
	}
	var out []row
	for _, r := range snapshot.Rows {
		m := r.Envelope
		_, body, err := harness.UnwrapPayload(m.Payload)
		if err != nil {
			return nil, fmt.Errorf("invalid ledger row %s: %w", m.ID, err)
		}
		out = append(out, row{Seq: r.Seq, ID: m.ID, Parent: m.ParentID, Kind: m.Kind, Type: m.Type, Body: body, Sender: m.Sender.ID, To: m.Audience})
	}
	return out, nil
}

func relations(sys actorbase.Sys, cfg Config) (map[string]string, map[string]bool, error) {
	mainRows, err := rows(sys, cfg.Session)
	if err != nil {
		return nil, nil, err
	}
	policies := map[string]string{}
	for _, item := range mainRows {
		if item.Type != "session.track" {
			continue
		}
		var track struct {
			From  string `json:"from_session"`
			Merge string `json:"merge"`
		}
		_ = json.Unmarshal(item.Body, &track)
		if track.From != "" {
			policies[track.From] = track.Merge
		}
	}
	archived := map[string]bool{}
	commands, err := queryRows(sys, "")
	if err != nil {
		return nil, nil, err
	}
	lastCommand := map[string]row{}
	controllers := controllerSeats(commands)
	for _, item := range commands {
		if item.Kind != message.KindRequest || (item.Type != "loop.start" && item.Type != "loop.input" && item.Type != "loop.stop" && item.Type != "loop.reset" && item.Type != "loop.sync" && item.Type != "loop.rename") {
			continue
		}
		var stop struct {
			Session string `json:"session_id"`
			Archive bool   `json:"archive"`
		}
		_ = json.Unmarshal(item.Body, &stop)
		if stop.Session != "" && len(item.To) > 0 && sameSeatMain(controllers[stop.Session], item.Sender.String()) {
			lastCommand[stop.Session] = item
		}
	}
	for session, item := range lastCommand {
		if item.Type != "loop.stop" {
			continue
		}
		var stop struct {
			Archive bool `json:"archive"`
		}
		_ = json.Unmarshal(item.Body, &stop)
		if !stop.Archive {
			continue
		}
		archived[session] = true
		if _, tracked := policies[session]; !tracked {
			policies[session] = "auto"
		}
	}
	return policies, archived, nil
}

func sameSeatMain(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	a, b := strings.Split(left, ":"), strings.Split(right, ":")
	return len(a) == 3 && len(b) == 3 && a[0] == b[0] && a[1] == b[1]
}

func closedBoundaries(items []row) []row {
	controllers := controllerSeats(items)
	controller, holder := "", ""
	for _, seat := range controllers {
		controller = seat
		break
	}
	turns := map[string]bool{}
	started := map[string]bool{}
	startTurns := map[message.ID]string{}
	accepted := map[string]bool{}
	var boundaries []row
	for _, item := range items {
		if item.Kind == message.KindRequest && (item.Type == "loop.start" || item.Type == "loop.input" || item.Type == "loop.stop" || item.Type == "loop.reset" || item.Type == "loop.sync" || item.Type == "loop.rename") && len(item.To) > 0 {
			if sameSeatMain(controller, item.Sender.String()) {
				holder = item.To[0].String()
				if item.Type == "loop.start" {
					var start struct {
						Turn string `json:"turn_id"`
					}
					_ = json.Unmarshal(item.Body, &start)
					if start.Turn != "" {
						started[start.Turn] = true
						startTurns[item.ID] = start.Turn
					}
				}
			}
		}
		if item.Kind == message.KindResponse {
			var ack struct{ Status, Disposition string }
			if json.Unmarshal(item.Body, &ack) == nil && (ack.Status == "" || ack.Status == "completed") && (ack.Disposition == "accepted" || ack.Disposition == "already_accepted") {
				if turn := startTurns[item.Parent]; turn != "" {
					accepted[turn] = true
				}
			}
		}
		if item.Kind != message.KindRequest || item.Type != "loop.report" || item.Sender.String() != holder {
			continue
		}
		var report struct {
			Turn  string `json:"turn_id"`
			State string `json:"state"`
		}
		_ = json.Unmarshal(item.Body, &report)
		terminal := report.State == "completed" || report.State == "failed" || report.State == "cancelled" || report.State == "timeout" || report.State == "execution_unknown"
		if report.Turn != "" && started[report.Turn] && accepted[report.Turn] && terminal && !turns[report.Turn] {
			turns[report.Turn] = true
			boundaries = append(boundaries, item)
		}
	}
	return boundaries
}

func controllerSeats(items []row) map[string]string {
	type start struct {
		session string
		sender  string
	}
	starts := map[message.ID]start{}
	controllers := map[string]string{}
	for _, item := range items {
		if item.Kind == message.KindRequest && item.Type == "loop.start" {
			var req struct {
				Session string `json:"session_id"`
			}
			_ = json.Unmarshal(item.Body, &req)
			if req.Session != "" {
				starts[item.ID] = start{session: req.Session, sender: item.Sender.String()}
			}
			continue
		}
		if item.Kind != message.KindResponse {
			continue
		}
		candidate, ok := starts[item.Parent]
		if !ok || controllers[candidate.session] != "" {
			continue
		}
		var ack struct {
			Disposition string `json:"disposition"`
		}
		_ = json.Unmarshal(item.Body, &ack)
		if ack.Disposition == "accepted" || ack.Disposition == "already_accepted" {
			controllers[candidate.session] = candidate.sender
		}
	}
	return controllers
}

func mergeBranch(sys actorbase.Sys, cfg Config, branch, mode string) (bool, error) {
	br, e := rows(sys, branch)
	if e != nil {
		return false, e
	}
	main, e := rows(sys, cfg.Session)
	if e != nil {
		return false, e
	}
	object, _, e := materializeMain(main)
	if e != nil {
		return false, e
	}
	merged := map[string]bool{}
	policy := "auto"
	var parent message.ID
	for _, r := range main {
		parent = r.ID
		if r.Type == "session.track" {
			var x struct {
				From, Merge string `json:"-"`
			}
			var raw struct {
				From  string `json:"from_session"`
				Merge string `json:"merge"`
			}
			_ = json.Unmarshal(r.Body, &raw)
			x.From, x.Merge = raw.From, raw.Merge
			if x.From == branch {
				policy = x.Merge
			}
		}
		if r.Type == "session.merge" || r.Type == "session.compact" {
			var x struct {
				Refs []string `json:"refs"`
			}
			_ = json.Unmarshal(r.Body, &x)
			for _, id := range x.Refs {
				merged[id] = true
			}
		}
	}
	var refs []string
	var lastMerged, through int64
	for _, r := range br {
		if merged[string(r.ID)] && r.Seq > lastMerged {
			lastMerged = r.Seq
		}
	}
	var texts []string
	for _, r := range closedBoundaries(br) {
		if !merged[string(r.ID)] {
			refs = append(refs, string(r.ID))
			through = r.Seq
		}
	}
	if len(refs) == 0 {
		return false, writeMainContext(sys, cfg, object)
	}
	if policy == "never" {
		_, e := emitMain(sys, "session.merge", map[string]any{"session_id": cfg.Session, "parent": parent, "from_session": branch, "boundaries": refs, "decision": "skipped", "summary": nil, "refs": refs, "mode": mode}, cfg)
		return false, e
	}
	for _, r := range br {
		if r.Seq <= lastMerged || r.Seq > through {
			continue
		}
		switch {
		case r.Kind == message.KindRequest && r.Type == "agent.ask":
			texts = append(texts, "user: "+string(r.Body))
		case r.Kind == message.KindResponse && r.Type == llmproto.TypeGenerate:
			var generated llmproto.GenerateResponse
			if json.Unmarshal(r.Body, &generated) == nil && len(generated.Message) > 0 {
				texts = append(texts, "assistant: "+string(generated.Message))
			}
		case r.Kind == message.KindResponse:
			texts = append(texts, "tool response pointer: "+string(r.ID))
		}
	}
	summary, e := summarize(sys, cfg, branch, texts)
	if e != nil {
		return false, e
	}
	before, e := countObject(sys, cfg, object.Messages)
	if e != nil {
		return false, e
	}
	candidate := append(append([]json.RawMessage(nil), object.Messages...), json.RawMessage(summary))
	after, e := countObject(sys, cfg, candidate)
	if e != nil {
		return false, e
	}
	id, e := emitMain(sys, "session.merge", map[string]any{"session_id": cfg.Session, "parent": parent, "from_session": branch, "boundaries": refs, "decision": "merged", "summary": summary, "refs": refs, "mode": mode, "size": map[string]int{"before": before, "after": after}}, cfg)
	if e != nil {
		return false, e
	}
	object.Messages = candidate
	object.Version = id
	e = writeMainContext(sys, cfg, object)
	if e == nil {
		e = compactMain(sys, cfg)
	}
	return e == nil, e
}

func countObject(sys actorbase.Sys, cfg Config, messages []json.RawMessage) (int, error) {
	temp := "main-count-" + uuid.NewString()
	ref, err := agentbase.WriteContext(sys, temp, agentbase.ContextObject{Messages: append([]json.RawMessage(nil), messages...), Version: message.ID(uuid.NewString())})
	if err != nil {
		return 0, err
	}
	defer agentbase.DeleteContext(sys, temp)
	pd, err := sys.Call(message.Root(), actor.ActorID(cfg.LLMActor), llmproto.TypeCount, llmproto.CountRequest{RootSession: cfg.Session, Context: ref})
	if err != nil {
		return 0, err
	}
	response, err := pd.Wait(sys.Life(), 0)
	if err != nil {
		return 0, err
	}
	var counted llmproto.CountResponse
	if json.Unmarshal(response.Payload, &counted) != nil {
		return 0, errors.New("invalid llm.count response")
	}
	return counted.ContextTokens, nil
}
func summarize(sys actorbase.Sys, cfg Config, branch string, texts []string) (json.RawMessage, error) {
	content := "Summarize branch " + branch + " for the main long-term context. Preserve goals, constraints, decisions, progress and next steps.\n" + strings.Join(texts, "\n")
	msg, _ := json.Marshal(map[string]any{"role": "user", "content": []map[string]any{{"type": "text", "text": content}}})
	session := "main-merge-" + uuid.NewString()
	ref, e := agentbase.WriteContext(sys, session, agentbase.ContextObject{Messages: []json.RawMessage{msg}, Version: message.ID(uuid.NewString())})
	if e != nil {
		return nil, e
	}
	defer agentbase.DeleteContext(sys, session)
	pd, e := sys.Call(message.Root(), actor.ActorID(cfg.LLMActor), llmproto.TypeGenerate, llmproto.GenerateRequest{RootSession: cfg.Session, ModelRef: parseModel(cfg.Model), Context: ref})
	if e != nil {
		return nil, e
	}
	res, e := pd.Wait(sys.Life(), 0)
	if e != nil {
		return nil, e
	}
	var generated llmproto.GenerateResponse
	if json.Unmarshal(res.Payload, &generated) != nil || len(generated.Message) == 0 {
		return nil, fmt.Errorf("invalid merge summary")
	}
	return generated.Message, nil
}

func compactMain(sys actorbase.Sys, cfg Config) error {
	mainRows, err := rows(sys, cfg.Session)
	if err != nil {
		return err
	}
	object, _, err := materializeMain(mainRows)
	if err != nil {
		return err
	}
	if err := writeMainContext(sys, cfg, object); err != nil {
		return err
	}
	if len(object.Messages) < 2 {
		return nil
	}
	pd, err := sys.Call(message.Root(), actor.ActorID(cfg.LLMActor), llmproto.TypeCount, llmproto.CountRequest{RootSession: cfg.Session, Context: agentbase.ContextRef{Resource: mustContextResource(cfg.Session), Version: object.Version}})
	if err != nil {
		return err
	}
	counted, err := pd.Wait(sys.Life(), 0)
	if err != nil {
		return err
	}
	var count struct {
		ContextTokens int `json:"context_tokens"`
	}
	if json.Unmarshal(counted.Payload, &count) != nil || count.ContextTokens <= cfg.ContextWindow-cfg.ReserveTokens {
		return nil
	}
	cut, kept := len(object.Messages), 0
	for i := len(object.Messages) - 1; i >= 0; i-- {
		kept += (len(object.Messages[i]) + 3) / 4
		if kept > cfg.KeepRecentTokens {
			break
		}
		cut = i
	}
	if cut <= 0 || cut >= len(object.Messages) {
		return errors.New("main context has no safe compact prefix")
	}
	temp := "main-compact-" + uuid.NewString()
	ref, err := agentbase.WriteContext(sys, temp, agentbase.ContextObject{Messages: append([]json.RawMessage(nil), object.Messages[:cut]...), Version: message.ID(uuid.NewString())})
	if err != nil {
		return err
	}
	defer agentbase.DeleteContext(sys, temp)
	pd, err = sys.Call(message.Root(), actor.ActorID(cfg.LLMActor), llmproto.TypeGenerate, llmproto.GenerateRequest{RootSession: cfg.Session, ModelRef: parseModel(cfg.Model), Purpose: "compact", Context: ref, SystemPrompt: "Summarize this main context. Preserve goals, constraints, decisions, progress, unresolved risks and next steps."})
	if err != nil {
		return err
	}
	generatedMsg, err := pd.Wait(sys.Life(), 0)
	if err != nil {
		return err
	}
	var generated llmproto.GenerateResponse
	if json.Unmarshal(generatedMsg.Payload, &generated) != nil || len(generated.Message) == 0 {
		return errors.New("invalid main compact summary")
	}
	compacted := append([]json.RawMessage{generated.Message}, object.Messages[cut:]...)
	refSet := map[string]bool{}
	for _, item := range mainRows {
		if item.Type != "session.merge" && item.Type != "session.compact" {
			continue
		}
		var x struct {
			Refs []string `json:"refs"`
		}
		_ = json.Unmarshal(item.Body, &x)
		for _, id := range x.Refs {
			refSet[id] = true
		}
	}
	refs := make([]string, 0, len(refSet))
	for id := range refSet {
		refs = append(refs, id)
	}
	sort.Strings(refs)
	after, err := countObject(sys, cfg, compacted)
	if err != nil {
		return err
	}
	id, err := emitMain(sys, "session.compact", map[string]any{"session_id": cfg.Session, "context": compacted, "refs": refs, "tokens_before": count.ContextTokens, "size": map[string]int{"before": count.ContextTokens, "after": after}}, cfg)
	if err != nil {
		return err
	}
	return writeMainContext(sys, cfg, agentbase.ContextObject{Messages: compacted, Version: id})
}

func mustContextResource(session string) resource.ResourceID {
	id, _ := agentbase.ContextResource(session)
	return id
}

func parseModel(v string) llmproto.ModelRef {
	p := strings.SplitN(v, "/", 2)
	if len(p) == 2 {
		return llmproto.ModelRef{Provider: p[0], Model: p[1]}
	}
	return llmproto.ModelRef{Model: v}
}
