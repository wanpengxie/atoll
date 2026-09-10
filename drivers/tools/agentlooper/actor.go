package agentlooper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
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

const Class = "agent-looper"
const maxResultTextBytes = 64 << 10
const channelCallWord = "channel.call"

const (
	defaultToolResultMaxLines = 2000
	defaultToolResultMaxBytes = 50 << 10
	defaultToolImageMaxBytes  = 3 << 20
)

var validToolName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)

type resolvedTool struct {
	Name       string
	Actor      string
	Word       string
	Definition json.RawMessage
}

type Config struct {
	ControllerActor string `json:"controller_actor,omitempty"`
	LLMActor        string `json:"llm_actor,omitempty"`
	WorkspaceActor  string `json:"workspace_actor,omitempty"`
	HostActor       string `json:"host_actor,omitempty"`
	MaxAssignments  int    `json:"max_assignments,omitempty"`
}

func defaultConfig() json.RawMessage {
	return json.RawMessage(`{"controller_actor":"native-agent","llm_actor":"pi-llm","workspace_actor":"pi-workspace","max_assignments":32}`)
}
func parseConfig(raw json.RawMessage) (Config, error) {
	cfg := Config{ControllerActor: "native-agent", LLMActor: "pi-llm", WorkspaceActor: "pi-workspace", MaxAssignments: 32}
	if len(raw) > 0 {
		if err := actorbase.DecodeStrict(raw, &cfg); err != nil {
			return Config{}, err
		}
	}
	if cfg.MaxAssignments < 1 || cfg.MaxAssignments > 10000 {
		return Config{}, errors.New("agent-looper config: max_assignments must be 1..10000")
	}
	if strings.TrimSpace(cfg.ControllerActor) == "" {
		return Config{}, errors.New("agent-looper config: controller_actor is required")
	}
	if strings.TrimSpace(cfg.LLMActor) == "" {
		return Config{}, errors.New("agent-looper config: llm_actor is required")
	}
	return cfg, nil
}
func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct, DefaultConfig: defaultConfig, ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err }, ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"controller_actor":{"type":"string","minLength":1},"llm_actor":{"type":"string","minLength":1},"workspace_actor":{"type":"string"},"host_actor":{"type":"string"},"max_assignments":{"type":"integer","minimum":1,"maximum":10000}}}`)})
}
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) { return proc(cfg), nil }}}}, nil
}
func manifest() introspect.Manifest {
	return introspect.Manifest{Class: Class, Interfaces: []string{"actor", "agent-loop"}, Capabilities: map[string]bool{"multi_assignment": true, "independent_assignment": true, "targeted_stop": true}, Words: map[string]introspect.WordSpec{
		agentloop.TypeStart:   {Description: "Accept one of a bounded set of independently-lived Agent episode assignments from the configured Controller and return before its LLM/tool loop completes.", InputSchema: json.RawMessage(agentloop.StartInputSchema), OutputSchema: json.RawMessage(agentloop.AckOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "capacity", "busy", "assignment_conflict", "session_context_unavailable"}},
		agentloop.TypeInput:   {Description: "Deliver one already accepted input from the configured Controller to the matching assignment; it is adopted only at a later safe context boundary.", InputSchema: json.RawMessage(agentloop.InputInputSchema), OutputSchema: json.RawMessage(agentloop.AckOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "assignment_not_found", "operation_mismatch"}},
		agentloop.TypeStop:    {Description: "Stop only the matching assignment; a stale assignment id cannot cancel its successor.", InputSchema: json.RawMessage(agentloop.StopInputSchema), OutputSchema: json.RawMessage(agentloop.AckOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "assignment_not_found", "operation_mismatch"}},
		agentloop.TypeReset:   {Description: "Reset an idle session context.", InputSchema: json.RawMessage(`{"type":"object","required":["session_id"],"properties":{"session_id":{"type":"string"}},"additionalProperties":false}`)},
		agentloop.TypeSync:    {Description: "Append a closed source session snapshot to an idle session.", InputSchema: json.RawMessage(`{"type":"object","required":["session_id","from"],"properties":{"session_id":{"type":"string"},"from":{"type":"object"}},"additionalProperties":false}`)},
		agentloop.TypeRename:  {Description: "Rename a session.", InputSchema: json.RawMessage(`{"type":"object","required":["session_id","name"],"properties":{"session_id":{"type":"string"},"name":{"type":"string"}},"additionalProperties":false}`)},
		agentloop.TypeInspect: {Description: "Inspect process-local assignment activity. Controller work state remains the external authority.", InputSchema: json.RawMessage(agentloop.InspectInputSchema), OutputSchema: json.RawMessage(agentloop.InspectOutputSchema), ErrorCodes: []string{"invalid_args", "permission_denied", "assignment_not_found"}},
	}}
}

type assignment struct {
	start    agentloop.StartRequest
	cause    message.Cause
	cancel   context.CancelFunc
	mu       sync.Mutex
	inputs   []agentloop.Input
	phase    string
	history  []json.RawMessage
	closed   bool
	consumed int
	controls []agentloop.ControlResult
	version  message.ID
	acks     []pendingInputAck
	archive  bool
}
type pendingInputAck struct {
	msg      actorbase.Msg
	decision agentloop.ControlResult
}
type looper struct {
	cfg           Config
	mu            sync.Mutex
	active        map[string]*assignment
	finished      map[string]*assignment
	finishedOrder []string
	wg            sync.WaitGroup
}

func proc(cfg Config) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		life, stop := context.WithCancel(sys.Life())
		sys = looperLifeSys{Sys: sys, life: life}
		l := &looper{cfg: cfg, active: map[string]*assignment{}}
		defer func() { stop(); l.wg.Wait() }()
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			if msg.Kind != message.KindRequest {
				continue
			}
			switch msg.Type {
			case agentloop.TypeStart:
				l.start(sys, msg)
			case agentloop.TypeInput:
				l.input(sys, msg)
			case agentloop.TypeStop:
				l.stop(sys, msg)
			case agentloop.TypeReset:
				l.reset(sys, msg)
			case agentloop.TypeSync:
				l.syncSession(sys, msg)
			case agentloop.TypeRename:
				l.rename(sys, msg)
			case agentloop.TypeInspect:
				l.inspect(sys, msg)
			default:
				_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("Agent looper does not answer %q", msg.Type))
			}
		}
	}
}

func (l *looper) activeSession(session string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, a := range l.active {
		if a.start.SessionID == session {
			return true
		}
	}
	return false
}

type ledgerTurnState struct {
	start            ledgerRow
	accepted, closed bool
	boundary         ledgerRow
}

// sessionTurnStates evaluates holder and report validity at each row, rather
// than comparing old reports with the session's final holder.
func sessionTurnStates(rows []ledgerRow) map[string]*ledgerTurnState {
	turns := map[string]*ledgerTurnState{}
	startTurn := map[message.ID]string{}
	controller, holder := controllerSeat(rows), ""
	for _, row := range rows {
		if row.Kind == message.KindRequest && isLoopCommand(row.Type) && len(row.Audience) > 0 {
			if controller != "" && sameActorSeat(controller, row.Sender.String()) {
				holder = row.Audience[0].String()
			}
		}
		if row.Kind == message.KindRequest && row.Type == agentloop.TypeStart {
			var start agentloop.StartRequest
			if json.Unmarshal(row.Body, &start) == nil && start.TurnID != "" {
				state := turns[start.TurnID]
				if state == nil {
					state = &ledgerTurnState{}
					turns[start.TurnID] = state
				}
				if state.start.ID == "" {
					state.start = row
				}
				startTurn[row.ID] = start.TurnID
			}
		}
		if row.Kind == message.KindResponse {
			turn := startTurn[row.Parent]
			state := turns[turn]
			if state != nil {
				var ack struct {
					Disposition string `json:"disposition"`
					Status      string `json:"status"`
				}
				_ = json.Unmarshal(row.Body, &ack)
				if (ack.Status == "" || ack.Status == "completed") && (ack.Disposition == "accepted" || ack.Disposition == "already_accepted") {
					state.accepted = true
				}
			}
		}
		if row.Kind == message.KindRequest && row.Type == agentloop.TypeReport && row.Sender.String() == holder {
			var report agentloop.ReportRequest
			if json.Unmarshal(row.Body, &report) == nil && terminalTurnState(report.State) {
				if state := turns[report.TurnID]; state != nil && state.accepted {
					if !state.closed {
						state.boundary = row
					}
					state.closed = true
				}
			}
		}
	}
	return turns
}

// sessionHolder projects the sender authorized to close historical turns.
// The first start identifies the Controller seat; later incarnations with the
// same kind/declaration remain that Controller, while unrelated senders cannot
// steal ownership by writing a loop-shaped request.
func sessionHolder(rows []ledgerRow) string {
	controller := controllerSeat(rows)
	holder := ""
	for _, row := range rows {
		if row.Kind != message.KindRequest || !isLoopCommand(row.Type) || len(row.Audience) == 0 {
			continue
		}
		if controller != "" && sameActorSeat(controller, row.Sender.String()) {
			holder = row.Audience[0].String()
		}
	}
	return holder
}

func controllerSeat(rows []ledgerRow) string {
	starts := map[message.ID]actor.ActorID{}
	for _, row := range rows {
		if row.Kind == message.KindRequest && row.Type == agentloop.TypeStart {
			starts[row.ID] = row.Sender
			continue
		}
		if row.Kind != message.KindResponse {
			continue
		}
		sender := starts[row.Parent]
		if sender == "" {
			continue
		}
		var ack struct {
			Disposition string `json:"disposition"`
		}
		_ = json.Unmarshal(row.Body, &ack)
		if ack.Disposition == "accepted" || ack.Disposition == "already_accepted" {
			return sender.String()
		}
	}
	return ""
}

func sameActorSeat(left, right string) bool {
	a, b := strings.Split(left, ":"), strings.Split(right, ":")
	return len(a) == 3 && len(b) == 3 && a[0] == b[0] && a[1] == b[1]
}

func isLoopCommand(typ string) bool {
	switch typ {
	case agentloop.TypeStart, agentloop.TypeInput, agentloop.TypeStop, agentloop.TypeReset, agentloop.TypeSync, agentloop.TypeRename:
		return true
	}
	return false
}
func terminalTurnState(state string) bool {
	switch state {
	case "completed", "failed", "cancelled", "timeout", "execution_unknown":
		return true
	}
	return false
}
func (l *looper) reset(sys actorbase.Sys, msg actorbase.Msg) {
	var req agentloop.ResetRequest
	if actorbase.DecodeStrict(msg.Payload, &req) != nil || req.SessionID == "" || req.SessionID != msg.Context().Session {
		_, _ = sys.Fail(msg, "invalid_args", "session_id must match message context")
		return
	}
	if l.activeSession(req.SessionID) {
		_, _ = sys.Fail(msg, "busy", "session has an active turn")
		return
	}
	spec, _ := behavior.EventSpecJSON(message.Anchored(msg.ID, msg.ID), agentloop.TypeSessionReset, req)
	id, err := sys.Emit(spec)
	if err != nil {
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	_, err = agentbase.WriteContext(sys, req.SessionID, agentbase.ContextObject{Messages: []json.RawMessage{}, Version: id})
	if err != nil {
		_, _ = sys.Fail(msg, "context_failed", err.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "reset", "session_id": req.SessionID})
}

func (l *looper) rename(sys actorbase.Sys, msg actorbase.Msg) {
	var req agentloop.RenameRequest
	if actorbase.DecodeStrict(msg.Payload, &req) != nil || req.SessionID == "" || req.SessionID != msg.Context().Session || strings.TrimSpace(req.Name) == "" {
		_, _ = sys.Fail(msg, "invalid_args", "session_id and name are required")
		return
	}
	spec, _ := behavior.EventSpecJSON(message.Anchored(msg.ID, msg.ID), agentloop.TypeSessionRename, req)
	if _, err := sys.Emit(spec); err != nil {
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "renamed", "session_id": req.SessionID, "name": req.Name})
}

func (l *looper) syncSession(sys actorbase.Sys, msg actorbase.Msg) {
	var req agentloop.SyncRequest
	if actorbase.DecodeStrict(msg.Payload, &req) != nil || req.SessionID == "" || req.From.Session == "" || req.From.Through == "" || req.SessionID != msg.Context().Session {
		_, _ = sys.Fail(msg, "invalid_args", "session_id and a closed from range are required")
		return
	}
	if l.activeSession(req.SessionID) {
		_, _ = sys.Fail(msg, "busy", "session has an active turn")
		return
	}
	historyCtx, historyCancel := newHistoryContext(msg.Ctx())
	defer historyCancel()
	valid, err := validSessionBoundary(historyCtx, sys, message.Anchored(msg.ID, msg.ID), agentloop.BoundaryRef{Session: req.From.Session, At: req.From.Through})
	if err != nil || !valid {
		_, _ = sys.Fail(msg, "invalid_args", "source through is not a boundary")
		return
	}
	target, _, err := sessionContext(historyCtx, sys, message.Anchored(msg.ID, msg.ID), req.SessionID)
	if err != nil {
		failSessionContext(sys, msg, err)
		return
	}
	source, err := materializeSession(historyCtx, sys, message.Anchored(msg.ID, msg.ID), req.From.Session, message.ID(req.From.Through), nil)
	if err != nil {
		failSessionContext(sys, msg, err)
		return
	}
	delta := source.Messages
	if req.From.After != "" {
		valid, err = validSessionBoundary(historyCtx, sys, message.Anchored(msg.ID, msg.ID), agentloop.BoundaryRef{Session: req.From.Session, At: req.From.After})
		if err != nil || !valid {
			_, _ = sys.Fail(msg, "invalid_args", "source after is not a boundary")
			return
		}
		previous, err := materializeSession(historyCtx, sys, message.Anchored(msg.ID, msg.ID), req.From.Session, message.ID(req.From.After), nil)
		if err != nil || len(previous.Messages) > len(source.Messages) || !messagePrefix(previous.Messages, source.Messages) {
			_, _ = sys.Fail(msg, "invalid_args", "source range does not extend its after boundary")
			return
		}
		delta = source.Messages[len(previous.Messages):]
	}
	target.Messages = append(target.Messages, delta...)
	if err := validateSessionContext(target); err != nil {
		failSessionContext(sys, msg, err)
		return
	}
	row := agentloop.Synced{SessionID: req.SessionID, Context: target.Messages, TokensBefore: agentbase.ContextTokens(target.Messages)}
	row.From.Session, row.From.After, row.From.Through = req.From.Session, req.From.After, req.From.Through
	spec, _ := behavior.EventSpecJSON(message.Anchored(msg.ID, msg.ID), agentloop.TypeSessionSync, row)
	id, err := sys.Emit(spec)
	if err != nil {
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	target.Version = id
	if _, err = agentbase.WriteContext(sys, req.SessionID, target); err != nil {
		_, _ = sys.Fail(msg, "context_failed", err.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "synced", "session_id": req.SessionID, "through": req.From.Through})
}

func messagePrefix(prefix, whole []json.RawMessage) bool {
	for i := range prefix {
		if string(prefix[i]) != string(whole[i]) {
			return false
		}
	}
	return true
}

func (l *looper) start(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may start an assignment")
		return
	}
	var req agentloop.StartRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	req.ControllerActor = msg.Sender.ID.String()
	req.LLMActor = l.cfg.LLMActor
	req.WorkspaceActor = l.cfg.WorkspaceActor
	req.HostActor = l.cfg.HostActor
	if req.TurnID == "" {
		req.TurnID = req.AssignmentID
	}
	if req.AssignmentID == "" {
		req.AssignmentID = req.TurnID
	}
	if req.SessionID == "" {
		req.SessionID = msg.Context().Session
	}
	if req.SessionID == "" || req.TurnID == "" || req.ControllerActor == "" || req.LLMActor == "" || len(req.Inputs) == 0 {
		_, _ = sys.Fail(msg, "invalid_args", "session_id, turn_id, controller_actor, llm_actor, and inputs are required")
		return
	}
	if msg.Context().Session != req.SessionID {
		_, _ = sys.Fail(msg, "operation_mismatch", "loop.start session differs from _context.session")
		return
	}
	if req.ControllerActor != msg.Sender.ID.String() {
		_, _ = sys.Fail(msg, "invalid_args", "controller_actor must be the actual request sender")
		return
	}
	l.mu.Lock()
	old := l.active[req.AssignmentID]
	if old == nil {
		old = l.finished[req.AssignmentID]
	}
	if old != nil {
		same := string(mustJSON(old.start)) == string(mustJSON(req))
		l.mu.Unlock()
		if !same {
			_, _ = sys.Fail(msg, "assignment_conflict", "assignment id reused with different request")
			return
		}
		old.mu.Lock()
		closed := old.closed
		old.mu.Unlock()
		disposition := "already_accepted"
		if closed {
			disposition = "already_finished"
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": disposition, "assignment_id": req.AssignmentID})
		return
	}

	for _, running := range l.active {
		if running.start.SessionID == req.SessionID {
			l.mu.Unlock()
			_, _ = sys.Fail(msg, "busy", "session already has an active turn")
			return
		}
	}
	if len(l.active) >= l.cfg.MaxAssignments {
		l.mu.Unlock()
		_, _ = sys.Fail(msg, "capacity", "looper has reached max_assignments")
		return
	}
	if req.ToolTimeoutMS < 0 || req.ExecutionTimeoutMS < 0 {
		l.mu.Unlock()
		_, _ = sys.Fail(msg, "invalid_args", "timeouts must be positive when configured")
		return
	}
	duration := 30 * time.Minute
	if req.ExecutionTimeoutMS > 0 {
		duration = time.Duration(req.ExecutionTimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(sys.Life(), duration)
	historyCtx, historyCancel := newHistoryContext(msg.Ctx())
	defer historyCancel()
	object, exists, historyErr := sessionContext(historyCtx, sys, msg.Cause(), req.SessionID)
	if historyErr == nil {
		// Local duplicate starts were handled above. Reusing an old turn ID would
		// make future ledger pairing ambiguous; reject it without restoring a table.
		for _, row := range historyCtx.Value(historyBudgetKey{}).(*historyBudget).cache[req.SessionID] {
			if row.Type != agentloop.TypeStart || row.Kind != message.KindRequest || row.ID == msg.ID {
				continue
			}
			var prior agentloop.StartRequest
			if json.Unmarshal(row.Body, &prior) == nil && prior.TurnID == req.TurnID {
				historyErr = errors.New("turn_id already belongs to historical execution")
				break
			}
		}
	}
	if historyErr == nil && !exists && req.Open != nil && req.Open.Base != nil {
		var valid bool
		valid, historyErr = validSessionBoundary(historyCtx, sys, msg.Cause(), *req.Open.Base)
		if historyErr == nil && !valid {
			historyErr = errors.New("open base is not a committed boundary")
		}
		if historyErr == nil {
			object, historyErr = materializeSession(historyCtx, sys, msg.Cause(), req.Open.Base.Session, message.ID(req.Open.Base.At), nil)
		}
	}
	if historyErr == nil && !exists && req.Open == nil {
		historyErr = errors.New("session has no opening boundary")
	}
	if historyErr == nil {
		historyErr = validateSessionContext(object)
	}
	if historyErr != nil {
		l.mu.Unlock()
		cancel()
		failSessionContext(sys, msg, historyErr)
		return
	}
	turnCause := message.Anchored(msg.ID, msg.ID)
	a := &assignment{start: req, cause: turnCause, cancel: cancel, inputs: append([]agentloop.Input(nil), req.Inputs...), phase: "accepted", history: append([]json.RawMessage(nil), object.Messages...), version: object.Version}
	l.active[req.AssignmentID] = a
	l.mu.Unlock()
	if !exists {
		opened, _ := behavior.EventSpecJSON(turnCause, agentloop.TypeSessionOpened, agentloop.Opened{SessionID: req.SessionID, Base: func() *agentloop.BoundaryRef {
			if req.Open != nil {
				return req.Open.Base
			}
			return nil
		}()})
		if _, err := sys.Emit(opened); err != nil {
			l.mu.Lock()
			delete(l.active, req.AssignmentID)
			l.mu.Unlock()
			cancel()
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
	}
	if _, err := sys.Reply(msg, map[string]any{"disposition": "accepted", "session_id": req.SessionID, "turn_id": req.TurnID}); err != nil {
		l.mu.Lock()
		delete(l.active, req.AssignmentID)
		l.mu.Unlock()
		cancel()
		return
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer cancel()
		l.drive(ctx, sys, a)
		a.mu.Lock()
		archive := a.archive
		a.mu.Unlock()
		if archive && sys.Life().Err() == nil {
			_ = agentbase.DeleteContext(sys, a.start.SessionID)
		}
		l.mu.Lock()
		if l.active[req.AssignmentID] == a {
			delete(l.active, req.AssignmentID)
		}
		l.mu.Unlock()
	}()
}

func (l *looper) input(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may deliver input")
		return
	}
	var req agentloop.InputRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	l.mu.Lock()
	a := l.active[req.AssignmentID]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Fail(msg, "assignment_not_found", "no active assignment for work")
		return
	}
	a.mu.Lock()
	var nextSeq int64
	for _, input := range a.inputs {
		if input.Seq > nextSeq {
			nextSeq = input.Seq
		}
	}
	for i := range req.Inputs {
		nextSeq++
		req.Inputs[i].Seq = nextSeq
	}
	a.mu.Unlock()
	if a.start.ControllerActor != msg.Sender.ID.String() || a.start.SessionID != req.SessionID {
		_, _ = sys.Fail(msg, "operation_mismatch", "input targets a stale assignment")
		return
	}
	decision, err := a.acceptInput(req)
	if err != nil {
		_, _ = sys.Fail(msg, "operation_conflict", err.Error())
		return
	}
	a.mu.Lock()
	if decision.Disposition == "accepted" {
		a.acks = append(a.acks, pendingInputAck{msg: msg, decision: decision})
	}
	a.mu.Unlock()
	if decision.Disposition != "accepted" {
		_, _ = sys.Reply(msg, decision)
	}
}

func (l *looper) stop(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may stop an assignment")
		return
	}
	var req agentloop.StopRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if req.Archive && req.AssignmentID == "" && req.TurnID == "" {
		l.mu.Lock()
		var found *assignment
		for _, candidate := range l.active {
			if candidate.start.SessionID == req.SessionID {
				found = candidate
				break
			}
		}
		l.mu.Unlock()
		if found == nil {
			_ = agentbase.DeleteContext(sys, req.SessionID)
			_, _ = sys.Reply(msg, map[string]any{"disposition": "archived", "session_id": req.SessionID})
			return
		}
		found.mu.Lock()
		found.archive = true
		found.closed = true
		found.cancel()
		found.mu.Unlock()
		_, _ = sys.Reply(msg, map[string]any{"disposition": "archive_pending", "session_id": req.SessionID})
		return
	}
	if req.AssignmentID == "" {
		req.AssignmentID = req.TurnID
	}
	l.mu.Lock()
	a := l.active[req.AssignmentID]
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Reply(msg, map[string]any{"disposition": "already_stopped", "work_id": req.WorkID})
		return
	}
	if a.start.ControllerActor != msg.Sender.ID.String() || a.start.SessionID != req.SessionID {
		_, _ = sys.Fail(msg, "operation_mismatch", "stop targets a stale assignment")
		return
	}
	a.mu.Lock()
	if req.Archive {
		a.archive = true
	}
	a.closed = true
	a.cancel()
	a.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"disposition": "stop_requested", "work_id": req.WorkID, "assignment_id": req.AssignmentID})
}
func (l *looper) inspect(sys actorbase.Sys, msg actorbase.Msg) {
	if !l.authorized(msg) {
		_, _ = sys.Fail(msg, "permission_denied", "only the configured Agent Controller may inspect an assignment")
		return
	}
	var req agentloop.InspectRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	l.mu.Lock()
	a := l.active[req.AssignmentID]
	if a == nil {
		a = l.finished[req.AssignmentID]
	}
	l.mu.Unlock()
	if a == nil {
		_, _ = sys.Fail(msg, "assignment_not_found", "no active assignment for work")
		return
	}
	if a.start.ControllerActor != msg.Sender.ID.String() || a.start.SessionID != req.SessionID {
		_, _ = sys.Fail(msg, "operation_mismatch", "inspect targets a different session or incarnation")
		return
	}
	a.mu.Lock()
	phase, n := a.phase, len(a.inputs)
	controls := append([]agentloop.ControlResult(nil), a.controls...)
	consumed := a.consumed
	a.mu.Unlock()
	_, _ = sys.Reply(msg, map[string]any{"work_id": req.WorkID, "assignment_id": a.start.AssignmentID, "phase": phase, "input_count": n, "controls": controls, "consumed_count": consumed})
}

func (l *looper) authorized(msg actorbase.Msg) bool {
	return targetMatches(l.cfg.ControllerActor, msg.Sender.ID.String())
}

func targetMatches(configured, actual string) bool {
	if configured == actual {
		return true
	}
	parts := strings.Split(actual, ":")
	if len(parts) != 3 {
		return false
	}
	if !strings.Contains(configured, ":") {
		return configured == parts[1]
	}
	want := strings.Split(configured, ":")
	return len(want) == 2 && want[0] == parts[0] && want[1] == parts[1]
}

func (l *looper) drive(ctx context.Context, sys actorbase.Sys, a *assignment) {
	a.mu.Lock()
	inputs := append([]agentloop.Input(nil), a.inputs...)
	a.phase = "context"
	a.mu.Unlock()
	var through int64
	for _, in := range inputs {
		if in.Seq > through {
			through = in.Seq
		}
	}
	history := append([]json.RawMessage(nil), a.history...)
	for i, in := range inputs {
		if in.Text == "" {
			hydrated, err := hydrateInput(ctx, sys, a.cause, in.ID, in.Seq)
			if err != nil {
				l.report(sys, a, "failed", through, nil, "input_unavailable", err.Error(), "confirmed")
				return
			}
			in = hydrated
			inputs[i] = in
		}
		history = append(history, inputMessage(in))
		a.version = message.ID(in.ID)
	}
	if err := validateHistory(history); err != nil {
		l.report(sys, a, "failed", 0, nil, "context_invalid", err.Error(), "confirmed")
		return
	}
	a.mu.Lock()
	a.consumed = len(inputs)
	a.mu.Unlock()
	a.setHistory(history)
	turns := a.start.MaxTurns
	if turns <= 0 {
		turns = 12
	}
	tools, err := resolveTools(ctx, sys, a)
	if err != nil {
		l.report(sys, a, "failed", through, nil, "tool_discovery_failed", err.Error(), stateForError(err))
		return
	}
	definitions := make([]json.RawMessage, 0, len(tools))
	targets := make(map[string]resolvedTool, len(tools))
	for _, tool := range tools {
		definitions = append(definitions, tool.Definition)
		targets[tool.Name] = tool
	}
	overflowRetried := false
	for turn := 0; turn < turns; turn++ {
		if ctx.Err() != nil {
			state, code := "cancelled", "cancelled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				state, code = "timeout", "timeout"
			}
			l.report(sys, a, state, through, nil, code, ctx.Err().Error(), "confirmed_stopped")
			return
		}
		pending := a.pendingInputs()
		if len(pending) > 0 {
			for i, in := range pending {
				if in.Text == "" {
					hydrated, err := hydrateInput(ctx, sys, a.cause, in.ID, in.Seq)
					if err != nil {
						l.report(sys, a, "failed", through, nil, "input_unavailable", err.Error(), "confirmed")
						return
					}
					in = hydrated
					pending[i] = in
				}
				history = append(history, inputMessage(in))
				a.version = message.ID(in.ID)
			}
			a.mu.Lock()
			a.consumed += len(pending)
			acks := append([]pendingInputAck(nil), a.acks...)
			a.acks = nil
			a.mu.Unlock()
			for _, ack := range acks {
				if _, err := sys.Reply(ack.msg, ack.decision); err != nil {
					l.report(sys, a, "failed", through, nil, "ledger_unavailable", "input acceptance could not be recorded: "+err.Error(), "confirmed")
					return
				}
			}
			for _, in := range pending {
				if in.Seq > through {
					through = in.Seq
				}
			}
			a.setHistory(history)
		}
		if err := validateHistory(history); err != nil {
			l.report(sys, a, "failed", through, nil, "context_invalid", err.Error(), "confirmed")
			return
		}
		a.mu.Lock()
		a.phase = "thinking"
		a.mu.Unlock()
		ref, writeErr := agentbase.WriteContext(sys, a.start.SessionID, agentbase.ContextObject{Messages: history, Version: a.version})
		if writeErr != nil {
			l.report(sys, a, "failed", through, nil, "context_failed", writeErr.Error(), "confirmed")
			return
		}
		history, ref, writeErr = l.compactIfNeeded(ctx, sys, a, history, ref, false)
		if writeErr != nil {
			l.report(sys, a, "failed", through, nil, "context_limit", writeErr.Error(), "confirmed")
			return
		}
		a.setHistory(history)
		options := json.RawMessage(nil)
		if a.start.Effort != "" {
			options = mustRaw(map[string]any{"thinkingLevel": a.start.Effort})
		}
		generatedMsg, err := callMessage(ctx, sys, a.cause, actor.ActorID(a.start.LLMActor), llmproto.TypeGenerate, llmproto.GenerateRequest{ModelRef: parseModel(a.start.Model), SystemPrompt: a.start.Prompt, Context: ref, Tools: definitions, Options: options})
		if err != nil {
			state := "confirmed"
			kind := "llm_failed"
			var failure *callFailure
			if errors.As(err, &failure) && failure.Code != "" {
				kind = failure.Code
			}
			if errors.Is(err, context.Canceled) {
				kind = "cancelled"
				state = "confirmed_stopped"
				l.report(sys, a, "cancelled", through, nil, kind, err.Error(), state)
				return
			}
			l.report(sys, a, "failed", through, nil, kind, err.Error(), state)
			return
		}
		raw := generatedMsg.Payload
		var generated llmproto.GenerateResponse
		if err := json.Unmarshal(raw, &generated); err != nil {
			l.report(sys, a, "failed", through, nil, "llm_invalid", err.Error(), "confirmed")
			return
		}
		calls, text, err := assistantParts(generated.Message)
		if err != nil {
			l.report(sys, a, "failed", through, nil, "invalid_model_response", err.Error(), "confirmed")
			return
		}
		reason := stopReason(generated.Message)
		if generated.ErrorCode == "context_overflow" || generated.ErrorCode == "length_recoverable" {
			if overflowRetried {
				l.report(sys, a, "failed", through, nil, "context_limit", "model context overflowed again after compaction", "confirmed")
				return
			}
			history, ref, err = l.compactIfNeeded(ctx, sys, a, history, ref, true)
			if err != nil {
				l.report(sys, a, "failed", through, nil, "context_limit", err.Error(), "confirmed")
				return
			}
			a.setHistory(history)
			overflowRetried = true
			turn--
			continue
		}
		history = append(history, generated.Message)
		a.version = generatedMsg.ID
		a.setHistory(history)
		if reason == "error" || reason == "aborted" {
			for _, tc := range calls {
				history = append(history, toolResult(tc, true, "Looper: tool was not executed because the assistant response did not complete successfully"))
			}
			a.setHistory(history)
			_, _ = agentbase.WriteContext(sys, a.start.SessionID, agentbase.ContextObject{Messages: history, Version: a.version})
			l.report(sys, a, "failed", through, nil, "provider_error", "provider did not deliver a complete successful assistant", "confirmed")
			return
		}
		if len(calls) == 0 {
			if !a.seal() {
				continue
			}
			resultText, truncated := boundedResultText(text)
			result, _ := json.Marshal(map[string]any{"text": resultText, "text_truncated": truncated, "message": generated.Message, "provider": generated.Provider, "model": generated.Model})
			if _, err := agentbase.WriteContext(sys, a.start.SessionID, agentbase.ContextObject{Messages: history, Version: a.version}); err != nil {
				l.report(sys, a, "failed", through, nil, "context_failed", err.Error(), "confirmed")
				return
			}
			l.report(sys, a, "completed", through, result, "", "", "confirmed")
			return
		}
		a.mu.Lock()
		a.phase = "acting"
		a.mu.Unlock()
		stopBatch := false
		for _, tc := range calls {
			if stopBatch {
				history = append(history, toolResult(tc, true, "Looper: tool was not executed"))
				continue
			}
			if reason == "length" {
				history = append(history, toolResult(tc, true, "Looper: tool was not executed"))
				continue
			}
			if ctx.Err() != nil {
				history = append(history, toolResult(tc, true, "Looper: tool was not executed"))
				continue
			}
			tool, ok := targets[tc.Name]
			if !ok {
				history = append(history, toolResult(tc, true, "Looper: tool was not executed"))
				a.setHistory(history)
				continue
			}
			toolTimeout := 2 * time.Minute
			if a.start.ToolTimeoutMS > 0 {
				toolTimeout = time.Duration(a.start.ToolTimeoutMS) * time.Millisecond
			}
			toolCtx, cancelTool := context.WithTimeout(ctx, toolTimeout)
			toolMsg, callErr := callMessage(toolCtx, sys, generatedMsg.Cause(), actor.ActorID(tool.Actor), tool.Word, json.RawMessage(tc.Arguments))
			cancelTool()
			if callErr != nil {
				detail := callErr.Error()
				if errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded) {
					detail = "Looper: no tool result received; external execution/effects unknown: " + detail
				}
				result := toolResult(tc, true, detail)
				var failure *callFailure
				if errors.As(callErr, &failure) {
					var fields map[string]json.RawMessage
					_ = json.Unmarshal(result, &fields)
					fields["details"] = mustRaw(map[string]any{"source": "looper_wait", "request_id": failure.RequestID, "error_code": failure.Code, "external_effects_unknown": errors.Is(callErr, context.Canceled) || errors.Is(callErr, context.DeadlineExceeded)})
					result = mustRaw(fields)
					if failure.Response.ID != "" {
						result = toolResultFailure(tc, failure.Response.Payload)
						a.version = failure.Response.ID
					}
				} else {
					result = toolResult(tc, true, "Looper: tool was not executed")
					stopBatch = true
				}
				history = append(history, result)
				a.setHistory(history)
				continue
			}
			a.version = toolMsg.ID
			history = append(history, l.toolResult(ctx, sys, a, tc, tool, toolMsg.Payload))
			a.setHistory(history)
		}
		a.setHistory(history)
		if ctx.Err() != nil {
			state, code := "cancelled", "cancelled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				state, code = "timeout", "timeout"
			}
			l.report(sys, a, state, through, nil, code, "local execution stopped; dispatched tool effects may be unknown", "confirmed_stopped")
			return
		}
	}
	l.report(sys, a, "failed", through, nil, "turn_limit", "Agent loop reached max_turns", "confirmed")
}

func boundedResultText(text string) (string, bool) {
	if len(text) <= maxResultTextBytes {
		return text, false
	}
	cut := maxResultTextBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut], true
}

type toolCall struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func inputMessage(in agentloop.Input) json.RawMessage {
	content := []map[string]any{{"type": "text", "text": in.Text}}
	for _, attachment := range in.Attachments {
		content = append(content, map[string]any{"type": "attachment", "value": attachment})
	}
	return mustJSON(map[string]any{"role": "user", "content": content, "source_message_id": in.ID})
}

func hydrateInput(ctx context.Context, sys actorbase.Sys, cause message.Cause, id string, seq int64) (agentloop.Input, error) {
	raw, err := call(ctx, sys, cause, actor.SystemActorID, message.TypeSystemLogQuery, map[string]any{"view": "raw", "read_id": id})
	if err != nil {
		return agentloop.Input{}, err
	}
	var response channelspec.LogQueryResponse
	if err := json.Unmarshal(raw, &response); err != nil || response.Message == nil {
		return agentloop.Input{}, errors.New("input ledger row not found")
	}
	payloadText := response.Message.PayloadText
	if response.Message.Truncated {
		payloadText, err = readLedgerPayload(ctx, sys, cause, response.Message.Seq, response.HeadSeq)
		if err != nil {
			return agentloop.Input{}, err
		}
	}
	payload := json.RawMessage(payloadText)
	_, body, err := harness.UnwrapPayload(payload)
	if err != nil {
		return agentloop.Input{}, fmt.Errorf("input payload: %w", err)
	}
	var ask struct {
		Text        string             `json:"text"`
		Attachments []json.RawMessage  `json:"attachments"`
		Origin      *agentproto.Origin `json:"origin"`
	}
	if err := json.Unmarshal(body, &ask); err != nil || strings.TrimSpace(ask.Text) == "" {
		return agentloop.Input{}, errors.New("input row has no user text")
	}
	return agentloop.Input{ID: id, Seq: seq, Text: ask.Text, Attachments: ask.Attachments, Origin: ask.Origin}, nil
}

func assistantParts(raw json.RawMessage) ([]toolCall, string, error) {
	var m struct {
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || m.Role != "assistant" || m.Content == nil {
		return nil, "", errors.New("LLM message must be an assistant object with content blocks")
	}
	var calls []toolCall
	seen := map[string]bool{}
	var text string
	for _, block := range m.Content {
		var head struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(block, &head) != nil || head.Type == "" {
			return nil, "", errors.New("LLM message contains an invalid content block")
		}
		if head.Type == "text" {
			text += head.Text
		}
		if head.Type == "toolCall" {
			var tc toolCall
			if json.Unmarshal(block, &tc) != nil || strings.TrimSpace(tc.ID) == "" || strings.TrimSpace(tc.Name) == "" || seen[tc.ID] {
				return nil, "", errors.New("LLM tool call requires a unique nonempty id and name")
			}
			seen[tc.ID] = true
			calls = append(calls, tc)
		}
	}
	return calls, text, nil
}
func toolResult(tc toolCall, isErr bool, text string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"role": "toolResult", "toolCallId": tc.ID, "toolName": tc.Name, "content": []map[string]any{{"type": "text", "text": text}}, "isError": isErr})
	return raw
}
func toolResultRaw(tc toolCall, raw json.RawMessage) json.RawMessage {
	var result struct {
		Content        []json.RawMessage `json:"content"`
		Details        json.RawMessage   `json:"details"`
		Usage          json.RawMessage   `json:"usage"`
		AddedToolNames []string          `json:"addedToolNames"`
	}
	if json.Unmarshal(raw, &result) != nil || len(result.Content) == 0 {
		return toolResult(tc, false, string(raw))
	}
	message := map[string]any{"role": "toolResult", "toolCallId": tc.ID, "toolName": tc.Name, "content": result.Content, "isError": false}
	if len(result.Details) > 0 && string(result.Details) != "null" {
		message["details"] = result.Details
	}
	if len(result.Usage) > 0 && string(result.Usage) != "null" {
		message["usage"] = result.Usage
	}
	if len(result.AddedToolNames) > 0 {
		message["addedToolNames"] = result.AddedToolNames
	}
	out, _ := json.Marshal(message)
	return out
}
func toolResultFailure(tc toolCall, raw json.RawMessage) json.RawMessage {
	var failure struct {
		ErrorCode string `json:"error_code"`
		Detail    string `json:"detail"`
		Reason    string `json:"reason"`
	}
	_ = json.Unmarshal(raw, &failure)
	text := failure.ErrorCode
	if failure.Detail != "" {
		if text != "" {
			text += ": "
		}
		text += failure.Detail
	}
	if text == "" {
		text = string(raw)
	}
	result := toolResult(tc, true, text)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(result, &fields)
	fields["details"] = mustRaw(map[string]any{"error_code": failure.ErrorCode, "reason": failure.Reason})
	return mustRaw(fields)
}

func (l *looper) toolResult(_ context.Context, _ actorbase.Sys, a *assignment, tc toolCall, tool resolvedTool, raw json.RawMessage) json.RawMessage {
	maxLines, maxBytes, maxImage := a.start.ToolResultMaxLines, a.start.ToolResultMaxBytes, a.start.ToolImageMaxBytes
	if maxLines <= 0 {
		maxLines = defaultToolResultMaxLines
	}
	if maxBytes <= 0 {
		maxBytes = defaultToolResultMaxBytes
	}
	if maxImage <= 0 {
		maxImage = defaultToolImageMaxBytes
	}
	var result struct {
		Content        []json.RawMessage `json:"content"`
		Details        json.RawMessage   `json:"details"`
		Usage          json.RawMessage   `json:"usage"`
		AddedToolNames []string          `json:"addedToolNames"`
		IsError        bool              `json:"isError"`
	}
	piShape := json.Unmarshal(raw, &result) == nil && len(result.Content) > 0
	if !piShape {
		result.Content = []json.RawMessage{mustRaw(map[string]any{"type": "text", "text": string(raw)})}
	}
	var fullText strings.Builder
	imageBytes := 0
	for _, block := range result.Content {
		var value struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Data     string `json:"data"`
			MimeType string `json:"mimeType"`
		}
		if err := json.Unmarshal(block, &value); err != nil {
			return toolResult(tc, true, "tool returned an invalid content block: "+err.Error())
		}
		switch value.Type {
		case "text":
			fullText.WriteString(value.Text)
		case "image":
			if value.MimeType != "image/png" && value.MimeType != "image/jpeg" && value.MimeType != "image/gif" && value.MimeType != "image/webp" {
				return toolResult(tc, true, "tool returned an unsupported image MIME type")
			}
			if len(value.Data) > (maxImage-imageBytes+2)/3*4+4 {
				return toolResult(tc, true, fmt.Sprintf("tool image is invalid or exceeds %d bytes", maxImage))
			}
			decoded, err := base64.StdEncoding.DecodeString(value.Data)
			imageBytes += len(decoded)
			if err != nil || imageBytes > maxImage {
				return toolResult(tc, true, fmt.Sprintf("tool image is invalid or exceeds %d bytes", maxImage))
			}
		default:
			return toolResult(tc, true, "tool returned an unsupported content block type")
		}
	}
	full := fullText.String()
	tail := tool.Word == workspaceproto.TypeBash || tool.Word == workspaceproto.TypePowerShell
	excerpt, truncated := boundedToolText(full, maxLines, maxBytes, tail)
	if !truncated {
		out := toolResultRaw(tc, raw)
		if !piShape {
			return toolResult(tc, result.IsError, full)
		}
		if result.IsError {
			var message map[string]any
			if json.Unmarshal(out, &message) == nil {
				message["isError"] = true
				return mustRaw(message)
			}
		}
		return out
	}
	noticePrefix := " "
	excerptLines := maxLines
	if maxLines > 1 {
		noticePrefix = "\n"
		excerptLines--
	}
	notice := noticePrefix + "[Output truncated to configured limit]"
	excerptBytes := maxBytes - len(notice)
	if excerptLines < 1 {
		excerptLines = 1
	}
	if excerptBytes < 0 {
		excerptBytes = 0
	}
	excerpt, _ = boundedToolText(full, excerptLines, excerptBytes, tail)
	content := make([]json.RawMessage, 0, len(result.Content))
	textAdded := false
	for _, block := range result.Content {
		var head struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(block, &head)
		if head.Type == "text" {
			if !textAdded {
				content = append(content, mustRaw(map[string]any{"type": "text", "text": excerpt + notice}))
				textAdded = true
			}
			continue
		}
		content = append(content, block)
	}
	details := map[string]any{}
	if len(result.Details) > 0 && string(result.Details) != "null" {
		if json.Unmarshal(result.Details, &details) != nil {
			details = map[string]any{"tool_details": result.Details}
		}
	}
	details["atoll_output"] = map[string]any{"truncated": true, "retained": map[bool]string{true: "tail", false: "head"}[tail]}
	message := map[string]any{"role": "toolResult", "toolCallId": tc.ID, "toolName": tc.Name, "content": content, "details": details, "isError": result.IsError}
	if len(result.Usage) > 0 && string(result.Usage) != "null" {
		message["usage"] = result.Usage
	}
	if len(result.AddedToolNames) > 0 {
		message["addedToolNames"] = result.AddedToolNames
	}
	return mustRaw(message)
}

func boundedToolText(text string, maxLines, maxBytes int, tail bool) (string, bool) {
	if maxLines < 1 || maxBytes < 1 {
		return "", text != ""
	}
	if len(text) <= maxBytes && strings.Count(text, "\n") < maxLines {
		return text, false
	}
	if tail {
		start := len(text) - maxBytes
		if start < 0 {
			start = 0
		}
		for start < len(text) && !utf8.RuneStart(text[start]) {
			start++
		}
		part := text[start:]
		for strings.Count(part, "\n") >= maxLines {
			if i := strings.IndexByte(part, '\n'); i >= 0 {
				part = part[i+1:]
			} else {
				break
			}
		}
		return part, true
	}
	end := len(text)
	if end > maxBytes {
		end = maxBytes
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
	}
	part := text[:end]
	if strings.Count(part, "\n") >= maxLines {
		position := 0
		for range maxLines {
			i := strings.IndexByte(part[position:], '\n')
			if i < 0 {
				break
			}
			position += i + 1
		}
		part = strings.TrimSuffix(part[:position], "\n")
	}
	return part, true
}

func mustRaw(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}
func configuredTools(start agentloop.StartRequest) []agentloop.ToolBinding {
	if start.Tools != nil {
		return append([]agentloop.ToolBinding(nil), (*start.Tools)...)
	}
	var tools []agentloop.ToolBinding
	if start.WorkspaceActor != "" {
		for _, item := range []struct{ name, word string }{
			{"read", workspaceproto.TypeRead}, {"write", workspaceproto.TypeWrite},
			{"edit", workspaceproto.TypeEdit}, {"bash", workspaceproto.TypeBash},
		} {
			tools = append(tools, agentloop.ToolBinding{Name: item.name, Actor: start.WorkspaceActor, Word: item.word})
		}
	}
	if start.HostActor != "" {
		for _, item := range []struct{ name, word string }{
			{"channel_call", channelCallWord}, {"channel_post", "channel.post"}, {"channel_emit", "channel.emit"},
		} {
			tools = append(tools, agentloop.ToolBinding{Name: item.name, Actor: start.HostActor, Word: item.word})
		}
	}
	return tools
}

func resolveTools(ctx context.Context, sys actorbase.Sys, a *assignment) ([]resolvedTool, error) {
	bindings := configuredTools(a.start)
	seen := make(map[string]struct{}, len(bindings))
	manifests := make(map[string]introspect.Describe)
	out := make([]resolvedTool, 0, len(bindings))
	for _, binding := range bindings {
		if !validToolName.MatchString(binding.Name) || strings.TrimSpace(binding.Actor) == "" || strings.TrimSpace(binding.Word) == "" {
			return nil, fmt.Errorf("invalid tool binding %q", binding.Name)
		}
		if _, exists := seen[binding.Name]; exists {
			return nil, fmt.Errorf("duplicate tool name %q", binding.Name)
		}
		seen[binding.Name] = struct{}{}
		describe, ok := manifests[binding.Actor]
		if !ok {
			raw, err := call(ctx, sys, a.cause, actor.ActorID(binding.Actor), introspect.QueryDescribe, introspect.DescribeRequest{})
			if err != nil {
				return nil, fmt.Errorf("describe tool actor %q: %w", binding.Actor, err)
			}
			if err := json.Unmarshal(raw, &describe); err != nil {
				return nil, fmt.Errorf("describe tool actor %q returned invalid manifest: %w", binding.Actor, err)
			}
			manifests[binding.Actor] = describe
		}
		spec, ok := describe.Words[binding.Word]
		if !ok {
			return nil, fmt.Errorf("tool actor %q declares no request word %q", binding.Actor, binding.Word)
		}
		if err := validInputSchema(spec.InputSchema); err != nil {
			return nil, fmt.Errorf("tool %q word %q has invalid input schema: %w", binding.Name, binding.Word, err)
		}
		definition, err := json.Marshal(map[string]any{"name": binding.Name, "description": spec.Description, "parameters": spec.InputSchema})
		if err != nil {
			return nil, err
		}
		out = append(out, resolvedTool{Name: binding.Name, Actor: binding.Actor, Word: binding.Word, Definition: definition})
	}
	return out, nil
}

func validInputSchema(raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return errors.New("schema is missing or invalid JSON")
	}
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(raw, &schema); err != nil || schema == nil {
		return errors.New("schema must be a JSON object")
	}
	if err := validateSchemaShape(schema); err != nil {
		return err
	}
	var typ string
	if rawType, ok := schema["type"]; ok {
		if err := json.Unmarshal(rawType, &typ); err != nil || typ != "object" {
			return errors.New("schema must describe an object")
		}
		return nil
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf"} {
		raw, ok := schema[keyword]
		if !ok {
			continue
		}
		var alternatives []json.RawMessage
		_ = json.Unmarshal(raw, &alternatives)
		for _, alternative := range alternatives {
			if err := validInputSchema(alternative); err != nil {
				return fmt.Errorf("schema %s alternative: %w", keyword, err)
			}
		}
		return nil
	}
	return errors.New("schema must describe an object or object alternatives")
}

func validateSchemaShape(schema map[string]json.RawMessage) error {
	if raw, ok := schema["type"]; ok {
		var typ string
		if json.Unmarshal(raw, &typ) != nil || typ == "" {
			return errors.New("schema type must be a non-empty string")
		}
	}
	if raw, ok := schema["properties"]; ok {
		var properties map[string]json.RawMessage
		if json.Unmarshal(raw, &properties) != nil || properties == nil {
			return errors.New("schema properties must be an object")
		}
		for name, child := range properties {
			var nested map[string]json.RawMessage
			if json.Unmarshal(child, &nested) != nil || nested == nil {
				return fmt.Errorf("schema property %q must be an object", name)
			}
			if err := validateSchemaShape(nested); err != nil {
				return fmt.Errorf("schema property %q: %w", name, err)
			}
		}
	}
	if raw, ok := schema["required"]; ok {
		var required []string
		if json.Unmarshal(raw, &required) != nil {
			return errors.New("schema required must be a string array")
		}
	}
	if raw, ok := schema["items"]; ok {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw, &nested) != nil || nested == nil {
			return errors.New("schema items must be an object")
		}
		if err := validateSchemaShape(nested); err != nil {
			return fmt.Errorf("schema items: %w", err)
		}
	}
	if raw, ok := schema["additionalProperties"]; ok {
		var allowed bool
		if json.Unmarshal(raw, &allowed) != nil {
			var nested map[string]json.RawMessage
			if json.Unmarshal(raw, &nested) != nil || nested == nil {
				return errors.New("schema additionalProperties must be boolean or object")
			}
			if err := validateSchemaShape(nested); err != nil {
				return fmt.Errorf("schema additionalProperties: %w", err)
			}
		}
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf"} {
		raw, ok := schema[keyword]
		if !ok {
			continue
		}
		var alternatives []map[string]json.RawMessage
		if json.Unmarshal(raw, &alternatives) != nil || len(alternatives) == 0 {
			return fmt.Errorf("schema %s must be a non-empty object array", keyword)
		}
		for _, alternative := range alternatives {
			if err := validateSchemaShape(alternative); err != nil {
				return fmt.Errorf("schema %s: %w", keyword, err)
			}
		}
	}
	return nil
}
func parseModel(value string) llmproto.ModelRef {
	for i := 0; i < len(value); i++ {
		if value[i] == '/' {
			return llmproto.ModelRef{Provider: value[:i], Model: value[i+1:]}
		}
	}
	return llmproto.ModelRef{Model: value}
}
func call(ctx context.Context, sys actorbase.Sys, cause message.Cause, target actor.ActorID, typ string, payload any) (json.RawMessage, error) {
	msg, err := callMessage(ctx, sys, cause, target, typ, payload)
	if err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), msg.Payload...), nil
}
func callMessage(ctx context.Context, sys actorbase.Sys, cause message.Cause, target actor.ActorID, typ string, payload any) (actorbase.Msg, error) {
	if err := ctx.Err(); err != nil {
		return actorbase.Msg{}, err
	}
	pd, err := sys.Call(cause, target, typ, payload)
	if err != nil {
		return actorbase.Msg{}, err
	}
	progressDone := make(chan struct{})
	drainCtx, stopDrain := context.WithCancel(ctx)
	defer stopDrain()
	go func() {
		defer close(progressDone)
		for {
			select {
			case _, ok := <-pd.Progress():
				if !ok {
					return
				}
			case <-drainCtx.Done():
				return
			}
		}
	}()
	msg, err := pd.Wait(ctx, 0)
	stopDrain()
	if err != nil {
		if sys.Life().Err() == nil {
			_ = pd.Cancel()
		}
		<-progressDone
		code := "call_failed"
		if errors.Is(err, context.Canceled) {
			code = "cancelled"
		}
		if errors.Is(err, context.DeadlineExceeded) {
			code = "deadline_exceeded"
		}
		return actorbase.Msg{}, &callFailure{Code: code, Detail: err.Error(), RequestID: string(pd.RequestID()), cause: err}
	}
	<-progressDone
	var state struct {
		Status string `json:"status"`
		message.Failure
	}
	if json.Unmarshal(msg.Payload, &state) != nil || state.Status != "completed" {
		if state.ErrorCode == "" {
			state.ErrorCode = "call_failed"
		}
		return actorbase.Msg{}, &callFailure{Code: state.ErrorCode, Detail: state.Detail, RequestID: string(pd.RequestID()), Response: msg}
	}
	return msg, nil
}
func (l *looper) report(sys actorbase.Sys, a *assignment, state string, _ int64, _ json.RawMessage, _, _, _ string) {
	if sys.Life().Err() != nil {
		return
	}
	a.mu.Lock()
	a.phase = state
	if state != "accepted" {
		a.closed = true
	}
	acks := append([]pendingInputAck(nil), a.acks...)
	if state != "accepted" {
		a.acks = nil
	}
	a.mu.Unlock()
	if state != "accepted" {
		for _, ack := range acks {
			ack.decision.Disposition = "target_gone"
			_, _ = sys.Reply(ack.msg, ack.decision)
		}
	}
	// A terminal report releases the lane before it is published. The
	// Controller may schedule a successor as soon as it adopts this report; if
	// the old assignment remained in active until the goroutine returned, that
	// successor could be rejected by a lane that was already logically free.
	if state != "accepted" {
		l.mu.Lock()
		if l.active[a.start.AssignmentID] == a {
			delete(l.active, a.start.AssignmentID)
		}
		if l.finished == nil {
			l.finished = map[string]*assignment{}
		}
		l.finished[a.start.AssignmentID] = a
		l.finishedOrder = append(l.finishedOrder, a.start.AssignmentID)
		for len(l.finishedOrder) > 256 {
			delete(l.finished, l.finishedOrder[0])
			l.finishedOrder = l.finishedOrder[1:]
		}
		l.mu.Unlock()
	}
	turn := a.start.TurnID
	if turn == "" {
		turn = a.start.AssignmentID
	}
	payload := agentloop.ReportRequest{TurnID: turn, SessionID: a.start.SessionID, State: state}
	_, _ = sys.Post(behavior.RequestSpec{Cause: a.cause, Type: agentloop.TypeReport, Audience: message.Audience{actor.ActorID(a.start.ControllerActor)}, Payload: mustJSON(payload)})
}

func (l *looper) compactIfNeeded(ctx context.Context, sys actorbase.Sys, a *assignment, history []json.RawMessage, ref agentbase.ContextRef, force bool) ([]json.RawMessage, agentbase.ContextRef, error) {
	window := a.start.ContextWindow
	if window <= 0 {
		window = 128000
	}
	reserve := a.start.ReserveTokens
	if reserve <= 0 {
		reserve = 16384
	}
	before := agentbase.ContextTokens(history)
	if !force && before <= window-reserve {
		return history, ref, nil
	}
	keep := a.start.KeepRecentTokens
	if keep <= 0 {
		keep = 20000
	}
	cut, bytesKept := len(history), 0
	for i := len(history) - 1; i >= 0; i-- {
		bytesKept += len(history[i])
		if bytesKept > keep*4 {
			break
		}
		if modelMessageRole(history[i]) == "user" || modelMessageRole(history[i]) == "assistant" {
			cut = i
		}
	}
	if cut <= 0 || cut >= len(history) {
		return nil, ref, errors.New("context exceeds the model window and has no safe compaction cut")
	}
	model := a.start.CompactModel
	if model == "" {
		model = a.start.Model
	}
	summaryMsg, err := callMessage(ctx, sys, a.cause, actor.ActorID(a.start.LLMActor), llmproto.TypeGenerate, llmproto.GenerateRequest{
		ModelRef: parseModel(model), Purpose: "compact", Context: ref,
		Options: func() json.RawMessage {
			if a.start.Effort == "" {
				return nil
			}
			return mustRaw(map[string]any{"thinkingLevel": a.start.Effort})
		}(),
		SystemPrompt: "Summarize the conversation before the recent messages. Preserve Goal, Constraints, Progress, Key Decisions, Next Steps, and Critical Context. Return a concise assistant summary without tool calls.",
	})
	if err != nil {
		return nil, ref, fmt.Errorf("compact context: %w", err)
	}
	var generated llmproto.GenerateResponse
	if json.Unmarshal(summaryMsg.Payload, &generated) != nil || len(generated.Message) == 0 || stopReason(generated.Message) == "error" {
		return nil, ref, errors.New("compact context: summarizer returned no usable assistant")
	}
	compacted := append([]json.RawMessage{generated.Message}, cloneMessages(history[cut:])...)
	if err := validateHistory(compacted); err != nil {
		return nil, ref, fmt.Errorf("compact context: %w", err)
	}
	row := agentloop.Compact{SessionID: a.start.SessionID, Context: compacted, TokensBefore: before, Size: agentloop.ContextSize{Before: before, After: agentbase.ContextTokens(compacted)}}
	spec, err := behavior.EventSpecJSON(message.Anchored(summaryMsg.ID, summaryMsg.ID), agentloop.TypeSessionCompact, row)
	if err != nil {
		return nil, ref, err
	}
	id, err := sys.Emit(spec)
	if err != nil {
		return nil, ref, err
	}
	newRef, err := agentbase.WriteContext(sys, a.start.SessionID, agentbase.ContextObject{Messages: compacted, Version: id})
	if err != nil {
		return nil, ref, err
	}
	a.version = id
	return compacted, newRef, nil
}

func modelMessageRole(raw json.RawMessage) string {
	var message struct {
		Role string `json:"role"`
	}
	_ = json.Unmarshal(raw, &message)
	return message.Role
}

func (a *assignment) setHistory(history []json.RawMessage) {
	a.mu.Lock()
	a.history = append([]json.RawMessage(nil), history...)
	a.mu.Unlock()
}
func mustJSON(v any) json.RawMessage { raw, _ := json.Marshal(v); return raw }
func stateForError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "confirmed_stopped"
	}
	return "confirmed"
}
