package native

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const maxWorkInputBytes = 4 << 20

type controller struct {
	cfg          Config
	data         workTable
	wait         map[agentproto.WorkID][]actorbase.Msg
	sessions     map[string]*session
	sessionOrder []string
	nextSession  int
	selection    *runtimeproto.TurnOptions
}

func run(sys actorbase.Sys, cfg Config) error {
	c := &controller{cfg: cfg, data: newWorkTable(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	if err := c.refreshSessionRelations(sys); err != nil {
		return err
	}
	if _, err := sys.After(10*time.Second, pulseType, map[string]any{}, schedule.TimerHomeMemory); err != nil {
		return err
	}
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind == message.KindEvent && msg.Type == pulseType && msg.Sender.ID == sys.Self() {
			if err := c.pulse(sys, msg); err != nil {
				return err
			}
			continue
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		switch msg.Type {
		case agentproto.TypeAsk:
			c.handleAsk(sys, msg)
		case agentproto.TypeStatus:
			c.handleStatus(sys, msg)
		case agentproto.TypeResult:
			c.handleResult(sys, msg)
		case agentproto.TypeSteer:
			c.steer(sys, msg)
		case agentproto.TypeReplace, agentproto.TypeHold, agentproto.TypeUnhold:
			c.editControl(sys, msg)
		case controlDoneType:
			c.controlDone(sys, msg)
		case startDoneType:
			c.startDone(sys, msg)
		case agentproto.TypeInterrupt:
			c.handleInterrupt(sys, msg)
		case agentproto.TypeSessionList, agentproto.TypeSessionGet, agentproto.TypeSessionRename, agentproto.TypeSessionArchive, agentproto.TypeSessionReset, agentproto.TypeSessionSync:
			c.handleSession(sys, msg)
		case agentbase.TypeOptions:
			c.handleOptions(sys, msg)
		case agentbase.TypeSelect:
			c.handleSelect(sys, msg)
		case agentloop.TypeReport:
			c.handleReport(sys, msg)
		case waitClosedType:
			c.handleWaitClosed(sys, msg)
		default:
			_, _ = fail(sys, msg, "type_unsupported", fmt.Sprintf("native Agent does not answer %q", msg.Type))
		}
	}
}

const waitClosedType = "agent.internal.wait_closed"
const startDoneType = "agent.internal.start_done"

type startDone struct {
	Session     string `json:"session_id"`
	Turn        string `json:"turn_id"`
	Looper      string `json:"looper"`
	Disposition string `json:"disposition,omitempty"`
	Error       string `json:"error,omitempty"`
	Detail      string `json:"detail,omitempty"`
}

func cloneWork(w *workRecord) *workRecord {
	raw, _ := json.Marshal(w)
	var out workRecord
	_ = json.Unmarshal(raw, &out)
	out.SourceCause = w.SourceCause
	out.SourceContext = w.SourceContext.Clone()
	return &out
}

type logMessage struct {
	Seq         int64            `json:"seq"`
	ID          message.ID       `json:"id"`
	Sender      message.Sender   `json:"sender"`
	Audience    message.Audience `json:"audience"`
	Kind        message.Kind     `json:"kind"`
	MessageType string           `json:"message_type"`
	ParentID    message.ID       `json:"parent_id"`
	Terminal    bool             `json:"terminal"`
	TSReceived  int64            `json:"ts_received"`
	PayloadText string           `json:"payload_text"`
}

// refreshSessionRelations projects session relationships and historical boundaries.
// It never reconstructs or overwrites this process's execution or request state.
func (c *controller) refreshSessionRelations(sys actorbase.Sys) error {
	items, head, err := readRelationHistory(sys)
	if err != nil {
		return err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Seq < items[j].Seq })
	acceptedReports, err := reportAcceptance(sys, items, head)
	if err != nil {
		return err
	}
	// Only replace a projection once its entire history has been read within
	// the budget. A failed or truncated read must preserve the prior projection.
	if c.sessions == nil {
		c.sessions = map[string]*session{}
	}
	for _, s := range c.sessions {
		s.Holder, s.Base, s.Opened, s.Archived = "", nil, false, false
		s.Merge, s.Merged, s.Skipped, s.Synced, s.LastBoundary, s.ForkPoint = "", nil, nil, "", "", ""
	}
	turnSession := map[string]string{}
	closedTurns := map[string]bool{}
	for _, item := range items {
		text := item.PayloadText
		app, body, err := harness.UnwrapPayload(json.RawMessage(text))
		if err != nil || app.Session == "" {
			continue
		}
		if app.Session == "main" {
			switch item.MessageType {
			case "session.track":
				var x struct {
					From  string `json:"from_session"`
					Merge string `json:"merge"`
				}
				if json.Unmarshal(body, &x) == nil && x.From != "" {
					s := c.ensureProjectedSession(x.From)
					s.Merge = x.Merge
				}
			case "session.merge":
				var x struct {
					From     string   `json:"from_session"`
					Decision string   `json:"decision"`
					Refs     []string `json:"refs"`
				}
				if json.Unmarshal(body, &x) == nil && x.From != "" {
					s := c.ensureProjectedSession(x.From)
					if x.Decision == "skipped" {
						s.Skipped = appendUnique(s.Skipped, x.Refs...)
					} else if x.Decision == "merged" {
						s.Merged = appendUnique(s.Merged, x.Refs...)
					}
				}
			}
			continue
		}
		s := c.sessions[app.Session]
		if s == nil {
			s = &session{ID: app.Session, LastUsed: nowMillis()}
			c.sessions[s.ID] = s
			c.sessionOrder = append(c.sessionOrder, s.ID)
		}
		if item.TSReceived > s.LastUsed {
			s.LastUsed = item.TSReceived
		}
		switch item.MessageType {
		case agentloop.TypeStart:
			if len(item.Audience) == 0 || !sameSeat(string(sys.Self()), item.Sender.ID.String()) {
				continue
			}
			var start agentloop.StartRequest
			if json.Unmarshal(body, &start) == nil {
				turnSession[start.TurnID] = s.ID
				s.Archived = false
			}
			s.Holder = item.Audience[0].String()
		case agentloop.TypeInput, agentloop.TypeReset, agentloop.TypeRename:
			if len(item.Audience) > 0 && sameSeat(string(sys.Self()), item.Sender.ID.String()) {
				s.Holder = item.Audience[0].String()
				s.Archived = false
			}
		case agentloop.TypeStop:
			var stop agentloop.StopRequest
			_ = json.Unmarshal(body, &stop)
			if len(item.Audience) > 0 && sameSeat(string(sys.Self()), item.Sender.ID.String()) {
				s.Holder = item.Audience[0].String()
				s.Archived = stop.Archive
			}
		case agentloop.TypeSessionOpened:
			var opened agentloop.Opened
			if json.Unmarshal(body, &opened) == nil {
				s.Opened = true
				s.Base = opened.Base
				if s.ForkPoint == "" {
					s.ForkPoint = string(item.ID)
				}
			}
		case agentloop.TypeReport:
			var report agentloop.ReportRequest
			if json.Unmarshal(body, &report) == nil && acceptedReports[item.ID] && terminalTurnStateNative(report.State) && turnSession[report.TurnID] == s.ID && targetMatches(s.Holder, item.Sender.ID.String()) && !closedTurns[report.TurnID] {
				closedTurns[report.TurnID] = true
				s.LastBoundary = string(item.ID)
				s.ForkPoint = string(item.ID)
			}
		case agentloop.TypeSessionSync:
			var synced agentloop.Synced
			if json.Unmarshal(body, &synced) == nil {
				s.Synced = synced.From.Through
			}
		case agentloop.TypeSync:
			if len(item.Audience) > 0 && sameSeat(string(sys.Self()), item.Sender.ID.String()) {
				s.Holder = item.Audience[0].String()
				s.Archived = false
			}
		}
	}
	for _, s := range c.sessions {
		if s.Merge == "" {
			s.Merge = "auto"
		}
	}
	return nil
}

func (c *controller) ensureProjectedSession(id string) *session {
	if s := c.sessions[id]; s != nil {
		return s
	}
	s := &session{ID: id, LastUsed: nowMillis()}
	c.sessions[id] = s
	c.sessionOrder = append(c.sessionOrder, id)
	return s
}

func appendUnique(dst []string, values ...string) []string {
	seen := make(map[string]bool, len(dst))
	for _, value := range dst {
		seen[value] = true
	}
	for _, value := range values {
		if value != "" && !seen[value] {
			dst = append(dst, value)
			seen[value] = true
		}
	}
	return dst
}

func sameSeat(left, right string) bool {
	a, b := strings.Split(left, ":"), strings.Split(right, ":")
	return len(a) == 3 && len(b) == 3 && a[0] == b[0] && a[1] == b[1]
}

func (c *controller) emit(sys actorbase.Sys, cause message.Cause, app harness.Context, typ string, value any) {
	spec, err := behavior.EventSpecJSON(cause, app, typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}

func awaitStart(sys actorbase.Sys, cause message.Cause, app harness.Context, session, turn, looper string, pending actorbase.Pending) {
	ctx, cancel := context.WithTimeout(sys.Life(), 15*time.Second)
	defer cancel()
	done := startDone{Session: session, Turn: turn, Looper: looper}
	response, err := pending.Wait(ctx, 0)
	if err != nil {
		if sys.Life().Err() != nil {
			return
		}
		_ = pending.Cancel()
		done.Error = err.Error()
	} else {
		var body struct {
			Status      string `json:"status"`
			Disposition string `json:"disposition"`
			ErrorCode   string `json:"error_code"`
			Detail      string `json:"detail"`
		}
		if json.Unmarshal(response.Payload, &body) != nil || (body.Status != "" && body.Status != "completed") {
			done.Error, done.Detail = body.ErrorCode, body.Detail
			if done.Error == "" {
				done.Error = "start_rejected"
			}
		} else {
			done.Disposition = body.Disposition
		}
	}
	if sys.Life().Err() != nil {
		return
	}
	_, _ = sys.Post(behavior.RequestSpec{Cause: cause, Type: startDoneType, Audience: message.Audience{sys.Self()}, Payload: mustJSON(done), Context: app})
}

func (c *controller) startDone(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Sender.ID != sys.Self() {
		_, _ = fail(sys, msg, "permission_denied", "start completion is Controller-internal")
		return
	}
	var done startDone
	if actorbase.DecodeStrict(msg.Payload, &done) != nil || done.Session == "" || done.Turn == "" {
		_, _ = fail(sys, msg, "invalid_args", "session_id and turn_id are required")
		return
	}
	s := c.sessions[done.Session]
	if s == nil || s.Execution != done.Turn || !targetMatches(s.Holder, done.Looper) {
		_, _ = sys.Reply(msg, map[string]any{"disposition": "stale"})
		return
	}
	w := c.data.Works[string(s.Owner)]
	if w == nil || w.State != agentproto.WorkOpen || w.AssignmentID != done.Turn {
		_, _ = sys.Reply(msg, map[string]any{"disposition": "stale"})
		return
	}
	if done.Error == "" && (done.Disposition == "accepted" || done.Disposition == "already_accepted" || done.Disposition == "already_finished") {
		w.Stage, w.ExecutionState, w.UpdatedAt = "thinking", "confirmed_running", nowMillis()
		_, _ = sys.Reply(msg, map[string]any{"disposition": "adopted"})
		return
	}
	if done.Error == "session_context_unavailable" {
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeFailed, "start_rejected"
		w.UpdatedAt = nowMillis()
		w.Result = mustJSON(map[string]any{"error_code": done.Error, "detail": done.Detail, "guidance": "Open a new session; this session's context could not be established consistently."})
		s.Execution, s.Owner = "", ""
		c.emit(sys, msg.Cause(), msg.Context(), "agent.work.closed", map[string]any{"work": publicWork(w), "result": json.RawMessage(w.Result)})
		c.finishWaiters(sys, w)
		_, _ = sys.Reply(msg, map[string]any{"disposition": "rejected"})
		c.scheduleQueued(sys, msg.Cause(), msg.Context())
		return
	}
	for i := range w.Inputs {
		if w.Inputs[i].Disposition == "assigned" {
			w.Inputs[i].Disposition = "accepted"
		}
	}
	w.AssignmentID, w.Looper, w.AssignedThrough = "", "", 0
	w.Stage, w.ExecutionState, w.UpdatedAt = "queued", "waiting_capacity", nowMillis()
	s.Execution, s.Owner, s.Holder = "", "", ""
	if s.ForkPoint == "" {
		s.Opened = false
	}
	s.Buffer = append([]agentproto.WorkID{w.ID}, s.Buffer...)
	_, _ = sys.Reply(msg, map[string]any{"disposition": "rerouted"})
	c.scheduleQueued(sys, msg.Cause(), msg.Context())
}

func (c *controller) toolBindings() []agentloop.ToolBinding {
	if c.cfg.ToolsConfigured {
		bindings := make([]agentloop.ToolBinding, len(c.cfg.Tools))
		for i, tool := range c.cfg.Tools {
			bindings[i] = agentloop.ToolBinding{Name: tool.Name, Actor: tool.Actor, Word: tool.Word}
		}
		return bindings
	}
	var bindings []agentloop.ToolBinding
	if c.cfg.WorkspaceActor != "" {
		for _, item := range []struct{ name, word string }{{"read", "workspace.read"}, {"write", "workspace.write"}, {"edit", "workspace.edit"}, {"bash", "workspace.bash"}} {
			bindings = append(bindings, agentloop.ToolBinding{Name: item.name, Actor: c.cfg.WorkspaceActor, Word: item.word})
		}
	}
	if c.cfg.HostActor != "" {
		for _, item := range []struct{ name, word string }{{"channel_call", "channel.call"}, {"channel_post", "channel.post"}, {"channel_emit", "channel.emit"}} {
			bindings = append(bindings, agentloop.ToolBinding{Name: item.name, Actor: c.cfg.HostActor, Word: item.word})
		}
	}
	return bindings
}

func (c *controller) handleAsk(sys actorbase.Sys, msg actorbase.Msg) {
	var accepted bool
	msg, accepted = sessionInput(sys, msg)
	if !accepted {
		return
	}
	req, err := agentproto.DecodeAsk(msg.Payload)
	if err != nil {
		_, _ = fail(sys, msg, "invalid_args", err.Error())
		return
	}
	caller := actorbase.AttributedCaller(msg)
	if existing := c.data.findSubmission(msg.Sender.ID, req.SubmissionKey); existing != nil {
		if existing.SubmissionHash != submissionHash(req) {
			_, _ = fail(sys, msg, "submission_conflict", "submission_key already names different input")
			return
		}
		app := msg.Context()
		app.Session = existing.SessionID
		msg = msg.WithContext(app)
		if req.Delivery == agentproto.DeliveryWait && existing.State == agentproto.WorkOpen {
			c.attachWaiter(sys, msg, existing)
		} else {
			c.answerAsk(sys, msg, existing)
		}
		return
	}
	if c.data.openCount() >= c.cfg.MaxOpenWorks {
		_, _ = fail(sys, msg, "capacity", "the Agent has reached max_open_works")
		return
	}
	now := nowMillis()
	w := &workRecord{ID: newWorkID(), Submitter: msg.Sender.ID, Owner: caller, SourceRequest: string(msg.ID), SubmissionKey: req.SubmissionKey,
		SubmissionHash: submissionHash(req), RelatedWorkID: req.RelatedWorkID, Delivery: req.Delivery,
		State: agentproto.WorkOpen, Stage: "queued", ExecutionState: "not_started", CreatedAt: now, UpdatedAt: now}
	w.Inputs = []inputRecord{{Input: agentloop.Input{
		ID: string(msg.ID), Seq: 1, Text: req.Text,
		Attachments:   append([]json.RawMessage(nil), req.Attachments...),
		CallerChannel: caller.Channel, CallerActor: caller.Actor, Origin: req.Origin,
	}, Disposition: "accepted"}}
	if inputRecordsSize(w.Inputs) > maxWorkInputBytes {
		_, _ = fail(sys, msg, "limit_exceeded", "the work input exceeds the input size limit")
		return
	}
	s, sessionErr := c.sessionForAsk(sys, &msg, req)
	if sessionErr != nil {
		_, _ = fail(sys, msg, sessionErr.Error(), "cannot select conversation")
		return
	}
	if len(s.Buffer) >= maxSessionQueue {
		_, _ = fail(sys, msg, "capacity", "session waiting queue full")
		return
	}
	var waitingBytes int
	for _, id := range s.Buffer {
		if queued := c.data.Works[string(id)]; queued != nil {
			waitingBytes += inputRecordsSize(queued.Inputs)
		}
	}
	if waitingBytes+inputRecordsSize(w.Inputs) > maxWorkInputBytes {
		_, _ = fail(sys, msg, "capacity", "session waiting input bytes exceeded")
		return
	}
	w.SessionID = s.ID
	w.SourceCause = msg.Cause()
	w.SourceContext = msg.Context()
	c.data.Works[string(w.ID)] = w
	c.data.Order = append(c.data.Order, string(w.ID))
	c.emit(sys, msg.Cause(), msg.Context(), "agent.work.accepted", map[string]any{"work": publicWork(w), "owner": caller, "input_id": w.Inputs[0].ID})
	s.Buffer = append(s.Buffer, w.ID)
	s.Freeze = ""
	s.RestoreInterrupt = false
	s.LastUsed = nowMillis()
	if req.Delivery == agentproto.DeliveryWait {
		c.attachWaiter(sys, msg, w)
	} else {
		c.answerAsk(sys, msg, w)
	}
	c.scheduleQueued(sys, msg.Cause(), msg.Context())
}

func (c *controller) attachWaiter(sys actorbase.Sys, msg actorbase.Msg, w *workRecord) {
	c.wait[w.ID] = append(c.wait[w.ID], msg)
	_, _ = sys.Progress(msg, w.Stage, map[string]any{"work_id": w.ID, "work_state": w.State, "stage": w.Stage, "execution_state": w.ExecutionState, "controls": []map[string]any{{"word": agentproto.TypeInterrupt, "label": "停止", "payload": map[string]any{"work_id": w.ID}}}})
	go func(ctx context.Context, life context.Context, self actor.ActorID, cause message.Cause, app harness.Context, workID agentproto.WorkID, requestID message.ID) {
		select {
		case <-ctx.Done():
			if life.Err() == nil {
				_, _ = sys.Post(behavior.RequestSpec{Cause: cause, Type: waitClosedType, Audience: message.Audience{self}, Payload: mustJSON(map[string]any{"work_id": workID, "request_id": requestID}), Context: app})
			}
		case <-life.Done():
		}
	}(msg.Ctx(), sys.Life(), sys.Self(), msg.Cause(), msg.Context(), w.ID, msg.ID)
}

func (c *controller) handleWaitClosed(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Sender.ID != sys.Self() {
		_, _ = fail(sys, msg, "permission_denied", "wait lifecycle reports are Controller-internal")
		return
	}
	var req struct {
		WorkID    agentproto.WorkID `json:"work_id"`
		RequestID message.ID        `json:"request_id"`
	}
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil || req.WorkID == "" || req.RequestID == "" {
		_, _ = fail(sys, msg, "invalid_args", "work_id and request_id are required")
		return
	}
	held := c.wait[req.WorkID]
	remaining := held[:0]
	for _, waiter := range held {
		if waiter.ID != req.RequestID {
			remaining = append(remaining, waiter)
		}
	}
	if len(remaining) == 0 {
		delete(c.wait, req.WorkID)
	} else {
		c.wait[req.WorkID] = remaining
	}
	w := c.data.Works[string(req.WorkID)]
	stopped := false
	if len(held) != len(remaining) && len(remaining) == 0 && w != nil && w.Delivery == agentproto.DeliveryWait && w.State == agentproto.WorkOpen {
		if err := c.stop(sys, msg, w, true); err != nil {
			_, _ = fail(sys, msg, "ledger_unavailable", err.Error())
			return
		}
		stopped = true
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "observed", "work_id": req.WorkID, "remaining_waiters": len(remaining), "stop_requested": stopped})
}

func (c *controller) answerAsk(sys actorbase.Sys, msg actorbase.Msg, w *workRecord) {
	if w.Delivery == agentproto.DeliveryWait && w.State == agentproto.WorkClosed {
		_, _ = sys.Reply(msg, resultPayload(w))
		return
	}
	_, _ = sys.Reply(msg, map[string]any{"disposition": "accepted", "work_id": w.ID, "session_id": w.SessionID, "work_state": w.State,
		"guidance": "Keep work_id as the stable address. Poll agent.result for completion or use an actor-authored targeted control from next.",
		"next":     nextFor(w)})
}

func (c *controller) freeLooper() (string, bool) {
	active := make(map[string]int, len(c.cfg.Loopers))
	for _, w := range c.data.Works {
		if w.State == agentproto.WorkOpen && w.AssignmentID != "" {
			active[w.Looper]++
		}
	}
	limit := c.cfg.MaxAssignmentsPerLooper
	// Tests and old in-memory configurations that predate the explicit field
	// retain their former one-assignment capacity. Parsed declarations always
	// materialize a positive, bounded value.
	if limit <= 0 {
		limit = 1
	}
	for offset := range c.cfg.Loopers {
		i := (c.data.NextLooper + offset) % len(c.cfg.Loopers)
		if active[c.cfg.Loopers[i]] < limit {
			c.data.NextLooper = (i + 1) % len(c.cfg.Loopers)
			return c.cfg.Loopers[i], true
		}
	}
	return "", false
}

func (c *controller) scheduleQueued(sys actorbase.Sys, cause message.Cause, app harness.Context) {
	if len(c.sessions) > 0 {
		c.scheduleSessions(sys, cause, app)
		return
	}
	for _, id := range c.data.Order {
		w := c.data.Works[id]
		if w == nil || w.State != agentproto.WorkOpen || w.Stage != "queued" || w.AssignmentID != "" {
			continue
		}
		if !c.dispatch(sys, w, cause, app) {
			return
		}
	}
}

func (c *controller) dispatch(sys actorbase.Sys, w *workRecord, cause message.Cause, app harness.Context) bool {
	if w.State != agentproto.WorkOpen || len(c.cfg.Loopers) == 0 {
		return false
	}
	s := c.sessions[w.SessionID]
	looper := ""
	if s != nil {
		looper = s.Holder
	}
	ok := looper != ""
	if ok && c.looperLoad(looper) >= c.looperLimit() {
		if w.ExecutionState != "waiting_capacity" {
			w.ExecutionState, w.UpdatedAt = "waiting_capacity", nowMillis()
		}
		return false
	}
	if !ok {
		looper, ok = c.freeLooper()
	}
	if !ok {
		if w.ExecutionState != "waiting_capacity" {
			w.ExecutionState, w.UpdatedAt = "waiting_capacity", nowMillis()
		}
		return false
	}
	if s != nil && s.Holder == "" {
		s.Holder = looper
	}
	w.AssignmentID, w.Looper = newAssignmentID(), looper
	w.AssignedThrough = 0
	w.Stage, w.ExecutionState, w.UpdatedAt = "dispatching", "start_requested", nowMillis()
	for i := range w.Inputs {
		if w.Inputs[i].Disposition == "accepted" {
			w.Inputs[i].Disposition = "assigned"
		}
		if w.Inputs[i].Disposition == "assigned" && w.Inputs[i].Seq > w.AssignedThrough {
			w.AssignedThrough = w.Inputs[i].Seq
		}
	}
	inputs := make([]agentloop.Input, 0, len(w.Inputs))
	for i := range w.Inputs {
		if w.Inputs[i].Disposition == "assigned" {
			inputs = append(inputs, w.Inputs[i].Input)
		}
	}
	tools := c.toolBindings()
	selection := c.selected(sys)
	startCause := w.SourceCause
	var open *agentloop.OpenRequest
	if s != nil && !s.Opened {
		open = &agentloop.OpenRequest{Base: s.Base}
	}
	request := agentloop.StartRequest{
		WorkID: w.ID, AssignmentID: w.AssignmentID, TurnID: w.AssignmentID, SessionID: w.SessionID, Open: open, ToolTimeoutMS: c.cfg.ToolTimeoutMS, ExecutionTimeoutMS: c.cfg.ExecutionTimeoutMS, ControllerActor: string(sys.Self()), Inputs: inputs, Prompt: c.cfg.Prompt, LLMActor: c.cfg.LLMActor,
		WorkspaceActor: c.cfg.WorkspaceActor, HostActor: c.cfg.HostActor, Model: selection.Model, Effort: selection.Effort, MaxTurns: c.cfg.MaxTurns, Tools: &tools,
		ToolResultMaxLines: c.cfg.ToolResultMaxLines, ToolResultMaxBytes: c.cfg.ToolResultMaxBytes, ToolImageMaxBytes: c.cfg.ToolImageMaxBytes,
		ContextWindow: c.cfg.Compact.ContextWindow, ReserveTokens: c.cfg.Compact.ReserveTokens, KeepRecentTokens: c.cfg.Compact.KeepRecentTokens, CompactModel: c.cfg.Compact.Model}
	pending, err := sys.Call(startCause, w.SourceContext, actorID(looper), agentloop.TypeStart, request)
	if err != nil {
		if s != nil {
			// A stale holder is discovered only when the next instruction is
			// routed. Keep the work queued and let the next scheduling pass
			// resolve a current Looper for the same declaration.
			s.Holder = ""
			if s.ForkPoint == "" {
				s.Opened = false
			}
			for i := range w.Inputs {
				if w.Inputs[i].Disposition == "assigned" {
					w.Inputs[i].Disposition = "accepted"
				}
			}
			w.AssignmentID, w.Looper, w.AssignedThrough = "", "", 0
			w.Stage, w.ExecutionState, w.UpdatedAt = "queued", "waiting_capacity", nowMillis()
			return false
		}
		w.AssignmentID, w.Looper = "", ""
		w.State, w.Outcome = agentproto.WorkClosed, agentproto.OutcomeFailed
		w.Stage, w.ExecutionState, w.UpdatedAt = "", "dispatch_failed", nowMillis()
		w.Result = mustJSON(map[string]any{"error_code": "dispatch_failed", "detail": err.Error()})
		c.emit(sys, cause, app, "agent.work.dispatch_failed", map[string]any{"work_id": w.ID, "detail": err.Error()})
		c.finishWaiters(sys, w)
		return true
	}
	if s != nil {
		s.Opened = true
	}
	go awaitStart(sys, startCause, w.SourceContext, w.SessionID, w.AssignmentID, looper, pending)
	// The accepted response to loop.start is ledger evidence for report validation;
	// Controller does not mirror it into a session fact.
	c.emit(sys, cause, app, "agent.work.dispatched", publicWork(w))
	return true
}

func (c *controller) looperLimit() int {
	if c.cfg.MaxAssignmentsPerLooper > 0 {
		return c.cfg.MaxAssignmentsPerLooper
	}
	return 1
}

func (c *controller) looperLoad(looper string) int {
	count := 0
	for _, work := range c.data.Works {
		if work.State == agentproto.WorkOpen && work.AssignmentID != "" && work.Looper == looper {
			count++
		}
	}
	return count
}

func (c *controller) workByID(id agentproto.WorkID) (*workRecord, bool) {
	w := c.data.Works[string(id)]
	return w, w != nil
}

func (c *controller) handleStatus(sys actorbase.Sys, msg actorbase.Msg) {
	var accepted bool
	msg, accepted = sessionInput(sys, msg)
	if !accepted {
		return
	}
	req, err := agentproto.DecodeStatus(msg.Payload)
	if err != nil {
		_, _ = fail(sys, msg, "invalid_args", err.Error())
		return
	}
	if req.WorkID != "" {
		w, ok := c.workByID(req.WorkID)
		if !ok {
			_, _ = fail(sys, msg, "work_not_found", "no visible work has that id")
			return
		}
		_, _ = sys.Reply(msg, statusPayload(w))
		return
	}
	if req.SubmissionKey != "" {
		w := c.data.findSubmission(msg.Sender.ID, req.SubmissionKey)
		if w == nil {
			_, _ = fail(sys, msg, "work_not_found", "no visible work has that submission_key")
			return
		}
		_, _ = sys.Reply(msg, statusPayload(w))
		return
	}
	visible := c.data.orderedWorks()
	start, err := decodeCursor(req.Cursor, visible)
	if err != nil {
		_, _ = fail(sys, msg, "invalid_args", err.Error())
		return
	}
	end := start + req.Limit
	if end > len(visible) {
		end = len(visible)
	}
	works := make([]agentproto.Work, 0, end-start)
	for _, w := range visible[start:end] {
		works = append(works, publicWork(w))
	}
	next := ""
	if end < len(visible) {
		next = encodeCursor(visible[end-1])
	}
	response := agentproto.StatusResponse{Works: works, NextCursor: next, Guidance: "This is an Agent work page. Use work_id with agent.status or agent.result; follow next_cursor without changing the query."}
	if next != "" {
		response.Next = []agentproto.NextAction{nextAction(agentproto.TypeStatus, "下一页", map[string]any{"cursor": next, "limit": req.Limit})}
	}
	_, _ = sys.Reply(msg, response)
}

func (c *controller) handleResult(sys actorbase.Sys, msg actorbase.Msg) {
	var accepted bool
	msg, accepted = sessionInput(sys, msg)
	if !accepted {
		return
	}
	req, err := agentproto.DecodeResult(msg.Payload)
	if err != nil {
		_, _ = fail(sys, msg, "invalid_args", err.Error())
		return
	}
	w, ok := c.workByID(req.WorkID)
	if !ok {
		_, _ = fail(sys, msg, "work_not_found", "no visible work has that id")
		return
	}
	_, _ = sys.Reply(msg, resultPayload(w))
}

func (c *controller) handleInterrupt(sys actorbase.Sys, msg actorbase.Msg) {
	var accepted bool
	msg, accepted = sessionInput(sys, msg)
	if !accepted {
		return
	}
	req, err := agentproto.DecodeInterrupt(msg.Payload)
	if err != nil {
		_, _ = fail(sys, msg, "invalid_args", err.Error())
		return
	}
	if len(c.sessions) > 0 {
		opKey := operationIndexKey(msg.Sender.ID, req.OperationKey)
		opHash := requestHash(req)
		if req.OperationKey != "" {
			for _, w := range c.data.Works {
				if prior, ok := w.Operations[opKey]; ok {
					if prior.Kind != agentproto.TypeInterrupt || prior.Hash != opHash {
						_, _ = fail(sys, msg, "operation_conflict", "operation_key already names different control")
						return
					}
					_, _ = sys.Reply(msg, json.RawMessage(prior.Response))
					return
				}
			}
		}
		var selected []*session
		if req.WorkID != "" || req.SessionID != "" {
			s, err := c.selectSession(msg, req.SessionID, req.WorkID, "")
			if err != nil {
				_, _ = fail(sys, msg, err.Error(), "cannot select interrupt session")
				return
			}
			selected = []*session{s}
		} else {
			for _, s := range c.sessions {
				selected = append(selected, s)
			}
		}
		if req.WorkID != "" {
			w := c.data.Works[string(req.WorkID)]
			if w.State == agentproto.WorkClosed {
				_, _ = sys.Reply(msg, map[string]any{"disposition": "already_closed", "work_id": w.ID})
				return
			}
		}
		if req.OperationKey != "" && c.cfg.MaxOperationKeys > 0 {
			for _, v := range selected {
				for _, w := range c.data.Works {
					if w.SessionID == v.ID && len(w.Operations) >= c.cfg.MaxOperationKeys {
						_, _ = fail(sys, msg, "limit_exceeded", "max_operation_keys reached")
						return
					}
				}
			}
		}
		results := []map[string]any{}
		for _, s := range selected {
			if s.Control != nil {
				results = append(results, map[string]any{"session_id": s.ID, "disposition": "busy"})
				continue
			}
			if req.WorkID != "" && req.WorkID != s.Owner {
				w := c.data.Works[string(req.WorkID)]
				err := c.stop(sys, msg, w, false)
				if err != nil {
					results = append(results, map[string]any{"session_id": s.ID, "disposition": "failed", "detail": err.Error()})
				} else {
					results = append(results, map[string]any{"session_id": s.ID, "disposition": "stop_requested"})
				}
				continue
			}
			s.Freeze = "interrupt"
			s.RestoreInterrupt = false
			if w := c.data.Works[string(s.Owner)]; w != nil && s.Execution != "" {
				if err := c.stop(sys, msg, w, false); err != nil {
					results = append(results, map[string]any{"session_id": s.ID, "disposition": "failed", "detail": err.Error()})
					continue
				}
			}
			if req.WorkID == "" && req.SessionID == "" {
				for _, id := range append([]agentproto.WorkID(nil), s.Buffer...) {
					if w := c.data.Works[string(id)]; w != nil {
						_ = c.stop(sys, msg, w, false)
					}
				}
				s.Buffer = nil
			}
			results = append(results, map[string]any{"session_id": s.ID, "disposition": "stop_requested"})
		}
		scope := "view"
		if req.WorkID == "" && req.SessionID == "" {
			scope = "agent"
		}
		stopped := 0
		for _, result := range results {
			if result["disposition"] == "stop_requested" {
				stopped++
			}
		}
		response := mustJSON(map[string]any{"scope": scope, "stop_requested": stopped, "sessions": results, "disposition": "stop_requested"})
		if req.OperationKey != "" {
			for _, w := range c.data.Works {
				for _, s := range selected {
					if w.SessionID == s.ID {
						if w.Operations == nil {
							w.Operations = map[string]operationRecord{}
						}
						w.Operations[opKey] = operationRecord{Kind: agentproto.TypeInterrupt, Hash: opHash, Response: response}
					}
				}
			}
		}
		_, _ = sys.Reply(msg, json.RawMessage(response))
		return
	}
	if req.WorkID == "" {
		if req.OperationKey != "" {
			_, _ = fail(sys, msg, "unsupported_scope", "Agent-wide interrupt does not support operation_key; address one work")
			return
		}
		count := 0
		for _, w := range c.data.Works {
			if w.State == agentproto.WorkOpen {
				if err := c.stop(sys, msg, w, false); err != nil {
					_, _ = fail(sys, msg, "ledger_unavailable", "interrupt partially applied before the ledger failed: "+err.Error())
					return
				}
				count++
			}
		}
		c.scheduleQueued(sys, msg.Cause(), msg.Context())
		_, _ = sys.Reply(msg, map[string]any{"scope": "agent", "stop_requested": count})
		return
	}
	w, ok := c.workByID(req.WorkID)
	if !ok {
		_, _ = fail(sys, msg, "work_not_found", "no visible work has that id")
		return
	}
	opKey := operationIndexKey(msg.Sender.ID, req.OperationKey)
	opHash := requestHash(struct {
		WorkID agentproto.WorkID `json:"work_id"`
	}{req.WorkID})
	if req.OperationKey != "" && w.Operations != nil {
		if prior, found := w.Operations[opKey]; found {
			if prior.Kind != agentproto.TypeInterrupt || prior.Hash != opHash {
				_, _ = fail(sys, msg, "operation_conflict", "operation_key already names a different control")
				return
			}
			_, _ = sys.Reply(msg, json.RawMessage(prior.Response))
			return
		}
	}
	if w.State == agentproto.WorkClosed {
		_, _ = sys.Reply(msg, map[string]any{"work_id": w.ID, "disposition": "already_closed", "state": w.State})
		return
	}
	if req.OperationKey != "" && c.cfg.MaxOperationKeys > 0 && len(w.Operations) >= c.cfg.MaxOperationKeys {
		_, _ = fail(sys, msg, "limit_exceeded", "the work has reached max_operation_keys")
		return
	}
	if err := c.stop(sys, msg, w, true); err != nil {
		_, _ = fail(sys, msg, "ledger_unavailable", err.Error())
		return
	}
	response := mustJSON(map[string]any{"work_id": w.ID, "disposition": "stop_requested", "execution_state": w.ExecutionState})
	if req.OperationKey != "" {
		if w.Operations == nil {
			w.Operations = map[string]operationRecord{}
		}
		w.Operations[opKey] = operationRecord{Kind: agentproto.TypeInterrupt, Hash: opHash, Response: response}
	}
	_, _ = sys.Reply(msg, json.RawMessage(response))
}

func (c *controller) stop(sys actorbase.Sys, msg actorbase.Msg, w *workRecord, schedule bool) error {
	if w.State == agentproto.WorkClosed || w.Stage == "stopping" {
		return nil
	}
	if w.AssignmentID == "" {
		w.State, w.Stage, w.Outcome, w.UpdatedAt = agentproto.WorkClosed, "", agentproto.OutcomeCancelled, nowMillis()
		w.ExecutionState = "not_started"
		c.emit(sys, msg.Cause(), msg.Context(), "agent.work.closed", map[string]any{"work": publicWork(w), "result": json.RawMessage(w.Result)})
		c.finishWaiters(sys, w)
		if s := c.sessions[w.SessionID]; s != nil {
			removeQueue(s, w.ID)
		}
		if schedule {
			c.scheduleQueued(sys, msg.Cause(), msg.Context())
		}
		return nil
	}
	w.Stage, w.ExecutionState, w.UpdatedAt = "stopping", "stop_requested", nowMillis()
	_, _ = sys.Post(behavior.RequestSpec{Cause: msg.Cause(), Type: agentloop.TypeStop, Audience: message.Audience{actorID(w.Looper)}, Payload: mustJSON(agentloop.StopRequest{WorkID: w.ID, SessionID: w.SessionID, AssignmentID: w.AssignmentID, TurnID: w.AssignmentID, Reason: "agent.interrupt"}), Context: msg.Context()})
	c.emit(sys, msg.Cause(), msg.Context(), "agent.work.stop_requested", publicWork(w))
	return nil
}

func (c *controller) handleReport(sys actorbase.Sys, msg actorbase.Msg) {
	var req agentloop.ReportRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = fail(sys, msg, "invalid_args", err.Error())
		return
	}
	if req.AssignmentID == "" {
		req.AssignmentID = req.TurnID
	}
	if req.TurnID == "" {
		req.TurnID = req.AssignmentID
	}
	w := c.data.Works[string(req.WorkID)]
	if w == nil || w.AssignmentID != req.AssignmentID {
		w = nil
		for _, candidate := range c.data.Works {
			if candidate.AssignmentID == req.AssignmentID {
				w = candidate
				req.WorkID = candidate.ID
				break
			}
		}
	}
	// Reports can only settle assignments issued by this Controller process.
	// Do not consult history or touch a control slot for an unmatched old report.
	if w == nil || w.State != agentproto.WorkOpen || req.AssignmentID == "" || req.AssignmentID != w.AssignmentID || !targetMatches(w.Looper, msg.Sender.ID.String()) {
		_, _ = fail(sys, msg, "stale_assignment", "report does not belong to a current assignment")
		return
	}
	if req.SessionID != "" && req.SessionID != w.SessionID {
		_, _ = fail(sys, msg, "stale_assignment", "report session does not match the current assignment")
		return
	}
	s := c.sessions[req.SessionID]
	if s == nil && w != nil {
		s = c.sessions[w.SessionID]
	}
	if s == nil || !targetMatches(s.Holder, msg.Sender.ID.String()) {
		_, _ = fail(sys, msg, "stale_assignment", "report sender is not the ledger-projected session holder")
		return
	}
	accepted, err := reportHasAcceptedStart(sys, s.ID, msg.ID)
	if err != nil {
		_, _ = fail(sys, msg, "ledger_unavailable", err.Error())
		return
	}
	if !accepted {
		_, _ = fail(sys, msg, "turn_not_accepted", "report has no preceding accepted loop.start")
		return
	}
	if s.Execution == req.AssignmentID {
		current := c.data.Works[string(s.Owner)]
		if current == nil || !targetMatches(current.Looper, msg.Sender.ID.String()) {
			_, _ = fail(sys, msg, "stale_assignment", "report session identity or version mismatch")
			return
		}
		if s.Control != nil {
			c.settleControl(sys, s, agentloop.ControlResult{ControlID: s.Control.ID, Disposition: "target_gone"}, false)
		}
		w = c.data.Works[string(s.Owner)]
	}
	if w == nil || req.AssignmentID != w.AssignmentID || !targetMatches(w.Looper, msg.Sender.ID.String()) {
		_, _ = fail(sys, msg, "stale_assignment", "report does not belong to the current assignment")
		return
	}
	if req.State != "completed" && req.State != "failed" && req.State != "cancelled" && req.State != "timeout" && req.State != "execution_unknown" {
		_, _ = fail(sys, msg, "invalid_args", "invalid terminal report state")
		return
	}
	if req.State == "completed" && len(req.Result) == 0 {
		req.Result = c.sessionResult(sys, req.SessionID, req.TurnID, msg.ID)
	}
	if req.State == "completed" && (len(req.Result) == 0 || !json.Valid(req.Result)) {
		req.Result = mustJSON(map[string]any{"text": "", "message": map[string]any{"role": "assistant", "content": []any{}}})
	}
	for i := range w.Inputs {
		if w.Inputs[i].Disposition == "assigned" {
			w.Inputs[i].Disposition = "included"
		}
	}
	if s.Execution == req.AssignmentID {
		s.Execution = ""
		s.Owner = ""
		s.LastUsed = nowMillis()
		if s.Rebuffer {
			s.Rebuffer = false
			w.AssignmentID = ""
			w.Looper = ""
			w.Stage = "queued"
			w.ExecutionState = "interrupted_for_edit"
			w.Resumed = true
			text := "Continue interrupted work after editing. Previously dispatched tool effects may be unknown."
			if len(w.Inputs) > 0 {
				text += "\n" + w.Inputs[len(w.Inputs)-1].Text
			}
			w.Inputs = []inputRecord{{Input: agentloop.Input{ID: w.SourceRequest, Seq: 1, Text: text, CallerActor: w.Owner.Actor, CallerChannel: w.Owner.Channel}, Disposition: "accepted"}}

			s.Buffer = append([]agentproto.WorkID{w.ID}, s.Buffer...)
			_, _ = sys.Reply(msg, map[string]any{"disposition": "rebuffered"})
			c.scheduleQueued(sys, msg.Cause(), msg.Context())
			return
		}
	}
	if w.Stage == "stopping" || req.State == "cancelled" {
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeCancelled, req.ExecutionState
	} else if req.State == "completed" {
		pending := false
		for _, in := range w.Inputs {
			if in.Disposition != "included" {
				pending = true
				break
			}
		}
		if pending {
			w.State, w.Stage, w.Outcome = agentproto.WorkClosed, "", agentproto.OutcomeFailed
			w.ExecutionState = "unconsumed_input"
			w.Result = mustJSON(map[string]any{"error_code": "unconsumed_input", "detail": "execution ended with accepted inputs not consumed; no continuation was replayed"})
			w.UpdatedAt = nowMillis()
			c.finishWaiters(sys, w)
			_, _ = sys.Reply(msg, map[string]any{"disposition": "closed_unconsumed"})
			c.scheduleQueued(sys, msg.Cause(), msg.Context())
			return
		}
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeCompleted, "confirmed"
		w.Result = append(json.RawMessage(nil), req.Result...)
	} else {
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeFailed, req.ExecutionState
		if req.State == "timeout" {
			req.ErrorCode = "timeout"
		}
		if req.State == "execution_unknown" {
			req.ErrorCode = "execution_unknown"
		}
		w.Result = mustJSON(map[string]any{"error_code": req.ErrorCode, "detail": req.Detail})
	}
	w.UpdatedAt = nowMillis()
	w.BoundaryID = string(msg.ID)
	s.LastBoundary = string(msg.ID)
	s.ForkPoint = string(msg.ID)
	c.emit(sys, msg.Cause(), msg.Context(), "agent.work.closed", map[string]any{"work": publicWork(w), "result": json.RawMessage(w.Result)})
	c.finishWaiters(sys, w)
	_, _ = sys.Reply(msg, map[string]any{"disposition": "adopted", "work_id": w.ID, "state": w.State})
	c.scheduleQueued(sys, msg.Cause(), msg.Context())
}

func terminalTurnStateNative(state string) bool {
	switch state {
	case "completed", "failed", "cancelled", "timeout", "execution_unknown":
		return true
	}
	return false
}

func (c *controller) sessionResult(sys actorbase.Sys, session, turn string, upto message.ID) json.RawMessage {
	rows, head, err := nativeSessionRows(sys, session)
	if err != nil {
		return nil
	}
	limit, start := int64(^uint64(0)>>1), int64(0)
	for _, row := range rows {
		if row.ID == upto {
			limit = row.Seq
		}
		if row.Kind == message.KindRequest && row.MessageType == agentloop.TypeStart {
			body := nativeRowBody(sys, row, head)
			var req agentloop.StartRequest
			if json.Unmarshal(body, &req) == nil && req.TurnID == turn {
				start = row.Seq
			}
		}
	}
	requests := map[message.ID]bool{}
	for _, row := range rows {
		if row.Seq < start || row.Seq > limit || row.Kind != message.KindRequest || row.MessageType != llmproto.TypeGenerate {
			continue
		}
		var req llmproto.GenerateRequest
		_ = json.Unmarshal(nativeRowBody(sys, row, head), &req)
		requests[row.ID] = req.Purpose != "compact"
	}
	var latest logMessage
	var latestGenerated struct {
		Message  json.RawMessage `json:"message"`
		Provider string          `json:"provider"`
		Model    string          `json:"model"`
		Usage    json.RawMessage `json:"usage"`
	}
	total := map[string]float64{}
	for _, row := range rows {
		if row.Seq < start || row.Seq > limit || row.Kind != message.KindResponse || row.MessageType != llmproto.TypeGenerate || !requests[row.ParentID] {
			continue
		}
		var generated struct {
			Message  json.RawMessage `json:"message"`
			Provider string          `json:"provider"`
			Model    string          `json:"model"`
			Usage    json.RawMessage `json:"usage"`
		}
		if json.Unmarshal(nativeRowBody(sys, row, head), &generated) != nil || len(generated.Message) == 0 {
			continue
		}
		if len(generated.Usage) > 0 {
			accumulateUsage(total, generated.Usage)
		}
		if row.Seq > latest.Seq {
			latest, latestGenerated = row, generated
		}
	}
	if latest.Seq == 0 {
		return nil
	}
	_, answer, _ := assistantPartsNative(latestGenerated.Message)
	result := map[string]any{"text": answer, "message": latestGenerated.Message, "provider": latestGenerated.Provider, "model": latestGenerated.Model}
	if len(latestGenerated.Usage) > 0 {
		result["usage"] = latestGenerated.Usage
	}
	if len(total) > 0 {
		usageTotal, cost := map[string]any{}, map[string]float64{}
		for key, value := range total {
			if strings.HasPrefix(key, "cost.") {
				cost[strings.TrimPrefix(key, "cost.")] = value
			} else {
				usageTotal[key] = value
			}
		}
		if len(cost) > 0 {
			usageTotal["cost"] = cost
		}
		result["usage_total"] = usageTotal
	}
	return mustJSON(result)
}

func nativeSessionRows(sys actorbase.Sys, session string) ([]logMessage, int64, error) {
	snapshot, err := sys.View().Read(sys.Life(), actorcaps.LedgerRead{Session: session})
	if err != nil {
		return nil, 0, err
	}
	return nativeLedgerRows(snapshot.Rows), snapshot.HeadSeq, nil
}

func nativeRowBody(_ actorbase.Sys, row logMessage, _ int64) json.RawMessage {
	_, body, _ := harness.UnwrapPayload(json.RawMessage(row.PayloadText))
	return body
}

func accumulateUsage(total map[string]float64, raw json.RawMessage) {
	var usage map[string]any
	if json.Unmarshal(raw, &usage) != nil {
		return
	}
	for _, key := range []string{"input", "output", "cache_read", "cache_write", "total", "context_tokens"} {
		if n, ok := usage[key].(float64); ok {
			total[key] += n
		}
	}
	if cost, ok := usage["cost"].(map[string]any); ok {
		for key, value := range cost {
			if n, ok := value.(float64); ok {
				total["cost."+key] += n
			}
		}
	}
}

func assistantPartsNative(raw json.RawMessage) ([]json.RawMessage, string, error) {
	var m struct {
		Content []json.RawMessage `json:"content"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return nil, "", errors.New("invalid assistant")
	}
	var text string
	for _, b := range m.Content {
		var p struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		_ = json.Unmarshal(b, &p)
		if p.Type == "text" {
			text += p.Text
		}
	}
	return m.Content, text, nil
}

func resultPayload(w *workRecord) agentproto.ResultResponse {
	response := agentproto.ResultResponse{WorkID: w.ID, SessionID: w.SessionID, State: w.State, Outcome: w.Outcome, Result: append(json.RawMessage(nil), w.Result...), Next: nextFor(w)}
	var resultFields map[string]json.RawMessage
	if json.Unmarshal(w.Result, &resultFields) == nil {
		response.Usage = append(json.RawMessage(nil), resultFields["usage"]...)
		response.UsageTotal = append(json.RawMessage(nil), resultFields["usage_total"]...)
	}
	if w.State == agentproto.WorkOpen {
		response.Guidance = "This work is still open; no result is implied. Call agent.result again later, inspect agent.status, or use the targeted interrupt action."
	} else {
		response.Guidance = "This is the terminal result held by the current Controller process."
	}
	return response
}

func statusPayload(w *workRecord) agentproto.StatusResponse {
	return agentproto.StatusResponse{Work: ptrWork(detailedWork(w)), Guidance: "Use work_id as the stable address. inputs[].disposition reconciles accepted steering with what a completed episode actually included; work and execution state describe only this Controller process; after restart, submit a new request.", Next: nextFor(w)}
}

func nextFor(w *workRecord) []agentproto.NextAction {
	next := []agentproto.NextAction{nextAction(agentproto.TypeResult, "读取结果", map[string]any{"work_id": w.ID}), nextAction(agentproto.TypeStatus, "查看状态", map[string]any{"work_id": w.ID})}
	if w.State == agentproto.WorkOpen {
		next = append(next, nextAction(agentproto.TypeInterrupt, "停止", map[string]any{"work_id": w.ID}))
	}
	return next
}

func nextAction(word, label string, payload any) agentproto.NextAction {
	return agentproto.NextAction{Word: word, Label: label, Payload: mustJSON(payload)}
}

func (c *controller) finishWaiters(sys actorbase.Sys, w *workRecord) {
	// Completed receipts keep result data, not the originating request scope.
	w.SourceCause = message.Cause{}
	w.SourceContext = harness.Context{}
	held := c.wait[w.ID]
	delete(c.wait, w.ID)
	for _, msg := range held {
		_, _ = sys.Reply(msg, resultPayload(w))
	}
}
func ptrWork(w agentproto.Work) *agentproto.Work { return &w }
func mustJSON(v any) json.RawMessage             { raw, _ := json.Marshal(v); return raw }
func actorID(s string) actor.ActorID             { return actor.ActorID(s) }

func inputRecordsSize(inputs []inputRecord) int {
	raw, _ := json.Marshal(inputs)
	return len(raw)
}

type statusCursor struct {
	CreatedAt int64             `json:"created_at"`
	WorkID    agentproto.WorkID `json:"work_id"`
}

func encodeCursor(w *workRecord) string {
	raw, _ := json.Marshal(statusCursor{CreatedAt: w.CreatedAt, WorkID: w.ID})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(s string, visible []*workRecord) (int, error) {
	if s == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return 0, fmt.Errorf("status.cursor is invalid")
	}
	var cursor statusCursor
	if json.Unmarshal(raw, &cursor) != nil || cursor.WorkID == "" {
		return 0, fmt.Errorf("status.cursor is invalid")
	}
	for i, w := range visible {
		if w.ID == cursor.WorkID && w.CreatedAt == cursor.CreatedAt {
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("status.cursor does not belong to this caller-visible work list")
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
