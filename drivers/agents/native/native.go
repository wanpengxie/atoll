package native

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

const stateKey resource.ResourceID = "native-agent.work-projection.v1"
const maxWorkInputBytes = 4 << 20
const maxRecoveryRecordBytes = 14 << 20

type controller struct {
	cfg  Config
	data snapshot
	wait map[agentproto.WorkID][]actorbase.Msg
}

func run(sys actorbase.Sys, cfg Config) error {
	c := &controller{cfg: cfg, data: newSnapshot(), wait: map[agentproto.WorkID][]actorbase.Msg{}}
	if err := c.recoverLedger(sys); err != nil {
		return err
	}
	if err := c.reconcileAfterRestart(sys); err != nil {
		return err
	}
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
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
			c.handleSteer(sys, msg)
		case agentproto.TypeInterrupt:
			c.handleInterrupt(sys, msg)
		case agentloop.TypeReport:
			c.handleReport(sys, msg)
		case waitClosedType:
			c.handleWaitClosed(sys, msg)
		default:
			_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("native Agent does not answer %q", msg.Type))
		}
	}
}

const recoveryEventType = "agent.work.snapshot"
const waitClosedType = "agent.internal.wait_closed"

type recoveryRecord struct {
	Version int         `json:"version"`
	Work    *workRecord `json:"work"`
}

// commit appends the Controller decision to the Channel ledger before it is
// allowed to drive another Actor. actor_state is only a same-incarnation cache:
// its authority includes the live incarnation and therefore cannot be the
// restart source of truth.
func (c *controller) commit(sys actorbase.Sys, cause message.Cause, w *workRecord) error {
	record := recoveryRecord{Version: 1, Work: cloneRecoveryWork(w)}
	spec, err := behavior.EventSpecJSON(cause, recoveryEventType, record)
	if err != nil {
		return err
	}
	if len(spec.Payload) > maxRecoveryRecordBytes {
		return fmt.Errorf("work recovery snapshot exceeds %d bytes", maxRecoveryRecordBytes)
	}
	if _, err := sys.Emit(spec); err != nil {
		return fmt.Errorf("append work decision: %w", err)
	}
	_ = c.persist(sys)
	return nil
}

func cloneRecoveryWork(w *workRecord) *workRecord {
	out := cloneWork(w)
	// Context is needed for a known, not-yet-dispatched continuation and for an
	// explicit future branch from a completed checkpoint. An in-flight
	// assignment is never replayed, while a fresh unbranched work can rebuild
	// its first context from Inputs.
	if !(out.State == agentproto.WorkClosed || (out.Stage == "queued" && out.Continuation)) {
		out.Context = nil
	}
	return out
}

func cloneWork(w *workRecord) *workRecord {
	raw, _ := json.Marshal(w)
	var out workRecord
	_ = json.Unmarshal(raw, &out)
	return &out
}

type logQueryResponse struct {
	Turns         []logQueryTurn `json:"turns"`
	HeadSeq       int64          `json:"head_seq"`
	NextBeforeSeq int64          `json:"next_before_seq"`
	HasMore       bool           `json:"has_more"`
	Message       *logMessage    `json:"message,omitempty"`
}
type logQueryTurn struct {
	Messages []logMessage `json:"messages"`
}
type logMessage struct {
	Seq         int64          `json:"seq"`
	Sender      message.Sender `json:"sender"`
	MessageType string         `json:"message_type"`
	PayloadText string         `json:"payload_text"`
	NextOffset  *int           `json:"next_offset,omitempty"`
	Truncated   bool           `json:"truncated"`
}

func (c *controller) recoverLedger(sys actorbase.Sys) error {
	seen := map[string]bool{}
	before, head := int64(0), int64(0)
	for {
		req := map[string]any{"view": "raw", "message_type": recoveryEventType, "limit": 20}
		if before > 0 {
			req["before_seq"] = before
		}
		if head > 0 {
			req["head_seq"] = head
		}
		response, err := callSystemQuery(sys, req)
		if err != nil {
			return fmt.Errorf("recover work ledger: %w", err)
		}
		if head == 0 {
			head = response.HeadSeq
		}
		for _, turn := range response.Turns {
			for _, item := range turn.Messages {
				if item.MessageType != recoveryEventType || !sameSeat(item.Sender.ID.String(), sys.Self().String()) {
					continue
				}
				text := item.PayloadText
				if item.Truncated {
					text, err = readLogMessage(sys, item.Seq, head)
					if err != nil {
						return err
					}
				}
				var record recoveryRecord
				if json.Unmarshal([]byte(text), &record) != nil || record.Version != 1 || record.Work == nil || record.Work.ID == "" {
					return fmt.Errorf("recover work ledger: invalid snapshot at seq %d", item.Seq)
				}
				id := string(record.Work.ID)
				if seen[id] {
					continue
				}
				seen[id] = true
				c.data.Works[id] = record.Work
			}
		}
		if !response.HasMore {
			break
		}
		if response.NextBeforeSeq <= 0 || response.NextBeforeSeq == before {
			return errors.New("recover work ledger: pagination made no progress")
		}
		before = response.NextBeforeSeq
	}
	for id := range c.data.Works {
		c.data.Order = append(c.data.Order, id)
	}
	sort.Slice(c.data.Order, func(i, j int) bool {
		left, right := c.data.Works[c.data.Order[i]], c.data.Works[c.data.Order[j]]
		if left.CreatedAt == right.CreatedAt {
			return left.ID < right.ID
		}
		return left.CreatedAt < right.CreatedAt
	})
	return c.recoverReports(sys)
}

// recoverReports closes the crash window where a Looper durably posted its
// assignment report but the old Controller died before adopting it. A report
// can affect only the exact assignment still recorded as current; processed
// reports are therefore naturally idempotent.
func (c *controller) recoverReports(sys actorbase.Sys) error {
	seen := map[string]bool{}
	before, head := int64(0), int64(0)
	for {
		req := map[string]any{"view": "raw", "message_type": agentloop.TypeReport, "limit": 20}
		if before > 0 {
			req["before_seq"] = before
		}
		if head > 0 {
			req["head_seq"] = head
		}
		response, err := callSystemQuery(sys, req)
		if err != nil {
			return fmt.Errorf("recover looper reports: %w", err)
		}
		if head == 0 {
			head = response.HeadSeq
		}
		for _, turn := range response.Turns {
			for _, item := range turn.Messages {
				if item.MessageType != agentloop.TypeReport {
					continue
				}
				text := item.PayloadText
				if item.Truncated {
					text, err = readLogMessage(sys, item.Seq, head)
					if err != nil {
						return err
					}
				}
				var wrapped struct {
					Body json.RawMessage `json:"body"`
				}
				if json.Unmarshal([]byte(text), &wrapped) != nil || len(wrapped.Body) == 0 {
					continue
				}
				var report agentloop.ReportRequest
				if json.Unmarshal(wrapped.Body, &report) != nil || report.WorkID == "" || seen[string(report.WorkID)] {
					continue
				}
				w := c.data.Works[string(report.WorkID)]
				if w == nil || w.State != agentproto.WorkOpen || w.AssignmentID != report.AssignmentID || !targetMatches(w.Looper, item.Sender.ID.String()) {
					continue
				}
				seen[string(report.WorkID)] = true
				if report.State == "accepted" {
					continue
				}
				if err := c.adoptRecoveredReport(sys, w, report); err != nil {
					return err
				}
			}
		}
		if !response.HasMore {
			return nil
		}
		if response.NextBeforeSeq <= 0 || response.NextBeforeSeq == before {
			return errors.New("recover looper reports: pagination made no progress")
		}
		before = response.NextBeforeSeq
	}
}

func (c *controller) adoptRecoveredReport(sys actorbase.Sys, w *workRecord, req agentloop.ReportRequest) error {
	if req.ConsumedThrough < 0 || req.ConsumedThrough > w.AssignedThrough {
		return errors.New("recover looper reports: input fence violation")
	}
	for i := range w.Inputs {
		if w.Inputs[i].Seq <= req.ConsumedThrough {
			w.Inputs[i].Disposition = "included"
		}
	}
	if len(req.History) > 0 {
		for _, item := range req.History {
			if !json.Valid(item) {
				return errors.New("recover looper reports: invalid history")
			}
		}
		if historySize(req.History) > agentloop.MaxHistoryBytes {
			return errors.New("recover looper reports: history exceeds recovery limit")
		}
		w.Context = append([]json.RawMessage(nil), req.History...)
	}
	switch req.State {
	case "completed":
		if len(req.Result) == 0 || !json.Valid(req.Result) {
			return errors.New("recover looper reports: completed report has invalid result")
		}
		pending := false
		for _, input := range w.Inputs {
			pending = pending || input.Disposition != "included"
		}
		if pending {
			w.AssignmentID, w.AssignedThrough, w.Looper = "", 0, ""
			w.Stage, w.ExecutionState, w.Continuation = "queued", "not_started", true
		} else {
			w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeCompleted, "confirmed"
			w.Result, w.Continuation = append(json.RawMessage(nil), req.Result...), false
		}
	case "cancelled":
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeCancelled, req.ExecutionState
	case "failed":
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeFailed, req.ExecutionState
		w.Result = mustJSON(map[string]any{"error_code": req.ErrorCode, "detail": req.Detail})
	default:
		return errors.New("recover looper reports: invalid terminal state")
	}
	w.UpdatedAt = nowMillis()
	return c.commit(sys, message.Root(), w)
}

func callSystemQuery(sys actorbase.Sys, payload any) (logQueryResponse, error) {
	pending, err := sys.Call(message.Root(), actor.SystemActorID, message.TypeSystemLogQuery, payload)
	if err != nil {
		return logQueryResponse{}, err
	}
	terminal, err := pending.Wait(sys.Life(), 0)
	if err != nil {
		_ = pending.Cancel()
		return logQueryResponse{}, err
	}
	var status struct {
		Status string `json:"status"`
		message.Failure
	}
	if json.Unmarshal(terminal.Payload, &status) != nil || status.Status != "completed" {
		return logQueryResponse{}, fmt.Errorf("%s: %s", status.ErrorCode, status.Detail)
	}
	var response logQueryResponse
	if err := json.Unmarshal(terminal.Payload, &response); err != nil {
		return logQueryResponse{}, err
	}
	return response, nil
}

func readLogMessage(sys actorbase.Sys, seq, head int64) (string, error) {
	var text strings.Builder
	offset := 0
	for part := 0; part < 10000; part++ {
		response, err := callSystemQuery(sys, map[string]any{"view": "raw", "read_seq": seq, "head_seq": head, "offset": offset})
		if err != nil {
			return "", err
		}
		if response.Message == nil {
			return "", errors.New("recover work ledger: read returned no message")
		}
		text.WriteString(response.Message.PayloadText)
		if response.Message.NextOffset == nil {
			return text.String(), nil
		}
		if *response.Message.NextOffset <= offset {
			return "", errors.New("recover work ledger: read made no progress")
		}
		offset = *response.Message.NextOffset
	}
	return "", errors.New("recover work ledger: message part limit exceeded")
}

func sameSeat(left, right string) bool {
	a, b := strings.Split(left, ":"), strings.Split(right, ":")
	return len(a) == 3 && len(b) == 3 && a[0] == b[0] && a[1] == b[1]
}

func (c *controller) persist(sys actorbase.Sys) error {
	raw, err := json.Marshal(c.data)
	if err != nil {
		return err
	}
	out, err := sys.State().Put(stateKey, raw)
	if err != nil {
		return err
	}
	if !out.Accepted() {
		return fmt.Errorf("state put rejected: %s", out.RejectReason)
	}
	return nil
}

func (c *controller) emit(sys actorbase.Sys, cause message.Cause, typ string, value any) {
	spec, err := behavior.EventSpecJSON(cause, typ, value)
	if err == nil {
		_, _ = sys.Emit(spec)
	}
}

func (c *controller) reconcileAfterRestart(sys actorbase.Sys) error {
	for _, id := range c.data.Order {
		w := c.data.Works[id]
		if w.State != agentproto.WorkOpen {
			continue
		}
		if w.AssignmentID != "" || w.Stage != "queued" || (w.Continuation && len(w.Context) == 0) {
			w.Stage = "blocked"
			if w.Continuation && w.AssignmentID == "" {
				w.ExecutionState = "continuation_context_unavailable_after_restart"
			} else {
				w.ExecutionState = "unknown_after_restart"
			}
			w.UpdatedAt = nowMillis()
			if err := c.commit(sys, message.Root(), w); err != nil {
				return err
			}
			c.emit(sys, message.Root(), "agent.work.recovery_required", publicWork(w))
		}
	}
	c.scheduleQueued(sys, message.Root())
	return nil
}

func (c *controller) handleAsk(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := agentproto.DecodeAsk(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	caller := actorbase.EffectiveCaller(msg)
	if existing := c.data.findSubmission(caller, req.SubmissionKey); existing != nil {
		if existing.SubmissionHash != submissionHash(req) {
			_, _ = sys.Fail(msg, "submission_conflict", "submission_key already names different input")
			return
		}
		if req.Delivery == agentproto.DeliveryWait && existing.State == agentproto.WorkOpen {
			c.attachWaiter(sys, msg, existing)
		} else {
			c.answerAsk(sys, msg, existing)
		}
		return
	}
	if c.data.openCount() >= c.cfg.MaxOpenWorks {
		_, _ = sys.Fail(msg, "capacity", "the Agent has reached max_open_works")
		return
	}
	var prior []json.RawMessage
	if req.RelatedWorkID != "" {
		related, ok := c.owned(msg, req.RelatedWorkID)
		if !ok {
			_, _ = sys.Fail(msg, "work_not_found", "no visible related work has that id")
			return
		}
		if len(related.Context) == 0 {
			_, _ = sys.Fail(msg, "context_unavailable", "the related work has no durable context checkpoint to branch from")
			return
		}
		prior = append([]json.RawMessage(nil), related.Context...)
	}
	now := nowMillis()
	w := &workRecord{ID: newWorkID(), Owner: caller, SourceRequest: string(msg.ID), SubmissionKey: req.SubmissionKey,
		SubmissionHash: submissionHash(req), RelatedWorkID: req.RelatedWorkID, Delivery: req.Delivery,
		State: agentproto.WorkOpen, Stage: "queued", ExecutionState: "not_started", CreatedAt: now, UpdatedAt: now}
	if len(prior) > 0 {
		w.Context = prior
		w.Continuation = true
	}
	w.Inputs = []inputRecord{{Input: agentloop.Input{
		ID: newInputID(), Seq: 1, Text: req.Text,
		Attachments:   append([]json.RawMessage(nil), req.Attachments...),
		CallerChannel: caller.Channel, CallerActor: caller.Actor, Origin: req.Origin,
	}, Disposition: "accepted"}}
	if inputRecordsSize(w.Inputs) > maxWorkInputBytes {
		_, _ = sys.Fail(msg, "limit_exceeded", "the work input exceeds the phase-one recovery limit")
		return
	}
	c.data.Works[string(w.ID)] = w
	c.data.Order = append(c.data.Order, string(w.ID))
	if err := c.commit(sys, msg.Cause(), w); err != nil {
		delete(c.data.Works, string(w.ID))
		c.data.Order = c.data.Order[:len(c.data.Order)-1]
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	c.emit(sys, msg.Cause(), "agent.work.accepted", map[string]any{"work": publicWork(w), "owner": caller, "input_id": w.Inputs[0].ID})
	if req.Delivery == agentproto.DeliveryWait {
		c.attachWaiter(sys, msg, w)
	} else {
		c.answerAsk(sys, msg, w)
	}
	c.scheduleQueued(sys, msg.Cause())
}

func (c *controller) attachWaiter(sys actorbase.Sys, msg actorbase.Msg, w *workRecord) {
	c.wait[w.ID] = append(c.wait[w.ID], msg)
	_, _ = sys.Progress(msg, w.Stage, map[string]any{"work_id": w.ID, "work_state": w.State, "stage": w.Stage, "execution_state": w.ExecutionState, "controls": []map[string]any{{"word": agentproto.TypeInterrupt, "label": "停止", "payload": map[string]any{"work_id": w.ID}}}})
	go func(ctx context.Context, life context.Context, self actor.ActorID, cause message.Cause, workID agentproto.WorkID, requestID message.ID) {
		select {
		case <-ctx.Done():
			if life.Err() == nil {
				_, _ = sys.Post(behavior.RequestSpec{Cause: cause, Type: waitClosedType, Audience: message.Audience{self}, Payload: mustJSON(map[string]any{"work_id": workID, "request_id": requestID})})
			}
		case <-life.Done():
		}
	}(msg.Ctx(), sys.Life(), sys.Self(), msg.Cause(), w.ID, msg.ID)
}

func (c *controller) handleWaitClosed(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Sender.ID != sys.Self() {
		_, _ = sys.Fail(msg, "permission_denied", "wait lifecycle reports are Controller-internal")
		return
	}
	var req struct {
		WorkID    agentproto.WorkID `json:"work_id"`
		RequestID message.ID        `json:"request_id"`
	}
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil || req.WorkID == "" || req.RequestID == "" {
		_, _ = sys.Fail(msg, "invalid_args", "work_id and request_id are required")
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
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
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
	_, _ = sys.Reply(msg, map[string]any{"disposition": "accepted", "work_id": w.ID, "work_state": w.State,
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

func (c *controller) scheduleQueued(sys actorbase.Sys, cause message.Cause) {
	for _, id := range c.data.Order {
		w := c.data.Works[id]
		if w == nil || w.State != agentproto.WorkOpen || w.Stage != "queued" || w.AssignmentID != "" {
			continue
		}
		if !c.dispatch(sys, w, cause) {
			return
		}
	}
}

func (c *controller) dispatch(sys actorbase.Sys, w *workRecord, cause message.Cause) bool {
	if w.State != agentproto.WorkOpen || len(c.cfg.Loopers) == 0 {
		return false
	}
	looper, ok := c.freeLooper()
	if !ok {
		if w.ExecutionState != "waiting_capacity" {
			old := w.ExecutionState
			w.ExecutionState, w.UpdatedAt = "waiting_capacity", nowMillis()
			if err := c.commit(sys, cause, w); err != nil {
				w.ExecutionState = old
			}
		}
		return false
	}
	old := cloneWork(w)
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
	if err := c.commit(sys, cause, w); err != nil {
		*w = *old
		return false
	}
	inputs := make([]agentloop.Input, 0, len(w.Inputs))
	for i := range w.Inputs {
		if w.Inputs[i].Disposition == "assigned" {
			inputs = append(inputs, w.Inputs[i].Input)
		}
	}
	var tools *[]agentloop.ToolBinding
	if c.cfg.ToolsConfigured {
		bindings := make([]agentloop.ToolBinding, len(c.cfg.Tools))
		for i, tool := range c.cfg.Tools {
			bindings[i] = agentloop.ToolBinding{Name: tool.Name, Actor: tool.Actor, Word: tool.Word}
		}
		tools = &bindings
	}
	_, err := sys.Post(behavior.RequestSpec{Cause: cause, Type: agentloop.TypeStart, Audience: message.Audience{actorID(looper)}, Payload: mustJSON(agentloop.StartRequest{
		WorkID: w.ID, AssignmentID: w.AssignmentID, ControllerActor: string(sys.Self()), Inputs: inputs, Prior: append([]json.RawMessage(nil), w.Context...), ContextActor: c.cfg.ContextActor, LLMActor: c.cfg.LLMActor,
		WorkspaceActor: c.cfg.WorkspaceActor, HostActor: c.cfg.HostActor, Model: c.cfg.Model, MaxTurns: c.cfg.MaxTurns, Tools: tools,
		ToolResultMaxLines: c.cfg.ToolResultMaxLines, ToolResultMaxBytes: c.cfg.ToolResultMaxBytes, ToolImageMaxBytes: c.cfg.ToolImageMaxBytes})})
	if err != nil {
		w.AssignmentID, w.Looper = "", ""
		w.Stage, w.ExecutionState, w.UpdatedAt = "blocked", "dispatch_failed", nowMillis()
		_ = c.commit(sys, cause, w)
		c.emit(sys, cause, "agent.work.dispatch_failed", map[string]any{"work_id": w.ID, "detail": err.Error()})
		return true
	}
	// A successful Post proves only that the request entered the channel. The
	// looper reports "accepted" separately; until then status remains honest.
	c.emit(sys, cause, "agent.work.dispatched", publicWork(w))
	return true
}

func (c *controller) owned(msg actorbase.Msg, id agentproto.WorkID) (*workRecord, bool) {
	w := c.data.Works[string(id)]
	return w, w != nil && workScopeKey(w.Owner) == workScopeKey(actorbase.EffectiveCaller(msg))
}

func (c *controller) handleStatus(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := agentproto.DecodeStatus(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	caller := actorbase.EffectiveCaller(msg)
	if req.WorkID != "" {
		w, ok := c.owned(msg, req.WorkID)
		if !ok {
			_, _ = sys.Fail(msg, "work_not_found", "no visible work has that id")
			return
		}
		_, _ = sys.Reply(msg, statusPayload(w))
		return
	}
	if req.SubmissionKey != "" {
		w := c.data.findSubmission(caller, req.SubmissionKey)
		if w == nil {
			_, _ = sys.Fail(msg, "work_not_found", "no visible work has that submission_key")
			return
		}
		_, _ = sys.Reply(msg, statusPayload(w))
		return
	}
	visible := c.data.visible(caller)
	start, err := decodeCursor(req.Cursor, visible)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
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
	response := agentproto.StatusResponse{Works: works, NextCursor: next, Guidance: "This is a caller-scoped work page. Use work_id with agent.status or agent.result; follow next_cursor without changing the query."}
	if next != "" {
		response.Next = []agentproto.NextAction{nextAction(agentproto.TypeStatus, "下一页", map[string]any{"cursor": next, "limit": req.Limit})}
	}
	_, _ = sys.Reply(msg, response)
}

func (c *controller) handleResult(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := agentproto.DecodeResult(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	w, ok := c.owned(msg, req.WorkID)
	if !ok {
		_, _ = sys.Fail(msg, "work_not_found", "no visible work has that id")
		return
	}
	_, _ = sys.Reply(msg, resultPayload(w))
}

func (c *controller) handleSteer(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := agentproto.DecodeSteer(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if req.All || req.Target != "" || req.ExpectedTurnID != "" {
		_, _ = sys.Fail(msg, "unsupported_scope", "native Agent phase one accepts work-addressed text steering only")
		return
	}
	w, ok := c.owned(msg, req.WorkID)
	if !ok {
		_, _ = sys.Fail(msg, "work_not_found", "no visible work has that id")
		return
	}
	opKey := operationIndexKey(actorbase.EffectiveCaller(msg), req.OperationKey)
	opHash := requestHash(struct {
		WorkID agentproto.WorkID `json:"work_id"`
		Text   string            `json:"text"`
	}{req.WorkID, req.Text})
	if req.OperationKey != "" && w.Operations != nil {
		if prior, found := w.Operations[opKey]; found {
			if prior.Kind != agentproto.TypeSteer || prior.Hash != opHash {
				_, _ = sys.Fail(msg, "operation_conflict", "operation_key already names a different control")
				return
			}
			_, _ = sys.Reply(msg, json.RawMessage(prior.Response))
			return
		}
	}
	if w.State == agentproto.WorkClosed {
		_, _ = sys.Fail(msg, "work_closed", "the work is already closed")
		return
	}
	if c.cfg.MaxInputsPerWork > 0 && len(w.Inputs) >= c.cfg.MaxInputsPerWork {
		_, _ = sys.Fail(msg, "limit_exceeded", "the work has reached max_inputs_per_work")
		return
	}
	if req.OperationKey != "" && c.cfg.MaxOperationKeys > 0 && len(w.Operations) >= c.cfg.MaxOperationKeys {
		_, _ = sys.Fail(msg, "limit_exceeded", "the work has reached max_operation_keys")
		return
	}
	seq := int64(len(w.Inputs) + 1)
	caller := actorbase.EffectiveCaller(msg)
	in := inputRecord{Input: agentloop.Input{ID: newInputID(), Seq: seq, Text: req.Text, CallerChannel: caller.Channel, CallerActor: caller.Actor}, Disposition: "accepted"}
	if inputRecordsSize(append(append([]inputRecord(nil), w.Inputs...), in)) > maxWorkInputBytes {
		_, _ = sys.Fail(msg, "limit_exceeded", "the work input exceeds the phase-one recovery limit")
		return
	}
	old := cloneWork(w)
	w.Inputs = append(w.Inputs, in)
	if w.AssignmentID != "" {
		w.AssignedThrough = seq
	}
	w.UpdatedAt = nowMillis()
	response := mustJSON(map[string]any{"work_id": w.ID, "input_id": in.ID, "disposition": "accepted", "included": false})
	if req.OperationKey != "" {
		if w.Operations == nil {
			w.Operations = map[string]operationRecord{}
		}
		w.Operations[opKey] = operationRecord{Kind: agentproto.TypeSteer, Hash: opHash, Response: response}
	}
	if err := c.commit(sys, msg.Cause(), w); err != nil {
		*w = *old
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	c.emit(sys, msg.Cause(), "agent.work.input_accepted", map[string]any{"work_id": w.ID, "input_id": in.ID, "seq": seq})
	if w.AssignmentID != "" {
		_, _ = sys.Post(behavior.RequestSpec{Cause: msg.Cause(), Type: agentloop.TypeInput, Audience: message.Audience{actorID(w.Looper)}, Payload: mustJSON(agentloop.InputRequest{WorkID: w.ID, AssignmentID: w.AssignmentID, Input: in.Input})})
	}
	_, _ = sys.Reply(msg, json.RawMessage(response))
}

func (c *controller) handleInterrupt(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := agentproto.DecodeInterrupt(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	if req.WorkID == "" {
		if req.OperationKey != "" {
			_, _ = sys.Fail(msg, "unsupported_scope", "phase-one Agent-wide interrupt does not persist operation_key; address one work")
			return
		}
		count := 0
		caller := actorbase.EffectiveCaller(msg)
		for _, w := range c.data.Works {
			if w.State == agentproto.WorkOpen && workScopeKey(w.Owner) == workScopeKey(caller) {
				if err := c.stop(sys, msg, w, false); err != nil {
					_, _ = sys.Fail(msg, "ledger_unavailable", "interrupt partially applied before the ledger failed: "+err.Error())
					return
				}
				count++
			}
		}
		c.scheduleQueued(sys, msg.Cause())
		_, _ = sys.Reply(msg, map[string]any{"scope": "agent", "stop_requested": count})
		return
	}
	w, ok := c.owned(msg, req.WorkID)
	if !ok {
		_, _ = sys.Fail(msg, "work_not_found", "no visible work has that id")
		return
	}
	opKey := operationIndexKey(actorbase.EffectiveCaller(msg), req.OperationKey)
	opHash := requestHash(struct {
		WorkID agentproto.WorkID `json:"work_id"`
	}{req.WorkID})
	if req.OperationKey != "" && w.Operations != nil {
		if prior, found := w.Operations[opKey]; found {
			if prior.Kind != agentproto.TypeInterrupt || prior.Hash != opHash {
				_, _ = sys.Fail(msg, "operation_conflict", "operation_key already names a different control")
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
		_, _ = sys.Fail(msg, "limit_exceeded", "the work has reached max_operation_keys")
		return
	}
	if err := c.stop(sys, msg, w, true); err != nil {
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	response := mustJSON(map[string]any{"work_id": w.ID, "disposition": "stop_requested", "execution_state": w.ExecutionState})
	if req.OperationKey != "" {
		if w.Operations == nil {
			w.Operations = map[string]operationRecord{}
		}
		w.Operations[opKey] = operationRecord{Kind: agentproto.TypeInterrupt, Hash: opHash, Response: response}
		if err := c.commit(sys, msg.Cause(), w); err != nil {
			delete(w.Operations, opKey)
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
	}
	_, _ = sys.Reply(msg, json.RawMessage(response))
}

func (c *controller) stop(sys actorbase.Sys, msg actorbase.Msg, w *workRecord, schedule bool) error {
	if w.Stage == "stopping" {
		return nil
	}
	old := *w
	if w.AssignmentID == "" || w.ExecutionState == "unknown_after_restart" {
		w.State, w.Stage, w.Outcome, w.UpdatedAt = agentproto.WorkClosed, "", agentproto.OutcomeCancelled, nowMillis()
		if old.ExecutionState == "unknown_after_restart" {
			w.ExecutionState = "cancelled_after_unknown_restart"
		} else {
			w.ExecutionState = "not_started"
		}
		if err := c.commit(sys, msg.Cause(), w); err != nil {
			*w = old
			return err
		}
		c.emit(sys, msg.Cause(), "agent.work.closed", map[string]any{"work": publicWork(w), "result": json.RawMessage(w.Result)})
		c.finishWaiters(sys, w)
		if schedule {
			c.scheduleQueued(sys, msg.Cause())
		}
		return nil
	}
	w.Stage, w.ExecutionState, w.UpdatedAt = "stopping", "stop_requested", nowMillis()
	if err := c.commit(sys, msg.Cause(), w); err != nil {
		*w = old
		return err
	}
	_, _ = sys.Post(behavior.RequestSpec{Cause: msg.Cause(), Type: agentloop.TypeStop, Audience: message.Audience{actorID(w.Looper)}, Payload: mustJSON(agentloop.StopRequest{WorkID: w.ID, AssignmentID: w.AssignmentID, Reason: "agent.interrupt"})})
	c.emit(sys, msg.Cause(), "agent.work.stop_requested", publicWork(w))
	return nil
}

func (c *controller) handleReport(sys actorbase.Sys, msg actorbase.Msg) {
	var req agentloop.ReportRequest
	if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	w := c.data.Works[string(req.WorkID)]
	if w == nil || req.AssignmentID != w.AssignmentID || !targetMatches(w.Looper, msg.Sender.ID.String()) {
		_, _ = sys.Fail(msg, "stale_assignment", "report does not belong to the current assignment")
		return
	}
	if req.State != "accepted" && req.State != "completed" && req.State != "failed" && req.State != "cancelled" {
		_, _ = sys.Fail(msg, "invalid_args", "report.state must be accepted, completed, failed, or cancelled")
		return
	}
	if req.ConsumedThrough < 0 || req.ConsumedThrough > w.AssignedThrough {
		_, _ = sys.Fail(msg, "invalid_args", "report consumed_through exceeds the current assignment input fence")
		return
	}
	for _, item := range req.History {
		if !json.Valid(item) {
			_, _ = sys.Fail(msg, "invalid_args", "report history contains invalid JSON")
			return
		}
	}
	if historySize(req.History) > agentloop.MaxHistoryBytes {
		_, _ = sys.Fail(msg, "invalid_args", "report history exceeds the recovery limit")
		return
	}
	if req.State == "completed" && (len(req.Result) == 0 || !json.Valid(req.Result)) {
		_, _ = sys.Fail(msg, "invalid_args", "completed report requires a valid JSON result")
		return
	}
	old := cloneWork(w)
	if req.State == "accepted" {
		if w.Stage == "stopping" {
			_, _ = sys.Reply(msg, map[string]any{"disposition": "acknowledged", "work_id": w.ID, "stop_pending": true})
			return
		}
		w.Stage, w.ExecutionState, w.UpdatedAt = "thinking", "confirmed_running", nowMillis()
		if err := c.commit(sys, msg.Cause(), w); err != nil {
			*w = *old
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
		c.emit(sys, msg.Cause(), "agent.work.assignment_accepted", publicWork(w))
		_, _ = sys.Reply(msg, map[string]any{"disposition": "adopted", "work_id": w.ID, "state": w.State})
		return
	}
	for i := range w.Inputs {
		if w.Inputs[i].Seq <= req.ConsumedThrough {
			w.Inputs[i].Disposition = "included"
		}
	}
	if len(req.History) > 0 {
		w.Context = append([]json.RawMessage(nil), req.History...)
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
			w.AssignmentID = ""
			w.AssignedThrough = 0
			w.Looper = ""
			w.Stage = "queued"
			w.ExecutionState = "not_started"
			w.Continuation = true
			w.UpdatedAt = nowMillis()
			if err := c.commit(sys, msg.Cause(), w); err != nil {
				*w = *old
				_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
				return
			}
			_, _ = sys.Reply(msg, map[string]any{"disposition": "accepted_for_continuation"})
			c.scheduleQueued(sys, msg.Cause())
			return
		}
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeCompleted, "confirmed"
		w.Continuation = false
		w.Result = append(json.RawMessage(nil), req.Result...)
	} else {
		w.State, w.Stage, w.Outcome, w.ExecutionState = agentproto.WorkClosed, "", agentproto.OutcomeFailed, req.ExecutionState
		w.Result = mustJSON(map[string]any{"error_code": req.ErrorCode, "detail": req.Detail})
	}
	w.UpdatedAt = nowMillis()
	if err := c.commit(sys, msg.Cause(), w); err != nil {
		*w = *old
		_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
		return
	}
	c.emit(sys, msg.Cause(), "agent.work.closed", map[string]any{"work": publicWork(w), "result": json.RawMessage(w.Result)})
	c.finishWaiters(sys, w)
	_, _ = sys.Reply(msg, map[string]any{"disposition": "adopted", "work_id": w.ID, "state": w.State})
	c.scheduleQueued(sys, msg.Cause())
}

func resultPayload(w *workRecord) agentproto.ResultResponse {
	response := agentproto.ResultResponse{WorkID: w.ID, State: w.State, Outcome: w.Outcome, Result: append(json.RawMessage(nil), w.Result...), Next: nextFor(w)}
	if w.State == agentproto.WorkOpen {
		response.Guidance = "This work is still open; no result is implied. Call agent.result again later, inspect agent.status, or use the targeted interrupt action."
	} else {
		response.Guidance = "This is the durable terminal result for the addressed work."
	}
	return response
}

func statusPayload(w *workRecord) agentproto.StatusResponse {
	return agentproto.StatusResponse{Work: ptrWork(detailedWork(w)), Guidance: "Use work_id as the stable address. inputs[].disposition reconciles accepted steering with what a completed episode actually included; execution_state distinguishes confirmed execution from an unknown post-restart state.", Next: nextFor(w)}
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
	held := c.wait[w.ID]
	delete(c.wait, w.ID)
	for _, msg := range held {
		_, _ = sys.Reply(msg, resultPayload(w))
	}
}
func ptrWork(w agentproto.Work) *agentproto.Work { return &w }
func mustJSON(v any) json.RawMessage             { raw, _ := json.Marshal(v); return raw }
func actorID(s string) actor.ActorID             { return actor.ActorID(s) }

func historySize(history []json.RawMessage) int {
	total := 0
	for _, item := range history {
		total += len(item)
	}
	return total
}

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
