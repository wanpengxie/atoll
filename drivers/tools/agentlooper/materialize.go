package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	llmproto "github.com/wanpengxie/atoll/drivers/tools/pillm/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type ledgerRow struct {
	Seq      int64
	ID       message.ID
	Session  string
	Parent   message.ID
	Sender   actor.ActorID
	Audience message.Audience
	Kind     message.Kind
	Type     string
	Terminal bool
	Body     json.RawMessage
}

func validSessionBoundary(ctx context.Context, sys actorbase.Sys, cause message.Cause, ref agentloop.BoundaryRef) (bool, error) {
	ctx, cancel := newHistoryContext(ctx)
	defer cancel()
	if ref.Session == "" {
		return false, nil
	}
	if ref.At == "" {
		return true, nil
	}
	rows, err := readSessionRows(ctx, sys, cause, ref.Session)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if string(row.ID) != ref.At {
			continue
		}
		if ref.Session == "main" {
			return true, nil
		}
		switch row.Type {
		case agentloop.TypeSessionOpened, agentloop.TypeSessionCompact, agentloop.TypeSessionSync, agentloop.TypeSessionReset:
			return true, nil
		case agentloop.TypeReport:
			for _, state := range sessionTurnStates(rows) {
				if state.closed && state.boundary.ID == row.ID {
					return true, nil
				}
			}
			return false, nil
		}
		return false, nil
	}
	return false, nil
}

func materializeSession(ctx context.Context, sys actorbase.Sys, cause message.Cause, session string, upto message.ID) (agentbase.ContextObject, error) {
	ctx, cancel := newHistoryContext(ctx)
	defer cancel()
	if _, err := readLedgerRows(ctx, sys, cause, ""); err != nil {
		return agentbase.ContextObject{}, err
	}
	budget := ctx.Value(historyBudgetKey{}).(*historyBudget)
	expanded, err := sys.View().BuildSession(ctx, *budget.snapshot, session, upto)
	if err != nil {
		return agentbase.ContextObject{}, err
	}
	rows, err := decodeLedgerRows(expanded, "")
	if err != nil {
		return agentbase.ContextObject{}, err
	}
	object := agentbase.ContextObject{Messages: []json.RawMessage{}}
	// View supplies a flat chain: ancestor prefixes, then the requested session.
	// Render each segment independently so turn IDs and boundaries stay local.
	for i := 0; i < len(rows); {
		end := i + 1
		for end < len(rows) && rows[end].Session == rows[i].Session {
			end++
		}
		object, err = materializeRows(ctx, sys, rows[i:end], object)
		if err != nil {
			return agentbase.ContextObject{}, err
		}
		i = end
	}
	return object, nil
}

func materializeRows(ctx context.Context, sys actorbase.Sys, rows []ledgerRow, object agentbase.ContextObject) (agentbase.ContextObject, error) {
	for _, state := range sessionTurnStates(rows) {
		if state.accepted && !state.closed {
			return agentbase.ContextObject{}, errors.New("session has an accepted turn without a valid terminal boundary")
		}
	}
	if len(rows) == 0 {
		return agentbase.ContextObject{Messages: []json.RawMessage{}}, nil
	}
	byID := map[message.ID]ledgerRow{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	for _, row := range rows {
		visited := map[message.ID]bool{}
		for parent := row.ID; parent != ""; parent = byID[parent].Parent {
			if visited[parent] {
				return agentbase.ContextObject{}, errors.New("session history parent cycle")
			}
			visited[parent] = true
		}
	}
	startIndex := 0
	for i, r := range rows {
		switch r.Type {
		case agentloop.TypeSessionCompact:
			var x agentloop.Compact
			if json.Unmarshal(r.Body, &x) != nil || x.Context == nil {
				return object, errors.New("invalid session snapshot")
			}
			object.Messages = cloneMessages(x.Context)
			object.Version = r.ID
			startIndex = i + 1
		case agentloop.TypeSessionSync:
			var x agentloop.Synced
			if json.Unmarshal(r.Body, &x) != nil || x.Context == nil {
				return object, errors.New("invalid session snapshot")
			}
			object.Messages = cloneMessages(x.Context)
			object.Version = r.ID
			startIndex = i + 1
		case agentloop.TypeSessionReset:
			object.Messages = []json.RawMessage{}
			object.Version = r.ID
			startIndex = i + 1
		}
	}
	if startIndex == 0 {
		for _, r := range rows {
			if r.Type == agentloop.TypeSessionOpened {
				object.Version = r.ID
				break
			}
		}
	}
	type turn struct {
		start    ledgerRow
		req      agentloop.StartRequest
		boundary int64
	}
	turns := map[string]*turn{}
	states := sessionTurnStates(rows)
	for _, r := range rows {
		if r.Kind == message.KindRequest && r.Type == agentloop.TypeStart {
			var req agentloop.StartRequest
			if json.Unmarshal(r.Body, &req) == nil {
				turns[req.TurnID] = &turn{start: r, req: req}
			}
		}
	}
	for turnID, state := range states {
		if t := turns[turnID]; t != nil && state.closed {
			t.boundary = state.boundary.Seq
		}
	}
	responses := map[message.ID]ledgerRow{}
	for _, r := range rows {
		if r.Kind == message.KindResponse {
			responses[r.Parent] = r
		}
	}
	turnFor := func(r ledgerRow) *turn {
		for _, t := range turns {
			if r.ID == t.start.ID {
				return t
			}
			p := r.Parent
			visited := map[message.ID]bool{}
			for p != "" {
				if visited[p] {
					break
				}
				visited[p] = true
				if p == t.start.ID {
					return t
				}
				parent, ok := byID[p]
				if !ok {
					break
				}
				p = parent.Parent
			}
		}
		if r.Type == agentloop.TypeInput {
			var x agentloop.InputRequest
			if json.Unmarshal(r.Body, &x) == nil {
				return turns[x.TurnID]
			}
		}
		return nil
	}
	for i := startIndex; i < len(rows); i++ {
		r := rows[i]
		if err := ctx.Err(); err != nil {
			return object, err
		}
		if r.Session == "main" && r.Type == "session.merge" {
			var merge struct {
				Decision string          `json:"decision"`
				Summary  json.RawMessage `json:"summary"`
			}
			if json.Unmarshal(r.Body, &merge) != nil {
				return object, errors.New("invalid main merge")
			}
			if merge.Decision == "merged" {
				if len(merge.Summary) == 0 || string(merge.Summary) == "null" {
					return object, errors.New("main merge summary missing")
				}
				object.Messages = append(object.Messages, merge.Summary)
				object.Version = r.ID
			}
			continue
		}
		t := turnFor(r)
		if t == nil || t.boundary == 0 || r.Seq > t.boundary {
			continue
		}
		switch {
		case r.Type == agentloop.TypeStart && r.Kind == message.KindRequest:
			for _, id := range t.req.Inputs {
				inputRow, ok := byID[message.ID(id.ID)]
				if !ok {
					return object, errors.New("committed turn input is missing")
				}
				in, err := inputFromBody(id.ID, id.Seq, inputRow.Body)
				if err != nil {
					return object, err
				}
				object.Messages = append(object.Messages, inputMessage(in))
				object.Version = inputRow.ID
			}
		case r.Type == agentloop.TypeInput && r.Kind == message.KindResponse:
			var ack struct {
				Disposition string `json:"disposition"`
			}
			_ = json.Unmarshal(r.Body, &ack)
			if ack.Disposition != "accepted" {
				continue
			}
			request := byID[r.Parent]
			var inReq agentloop.InputRequest
			if json.Unmarshal(request.Body, &inReq) != nil {
				continue
			}
			inputs := inReq.Inputs
			if len(inputs) == 0 {
				inputs = []agentloop.Input{inReq.Input}
			}
			for _, in := range inputs {
				inputRow, ok := byID[message.ID(in.ID)]
				if !ok {
					return object, errors.New("committed steering input is missing")
				}
				hydrated, err := inputFromBody(in.ID, in.Seq, inputRow.Body)
				if err != nil {
					return object, err
				}
				object.Messages = append(object.Messages, inputMessage(hydrated))
				object.Version = inputRow.ID
			}
		case r.Type == llmproto.TypeGenerate && r.Kind == message.KindRequest:
			var generateRequest llmproto.GenerateRequest
			if json.Unmarshal(r.Body, &generateRequest) == nil && generateRequest.Purpose == "compact" {
				continue
			}
			response, ok := responses[r.ID]
			if !ok || response.Seq > t.boundary {
				object.Messages = append(object.Messages, mustJSON(map[string]any{"role": "assistant", "content": []any{}, "stopReason": "aborted", "errorMessage": "未得到回复"}))
				continue
			}
			var generated llmproto.GenerateResponse
			if json.Unmarshal(response.Body, &generated) != nil {
				return object, errors.New("invalid generation history")
			}
			if generated.ErrorCode == "context_overflow" || generated.ErrorCode == "length_recoverable" {
				continue
			}
			if len(generated.Message) == 0 {
				var status struct {
					Status string `json:"status"`
				}
				_ = json.Unmarshal(response.Body, &status)
				if status.Status == "failed" || status.Status == "cancelled" {
					object.Messages = append(object.Messages, mustJSON(map[string]any{"role": "assistant", "content": []any{}, "stopReason": "aborted", "errorMessage": "generation failed"}))
					continue
				}
				return object, errors.New("generation history has no assistant message")
			}
			object.Messages = append(object.Messages, generated.Message)
			object.Version = response.ID
			calls, _, _ := assistantParts(generated.Message)
			if len(calls) == 0 {
				continue
			}
			nextGen := t.boundary + 1
			for _, candidate := range rows {
				if candidate.Seq > response.Seq && candidate.Seq < nextGen && candidate.Type == llmproto.TypeGenerate && candidate.Kind == message.KindRequest && turnFor(candidate) == t {
					nextGen = candidate.Seq
				}
			}
			var toolReqs []ledgerRow
			for _, candidate := range rows {
				// Tool calls are the requests whose cause is the generate response.
				// Management requests and output-storage calls have a different
				// parent and cannot consume a model toolCall slot.
				if candidate.Seq > response.Seq && candidate.Seq < nextGen && candidate.Kind == message.KindRequest && candidate.Parent == response.ID {
					toolReqs = append(toolReqs, candidate)
				}
			}
			declared := map[string]bool{}
			toolByName := map[string]resolvedTool{}
			for _, tc := range calls {
				if tool, ok := fixedMaterializedTool(tc.Name); ok {
					declared[tc.Name] = true
					toolByName[tc.Name] = tool
				}
			}
			sendable := 0
			for _, tc := range calls {
				if !declared[tc.Name] || !json.Valid(tc.Arguments) {
					object.Messages = append(object.Messages, toolResult(tc, true, "Looper: tool was not executed"))
					continue
				}
				if toolByName[tc.Name].Kind == executeEnvironment {
					object.Messages = append(object.Messages, toolResult(tc, true, "Environment state is runtime-only; call environment_get to read the current value"))
					continue
				}
				if toolByName[tc.Name].Kind == executeHostMeta && !toolByName[tc.Name].LedgerCall {
					object.Messages = append(object.Messages, toolResult(tc, true, "Host request-account state is runtime-only; inspect the Host again"))
					continue
				}
				if sendable >= len(toolReqs) {
					object.Messages = append(object.Messages, toolResult(tc, true, "Looper: tool was not executed"))
					sendable++
					continue
				}
				toolReq := toolReqs[sendable]
				sendable++
				toolResp, ok := responses[toolReq.ID]
				if !ok || toolResp.Seq > t.boundary {
					object.Messages = append(object.Messages, toolResult(tc, true, "Looper: no tool result received; external execution/effects unknown"))
					continue
				}
				var failure struct {
					ErrorCode string `json:"error_code"`
				}
				_ = json.Unmarshal(toolResp.Body, &failure)
				if failure.ErrorCode != "" {
					object.Messages = append(object.Messages, toolResultFailure(tc, toolResp.Body))
				} else {
					a := &assignment{start: t.req}
					object.Messages = append(object.Messages, (&looper{}).toolResult(ctx, sys, a, tc, toolByName[tc.Name], toolResp.Body))
				}
				object.Version = toolResp.ID
			}
		}
	}
	if err := validateSessionContext(object); err != nil {
		return object, err
	}
	return object, nil
}

func sessionContext(ctx context.Context, sys actorbase.Sys, cause message.Cause, session string) (agentbase.ContextObject, bool, error) {
	ctx, cancel := newHistoryContext(ctx)
	defer cancel()
	rows, err := readSessionRows(ctx, sys, cause, session)
	if err != nil {
		return agentbase.ContextObject{}, false, err
	}
	for _, state := range sessionTurnStates(rows) {
		if state.accepted && !state.closed {
			return agentbase.ContextObject{}, false, errors.New("session has an accepted turn without a valid terminal boundary")
		}
	}
	opened := false
	for _, row := range rows {
		if row.Type == agentloop.TypeSessionOpened || row.Type == agentloop.TypeSessionReset || row.Type == agentloop.TypeSessionCompact || row.Type == agentloop.TypeSessionSync {
			opened = true
			break
		}
	}
	if !opened {
		return agentbase.ContextObject{Messages: []json.RawMessage{}}, false, nil
	}
	object, err := materializeSession(ctx, sys, cause, session, "")
	if err != nil {
		return object, true, err
	}
	// The ledger is authoritative for the accepted boundary and version. A KV
	// object with that same version is the Holder's disposable current context;
	// unlike ledger reconstruction it also retains local tool results and the
	// runtime-only environment hint.
	if cached, found, loadErr := agentbase.LoadContext(sys, session); loadErr == nil && found && cached.Version == object.Version {
		if validateErr := validateSessionContext(cached); validateErr == nil {
			return cached, true, nil
		}
	}
	// Context KV is a disposable runtime cache. A missing, unreadable, malformed,
	// stale, or invalid object must never make durable ledger history unusable.
	return object, true, nil
}

func readSessionRows(ctx context.Context, sys actorbase.Sys, cause message.Cause, session string) ([]ledgerRow, error) {
	return readLedgerRows(ctx, sys, cause, session)
}

func readLedgerRows(ctx context.Context, sys actorbase.Sys, cause message.Cause, session string) ([]ledgerRow, error) {
	ctx, cancel := newHistoryContext(ctx)
	defer cancel()
	budget := ctx.Value(historyBudgetKey{}).(*historyBudget)
	if rows, ok := budget.cache[session]; ok {
		return rows, nil
	}
	if budget.snapshot == nil {
		snapshot, err := sys.View().Read(ctx, actorcaps.LedgerRead{})
		if err != nil {
			return nil, err
		}
		budget.snapshot = &snapshot
	}
	out, err := decodeLedgerRows(budget.snapshot.Rows, session)
	if err != nil {
		return nil, err
	}
	budget.cache[session] = out
	return out, nil
}

func decodeLedgerRows(rows []actorcaps.LedgerRow, session string) ([]ledgerRow, error) {
	var out []ledgerRow
	for _, r := range rows {
		m := r.Envelope
		app, body, err := harness.UnwrapPayload(m.Payload)
		if err != nil {
			return nil, fmt.Errorf("invalid session history row %s: %w", m.ID, err)
		}
		if session != "" && app.Session != session {
			continue
		}
		out = append(out, ledgerRow{Seq: r.Seq, ID: m.ID, Session: app.Session, Parent: m.ParentID, Sender: m.Sender.ID, Audience: m.Audience, Kind: m.Kind, Type: m.Type, Terminal: r.IsTerminal, Body: body})
	}
	return out, nil
}

func inputFromBody(id string, seq int64, body json.RawMessage) (agentloop.Input, error) {
	var x struct {
		Text        string            `json:"text"`
		Attachments []json.RawMessage `json:"attachments"`
	}
	if json.Unmarshal(body, &x) != nil || x.Text == "" {
		return agentloop.Input{}, errors.New("input text missing")
	}
	return agentloop.Input{ID: id, Seq: seq, Text: x.Text, Attachments: x.Attachments}, nil
}
func cloneMessages(in []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, len(in))
	for i := range in {
		out[i] = append(json.RawMessage(nil), in[i]...)
	}
	return out
}
