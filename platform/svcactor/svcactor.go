package svcactor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const Class = "svcactor"

const ServiceStateKey resource.ResourceID = "_service"

type ServiceTable struct {
	SvcAgent  *string                  `json:"svc_agent"`
	Endpoints map[string]actor.ActorID `json:"endpoints"`
}

type MemberFacts struct {
	Kind actor.Kind
}

// Members is the only local roster surface available to the service actor.
type Members struct {
	IsActive         func(context.Context, actor.ActorID) (bool, error)
	ActorFacts       func(context.Context, actor.ActorID) (MemberFacts, bool, error)
	FirstActiveAgent func(context.Context) (actor.ActorID, bool, error)
}

type Audit func(context.Context, map[string]any) error

type Deps struct {
	Port    *Port
	Self    channel.ID
	Core    channel.ID
	Members Members
	Audit   Audit
	Logger  *slog.Logger
}

type service struct {
	deps  Deps
	mu    sync.RWMutex
	table ServiceTable
}

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: Class, Interfaces: []string{"actor", "agent", "svcactor"},
		Words: map[string]introspect.WordSpec{
			"agent.ask":    {Description: "delegate a request to the channel service agent"},
			"svcactor.set": {Description: "replace the channel service table"},
			"svcactor.get": {Description: "read the channel service table"},
		},
	}
}

func Def(deps Deps) actorbase.Def {
	return actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) {
		if deps.Port == nil || deps.Self == "" || deps.Core == "" || deps.Members.IsActive == nil || deps.Members.ActorFacts == nil || deps.Members.FirstActiveAgent == nil || deps.Audit == nil {
			return nil, errors.New("svcactor: incomplete dependencies")
		}
		if deps.Logger == nil {
			deps.Logger = slog.New(slog.DiscardHandler)
		}
		s := &service{deps: deps, table: emptyTable()}
		return s.serve, nil
	}}
}

func emptyTable() ServiceTable { return ServiceTable{Endpoints: map[string]actor.ActorID{}} }

func (s *service) serve(sys actorbase.Sys) error {
	if table, found, err := readTable(sys.State()); err != nil {
		return err
	} else if found {
		s.table = table
		if _, cardFound, _ := readDynamicWords(sys.State()); !cardFound {
			_ = s.materialize(sys, table)
		}
	}
	go s.servePort(sys)
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		s.handleMailbox(sys, msg)
	}
}

func (s *service) handleMailbox(sys actorbase.Sys, msg actorbase.Msg) {
	caller := actorbase.EffectiveCaller(msg)
	if caller.Channel != s.deps.Self {
		_, _ = sys.Fail(msg, "permission_denied", "svcactor mailbox is membrane-local")
		return
	}
	active, err := s.deps.Members.IsActive(msg.Ctx(), caller.Actor)
	if err != nil || !active {
		_, _ = sys.Fail(msg, "permission_denied", "caller is not an active channel member")
		return
	}
	switch msg.Type {
	case "svcactor.get":
		var empty struct{}
		if err := decodeStrict(msg.Payload, &empty); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		_, _ = sys.Reply(msg, s.snapshot())
	case "svcactor.set":
		if s.deps.Self == s.deps.Core {
			_, _ = sys.Fail(msg, "permission_denied", "c0 service table is fixed empty")
			return
		}
		var table ServiceTable
		if err := decodeStrict(msg.Payload, &table); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		if table.Endpoints == nil {
			table.Endpoints = map[string]actor.ActorID{}
		}
		if err := s.validateTable(msg.Ctx(), table); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		raw, _ := json.Marshal(table)
		if _, err := sys.State().Put(ServiceStateKey, raw); err != nil {
			_, _ = sys.Fail(msg, "internal_error", err.Error())
			return
		}
		s.mu.Lock()
		s.table = cloneTable(table)
		s.mu.Unlock()
		if err := s.materialize(sys, table); err != nil {
			_, _ = sys.Fail(msg, "internal_error", err.Error())
			return
		}
		_, _ = sys.Reply(msg, table)
	default:
		_, _ = sys.Fail(msg, "type_unsupported", "svcactor mailbox only accepts svcactor.set/get")
	}
}

func (s *service) validateTable(ctx context.Context, table ServiceTable) error {
	for word, receiver := range table.Endpoints {
		if word == "actor.describe" || word == "agent.ask" || strings.HasPrefix(word, "svcactor.") || strings.HasPrefix(word, "system.") {
			return errors.New("service word collides with a structural word")
		}
		if receiver == "" || !fullActorID(receiver) {
			return errors.New("endpoint receiver must be a full actor id")
		}
		facts, active, err := s.deps.Members.ActorFacts(ctx, receiver)
		if err != nil || !active {
			return errors.New("endpoint receiver is not an active channel member")
		}
		if facts.Kind != actor.KindTool && facts.Kind != actor.KindAgent && facts.Kind != actor.KindHuman {
			return errors.New("endpoint receiver kind is not serviceable")
		}
	}
	if table.SvcAgent != nil && *table.SvcAgent != "default" {
		id := actor.ActorID(*table.SvcAgent)
		if !fullActorID(id) {
			return errors.New("svc_agent must be default, null, or a full actor id")
		}
		facts, active, err := s.deps.Members.ActorFacts(ctx, id)
		if err != nil || !active || facts.Kind != actor.KindAgent {
			return errors.New("svc_agent must be an active agent member")
		}
	}
	return nil
}

func fullActorID(id actor.ActorID) bool {
	parts := strings.Split(string(id), ":")
	return len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != ""
}

func (s *service) materialize(sys actorbase.Sys, table ServiceTable) error {
	words := make(map[string]introspect.WordSpec, len(table.Endpoints))
	byReceiver := map[actor.ActorID][]string{}
	for word, receiver := range table.Endpoints {
		words[word] = introspect.WordSpec{}
		byReceiver[receiver] = append(byReceiver[receiver], word)
	}
	for receiver, names := range byReceiver {
		pending, err := sys.Call(receiver, introspect.QueryDescribe, map[string]any{})
		if err != nil {
			continue
		}
		terminal, err := pending.Wait(sys.Life(), 0)
		if err != nil {
			continue
		}
		var card introspect.Describe
		if json.Unmarshal(terminal.Payload, &card) != nil {
			continue
		}
		for _, name := range names {
			if spec, ok := card.Words[name]; ok {
				words[name] = spec
			}
		}
	}
	raw, _ := json.Marshal(words)
	_, err := sys.State().Put(actorbase.ManifestStateKey, raw)
	return err
}

func (s *service) servePort(sys actorbase.Sys) {
	var inFlight sync.WaitGroup
	defer inFlight.Wait()
	for {
		req, err := s.deps.Port.receive(sys.Life())
		if err != nil {
			return
		}
		inFlight.Add(1)
		go func(req portRequest) {
			defer inFlight.Done()
			result := s.dispatch(req.ctx, sys, req.caller, req.frame, req.progress)
			select {
			case req.done <- result:
			case <-sys.Life().Done():
			}
		}(req)
	}
}

func (s *service) dispatch(ctx context.Context, sys actorbase.Sys, caller channel.ID, req channel.Request, onProgress func(channel.Progress)) channel.Result {
	if req.From.Channel != caller {
		return gateFailure(channel.GateBadOrigin, "origin channel does not match the bound caller")
	}
	if req.Type == introspect.QueryDescribe {
		card, err := projectCard(sys.State())
		if err != nil {
			return receiverFailure("internal_error", err.Error())
		}
		raw, _ := json.Marshal(card)
		return channel.Result{Body: raw}
	}

	var target actor.ActorID
	switch {
	case req.Type == "agent.ask":
		table := s.snapshot()
		if table.SvcAgent == nil {
			return gateFailure(channel.GateNoServiceAgent, "channel has no service agent")
		}
		if *table.SvcAgent == "default" {
			id, found, err := s.deps.Members.FirstActiveAgent(ctx)
			if err != nil {
				return gateFailure(channel.GateChannelUnavailable, err.Error())
			}
			if !found {
				return gateFailure(channel.GateNoServiceAgent, "channel has no active service agent")
			}
			target = id
		} else {
			target = actor.ActorID(*table.SvcAgent)
		}
	case s.deps.Self == s.deps.Core && message.IsSpaceWord(req.Type):
		target = actor.ActorID("system:registrar")
	case req.From.Channel == s.deps.Core && message.IsMembraneWord(req.Type):
		target = actor.SystemActorID
	case message.IsSpaceWord(req.Type):
		target = actor.ActorID("system:registrar")
	default:
		target = s.snapshot().Endpoints[req.Type]
		if target == "" {
			return gateFailure(channel.GateEndpointNotFound, "service endpoint not found")
		}
		active, err := s.deps.Members.IsActive(ctx, target)
		if err != nil {
			return gateFailure(channel.GateChannelUnavailable, err.Error())
		}
		if !active {
			return gateFailure(channel.GateReceiverInactive, "service receiver is inactive")
		}
	}

	from := harnessCaller(req.From)
	pending, err := sys.CallFor(from, target, req.Type, json.RawMessage(req.Payload))
	if err != nil {
		var resolveErr *actorbase.TargetResolveError
		if errors.As(err, &resolveErr) {
			return gateFailure(channel.GateEndpointNotFound, resolveErr.Error())
		}
		return gateFailure(channel.GateChannelUnavailable, err.Error())
	}
	localRequestID := pending.RequestID()
	if err := s.deps.Audit(ctx, map[string]any{"from": req.From, "type": req.Type, "local_request_id": localRequestID}); err != nil {
		s.deps.Logger.Warn("svcactor.audit_failed", "request_id", localRequestID, "err", err)
	}
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		seq := 0
		for progress := range pending.Progress() {
			seq++
			status, body := progressBody(progress.Payload)
			if onProgress != nil {
				onProgress(channel.Progress{RequestID: req.From.RequestID, Seq: seq, Status: status, Body: body})
			}
		}
	}()
	terminal, err := pending.Wait(ctx, 0)
	if err != nil {
		_ = pending.Cancel()
		<-progressDone
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return receiverFailure(string(message.TerminalUnansweredTimeout), err.Error())
		}
		return receiverFailure("receiver_unavailable", err.Error())
	}
	<-progressDone
	return terminalResult(terminal.Payload)
}

func harnessCaller(from channel.From) harness.Caller {
	return harness.Caller{Channel: from.Channel, Actor: actor.ActorID(from.Actor)}
}

func progressBody(raw json.RawMessage) (string, json.RawMessage) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return message.StatusProcessing, nil
	}
	var status string
	_ = json.Unmarshal(fields["status"], &status)
	delete(fields, "status")
	delete(fields, "reason")
	body, _ := json.Marshal(fields)
	return status, body
}

func terminalResult(raw json.RawMessage) channel.Result {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return receiverFailure("receiver_unavailable", "invalid receiver terminal")
	}
	var status string
	_ = json.Unmarshal(fields["status"], &status)
	if status == message.StatusFailed {
		var code, detail, reason string
		_ = json.Unmarshal(fields["error_code"], &code)
		_ = json.Unmarshal(fields["detail"], &detail)
		_ = json.Unmarshal(fields["reason"], &reason)
		if code == "" {
			switch reason {
			case string(message.TerminalReceiverUnavailable), string(message.TerminalUnansweredTimeout):
				code = reason
			default:
				code = "receiver_unavailable"
			}
		}
		return receiverFailure(code, detail)
	}
	if status != message.StatusCompleted {
		return receiverFailure("receiver_unavailable", "receiver returned a non-terminal response")
	}
	delete(fields, "status")
	delete(fields, "reason")
	body, err := json.Marshal(fields)
	if err != nil {
		return receiverFailure("receiver_unavailable", err.Error())
	}
	return channel.Result{Body: body}
}

func gateFailure(code channel.GateCode, detail string) channel.Result {
	return channel.Result{Fail: &channel.Failure{Stage: channel.StageGate, Code: string(code), Detail: detail}}
}

func receiverFailure(code, detail string) channel.Result {
	return channel.Result{Fail: &channel.Failure{Stage: channel.StageReceiver, Code: code, Detail: detail}}
}

func (s *service) snapshot() ServiceTable {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneTable(s.table)
}

func cloneTable(in ServiceTable) ServiceTable {
	out := emptyTable()
	if in.SvcAgent != nil {
		value := *in.SvcAgent
		out.SvcAgent = &value
	}
	for word, id := range in.Endpoints {
		out.Endpoints[word] = id
	}
	return out
}

func readTable(state actorbase.StateHandle) (ServiceTable, bool, error) {
	out, err := state.Get(ServiceStateKey)
	if err != nil {
		return ServiceTable{}, false, err
	}
	if out.RejectReason == access.ResourceNotFound || !out.Found || len(out.Value) == 0 {
		return emptyTable(), false, nil
	}
	if !out.Accepted() {
		return ServiceTable{}, false, errors.New("svcactor: service state read rejected")
	}
	var table ServiceTable
	if err := json.Unmarshal(out.Value, &table); err != nil {
		return ServiceTable{}, false, err
	}
	if table.Endpoints == nil {
		table.Endpoints = map[string]actor.ActorID{}
	}
	return table, true, nil
}

func readDynamicWords(state actorbase.StateHandle) (map[string]introspect.WordSpec, bool, error) {
	out, err := state.Get(actorbase.ManifestStateKey)
	if err != nil {
		return nil, false, err
	}
	if out.RejectReason == access.ResourceNotFound || !out.Found || len(out.Value) == 0 {
		return nil, false, nil
	}
	if !out.Accepted() {
		return nil, false, errors.New("svcactor: manifest state read rejected")
	}
	var words map[string]introspect.WordSpec
	if err := json.Unmarshal(out.Value, &words); err != nil {
		return nil, false, err
	}
	return words, true, nil
}

func projectCard(state actorbase.StateHandle) (introspect.Describe, error) {
	m := manifest()
	words := introspect.CloneWords(m.Words)
	words[introspect.QueryDescribe] = introspect.WordSpec{Description: "project this actor's live manifest"}
	dynamic, _, err := readDynamicWords(state)
	if err != nil {
		return introspect.Describe{}, err
	}
	for name, spec := range dynamic {
		words[name] = spec
	}
	return introspect.Describe{Class: m.Class, Interfaces: append([]string(nil), m.Interfaces...), Capabilities: map[string]bool{}, Words: words}, nil
}

func decodeStrict(raw json.RawMessage, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}
