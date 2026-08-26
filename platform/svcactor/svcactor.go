package svcactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wanpengxie/atoll/lib/behavior"
	"log/slog"
	"strings"
	"sync"
	"time"

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

const cardDescribeTimeout = time.Second

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

// Audit records that a request crossed into this channel from another one. It
// takes a cause like every other write: the arrival is reported ABOUT the local
// request the frame was turned into, so it hangs from that request rather than
// floating loose beside it.
type Audit func(context.Context, message.Cause, map[string]any) error

type Deps struct {
	Port    *Port
	Self    channel.ID
	Core    channel.ID
	Members Members
	Audit   Audit
	Logger  *slog.Logger
}

type service struct {
	deps     Deps
	mu       sync.RWMutex
	table    ServiceTable
	card     channel.Card
	revision uint64
}

type persistedService struct {
	Table ServiceTable  `json:"table"`
	Card  *channel.Card `json:"card,omitempty"`
}

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: Class, Interfaces: []string{"actor", "svcactor"},
		Words: map[string]introspect.WordSpec{
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
		table := emptyTable()
		s := &service{deps: deps, table: table, card: skeletonCard(table)}
		return s.serve, nil
	}}
}

func emptyTable() ServiceTable { return ServiceTable{Endpoints: map[string]actor.ActorID{}} }

func (s *service) serve(sys actorbase.Sys) error {
	materializeInitial := false
	if state, found, err := readService(sys.State()); err != nil {
		return err
	} else if found {
		s.table = state.Table
		if state.Card == nil {
			s.card = skeletonCard(state.Table)
			materializeInitial = true
		} else {
			s.card = cloneCard(*state.Card)
		}
	}
	var startup sync.WaitGroup
	if materializeInitial {
		startup.Add(1)
		go func(table ServiceTable) {
			defer startup.Done()
			// Startup materialisation: nothing on this ledger asked for it.
			card := s.buildCard(sys, message.Root(), table)
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.revision != 0 || sys.Life().Err() != nil {
				return
			}
			if err := writeService(sys.State(), table, card); err != nil {
				s.deps.Logger.Warn("svcactor.initial_card_write_failed", "error", err)
				return
			}
			s.card = cloneCard(card)
		}(cloneTable(s.table))
	}
	portCtx, stopPort := context.WithCancel(sys.Life())
	portDone := make(chan struct{})
	go func() {
		defer close(portDone)
		s.servePort(portCtx, sys)
	}()
	for {
		msg, err := sys.Recv()
		if err != nil {
			stopPort()
			<-portDone
			startup.Wait()
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
		_, _ = sys.Fail(msg, "permission_denied", fmt.Sprintf("the service actor mailbox only answers members of its own channel %q, and this arrived from %q. To reach this channel from outside, send through its peer instead", s.deps.Self, caller.Channel))
		return
	}
	active, err := s.deps.Members.IsActive(msg.Ctx(), caller.Actor)
	if err != nil || !active {
		_, _ = sys.Fail(msg, "permission_denied", fmt.Sprintf("%q is not an active member of this channel; check the roster with system.member.list", caller.Actor))
		return
	}
	switch msg.Type {
	case "svcactor.get":
		var empty struct{}
		if err := actorbase.DecodeStrictEmpty(msg.Payload, &empty); err != nil {
			_, _ = sys.Fail(msg, "invalid_args", err.Error())
			return
		}
		_, _ = sys.Reply(msg, s.snapshot())
	case "svcactor.set":
		if s.deps.Self == s.deps.Core {
			_, _ = sys.Fail(msg, "permission_denied", "the registry channel serves a fixed set of words and its service table cannot be edited; set endpoints on an ordinary channel instead")
			return
		}
		var table ServiceTable
		if err := actorbase.DecodeStrict(msg.Payload, &table); err != nil {
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
		card := s.buildCard(sys, msg.Cause(), table)
		s.mu.Lock()
		if err := writeService(sys.State(), table, card); err != nil {
			s.mu.Unlock()
			_, _ = sys.Fail(msg, "internal_error", err.Error())
			return
		}
		s.table = cloneTable(table)
		s.card = cloneCard(card)
		s.revision++
		s.mu.Unlock()
		_, _ = sys.Reply(msg, table)
	default:
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("the service actor mailbox does not answer %q; it accepts only svcactor.get and svcactor.set. Channel membership words go to the system door, not here", msg.Type))
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

// buildCard asks each endpoint receiver to describe itself. cause says what
// prompted the asking: rebuilding the card because someone reset the service
// table continues that request's errand; materialising it at startup continues
// nothing, so it says Root.
func (s *service) buildCard(sys actorbase.Sys, cause message.Cause, table ServiceTable) channel.Card {
	card := skeletonCard(table)
	words := card.Words
	byReceiver := map[actor.ActorID][]string{}
	for word, receiver := range table.Endpoints {
		byReceiver[receiver] = append(byReceiver[receiver], word)
	}
	for receiver, names := range byReceiver {
		pending, err := sys.Call(cause, receiver, introspect.QueryDescribe, map[string]any{})
		if err != nil {
			continue
		}
		terminal, err := pending.Wait(sys.Life(), cardDescribeTimeout)
		if err != nil || len(terminal.Payload) == 0 {
			_ = pending.Cancel()
			continue
		}
		var card introspect.Describe
		if json.Unmarshal(terminal.Payload, &card) != nil {
			continue
		}
		for _, name := range names {
			if spec, ok := card.Words[name]; ok {
				raw, _ := json.Marshal(spec)
				words[name] = raw
			}
		}
	}
	return channel.Card{Words: words}
}

func skeletonCard(table ServiceTable) channel.Card {
	words := make(map[string]json.RawMessage, len(table.Endpoints)+1)
	agentSpec, _ := json.Marshal(introspect.WordSpec{Description: "delegate a request to the channel service agent"})
	words["agent.ask"] = agentSpec
	for word := range table.Endpoints {
		words[word] = json.RawMessage(`{}`)
	}
	return channel.Card{Words: words}
}

// membraneOpen is the SINGLE judgement behind both what the port accepts and
// what the card advertises. c0 is the one caller a channel's membrane answers
// to; every other origin reaches this channel through agent.ask or the
// endpoint table alone. Keeping dispatch and cardFor on one predicate is the
// whole point: a card that advertises more than dispatch accepts sends callers
// into guaranteed failures, and one that advertises less hides a real power —
// the second is what actually happened, and why c0 believed for an entire
// conversation that it could not read a sub-channel's roster.
func membraneOpen(caller, core channel.ID) bool { return caller == core }

// cardFor projects the service card for one caller. The stored card is the
// caller-independent half — agent.ask plus the explicit endpoint table, whose
// specs cost a round trip per receiver to materialise. The membrane words are
// static protocol facts, so they are added per caller and never persisted.
func (s *service) cardFor(caller channel.ID) channel.Card {
	card := s.cardSnapshot()
	if !membraneOpen(caller, s.deps.Core) {
		return card
	}
	docs := introspect.SystemWordSpecs()
	for _, entry := range message.SystemEntries() {
		if entry.Kind != message.KindRequest || entry.Locus != message.SystemLocusMembrane {
			continue
		}
		raw, err := json.Marshal(docs[entry.Name])
		if err != nil {
			continue
		}
		card.Words[entry.Name] = raw
	}
	return card
}

func (s *service) servePort(life context.Context, sys actorbase.Sys) {
	var inFlight sync.WaitGroup
	defer inFlight.Wait()
	for {
		req, err := s.deps.Port.receive(life)
		if err != nil {
			return
		}
		inFlight.Add(1)
		go func(req portRequest) {
			defer inFlight.Done()
			switch {
			case req.call != nil:
				dispatchCtx, cancel := context.WithCancel(req.call.ctx)
				stop := context.AfterFunc(life, cancel)
				result := s.dispatch(dispatchCtx, life, sys, req.call.caller, req.call.frame, req.call.progress)
				stop()
				cancel()
				req.call.done <- result
			case req.describe != nil:
				if req.describe.frame.From.Channel != req.describe.caller {
					req.describe.done <- describeResponse{err: errors.New("svcactor: describe origin channel does not match bound caller")}
					return
				}
				req.describe.done <- describeResponse{card: s.cardFor(req.describe.caller)}
			}
		}(req)
	}
}

func (s *service) dispatch(ctx, life context.Context, sys actorbase.Sys, caller channel.ID, req channel.Request, onProgress func(channel.Progress)) channel.Result {
	if req.From.Channel != caller {
		return gateFailure(channel.GateBadOrigin, "origin channel does not match the bound caller")
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
			active, err := s.deps.Members.IsActive(ctx, target)
			if err != nil {
				return gateFailure(channel.GateChannelUnavailable, err.Error())
			}
			if !active {
				return gateFailure(channel.GateReceiverInactive, "service agent is inactive")
			}
		}
	case membraneOpen(req.From.Channel, s.deps.Core) && message.IsMembraneWord(req.Type):
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
	// A frame crossing the membrane genuinely begins an errand HERE: its cause
	// is a message on the sending channel's ledger, which this one does not
	// hold and could not point at. The link between the two trees is recorded
	// as the inbound audit event below, carrying the远端 request id — that is
	// the seam between two ledgers, and it is not a parent relation.
	// The frame carries the remote caller's declared deadline (peeractor sends
	// it as Request.Deadline); it crosses the membrane as this local request's
	// own ExpiresAt so the receiver's window is the caller's, not this
	// engine's default. Absent → the default.
	spec := behavior.RequestSpec{Cause: message.Root(), Type: req.Type, Payload: json.RawMessage(req.Payload), Audience: message.Audience{target}}
	if req.Deadline > 0 {
		deadline := req.Deadline
		spec.ExpiresAt = &deadline
	}
	pending, err := sys.CallSpecFor(from, spec)
	if err != nil {
		var resolveErr *actorbase.TargetResolveError
		if errors.As(err, &resolveErr) {
			return gateFailure(channel.GateEndpointNotFound, resolveErr.Error())
		}
		return gateFailure(channel.GateChannelUnavailable, err.Error())
	}
	localRequestID := pending.RequestID()
	stopPendingCancel := context.AfterFunc(ctx, func() { _ = pending.Cancel() })
	defer stopPendingCancel()
	// The local request this frame became is a root here, so its correlation is
	// its own id; the audit note hangs from it.
	if err := s.deps.Audit(ctx, message.Anchored(localRequestID, localRequestID), map[string]any{"from": req.From, "type": req.Type, "local_request_id": localRequestID}); err != nil {
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
		if life.Err() != nil {
			return gateFailure(channel.GateChannelUnavailable, "service actor stopped while request was in flight")
		}
		if errors.Is(err, actorbase.ErrCallClosed) {
			// Our out-station account closed the relayed request on its sliding
			// deadline (author#2 already wrote the local terminal). Say so in the
			// caller's words — the remote reads this detail, not our error type.
			return receiverFailure(string(message.TerminalUnansweredTimeout), "downstream request closed by its deadline: no progress or response within the window")
		}
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
		if detail == "" {
			switch reason {
			case string(message.TerminalReceiverUnavailable):
				detail = "B-side receiver became unavailable"
			case string(message.TerminalUnansweredTimeout):
				detail = "B-side receiver did not answer before its deadline"
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

func (s *service) cardSnapshot() channel.Card {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneCard(s.card)
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

func readService(state actorbase.StateHandle) (persistedService, bool, error) {
	out, err := state.Get(ServiceStateKey)
	if err != nil {
		return persistedService{}, false, err
	}
	if out.RejectReason == access.ResourceNotFound || !out.Found || len(out.Value) == 0 {
		return persistedService{Table: emptyTable()}, false, nil
	}
	if !out.Accepted() {
		return persistedService{}, false, errors.New("svcactor: service state read rejected")
	}
	var persisted persistedService
	if err := json.Unmarshal(out.Value, &persisted); err != nil {
		return persistedService{}, false, err
	}
	if persisted.Table.Endpoints == nil {
		persisted.Table.Endpoints = map[string]actor.ActorID{}
	}
	return persisted, true, nil
}

func writeService(state actorbase.StateHandle, table ServiceTable, card channel.Card) error {
	persisted := persistedService{Table: cloneTable(table), Card: func() *channel.Card { cloned := cloneCard(card); return &cloned }()}
	raw, err := json.Marshal(persisted)
	if err != nil {
		return err
	}
	_, err = state.Put(ServiceStateKey, raw)
	return err
}

func BootstrapState(table ServiceTable) ([]byte, error) {
	return json.Marshal(persistedService{Table: cloneTable(table)})
}

func cloneCard(in channel.Card) channel.Card {
	out := channel.Card{Words: make(map[string]json.RawMessage, len(in.Words))}
	for word, spec := range in.Words {
		out.Words[word] = append(json.RawMessage(nil), spec...)
	}
	return out
}
