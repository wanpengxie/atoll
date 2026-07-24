package sysactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// memberRegistry answers Lookup from a fixed active/deregistered set (the gate's
// permission axis) — the base fakeRegistry always answers not-found.
type memberRegistry struct {
	active    map[actor.ActorID]bool
	lookupErr error
}

func (m memberRegistry) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	if m.lookupErr != nil {
		return storespec.ActorControlRow{}, false, m.lookupErr
	}
	if !m.active[id] {
		return storespec.ActorControlRow{}, false, nil
	}
	return storespec.ActorControlRow{ID: id, Kind: actor.KindAgent, CurrentDeclVersion: 1}, true, nil
}
func (m memberRegistry) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}

// failSys extends the reply-recording double with Fail capture — the operate gate
// exercises both terminals.
type failSys struct {
	actorbase.Sys
	replies []replyRec
	fails   []failRec
}

type failRec struct {
	code   string
	detail string
}

func (f *failSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyRec{msg: msg, v: v})
	return msg.ID, nil
}
func (f *failSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	f.fails = append(f.fails, failRec{code: code, detail: detail})
	return msg.ID, nil
}

// stubExecutor records the calls the gate routes to it.
type stubExecutor struct {
	introduced int
	removed    int
	restarted  int
	setDefault int
	err        error
	result     any
}

func (s *stubExecutor) Execute(ctx context.Context, operation string, req OperateRequest) (any, error) {
	switch operation {
	case TypeIntroduceActor:
		return s.Introduce(ctx, req)
	case TypeRemoveActor:
		return s.Remove(ctx, req)
	case TypeRestartActor:
		return s.Restart(ctx, req)
	case TypeSetDefaultAgent:
		return s.SetDefaultAgent(ctx, req)
	default:
		return nil, errors.New("unsupported operation")
	}
}

func (s *stubExecutor) Introduce(context.Context, OperateRequest) (any, error) {
	s.introduced++
	return s.result, s.err
}
func (s *stubExecutor) Remove(context.Context, OperateRequest) (any, error) {
	s.removed++
	return s.result, s.err
}
func (s *stubExecutor) Restart(context.Context, OperateRequest) (any, error) {
	s.restarted++
	return s.result, s.err
}
func (s *stubExecutor) SetDefaultAgent(context.Context, OperateRequest) (any, error) {
	s.setDefault++
	return s.result, s.err
}

func operateMsg(typ string, sender actor.ActorID) actorbase.Msg {
	return actorbase.NewMsg(context.Background(), message.Envelope{
		ID: "op1", ChannelID: "ch", Kind: message.KindRequest, Type: typ,
		Sender:   message.Sender{Kind: actor.KindAgent, ID: sender},
		Audience: message.Audience{actor.SystemActorID},
		Payload:  json.RawMessage(`{}`),
	})
}

// TestOperate_MemberAllowed proves an active member's control request routes to
// the executor and replies (NP-2=a).
func TestOperate_MemberAllowed(t *testing.T) {
	ex := &stubExecutor{result: map[string]string{"ok": "true"}}
	s := New(Deps{
		Authority: memberRegistry{active: map[actor.ActorID]bool{"user:alice": true}},
		Operate:   ex,
	})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeRemoveActor, "user:alice"))
	if ex.removed != 1 {
		t.Fatalf("executor.Remove called %d times, want 1", ex.removed)
	}
	if len(sys.replies) != 1 || len(sys.fails) != 0 {
		t.Fatalf("want 1 reply 0 fails, got %d replies %d fails", len(sys.replies), len(sys.fails))
	}
}

// TestOperate_NonMemberRejected pins the cheap-deny contract: a sender with no
// active membership in the unified authority is refused at the gate as the
// request's failed reply — it never reaches the executor and never touches any
// durable operation ledger (rejections are noise, not truth).
func TestOperate_NonMemberRejected(t *testing.T) {
	ex := &stubExecutor{}
	s := New(Deps{
		Authority: memberRegistry{active: map[actor.ActorID]bool{"user:alice": true}},
		Operate:   ex,
	})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeIntroduceActor, "user:mallory"))
	if ex.introduced != 0 {
		t.Fatalf("executor.Introduce reached for non-member (called %d)", ex.introduced)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "unauthorized_sender" {
		t.Fatalf("want 1 unauthorized_sender fail, got %+v", sys.fails)
	}
}

// TestOperate_ExecutorErrorCoded proves an *OperateError picks the fail code.
func TestOperate_ExecutorErrorCoded(t *testing.T) {
	ex := &stubExecutor{err: &OperateError{Code: "unknown_class", Detail: "no such class"}}
	s := New(Deps{
		Authority: memberRegistry{active: map[actor.ActorID]bool{"user:alice": true}},
		Operate:   ex,
	})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeIntroduceActor, "user:alice"))
	if len(sys.fails) != 1 || sys.fails[0].code != "unknown_class" {
		t.Fatalf("want 1 unknown_class fail, got %+v", sys.fails)
	}
}

// TestOperate_NilExecutorInert proves an unfilled injection point synthesizes
// nothing (no reply, no fail) — the caller's closure reaps it.
func TestOperate_NilExecutorInert(t *testing.T) {
	s := New(Deps{Authority: memberRegistry{active: map[actor.ActorID]bool{"user:alice": true}}})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeRestartActor, "user:alice"))
	if len(sys.replies) != 0 || len(sys.fails) != 0 {
		t.Fatalf("nil executor must synthesize nothing, got %d replies %d fails", len(sys.replies), len(sys.fails))
	}
}
