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

// memberRegistry answers the membership boolean from a fixed active set (the
// gate's permission axis) — the base fakeRegistry always answers not-found.
type memberRegistry struct {
	active    map[actor.ActorID]bool
	lookupErr error
}

func (m memberRegistry) IsActive(_ context.Context, id actor.ActorID) (bool, error) {
	if m.lookupErr != nil {
		return false, m.lookupErr
	}
	return m.active[id], nil
}
func (m memberRegistry) ActiveIdentities() ([]storespec.ActiveIdentity, error) {
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
	created    int
	deleted    int
	restarted  int
	err        error
	result     any
}

func (s *stubExecutor) Execute(ctx context.Context, operation string, req OperateRequest) (any, error) {
	switch operation {
	case TypeMemberCreate:
		return s.Create(ctx, req)
	case TypeMemberDelete:
		return s.Delete(ctx, req)
	case TypeMemberRestart:
		return s.Restart(ctx, req)
	default:
		return nil, errors.New("unsupported operation")
	}
}

func (s *stubExecutor) Create(context.Context, OperateRequest) (any, error) {
	s.created++
	return s.result, s.err
}
func (s *stubExecutor) Delete(context.Context, OperateRequest) (any, error) {
	s.deleted++
	return s.result, s.err
}
func (s *stubExecutor) Restart(context.Context, OperateRequest) (any, error) {
	s.restarted++
	return s.result, s.err
}

func operateMsg(typ string, sender actor.ActorID) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "op1", ChannelID: "ch", Kind: message.KindRequest, Type: typ,
		Sender:   message.Sender{Kind: actor.KindAgent, ID: sender},
		Audience: message.Audience{actor.SystemActorID},
		Payload:  json.RawMessage(`{"body":{}}`),
	})
}

// TestOperate_MemberAllowed proves an active member's control request routes to
// the executor and replies (NP-2=a).
func TestOperate_MemberAllowed(t *testing.T) {
	ex := &stubExecutor{result: map[string]string{"ok": "true"}}
	s := New(Deps{
		Authority: memberRegistry{active: map[actor.ActorID]bool{"agent:alice:1": true}},
		Operate:   ex,
	})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeMemberDelete, "agent:alice:1"))
	if ex.deleted != 1 {
		t.Fatalf("executor.Delete called %d times, want 1", ex.deleted)
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
		Authority: memberRegistry{active: map[actor.ActorID]bool{"agent:alice:1": true}},
		Operate:   ex,
	})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeMemberCreate, "agent:mallory:2"))
	if ex.created != 0 {
		t.Fatalf("executor.Create reached for non-member (called %d)", ex.created)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "unauthorized_sender" {
		t.Fatalf("want 1 unauthorized_sender fail, got %+v", sys.fails)
	}
}

// TestOperate_ExecutorErrorCoded proves an *OperateError picks the fail code.
func TestOperate_ExecutorErrorCoded(t *testing.T) {
	ex := &stubExecutor{err: &OperateError{Code: "unknown_class", Detail: "no such class"}}
	s := New(Deps{
		Authority: memberRegistry{active: map[actor.ActorID]bool{"agent:alice:1": true}},
		Operate:   ex,
	})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeMemberCreate, "agent:alice:1"))
	if len(sys.fails) != 1 || sys.fails[0].code != "unknown_class" {
		t.Fatalf("want 1 unknown_class fail, got %+v", sys.fails)
	}
}

// TestOperate_NilExecutorInert proves an unfilled injection point synthesizes
// nothing (no reply, no fail) — the caller's closure reaps it.
func TestOperate_NilExecutorInert(t *testing.T) {
	s := New(Deps{Authority: memberRegistry{active: map[actor.ActorID]bool{"agent:alice:1": true}}})
	sys := &failSys{}
	s.handle(sys, operateMsg(TypeMemberRestart, "agent:alice:1"))
	if len(sys.replies) != 0 || len(sys.fails) != 0 {
		t.Fatalf("nil executor must synthesize nothing, got %d replies %d fails", len(sys.replies), len(sys.fails))
	}
}
