package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// replyCall / failCall record one recorded write for assertions below.
type replyCall struct {
	msg Msg
	v   any
}

type failCall struct {
	msg    Msg
	code   string
	detail string
}

// fakeSys is a minimal Sys double: only Reply/Fail/Recv are exercised by
// dispatch/Serve, every other verb panics if ever reached — a test that hits
// one of those has drifted outside what this slice's routing logic touches.
type fakeSys struct {
	recvQueue []Msg
	recvErr   error // returned once, after recvQueue is drained

	replies []replyCall
	fails   []failCall

	// settleCalls records every dispatch settle hook invocation (spec S8/DoD
	// 4's Serve-道 completion signal: handler success → settleTimer(msg,
	// true) → the engine's real ack口; handler error/unrouted → settleTimer(
	// msg, false) → no ack). Previously fakeSys did not implement this
	// interface at all, so dispatch's `sys.(interface{ settleTimer(...) })`
	// type assertion silently failed and every existing settle-related
	// assertion in this file was an unreachable no-op.
	settleCalls []settleCall
}

type settleCall struct {
	msgID   message.ID
	handled bool
}

func (f *fakeSys) settleTimer(msg Msg, handled bool) {
	f.settleCalls = append(f.settleCalls, settleCall{msgID: msg.ID, handled: handled})
}

func (f *fakeSys) Reply(msg Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyCall{msg: msg, v: v})
	return "reply-id", nil
}

func (f *fakeSys) Fail(msg Msg, code, detail string) (message.ID, error) {
	f.fails = append(f.fails, failCall{msg: msg, code: code, detail: detail})
	return "fail-id", nil
}

func (f *fakeSys) Progress(msg Msg, v any) (message.ID, error) {
	panic("not implemented")
}

func (f *fakeSys) Emit(msgType string, payload any, audience ...actor.ActorID) (message.ID, error) {
	panic("not implemented")
}

func (f *fakeSys) Call(target actor.ActorID, msgType string, payload any) (Pending, error) {
	panic("not implemented")
}

func (f *fakeSys) State() StateHandle { panic("not implemented") }

func (f *fakeSys) Resource() ResourceHandle { panic("not implemented") }

func (f *fakeSys) After(d time.Duration, msgType string, payload any) (schedule.TimerID, error) {
	panic("not implemented")
}

func (f *fakeSys) CancelTimer(id schedule.TimerID) error { panic("not implemented") }

func (f *fakeSys) Fork(spec actorrt.ForkSpec) (actor.ActorID, error) {
	panic("not implemented")
}

func (f *fakeSys) DespawnChild(id actor.ActorID) error { panic("not implemented") }
func (f *fakeSys) End() error                          { panic("not implemented") }

func (f *fakeSys) PublishObs(kind actorrt.ObsKind, val actorrt.ObsValue) error {
	panic("not implemented")
}

func (f *fakeSys) Self() actor.ActorID { panic("not implemented") }

func (f *fakeSys) Recv() (Msg, error) {
	if len(f.recvQueue) > 0 {
		m := f.recvQueue[0]
		f.recvQueue = f.recvQueue[1:]
		return m, nil
	}
	return Msg{}, f.recvErr
}

func (f *fakeSys) Life() context.Context { return context.Background() }

// Identity-dimension variants (gateway 期 S1): dispatch/Serve never reach them.
func (f *fakeSys) SubmitEnvelope(behavior.SubjectWriteSpec) (message.ID, int64, error) {
	return "", 0, ErrUnsupported
}
func (f *fakeSys) RespondEnvelope(*message.Envelope, behavior.ResponseSpec) (message.ID, error) {
	return "", ErrUnsupported
}
func (f *fakeSys) AfterIdentity(time.Duration, string, json.RawMessage) (schedule.TimerID, error) {
	return "", ErrUnsupported
}
func (f *fakeSys) CancelTimerIdentity(schedule.TimerID) error { return ErrUnsupported }
func (f *fakeSys) ResourceIdentity() ResourceHandle           { return nil }

var _ Sys = (*fakeSys)(nil)

func newTestMsg(msgType string) Msg {
	return NewMsg(context.Background(), message.Envelope{
		ID:   "req-1",
		Kind: message.KindRequest,
		Type: msgType,
	})
}

func TestDispatch_RoutesToHandler_Replies(t *testing.T) {
	sys := &fakeSys{}
	msg := newTestMsg("greet")
	routes := map[string]Handler{
		"greet": func(ctx context.Context, msg Msg) (any, error) {
			return "hello", nil
		},
	}

	dispatch(sys, msg, routes)

	if len(sys.replies) != 1 || sys.replies[0].v != "hello" {
		t.Fatalf("expected one reply with %q, got %+v", "hello", sys.replies)
	}
	if len(sys.fails) != 0 {
		t.Fatalf("expected no fails, got %+v", sys.fails)
	}
}

func TestDispatch_HandlerError_FailsInternalError(t *testing.T) {
	sys := &fakeSys{}
	msg := newTestMsg("greet")
	wantErr := errors.New("boom")
	routes := map[string]Handler{
		"greet": func(ctx context.Context, msg Msg) (any, error) {
			return nil, wantErr
		},
	}

	dispatch(sys, msg, routes)

	if len(sys.replies) != 0 {
		t.Fatalf("expected no replies, got %+v", sys.replies)
	}
	if len(sys.fails) != 1 {
		t.Fatalf("expected one fail, got %+v", sys.fails)
	}
	got := sys.fails[0]
	if got.code != "internal_error" || got.detail != wantErr.Error() {
		t.Fatalf("fail = %+v; want code=internal_error detail=%q", got, wantErr.Error())
	}
}

func TestDispatch_UnknownType_FailsTypeUnsupported(t *testing.T) {
	sys := &fakeSys{}
	msg := newTestMsg("nonexistent")
	handlerRan := false
	routes := map[string]Handler{
		"greet": func(ctx context.Context, msg Msg) (any, error) {
			handlerRan = true
			return nil, nil
		},
	}

	dispatch(sys, msg, routes)

	if handlerRan {
		t.Fatalf("handler must not run for an unrouted type")
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "type_unsupported" {
		t.Fatalf("expected one type_unsupported fail, got %+v", sys.fails)
	}
}

func TestDispatch_HandlerCtx_IsMsgCtx(t *testing.T) {
	sys := &fakeSys{}
	type ctxKey struct{}
	want := context.WithValue(context.Background(), ctxKey{}, "value")
	msg := NewMsg(want, message.Envelope{ID: "req-1", Kind: message.KindRequest, Type: "greet"})

	var gotCtx context.Context
	routes := map[string]Handler{
		"greet": func(ctx context.Context, msg Msg) (any, error) {
			gotCtx = ctx
			return nil, nil
		},
	}

	dispatch(sys, msg, routes)

	if gotCtx != want {
		t.Fatalf("handler ctx = %v; want the Msg's own Ctx()", gotCtx)
	}
}

func TestServe_LoopsUntilRecvError(t *testing.T) {
	sys := &fakeSys{
		recvQueue: []Msg{newTestMsg("greet"), newTestMsg("other")},
		recvErr:   errRecvDone,
	}
	routes := map[string]Handler{
		"greet": func(ctx context.Context, msg Msg) (any, error) { return "ok", nil },
	}

	proc := Serve(routes)
	err := proc(sys)

	if !errors.Is(err, errRecvDone) {
		t.Fatalf("Serve returned %v; want errRecvDone", err)
	}
	if len(sys.replies) != 1 {
		t.Fatalf("expected one reply (greet), got %+v", sys.replies)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "type_unsupported" {
		t.Fatalf("expected one type_unsupported fail (other), got %+v", sys.fails)
	}
}

// TestDispatch_SettlesTimerHookOnEveryOutcome pins dispatch's own contract
// with Serve's completion hook (spec S8: "Serve 道 = handler 正常返回...经
// engine 内部销账口执行"): a handler that succeeds settles handled=true, a
// handler that errors OR an unrouted type settles handled=false — dispatch
// must always call the hook exactly once per delivery, before the write
// (Reply/Fail) that logically follows it, and the caller decides ack-or-not
// purely from that bool.
func TestDispatch_SettlesTimerHookOnEveryOutcome(t *testing.T) {
	t.Run("handler success settles handled=true", func(t *testing.T) {
		sys := &fakeSys{}
		msg := newTestMsg("greet")
		dispatch(sys, msg, map[string]Handler{
			"greet": func(ctx context.Context, msg Msg) (any, error) { return "hello", nil },
		})
		if len(sys.settleCalls) != 1 || !sys.settleCalls[0].handled || sys.settleCalls[0].msgID != msg.ID {
			t.Fatalf("settleCalls = %+v, want one handled=true call for %q", sys.settleCalls, msg.ID)
		}
	})

	t.Run("handler error settles handled=false", func(t *testing.T) {
		sys := &fakeSys{}
		msg := newTestMsg("greet")
		dispatch(sys, msg, map[string]Handler{
			"greet": func(ctx context.Context, msg Msg) (any, error) { return nil, errors.New("boom") },
		})
		if len(sys.settleCalls) != 1 || sys.settleCalls[0].handled || sys.settleCalls[0].msgID != msg.ID {
			t.Fatalf("settleCalls = %+v, want one handled=false call for %q", sys.settleCalls, msg.ID)
		}
	})

	t.Run("unrouted type settles handled=false without reaching a handler", func(t *testing.T) {
		sys := &fakeSys{}
		msg := newTestMsg("nonexistent")
		dispatch(sys, msg, map[string]Handler{})
		if len(sys.settleCalls) != 1 || sys.settleCalls[0].handled || sys.settleCalls[0].msgID != msg.ID {
			t.Fatalf("settleCalls = %+v, want one handled=false call for %q", sys.settleCalls, msg.ID)
		}
	})
}

var errRecvDone = errors.New("actorbase test: recv done")
