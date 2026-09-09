package channelmember

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type deliveryPending struct {
	err       error
	cancelled bool
}

func (*deliveryPending) RequestID() message.ID { return "local-call" }
func (*deliveryPending) Progress() <-chan actorbase.Msg {
	ch := make(chan actorbase.Msg)
	close(ch)
	return ch
}
func (p *deliveryPending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	return actorbase.NewBodyMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{Payload: []byte(`{"status":"completed","answer":42}`)}), p.err
}
func (p *deliveryPending) Cancel() error { p.cancelled = true; return nil }

type deliverySys struct {
	testSys
	pending         *deliveryPending
	writeErr        error
	audits, actions int
	extra           map[string]any
}

func (s *deliverySys) Emit(spec behavior.EventSpec) (message.ID, error) {
	if spec.Type == InboundEvent {
		s.audits++
		return "", errors.New("audit unavailable")
	}
	s.actions++
	return "local-event", s.writeErr
}
func (s *deliverySys) Post(behavior.RequestSpec) (message.ID, error) {
	s.actions++
	return "local-post", s.writeErr
}
func (s *deliverySys) CallSpecFor(harness.Caller, behavior.RequestSpec) (actorbase.Pending, error) {
	s.actions++
	if s.writeErr != nil {
		return nil, s.writeErr
	}
	return s.pending, nil
}
func (s *deliverySys) Fail(_ actorbase.Msg, code, detail string, fields ...map[string]any) (message.ID, error) {
	s.failure = code
	if len(fields) > 0 {
		s.extra = fields[0]
	}
	return "fail", nil
}

func TestAuditFailureDoesNotUndoDispatch(t *testing.T) {
	for _, mode := range []string{"call", "post", "event"} {
		t.Run(mode, func(t *testing.T) {
			sys := &deliverySys{pending: &deliveryPending{}}
			env := message.Envelope{ID: "source", Kind: message.KindRequest, Type: "echo", Audience: message.Audience{"tool:target:1"}, Payload: []byte(`{}`)}
			if mode == "event" {
				env.Kind = message.KindEvent
			}
			res, err := actLocal(context.Background(), sys, "local", Request{Envelope: wireEnvelope(env), Await: mode == "call"})
			if err != nil || len(res.Payload) == 0 || sys.actions != 1 || sys.audits != 1 || sys.pending.cancelled {
				t.Fatalf("response=%s err=%v actions=%d audits=%d cancelled=%v", res.Payload, err, sys.actions, sys.audits, sys.pending.cancelled)
			}
		})
	}
}

func TestDeliveryFailuresKeepTheirCategoryAndReceipt(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{context.Canceled, "cancelled"}, {context.DeadlineExceeded, "deadline_exceeded"},
		{actorbase.ErrCallClosed, "call_closed"},
		{&actorbase.TargetResolveError{Code: "actor_ambiguous"}, "actor_ambiguous"},
		{&actorbase.WriteRejected{Reason: "forbidden"}, "forbidden"},
		{&actorbase.InvalidVisibilityError{}, "bad_payload"},
		{errInvalidRequest, "bad_payload"}, {ErrUnreachable, "channel_unavailable"}, {errors.New("unexpected"), "internal_error"},
	} {
		sys := &deliverySys{}
		relay(sys, actorbase.Msg{}, Response{}, tc.err)
		if sys.failure != tc.code {
			t.Fatalf("%v mapped to %s want %s", tc.err, sys.failure, tc.code)
		}
	}
	sys := &deliverySys{pending: &deliveryPending{err: context.Canceled}}
	req := Request{Envelope: wireEnvelope(message.Envelope{ID: "source", Kind: message.KindRequest, Type: "echo", Audience: message.Audience{"tool:target:1"}, Payload: []byte(`{}`)}), Await: true}
	_, err := actLocal(context.Background(), sys, "local", req)
	relay(sys, actorbase.Msg{}, Response{}, err)
	if !errors.Is(err, context.Canceled) || !sys.pending.cancelled || sys.failure != "cancelled" || sys.extra["local_request_id"] != message.ID("local-call") || sys.extra["delivery"] != "dispatched" || sys.extra["outcome"] != "unknown" {
		t.Fatalf("err=%v result=%s %+v", err, sys.failure, sys.extra)
	}
	sys = &deliverySys{writeErr: &actorbase.WriteRejected{Reason: "forbidden"}}
	_, err = actLocal(context.Background(), sys, "local", req)
	relay(sys, actorbase.Msg{}, Response{}, err)
	if sys.failure != "forbidden" || sys.audits != 0 || sys.extra != nil {
		t.Fatalf("rejected write: %s %+v audits=%d", sys.failure, sys.extra, sys.audits)
	}
	sys = &deliverySys{}
	relay(sys, actorbase.Msg{}, Response{Payload: []byte(`{"status":"failed","error_code":"cancelled","detail":"dispatched","delivery":"dispatched","local_request_id":"remote-call","outcome":"unknown"}`)}, nil)
	if sys.failure != "cancelled" || sys.extra["local_request_id"] != "remote-call" || sys.extra["outcome"] != "unknown" {
		t.Fatalf("remote failure metadata lost: %s %+v", sys.failure, sys.extra)
	}
}
