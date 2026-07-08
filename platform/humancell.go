package platform

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Day-1 two of the three honest closure options (三层律 §3) a human cell
// declares per request type. fail-fast (device_unreachable) is not a human's
// option: the log IS the inbox, so a human is never structurally unreachable —
// an unrecognised type degrades to the deferred (收件箱) default rather than a
// fabricated failure.
const (
	// TypeHumanMessage is IMMEDIATE: a message to the human's inbox, answered
	// completed on receipt (durable delivery to the log IS the answer).
	TypeHumanMessage = "human.message"
	// TypeHumanApprove is DEFERRED: left OPEN until the person answers via the
	// door (HumanHandle.Resolve). Closure is the sender's caller-scoped timer.
	TypeHumanApprove = "human.approve"
)

// humanCellFactory is the platform's built-in home-side human embodiment. user域
// supply is platform internal政 — a per-channel human member's authority lives
// only in this channel's registry (the app cannot enumerate it), so the reconcile
// ring keeps a live human cell up whenever the member is admitted, without any
// app-injected factory. The cell captures the SHARED per-user Caller (author#2)
// so it Matches replies to requests the same subject Armed via HumanHandle.Submit.
//
// Proc shape (through the actorbase engine, NOT a raw actorrt.Actor implementer —
// archtest wall): a serve loop that answers each request per the three-choice type
// table (immediate human.message / deferred human.approve / describe self-answer)
// and Matches responses onto the author#2 Caller.
// TODO(human-canonical): author#2 caller 耦合已随 platform/human.go 旧形整删
// （2026-07-08）——本 cell（层1 embodiment + 三选应答）是正典要保留的半；subject
// 自发 request 的 caller 台账随债②（human 入站）按正典重建时接回。
func humanCellFactory(h *Home, id actor.ActorID) ActorFactory {
	return ActorFactory{Proc: actorbase.Def{
		Doc: "home-side human embodiment (subjectgate): callable; three-choice per-type closure (immediate human.message / deferred human.approve) + describe; the person answers deferred requests via the door",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error { return humanServe(sys) }, nil
		},
	}}
}

// humanServe is the human cell's serve loop: requests route through the
// three-choice type table. Returning on a Recv error is the cooperative
// termination contract (spec §1.6). (author#2 response Match detached — see
// TODO(human-canonical) above.)
func humanServe(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return nil
		}
		switch msg.Kind {
		case message.KindRequest:
			humanServeRequest(sys, msg)
		}
	}
}

// humanServeRequest answers one delivered request per the three-choice type
// table. It NEVER fabricates a Reply it did not earn: human.approve and any
// unrecognised type are left OPEN (deferred) — the person's Resolve via the door
// is the real answer, and closure is the sender's caller-scoped timer.
func humanServeRequest(sys actorbase.Sys, msg actorbase.Msg) {
	switch msg.Type {
	case introspect.QueryDescribe:
		req, err := introspect.ParseDescribeRequest(msg.Payload)
		if err != nil {
			_, _ = sys.Fail(msg, "payload_invalid", err.Error())
			return
		}
		answer, ok := introspect.AnswerDescribe(humanDescribe(string(sys.Self())), req)
		if !ok {
			_, _ = sys.Fail(msg, "type_unsupported", "human cell does not serve "+req.Type)
			return
		}
		_, _ = sys.Reply(msg, answer)
	case TypeHumanMessage:
		// immediate: 收件即 completed 回执 (log 即收件箱).
		_, _ = sys.Reply(msg, map[string]any{"delivered": true})
	default:
		// deferred (human.approve and any other type): leave OPEN — no Reply/Fail.
	}
}

// humanDescribe is the human cell's actor.describe self-answer catalog.
func humanDescribe(id string) introspect.Describe {
	return introspect.Describe{
		ActorID:     id,
		Description: "human subject — occupant off-process; the log is the inbox",
		Types: map[string]introspect.TypeMeta{
			TypeHumanMessage: {Description: "immediate: delivered to the human's inbox, answered completed on receipt"},
			TypeHumanApprove: {Description: "deferred: left open until the person answers via the door (Resolve)"},
		},
	}
}
