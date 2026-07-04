package actorcaps

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// ScheduledCallType is the fire envelope's Type — lib vocabulary (not a
// runtime-reserved prefix, message.ReservedTypePrefix is "system."), naming
// the self-message a scheduled call fires as.
const ScheduledCallType = "lib.schedule_call"

// ScheduledCall is the wire shape ScheduleCallTo arms and ParseScheduledCall
// decodes: "at FireAt, call TARGET with REQTYPE/PAYLOAD". A timer is
// structurally self-targeted (schedule.ScheduleReq has no target field —
// the fire is always authored by whoever scheduled it), so ScheduleCallTo
// composes that self-targeted closure over ONE JSON payload naming the
// actual OTHER recipient and request shape; zero new engine mechanism.
type ScheduledCall struct {
	Target  actor.ActorID `json:"target"`
	ReqType string        `json:"req_type"`
	Payload []byte        `json:"payload"`
}

// ScheduleCallTo arms a timer whose fire, at FireAt, is "call target with
// reqType/payload" — pure composition over schedule.ScheduleHandle.Schedule.
// TimerID is schedule's re-export (a Go alias of timerspec.TimerID); this
// file never imports runtime/timerspec directly — archtest confines that
// contract leaf to the runtime tree, and schedule.TimerID is the only name
// downstream ever needs.
func ScheduleCallTo(ctx context.Context, h schedule.ScheduleHandle, bind schedule.Bind, fireAt int64, target actor.ActorID, reqType string, payload []byte, corr string) (schedule.TimerID, error) {
	body, err := json.Marshal(ScheduledCall{Target: target, ReqType: reqType, Payload: payload})
	if err != nil {
		return "", fmt.Errorf("actorcaps: marshal scheduled call: %w", err)
	}
	return h.Schedule(ctx, schedule.ScheduleReq{
		Bind:          bind,
		FireAt:        fireAt,
		Type:          ScheduledCallType,
		Payload:       body,
		CorrelationID: corr,
	})
}

// ParseScheduledCall decodes a fired envelope back into a ScheduledCall —
// but ONLY if env.Sender.ID == self. A scheduled call is self-authored by
// construction (the engine's FireSink always mints the pen for the row's
// own author_id — schedule/types.go's FireSink doc), so a Type=
// lib.schedule_call event NOT sent by self is not a late timer fire: nothing
// structurally stops another member from writing its OWN pen with the same
// Type and an audience naming self, and treating that as a genuine fire
// would be an injection path into the self-triggering machinery. The
// assertion is mechanical and must hold for EVERY caller, no exceptions.
func ParseScheduledCall(self actor.ActorID, env *message.Envelope) (ScheduledCall, bool) {
	if env == nil || env.Type != ScheduledCallType || env.Sender.ID != self {
		return ScheduledCall{}, false
	}
	var call ScheduledCall
	if err := json.Unmarshal(env.Payload, &call); err != nil {
		return ScheduledCall{}, false
	}
	return call, true
}
