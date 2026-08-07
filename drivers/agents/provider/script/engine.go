package script

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type engine struct {
	sys    actorbase.Sys
	toolID actor.ActorID
	events base.EventPort
	mu     sync.Mutex
	booted bool
	closed bool
}

func newEngine(sys actorbase.Sys, toolID actor.ActorID, events base.EventPort) *engine {
	return &engine{sys: sys, toolID: toolID, events: events}
}
func (e *engine) Boot(context.Context, base.BootPort) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("script: closed")
	}
	e.booted = true
	return nil
}
func (e *engine) StartTurn(op base.OpID, batch []base.Trigger, _ []base.ContextItem) error {
	e.mu.Lock()
	ready := e.booted && !e.closed
	e.mu.Unlock()
	if !ready {
		return errors.New("script: engine unavailable")
	}
	if len(batch) == 0 {
		e.events.TurnRejected(op, "provider_failed", "empty batch")
		return nil
	}
	turnID := string(op)
	e.events.TurnStarted(op, turnID)
	trigger := batch[len(batch)-1]
	go func() {
		msg := actorbase.NewMsg(actorbase.OriginMailbox, e.sys.Life(), trigger.Envelope)
		capture := &captureSys{Sys: e.sys}
		switch msg.Type {
		case TypeChat:
			handleChat(capture, msg, e.toolID)
		case TypeVerify:
			handleVerify(capture, msg)
		default:
			capture.Fail(msg, "type_unsupported", fmt.Sprintf("script actor does not handle %s", msg.Type))
		}
		if capture.failure != nil {
			e.events.TurnEnded(turnID, base.TurnStatusFailed, "", capture.failure.detail)
			return
		}
		raw, _ := json.Marshal(capture.reply)
		e.events.TurnEnded(turnID, base.TurnStatusOK, string(raw), "")
	}()
	return nil
}
func (e *engine) Steer(op base.OpID, _ base.Trigger) error {
	e.events.ControlDone(op, base.ControlNotSteerable, "", "script is turn based")
	return nil
}
func (e *engine) Interrupt(op base.OpID) error {
	e.events.ControlDone(op, base.ControlNoActiveTurn, "", "")
	return nil
}
func (*engine) Terminate() error { return nil }
func (e *engine) EnsureAlive(op base.OpID) error {
	e.events.ControlDone(op, base.ControlAccepted, "", "")
	return nil
}
func (*engine) Describe() introspect.Describe {
	return introspect.Describe{Description: actorDoc, SkillDoc: "# script\n\nDeterministic loop.chat and loop.verify regression provider.", Types: map[string]introspect.TypeMeta{TypeChat: {Description: "call echo and persist payload", AllowedKinds: []string{string(message.KindRequest)}}, TypeVerify: {Description: "verify persisted resource", AllowedKinds: []string{string(message.KindRequest)}}}}
}
func (e *engine) Close() error { e.mu.Lock(); e.closed = true; e.mu.Unlock(); return nil }

type capturedFailure struct{ code, detail string }
type captureSys struct {
	actorbase.Sys
	reply   any
	failure *capturedFailure
}

func (s *captureSys) Reply(_ actorbase.Msg, v any) (message.ID, error) {
	s.reply = v
	return "captured", nil
}
func (s *captureSys) Fail(_ actorbase.Msg, code, detail string) (message.ID, error) {
	s.failure = &capturedFailure{code, detail}
	return "captured", nil
}

var _ base.Engine = (*engine)(nil)
