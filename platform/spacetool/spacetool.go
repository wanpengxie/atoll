// Package spacetool implements the per-channel actor that forwards ordinary
// actor messages to c0's registrar. It owns no registrar vocabulary or typed
// payload shapes.
package spacetool

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/message"
)

func Def(caller lagoon.C0Caller) actorbase.Def {
	return actorbase.Def{Doc: "channel-zero registry operations", New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error { return serve(sys, caller) }, nil
	}}
}

func serve(sys actorbase.Sys, caller lagoon.C0Caller) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return nil
		}
		if msg.Kind == message.KindRequest {
			handle(sys, caller, msg)
		}
	}
}

func handle(sys actorbase.Sys, caller lagoon.C0Caller, msg actorbase.Msg) {
	if caller == nil {
		_, _ = sys.Fail(msg, string(lagoon.CodeResultUnknown), "space-tool unavailable")
		return
	}
	payload := bytes.TrimSpace(msg.Payload)
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		_, _ = sys.Fail(msg, string(lagoon.CodeInvalidArgs), "invalid JSON payload")
		return
	}
	forwarded := msg.Envelope
	forwarded.Payload = append(json.RawMessage(nil), payload...)
	raw, err := caller.CallRegistrar(msg.Ctx(), lagoon.Word(msg.Type), forwarded)
	if err != nil {
		var le *lagoon.Error
		if errors.As(err, &le) {
			_, _ = sys.Fail(msg, string(le.Code), le.Detail)
			return
		}
		_, _ = sys.Fail(msg, string(lagoon.CodeResultUnknown), err.Error())
		return
	}
	var reply lagoon.Reply
	if err := json.Unmarshal(raw, &reply); err != nil {
		_, _ = sys.Fail(msg, string(lagoon.CodeResultUnknown), err.Error())
		return
	}
	if err := reply.ValidValue(); err != nil {
		_, _ = sys.Fail(msg, string(lagoon.CodeResultUnknown), err.Error())
		return
	}
	_, _ = sys.Reply(msg, json.RawMessage(reply.Value))
}
