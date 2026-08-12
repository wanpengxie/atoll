package home

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Call invokes one actor through Home's system-owned call port and returns the
// terminal payload unchanged. Business-specific seat lookup and interpretation
// belong to the assembly side of this generic mechanism boundary.
func Call(h *Home, ctx context.Context, target actor.ActorID, word string, payload any) (json.RawMessage, error) {
	if h == nil || h.callPort == nil || h.closed.Load() {
		return nil, ErrClosed
	}
	msg, err := h.callPort.Call(ctx, target, word, payload)
	if err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), msg.Payload...), nil
}
