package actorrt

import (
	"context"
	"io"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func attachTest(r *Runtime, hsCtx context.Context, conn io.ReadWriteCloser, sinks Sinks, resolve ResolveFunc, kindOf KindOf, onCancelRequest func(actor.ActorID, message.ID)) (Incarnation, error) {
	prepared, err := r.PrepareHandshake(hsCtx, conn, sinks, resolve, kindOf, onCancelRequest, func(Incarnation) {})
	if err != nil {
		return Incarnation{}, err
	}
	return prepared.Commit(func() bool { return true })
}
