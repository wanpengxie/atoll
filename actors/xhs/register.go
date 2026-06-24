package xhs

import (
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

func init() { registry.Register("xhs", construct) }

// construct: browser-extension adapter — owns a PRIVATE loopback WS endpoint the
// extension connects in to (keyless; the 127.0.0.1 bind is the trust boundary).
// id comes from the spec; blank → class default. The loopback addr is config (a
// transport detail like K8s hostPort — NOT essence-singleton; two xhs instances
// on different addrs are legal); day-0 uses the default addr.
func construct(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	id := spec.ID
	if id == "" {
		id = DefaultActorID
	}
	cfg := Config{ListenAddr: DefaultListenAddr, Logger: ctx.Logger}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
		Factory: func(w harness.Pen) actorrt.Actor { return NewActor(w, cfg) },
	}, nil
}
