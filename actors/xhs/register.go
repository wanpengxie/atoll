package xhs

import (
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func init() { registry.Register("xhs", registry.ClassDecl{Kind: actor.KindTool, New: construct}) }

// construct: browser-extension adapter — owns a PRIVATE loopback WS endpoint the
// extension connects in to (keyless; the 127.0.0.1 bind is the trust boundary).
// id comes from the spec; blank → class default. The listen addr is config (a
// transport detail like K8s hostPort — NOT essence-singleton; two xhs instances
// on different addrs are legal); day-0 uses the default addr. The Proc reads its
// id back through sys.Self().
func construct(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	id := spec.ID
	if id == "" {
		id = DefaultActorID
	}
	cfg := Config{ListenAddr: DefaultListenAddr, Logger: ctx.Logger}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Factory: platform.ActorFactory{Proc: Def(cfg)},
	}, nil
}
