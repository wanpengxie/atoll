package kimi

import (
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	registry.Register("kimi", registry.ClassDecl{Kind: actor.KindTool, Placement: channel.PlacementDaemon, New: construct})
}

// construct: the Kimi WebBridge browser-extension adapter. id comes from the
// spec; blank → class default. The listen addr is config (a transport detail,
// NOT an essence-singleton constraint — two kimi instances on different addrs
// are legal); day-0 uses the default addr. The Proc reads its id back through
// sys.Self().
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
