package feishu

import (
	"github.com/coagent-ai/coagent/adapters/framework"
	"github.com/coagent-ai/coagent/kernel/adapter"
)

// Name is the adapter identifier consumed by framework.Register /
// BuildRegistered / RegisteredFactories.
const Name = "feishu"

func init() {
	framework.Register(Name, defaultFactory)
}

// defaultFactory is the registered constructor. Production daemons
// receive a fully-populated Deps bundle; tests that want a custom
// BaseURL build the Module via feishu.New(...) directly.
func defaultFactory(deps framework.Deps) adapter.Module {
	return New(WithDeps(deps))
}
