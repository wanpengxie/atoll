package echo

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	registry.Register("echo", registry.ClassDecl{
		Kind:      actor.KindTool,
		Placement: channel.PlacementServer,
		New:       construct,
		// ValidateConfig is the acceptance gate: it lets a declaration be
		// refused at admit time ("this config can never build") instead of
		// failing later at build. Same parser as construct — one truth.
		ValidateConfig: func(raw json.RawMessage) error {
			_, err := parseConfig(raw)
			return err
		},
	})
}

// construct: the one-knob tool. id comes from the spec (multi-capable); a
// blank spec id falls back to the class default "echo" (the one-of-each case).
// The config is parsed fail-closed — a bad config fails THIS body's build, it
// never half-builds with silent defaults — and the parsed value is captured
// into the Proc closure by Def(cfg): config is a birth parameter, not a
// runtime lookup.
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	id := spec.ID
	if id == "" {
		id = actor.ActorID("echo")
	}
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Factory: platform.ActorFactory{Proc: Def(cfg)},
	}, nil
}
