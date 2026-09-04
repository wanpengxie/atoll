package xhs

import (
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// ConfigSchema is the declaration's knob set. See kimi/register.go for why the
// endpoint belongs to the declaration rather than to the class.
const ConfigSchema = `{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "listen_addr": {
      "type": "string",
      "description": "host:port the browser extension dials. Seats sharing an address share one browser connection; a different address is a different browser. Defaults to 127.0.0.1:10086, which is the extension's own default."
    }
  }
}`

type specConfig struct {
	ListenAddr string `json:"listen_addr,omitempty"`
}

// parseConfig is the ONE parser: the admit-time gate and the build both call it,
// so a config that admits can always build.
func parseConfig(raw json.RawMessage) (Config, error) {
	spec := specConfig{}
	if len(raw) > 0 {
		if err := actorbase.DecodeStrict(raw, &spec); err != nil {
			return Config{}, fmt.Errorf("xhs: %w", err)
		}
	}
	addr := spec.ListenAddr
	if addr == "" {
		addr = DefaultListenAddr
	}
	if err := plugindevice.ValidateAddr(addr); err != nil {
		return Config{}, fmt.Errorf("xhs: %w", err)
	}
	return Config{ListenAddr: addr}, nil
}

func init() {
	registry.Register("xhs", registry.ClassDecl{
		Kind:      actor.KindTool,
		Placement: channelspec.PlacementDaemon,
		Manifest:  manifest(),
		New:       construct,
		ValidateConfig: func(raw json.RawMessage) error {
			_, err := parseConfig(raw)
			return err
		},
		ConfigSchema: json.RawMessage(ConfigSchema),
	})
}

// construct: the xhs browser-extension adapter. id comes from the
// spec; blank → class default. The endpoint comes from the DECLARATION, and the
// session — the tab group this seat writes into — comes from the channel, so
// isolation is right by default rather than by anyone remembering.
func construct(spec registry.InstanceSpec, ctx registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	id := spec.ID
	if id == "" {
		id = DefaultActorID
	}
	cfg.Logger = ctx.Logger
	cfg.Session = "atoll:" + string(ctx.ChannelID)
	return platform.ActorDecl{
		ID:      id,
		Kind:    actor.KindTool,
		Factory: platform.ActorFactory{Proc: Def(cfg)},
	}, nil
}
