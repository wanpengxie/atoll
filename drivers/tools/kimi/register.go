package kimi

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

// ConfigSchema is the declaration's knob set. listen_addr names WHICH browser
// this seat drives: seats naming the same address share one endpoint and one
// extension (see plugindevice/shared.go), and a seat naming a different one
// joins a different pool — a second browser profile with its own extension.
//
// It lives in the declaration because that is where a deployment fact belongs.
// The class used to hard-code the default, which made the address unsettable at
// birth: the only way to move it was a runtime word, and a second seat could
// therefore never be born anywhere but on top of the first.
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
			return Config{}, fmt.Errorf("kimi: %w", err)
		}
	}
	addr := spec.ListenAddr
	if addr == "" {
		addr = DefaultListenAddr
	}
	if err := plugindevice.ValidateAddr(addr); err != nil {
		return Config{}, fmt.Errorf("kimi: %w", err)
	}
	return Config{ListenAddr: addr}, nil
}

func init() {
	registry.Register("kimi", registry.ClassDecl{
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

// construct: the Kimi WebBridge browser-extension adapter. id comes from the
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
