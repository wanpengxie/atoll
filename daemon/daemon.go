// Package daemon is the attached-compute assembly (v2): host (business cells) +
// homelink (connect to server) + localdevice. cloud daemon and user/proxy
// daemon are the same binary. cmd/daemon selects concrete adapters.
package daemon

import (
	"context"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/daemon/homelink"
	"github.com/wanpengxie/ActOS/daemon/host"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// Config configures the attached compute.
type Config struct {
	ServerWS  string
	APIKey    string
	ComputeID string
	ChannelID channel.ID
}

// Run connects to the channel home and hosts the supplied business adapters as
// cells. Dispatch frames from the home flow into host cells; their emits flow
// up via homelink (blocking on the home's EmitAck). No local truth.
func Run(ctx context.Context, cfg Config, modules []behavior.Module) error {
	hosts := make([]actor.ActorID, 0, len(modules))
	for _, mod := range modules {
		hosts = append(hosts, mod.Declares().ActorID)
	}
	computeID := cfg.ComputeID
	if computeID == "" {
		computeID = uuid.NewString()
	}

	var h *host.Host
	hl, err := homelink.Connect(ctx, cfg.ServerWS, cfg.APIKey, computeID, hosts,
		func(df computebus.DispatchFrame) {
			if h != nil {
				_ = h.Dispatch(ctx, df)
			}
		})
	if err != nil {
		return err
	}
	defer hl.Close()

	h = host.New(hl.Emit)
	defer h.Stop()
	for _, mod := range modules {
		if _, err := h.InstallAdapter(ctx, mod, adapterhost.InstallDeps{ChannelID: cfg.ChannelID}); err != nil {
			return err
		}
	}

	<-ctx.Done()
	return nil
}
