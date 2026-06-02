// Package daemon is the attached-compute assembly (v2): it composes host
// (business cells) + homelink (connect to server) + localdevice (local device
// host) into a runnable process. cloud daemon and user/proxy daemon are the
// same binary. cmd/daemon selects concrete adapters and injects obs.
package daemon

import (
	"context"

	"github.com/wanpengxie/ActOS/daemon/host"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// Config configures the attached compute.
type Config struct {
	ServerWS  string
	APIKey    string
	ChannelID channel.ID
}

// Run connects to the channel home and hosts the supplied business adapters as
// cells (no local truth — emit flows up via homelink). (Minimal homelink uplink
// for now; full WS attach/heartbeat lands incrementally — the host+uplink
// compose is the core.)
func Run(ctx context.Context, cfg Config, modules []behavior.Module) error {
	// homelink uplink seam (minimal): a real homelink dials cfg.ServerWS with
	// cfg.APIKey and forwards EmitFrames up; here it is a no-op placeholder so
	// the compose type-checks and runs.
	emit := func(ctx context.Context, frame computebus.EmitFrame) error { return nil }

	h := host.New(emit)
	defer h.Stop()
	for _, mod := range modules {
		if _, err := h.InstallAdapter(ctx, mod, adapterhost.InstallDeps{ChannelID: cfg.ChannelID}); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return nil
}
