// Package devicehost runs one authenticated device carrier: the compute link
// to a server's /compute plus a compartment for every bound channel. It is the
// shared guts of the standalone daemon binary (cmd/daemon) and the in-process
// local device inside `atoll up` (cmd/atoll) — one device implementation, two
// packagings. Availability (which classes are compiled in) stays the BINARY's
// choice via blank imports; this package never imports drivers.
package devicehost

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/wanpengxie/atoll/drivers/devicehost/internal/storagehost"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// Config is one carrier's identity and wiring.
type Config struct {
	ServerWS   string // ws(s)://host/compute
	Credential string // daemon api key
	// DeviceLabel is operator-facing only. The accepted carrier verdict supplies
	// the authoritative DeviceID used by every compartment and actor.
	DeviceLabel string
	AtollHome   string // device workspace root
	Logger      *slog.Logger
	// OnAttached relays compute.Config.OnAttached: the server-assigned daemon
	// id on every accepted attach — the assembly root's chance to complete the
	// persisted identity triple. Called from the carrier goroutine.
	OnAttached func(daemonID string)
}

// Run drives the carrier until ctx ends. It is exactly the standalone daemon's
// body: compute.Run with per-channel compartments backed by storagehost.
func Run(ctx context.Context, cfg Config) error {
	var attachedID atomic.Value
	return compute.Run(ctx, compute.Config{
		ServerWS:   cfg.ServerWS,
		Credential: cfg.Credential,
		AtollHome:  cfg.AtollHome,
		Logger:     cfg.Logger,
		OnAttached: func(daemonID string) {
			// compute calls OnAttached before it binds the carrier and starts any
			// compartment, so every constructor below observes the accepted id.
			attachedID.Store(daemonID)
			if cfg.OnAttached != nil {
				cfg.OnAttached(daemonID)
			}
		},
		BuildCompartment: func(chID, workspaceDir string) (compute.CompartmentResources, error) {
			deviceID, _ := attachedID.Load().(string)
			if deviceID == "" {
				return compute.CompartmentResources{}, errors.New("devicehost: compartment built before carrier identity was accepted")
			}
			sh, err := storagehost.Open(workspaceDir)
			if err != nil {
				return compute.CompartmentResources{}, err
			}
			adapter := storageHostAdapter{host: sh}
			return compute.CompartmentResources{
				Factories: classFactories{
					chID: chID, wsRoot: workspaceDir, deviceID: deviceID, deviceLabel: cfg.DeviceLabel,
					logger: cfg.Logger.With("channel", chID),
				},
				LocalFileOpener: adapter, Close: sh.Close,
			}, nil
		},
	})
}

// classFactories resolves one body's factory at BUILD time from the class and
// config that body's own desired carries — the daemon-side mirror of the server
// host's registry lookup. There is no plan snapshot here and no state at all:
// the desired the Host serves is the one plan ledger, and the registry is
// compiled-in code. A class this daemon cannot build fails that body alone
// (logged by the caller, retried on the Host's backoff) instead of holding the
// whole plan hostage.
type classFactories struct {
	chID, wsRoot, deviceID, deviceLabel string
	logger                              *slog.Logger
}

func (f classFactories) BuildClass(
	id actor.ActorID,
	class string,
	config json.RawMessage,
) (platform.ActorFactory, bool) {
	decl, err := registry.Build(class, registry.InstanceSpec{
		ID:     id,
		Config: config,
	}, registry.Deps{
		ChannelID:    channel.ID(f.chID),
		WorkspaceDir: f.wsRoot,
		DeviceID:     f.deviceID,
		DeviceLabel:  f.deviceLabel,
		Logger:       f.logger,
	})
	if err != nil {
		f.logger.Error("devicehost: build class failed",
			"channel", f.chID, "actor", id, "class", class, "err", err)
		return platform.ActorFactory{}, false
	}
	// A constructor that rewrites the id would produce a body claiming an
	// identity the plan never named. Refuse the build outright — there is no
	// table to file it under, so the only way it could leak is by being built,
	// and it is not.
	if decl.ID != id {
		f.logger.Warn("devicehost: class derived a different id",
			"actor", id, "class", class, "derived", decl.ID)
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}
