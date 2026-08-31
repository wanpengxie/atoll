package engineboot

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type registryObsAdapter struct{ registry *lagoon.Registry }

func (a registryObsAdapter) PrincipalPresent(ctx context.Context, principal string) (bool, error) {
	status, found, err := a.registry.GetPrincipalStatus(ctx, principal)
	return found && status == regspec.PrincipalPresent, err
}

func (a registryObsAdapter) Channels(ctx context.Context, parent *string) ([]obs.Row, bool, error) {
	rows, complete, err := a.registry.ObsChannels(ctx, parent)
	return marshalObsRows(rows, complete, err, func(row lagoon.ObsChannelRow) string { return string(row.ID) })
}

func (a registryObsAdapter) Channel(ctx context.Context, id string) (obs.Row, bool, error) {
	row, found, err := a.registry.ObsChannel(ctx, id)
	if err != nil || !found {
		return obs.Row{}, found, err
	}
	declared, err := json.Marshal(row)
	return obs.Row{Declared: declared}, err == nil, err
}

func (a registryObsAdapter) Principals(ctx context.Context) ([]obs.Row, bool, error) {
	rows, complete, err := a.registry.ObsPrincipals(ctx)
	return marshalObsRows(rows, complete, err, func(row lagoon.ObsPrincipalRow) string { return row.ID })
}

func (a registryObsAdapter) Daemons(ctx context.Context) ([]obs.Row, bool, error) {
	rows, complete, err := a.registry.ObsDaemons(ctx)
	return marshalObsRows(rows, complete, err, func(row lagoon.ObsDaemonRow) string { return row.ID })
}

func (a registryObsAdapter) ChannelDevices(ctx context.Context, id string) ([]obs.Row, bool, error) {
	rows, complete, err := a.registry.ObsChannelDevices(ctx, channel.ID(id))
	return marshalObsRows(rows, complete, err, func(row lagoon.ObsChannelDeviceRow) string { return row.DeviceID })
}

func (a registryObsAdapter) Decls(ctx context.Context) ([]obs.Row, bool, error) {
	rows, complete, err := a.registry.ObsDecls(ctx)
	return marshalObsRows(rows, complete, err, func(row lagoon.ObsDeclRow) string { return row.ID })
}

func marshalObsRows[T any](rows []T, complete bool, readErr error, key func(T) string) ([]obs.Row, bool, error) {
	if readErr != nil {
		return nil, complete, readErr
	}
	out := make([]obs.Row, 0, len(rows))
	for _, row := range rows {
		declared, err := json.Marshal(row)
		if err != nil {
			return nil, false, err
		}
		out = append(out, obs.Row{Key: key(row), Declared: declared})
	}
	return out, complete, nil
}

type obsChannelHost interface {
	Acquire(channel.ID) (channelhost.Bundle, bool)
}

type channelObsAdapter struct{ host obsChannelHost }

func (a channelObsAdapter) Open(id string) bool {
	_, serving := a.host.Acquire(channel.ID(id))
	return serving
}

func (a channelObsAdapter) Roster(ctx context.Context, id string) ([]obs.RosterEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	bundle, serving := a.host.Acquire(channel.ID(id))
	if !serving {
		return nil, false, nil
	}
	rows, err := bundle.View().Roster(ctx)
	if err != nil {
		return nil, true, err
	}
	out := make([]obs.RosterEntry, 0, len(rows))
	for _, row := range rows {
		declared, err := json.Marshal(row)
		if err != nil {
			return nil, true, err
		}
		out = append(out, obs.RosterEntry{
			Key: string(row.ID), Declared: declared, Bound: row.Bound,
			Device: obs.DeviceState{
				Kind: obs.DeviceStateKind(row.Device.Kind), Online: row.Device.Online,
				ReceivedAt: row.Device.ReceivedAt,
			},
		})
	}
	return out, true, nil
}

type obsDaemonHost interface {
	DaemonOnline(string) bool
	LaneAttached(string, string) bool
}

type daemonObsAdapter struct{ host obsDaemonHost }

func (a daemonObsAdapter) Online(id string) bool { return a.host.DaemonOnline(id) }
func (a daemonObsAdapter) OnlineInChannel(id, channelID string) bool {
	return a.host.LaneAttached(id, channelID)
}
