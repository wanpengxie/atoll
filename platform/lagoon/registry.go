package lagoon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon/internal/store"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
	"golang.org/x/crypto/bcrypt"
)

type Change struct {
	ChannelID     channel.ID
	Principal     string
	AllChannels   bool
	AllPrincipals bool
}

// Registry is lagoon's public, read-only registry face. SQL and scanning are
// confined to internal/store; registrar mutations use the same private store.
type Registry struct {
	store    *store.Store
	onCommit func(Change)
}

func Open(path string, onCommit func(Change)) (*Registry, error) {
	storage, err := store.Open(path)
	if err != nil {
		return nil, err
	}
	return &Registry{store: storage, onCommit: onCommit}, nil
}

func (r *Registry) Close() error {
	if r == nil {
		return nil
	}
	return r.store.Close()
}

func (r *Registry) VerifyCredential(ctx context.Context, email, presented string) (string, bool, error) {
	id, hash, found, err := r.store.CredentialHash(ctx, email)
	if err != nil || !found {
		return "", false, err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(presented)) != nil {
		return "", false, nil
	}
	return id, true, nil
}

func (r *Registry) ResolveDeviceKey(ctx context.Context, key string) (string, bool, error) {
	return r.store.ResolveDeviceKey(ctx, key)
}

func (r *Registry) GetDevice(ctx context.Context, id string) (regspec.DeviceRow, bool, error) {
	return r.store.GetDevice(ctx, id)
}

func (r *Registry) GetDeviceByName(ctx context.Context, name string) (regspec.DeviceRow, bool, error) {
	return r.store.GetDeviceByName(ctx, name)
}

func (r *Registry) ResolveDeviceName(ctx context.Context, name string) (string, bool, bool, error) {
	row, found, err := r.store.GetDeviceByName(ctx, name)
	return row.ID, row.Status == regspec.DevicePresent, found, err
}

func (r *Registry) GetDeviceFact(ctx context.Context, id string) (regspec.DeviceStatus, bool, error) {
	return r.store.GetDeviceStatus(ctx, id)
}

func (r *Registry) GetPrincipalStatus(ctx context.Context, id string) (regspec.PrincipalStatus, bool, error) {
	return r.store.GetPrincipalStatus(ctx, id)
}

func (r *Registry) ListPrincipals(ctx context.Context) ([]regspec.PrincipalRow, error) {
	return r.store.ListPrincipals(ctx)
}

func (r *Registry) ListDevices(ctx context.Context) ([]regspec.DeviceRow, error) {
	return r.store.ListDevices(ctx)
}

func (r *Registry) ListChannels(ctx context.Context) ([]regspec.ChannelRow, error) {
	rows, err := r.store.ListChannels(ctx)
	if err != nil {
		return nil, err
	}
	return qualifyChannelRows(rows)
}

func (r *Registry) ListPresentChannels(ctx context.Context) ([]regspec.ChannelRow, error) {
	rows, err := r.ListChannels(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]regspec.ChannelRow, 0, len(rows))
	for _, row := range rows {
		if row.Status == regspec.ChannelPresent {
			out = append(out, row)
		}
	}
	return out, nil
}

func (r *Registry) GetChannelDesired(ctx context.Context, id channel.ID) (regspec.ChannelRow, bool, error) {
	rows, err := r.ListChannels(ctx)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	for _, row := range rows {
		if row.ID == id {
			return row, true, nil
		}
	}
	return regspec.ChannelRow{}, false, nil
}

func (r *Registry) ResolveChannel(ctx context.Context, id channel.ID, qualified string) (regspec.ChannelRow, bool, error) {
	rows, err := r.ListChannels(ctx)
	if err != nil {
		return regspec.ChannelRow{}, false, err
	}
	for _, row := range rows {
		if (id != "" && row.ID == id) || (id == "" && qualified != "" && row.QualifiedName == qualified) {
			return row, true, nil
		}
	}
	return regspec.ChannelRow{}, false, nil
}

func (r *Registry) ListEndpoints(ctx context.Context, ch channel.ID) ([]regspec.EndpointRow, error) {
	return r.store.ListEndpoints(ctx, ch)
}

func (r *Registry) GetChannelTemplate(ctx context.Context, id string) (regspec.ChannelTemplateRow, bool, error) {
	return r.store.GetChannelTemplate(ctx, id)
}

func (r *Registry) ListChannelTemplates(ctx context.Context) ([]regspec.ChannelTemplateRow, error) {
	return r.store.ListChannelTemplates(ctx)
}

func (r *Registry) LocalDeviceKey(ctx context.Context) (string, error) {
	row, ok, err := r.store.GetDevice(ctx, protocol.LocalDeviceID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("lagoon: local device missing")
	}
	return row.Key, nil
}

func (r *Registry) Describe(ctx context.Context, target channel.ID, caller channel.ID) (introspect.Describe, error) {
	row, ok, err := r.GetChannelDesired(ctx, target)
	if err != nil {
		return introspect.Describe{}, err
	}
	if !ok || row.Status != regspec.ChannelPresent {
		return introspect.Describe{}, errors.New("lagoon: channel not found")
	}
	endpoints, err := r.ListEndpoints(ctx, target)
	if err != nil {
		return introspect.Describe{}, err
	}
	types := make(map[string]introspect.TypeMeta, len(endpoints)+3)
	for _, endpoint := range endpoints {
		meta := introspect.TypeMeta{Description: endpoint.Description}
		if len(endpoint.Meta) > 0 {
			var extra struct {
				Examples []json.RawMessage `json:"examples"`
				Schema   json.RawMessage   `json:"schema"`
			}
			if json.Unmarshal(endpoint.Meta, &extra) == nil {
				if len(extra.Examples) > 0 {
					meta.PayloadExample = extra.Examples[0]
				}
				meta.InputSchema = extra.Schema
			}
		}
		types[endpoint.Name] = meta
	}
	if target != protocol.C0ChannelID && (caller == protocol.C0ChannelID || caller == row.ParentID) {
		types["channel.introduce_actor"] = introspect.TypeMeta{Description: "Introduce a channel member."}
		types["channel.remove_actor"] = introspect.TypeMeta{Description: "Remove a channel member."}
		types["channel.restart_actor"] = introspect.TypeMeta{Description: "Restart a channel member."}
	}
	return introspect.Describe{ActorID: row.QualifiedName, Description: row.Description, Types: types}, nil
}

func qualifyChannelRows(rows []regspec.ChannelRow) ([]regspec.ChannelRow, error) {
	byID := make(map[channel.ID]int, len(rows))
	for i := range rows {
		if _, exists := byID[rows[i].ID]; exists {
			return nil, fmt.Errorf("lagoon: duplicate channel id %q", rows[i].ID)
		}
		byID[rows[i].ID] = i
	}
	state := make(map[channel.ID]uint8, len(rows))
	var qualify func(channel.ID) (string, error)
	qualify = func(id channel.ID) (string, error) {
		i, ok := byID[id]
		if !ok {
			return "", fmt.Errorf("lagoon: channel %q has no row", id)
		}
		if state[id] == 2 {
			return rows[i].QualifiedName, nil
		}
		if state[id] == 1 {
			return "", errors.New("lagoon: channel parent cycle")
		}
		state[id] = 1
		row := &rows[i]
		if row.ParentID == "" {
			if row.ID != protocol.C0ChannelID || row.Name != string(protocol.C0ChannelID) {
				return "", fmt.Errorf("lagoon: non-c0 channel %q has no parent", id)
			}
			row.QualifiedName = row.Name
		} else {
			parent, err := qualify(row.ParentID)
			if err != nil {
				return "", err
			}
			qualified, err := JoinName(parent, row.Name)
			if err != nil {
				return "", err
			}
			row.QualifiedName = qualified
		}
		state[id] = 2
		return row.QualifiedName, nil
	}
	for i := range rows {
		if _, err := qualify(rows[i].ID); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (r *Registry) GetDecl(ctx context.Context, id string) (regspec.DeclRow, bool, error) {
	return r.store.GetDecl(ctx, id)
}

func (r *Registry) ListDecls(ctx context.Context) ([]regspec.DeclRow, error) {
	return r.store.ListDecls(ctx)
}

func (r *Registry) GetOverlays(ctx context.Context, ch channel.ID) ([]regspec.OverlayRow, error) {
	return r.store.GetOverlays(ctx, ch)
}

func (r *Registry) IsBound(ctx context.Context, ch channel.ID, device string) (bool, error) {
	return r.store.IsBound(ctx, ch, device)
}

// ListBoundDeviceIDs is the narrow placement boundary used by channel homes;
// device credentials and other registry columns never cross it.
func (r *Registry) ListBoundDeviceIDs(ctx context.Context, ch channel.ID) ([]string, error) {
	return r.store.ListBoundDeviceIDs(ctx, ch)
}

func (r *Registry) ChannelDesired(ctx context.Context, id channel.ID) (channelspec.ChannelDesiredFacts, bool, error) {
	row, ok, err := r.GetChannelDesired(ctx, id)
	if err != nil || !ok {
		return channelspec.ChannelDesiredFacts{}, ok, err
	}
	return channelspec.ChannelDesiredFacts{Present: row.Status == regspec.ChannelPresent, ParentID: row.ParentID}, true, nil
}
