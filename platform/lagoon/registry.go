package lagoon

import (
	"context"
	"errors"
	"fmt"

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
