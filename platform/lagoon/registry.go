package lagoon

import (
	"context"

	"github.com/wanpengxie/atoll/platform/lagoon/internal/store"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/channel"
	"golang.org/x/crypto/bcrypt"
)

type Change struct {
	ChannelID   channel.ID
	Principal   string
	AllChannels bool
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

func (r *Registry) GetDeviceFact(ctx context.Context, id string) (regspec.DeviceStatus, bool, error) {
	return r.store.GetDeviceStatus(ctx, id)
}

func (r *Registry) GetPrincipalStatus(ctx context.Context, id string) (regspec.PrincipalStatus, bool, error) {
	return r.store.GetPrincipalStatus(ctx, id)
}

func (r *Registry) ListChannels(ctx context.Context) ([]regspec.ChannelRow, error) {
	return r.store.ListChannels(ctx)
}

func (r *Registry) ListPresentChannels(ctx context.Context) ([]regspec.ChannelRow, error) {
	return r.store.ListPresentChannels(ctx)
}

func (r *Registry) GetChannelDesired(ctx context.Context, id channel.ID) (regspec.ChannelRow, bool, error) {
	return r.store.GetChannel(ctx, id)
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

func (r *Registry) ListBoundDevices(ctx context.Context, ch channel.ID) ([]regspec.DeviceRow, error) {
	return r.store.ListBoundDevices(ctx, ch)
}

// ListBoundDeviceIDs is the narrow placement boundary used by channel homes;
// device credentials and other registry columns never cross it.
func (r *Registry) ListBoundDeviceIDs(ctx context.Context, ch channel.ID) ([]string, error) {
	return r.store.ListBoundDeviceIDs(ctx, ch)
}
