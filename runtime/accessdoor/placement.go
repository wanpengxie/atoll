package accessdoor

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// ErrNoStoragePlacement means the address names no registered, channel-bound
// storage daemon. It is a physical routing failure, not an access verdict.
var ErrNoStoragePlacement = errors.New("accessdoor: no online storage daemon available for this channel")

// choosePlacement resolves the address's mandatory host name on every use.
// No placement-selection policy runs here: the address itself is the sole
// placement choice.
func (d *door) choosePlacement(ctx context.Context, address resourcespec.FileAddress) (StorageMount, error) {
	if d.deps.StorageMounts == nil {
		return StorageMount{}, fmt.Errorf("accessdoor: file kind placement routing not wired (Deps.StorageMounts is nil)")
	}
	mount, found, err := d.deps.StorageMounts.ResolveStorageDaemon(ctx, d.deps.ChannelID, address.Host)
	if err != nil {
		return StorageMount{}, err
	}
	if !found {
		return StorageMount{}, fmt.Errorf("%w: daemon %q is not registered and bound to channel %q", ErrNoStoragePlacement, address.Host, d.deps.ChannelID)
	}
	if !mount.Online {
		return StorageMount{}, NewHostOfflineError(address.Host)
	}
	return mount, nil
}
