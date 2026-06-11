package runtime

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime/internal/store"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// ChannelStores is the channel-local store assembly exposed as segregated
// storespec interfaces. The raw *sql.DB is confined inside runtime/internal/store;
// this public type re-exports only the interface handles.
type ChannelStores struct {
	Log        storespec.MessageLog
	Query      storespec.MessageQuery
	Requests   storespec.RequestLookup
	Registry   storespec.Registry
	Membership storespec.MembershipControlPlane
	closer     func() error
}

// Close releases the underlying store resources.
func (c *ChannelStores) Close() error {
	if c.closer != nil {
		return c.closer()
	}
	return nil
}

// OpenChannelOptions tunes the store open.
type OpenChannelOptions struct {
	ReadOnly bool
	SkipDDL  bool
}

// OpenChannel opens the per-channel sqlite at dbPath and returns the segregated
// storespec interfaces. This is the public facade over runtime/internal/store
// (which confines the raw *sql.DB). Packages outside runtime/ use this entry
// point to obtain channel stores. channelID is the channel scope the membership
// control plane binds to (its mirror events carry this id, never a per-call arg).
func OpenChannel(ctx context.Context, channelID channel.ID, dbPath string, opts OpenChannelOptions) (*ChannelStores, error) {
	cs, err := store.OpenChannel(ctx, channelID, dbPath, store.OpenOptions{
		ReadOnly: opts.ReadOnly,
		SkipDDL:  opts.SkipDDL,
	})
	if err != nil {
		return nil, err
	}
	return &ChannelStores{
		Log:        cs.Log,
		Query:      cs.Query,
		Requests:   cs.Requests,
		Registry:   cs.Registry,
		Membership: cs.Membership,
		closer:     cs.Close,
	}, nil
}
