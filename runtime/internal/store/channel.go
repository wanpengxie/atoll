package store

import (
	"context"
	"database/sql"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// ChannelDeps are the non-db dependencies the channel store assembly needs.
type ChannelDeps struct {
	ChannelID channel.ID
}

// ChannelStores is the channel-local store assembly and the SINGLE public
// construction entry point for the per-channel sqlite.
//
// §4.5 substrate confinement: it OWNS the *sql.DB in an unexported field — the
// raw handle never crosses the store boundary (no OpenChannel returning
// *sql.DB, no NewX taking *sql.DB). The message-log surface is handed out as
// SEGREGATED storespec interfaces — Log (harness write port) / Query (reads) —
// so a reader can never obtain the harness-bypass Append (ISP/CQRS role-split,
// §4.5).
type ChannelStores struct {
	db *sql.DB

	Log      storespec.MessageLog   // harness write port (Append + terminal-uniqueness reads)
	Query    storespec.MessageQuery // tail reads (no Append)
	Cursors  storespec.Cursors
	Requests storespec.RequestLookup

	// Actor registry exposed via SEGREGATED interfaces (§4.5, forward-derived
	// from role — a reader never receives any membership write):
	Registry   storespec.Registry               // membership READS only (Lookup/Exists/ListActive)
	Membership storespec.MembershipControlPlane // membership WRITES: Insert/Deregister + ApplyMemberTransitions (log-emitting)
}

// OpenChannel opens the per-channel sqlite and assembles the channel stores.
// The raw *sql.DB is owned by the returned ChannelStores and never exposed.
func OpenChannel(ctx context.Context, dbPath string, opts OpenOptions, deps ChannelDeps) (*ChannelStores, error) {
	db, err := openChannelDB(ctx, dbPath, opts)
	if err != nil {
		return nil, err
	}
	msgs := newMessages(db)
	reg := newActorRegistry(db)
	cs := &ChannelStores{
		db:         db,
		Log:        msgs,
		Query:      msgs,
		Cursors:    newCursors(db),
		Requests:   newRequestLookup(msgs),
		Registry:   reg,
		Membership: reg,
	}
	return cs, nil
}

// Close releases the owned *sql.DB. After Close the assembly is unusable.
func (c *ChannelStores) Close() error { return c.db.Close() }
