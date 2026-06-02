package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// ChannelDeps are the non-db dependencies the channel store assembly needs.
type ChannelDeps struct {
	ChannelID channel.ID
	// NowFn is the TTL clock for type-install + adapter-state. Required.
	NowFn func() int64
	// Secret seals adapter credentials. Optional: nil disables AdapterCreds.
	Secret SecretBox
}

// ChannelStores is the channel-local store assembly and the SINGLE public
// construction entry point for the per-channel sqlite.
//
// §4.5 substrate confinement: it OWNS the *sql.DB in an unexported field — the
// raw handle never crosses the store boundary (no OpenChannel returning
// *sql.DB, no NewX taking *sql.DB). The message-log surface is handed out as
// SEGREGATED storespec interfaces — Log (harness write port) / Query (reads) /
// Delivery (delivery marks) — so no consumer can obtain the harness-bypass
// Append capability through a read handle (ISP/CQRS role-split, §4.5).
type ChannelStores struct {
	db *sql.DB

	Log      storespec.MessageLog   // harness write port (Append + dedup/terminal reads)
	Query    storespec.MessageQuery // scheduler / tail reads (no Append)
	Delivery storespec.DeliveryStore
	Cursors  storespec.Cursors
	Requests storespec.RequestLookup

	// Actor registry exposed via SEGREGATED interfaces (§4.5, forward-derived
	// from role — a reader never receives any membership write):
	Registry   storespec.Registry               // membership READS only (Lookup/Exists/ListActive)
	Readiness  storespec.ReadinessUpdater       // readiness projection write
	Membership storespec.MembershipControlPlane // membership WRITES: Insert/Deregister + ApplyMemberTransitions (log-emitting) + ListDesiredProxyMembers

	Ledger storespec.Ledger
	Types  storespec.TypeStore // full type_registry contract (install state machine + reads)

	AdapterState storespec.AdapterState       // channel-local adapter kv
	AdapterCreds storespec.AdapterCredentials // sealed; nil when Deps.Secret is nil
}

// OpenChannel opens the per-channel sqlite and assembles the channel stores.
// The raw *sql.DB is owned by the returned ChannelStores and never exposed.
func OpenChannel(ctx context.Context, dbPath string, opts OpenOptions, deps ChannelDeps) (*ChannelStores, error) {
	if deps.NowFn == nil {
		return nil, errors.New("store: OpenChannel requires Deps.NowFn")
	}
	db, err := openChannelDB(ctx, dbPath, opts)
	if err != nil {
		return nil, err
	}
	msgs := newMessages(db)
	reg := newActorRegistry(db)
	cs := &ChannelStores{
		db:           db,
		Log:          msgs,
		Query:        msgs,
		Delivery:     msgs,
		Cursors:      newCursors(db),
		Requests:     newRequestLookup(msgs, deps.ChannelID),
		Registry:     reg,
		Readiness:    reg,
		Membership:   reg,
		Ledger:       newLedger(db),
		Types:        newTypeRegistry(db, deps.NowFn),
		AdapterState: newAdapterStateStore(db, deps.NowFn),
	}
	if deps.Secret != nil {
		ac, err := newAdapterCredentialStore(db, deps.NowFn, deps.Secret)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		cs.AdapterCreds = ac
	}
	return cs, nil
}

// Close releases the owned *sql.DB. After Close the assembly is unusable.
func (c *ChannelStores) Close() error { return c.db.Close() }
