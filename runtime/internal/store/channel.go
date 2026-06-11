package store

import (
	"context"
	"database/sql"

	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

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
	Requests storespec.RequestLookup

	// Actor registry exposed via SEGREGATED interfaces (§4.5, forward-derived
	// from role — a reader never receives any membership write):
	Registry   storespec.Registry               // membership READS only (Lookup/Exists/ListActive)
	Membership storespec.MembershipControlPlane // membership WRITES: Insert/Deregister + ApplyMemberTransitions (log-emitting)
}

// OpenChannel opens the per-channel sqlite and assembles the channel stores.
// The raw *sql.DB is owned by the returned ChannelStores and never exposed.
//
// channelID is the channel scope, bound at construction. The store is bound to
// ONE channel — its scope is fixed here, not re-asserted per call. The
// membership control plane stamps this bound id into its mirror events (a
// per-call channelID would be a pseudo-parameter the caller could lie about,
// writing a foreign-channel row into this channel's sqlite — the same truth
// corruption the harness shape-step dies to prevent; cf. FindByID, whose channel
// scope is likewise the binding, not a per-call arg).
//
// onCommit is the post-commit signal source wired into BOTH write paths (the
// request-path Append and the control-plane mirror append): the append
// chokepoint produces "the log advanced" so a downstream tap is woken
// identically regardless of which path committed. nil = no subscriber. May be
// nil for read-only / test opens.
func OpenChannel(ctx context.Context, channelID channel.ID, dbPath string, opts OpenOptions, onCommit func()) (*ChannelStores, error) {
	db, err := openChannelDB(ctx, dbPath, opts)
	if err != nil {
		return nil, err
	}
	msgs := newMessages(db, onCommit)
	reg := newActorRegistry(db, channelID, onCommit)
	cs := &ChannelStores{
		db:         db,
		Log:        msgs,
		Query:      msgs,
		Requests:   newRequestLookup(msgs),
		Registry:   reg,
		Membership: reg,
	}
	return cs, nil
}

// Close releases the owned *sql.DB. After Close the assembly is unusable.
func (c *ChannelStores) Close() error { return c.db.Close() }
