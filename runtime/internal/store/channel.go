package store

import (
	"context"
	"database/sql"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
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

	// Plane-2 (access/resource) implementations over the SAME channel db. These
	// are the door's collaborators, handed up as resourcespec CONTRACTS (never
	// the concrete rows / raw db). They stay WITHIN the runtime tree — the door
	// is assembled one layer up (runtime.OpenChannel) and only the welded
	// AccessMinter is exposed downstream; the raw R + byte surfaces never leak
	// past that assembly (no bypass-the-door write path escapes).
	Resources resourcespec.Registry // R (authorization relation) + resource existence
	KVDriver  resourcespec.Driver   // KindKV byte realizer (inline bytes)

	// State is the owner-keyed byte realizer for the ACTOR-SCOPED locus (the
	// actor_state table), dual to Resources+KVDriver on the channel-scoped side.
	// It has no R and no kind routing — that absence IS the scope law (§12.9). It
	// too stays within the runtime tree: the collapsed branch is assembled into
	// the SAME door one layer up, reached downstream only through the welded
	// AccessMinter.MintState, never as a raw store.
	State resourcespec.StateStore

	// timers is the identity-level durable pending-timer store (the timers
	// table, forward §7 / timer-build-spec §3.3) — the ONE raw TimerStore
	// instance for this channel. Unlike Resources/State it is deliberately NOT
	// exported: a raw TimerStore reachable outside package store is a delayed
	// forged-author write path around the pen (红线❻), so unlike the door's
	// collapsed-branch stores (whose confinement is enforced by "only the welded
	// minter is exposed downstream"), the timer store has no minter-shaped
	// collaborator sitting between it and a caller yet — the schedule engine's
	// assembly (OpenScheduler, runtime 根装配缝, 期5 后续切片) is the only
	// intended reader, and it reaches this field from within the runtime tree,
	// never as a public ChannelStores member.
	timers timerspec.TimerStore
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
		Resources:  newResourceRegistry(db),
		KVDriver:   newKVDriver(db),
		State:      newStateStore(db),
		timers:     newTimerStore(db),
	}
	return cs, nil
}

// Close releases the owned *sql.DB. After Close the assembly is unusable.
func (c *ChannelStores) Close() error { return c.db.Close() }

// Timers exposes the raw timerspec.TimerStore for the one intended reader
// within the runtime tree — runtime.OpenScheduler (the schedule engine's
// assembly seam, timer-build-spec.md §3.4). It is a METHOD, not a promoted
// exported field: the accessor itself is the confinement marker (an
// unexported field plus one narrow reader), the same discipline the raw
// *sql.DB follows via Close/Open rather than a public field. The runtime
// package (a different Go package, even though it is the sole importer of
// this internal one) cannot reach an unexported struct field directly — this
// is the seam that lets it without promoting timers to a public
// ChannelStores member (红线❻: no raw TimerStore public surface).
func (c *ChannelStores) Timers() timerspec.TimerStore { return c.timers }
