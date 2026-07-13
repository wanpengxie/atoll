package runtime

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// ChannelStores is the channel-local store assembly exposed as segregated
// storespec interfaces. The raw *sql.DB is confined inside runtime/internal/store;
// this public type re-exports only the interface handles.
type ChannelStores struct {
	Log      storespec.MessageLog
	Query    storespec.MessageQuery
	Expiry   storespec.ExpiryQuery
	Requests storespec.RequestLookup
	Registry storespec.Registry

	// Principals is the principal-axis read face (LookupActivePrincipal — the
	// admission path's "which active instance embodies this subject" query),
	// wired EXPLICITLY as its own field even though the same concrete backend
	// serves Registry above. Precedent rule: a consumer needing a capability
	// face gets it declared here at assembly — never recovered by
	// type-asserting a narrower field back to the concrete's wider surface
	// (that assertion is a bypass valve: it voids the interface segregation
	// for every ChannelStores holder at once — 反旁路结构墙).
	Principals storespec.PrincipalRegistry

	Membership storespec.MembershipControlPlane

	// Access is the plane-2 door's single outward face — the welded AccessMinter.
	// The resourcespec.Registry / Driver behind it are deliberately NOT re-exported:
	// handing out the raw R + byte surfaces would be a bypass-the-door write path
	// (the anti-bypass wall). Downstream mints a caller-welded AccessHandle from
	// this and speaks only Invoke, exactly as it speaks harness.Pen for plane-1.
	Access accessdoor.AccessMinter

	// Outbox is the SAME underlying resource registry re-exported under the
	// narrow resourcespec.ResourceOutbox slice (期11 spec §4.7): reservation/
	// tombstone completion for the daemon control-RPC handlers platform
	// assembly wires (Committed/ReclaimAck/ReconcilePull). The general R
	// surface (ActorAllows/MembersAllow/SetGrant/Create/Delete/Resolve) stays
	// confined behind the door alone — the SAME anti-bypass wall Access's own
	// doc above names, drawn one field narrower here rather than widened.
	Outbox resourcespec.ResourceOutbox

	// timers is the identity-level durable pending-timer store. Unexported
	// for the same reason Access is public but its R/byte collaborators are
	// not: a raw TimerStore reachable downstream is a delayed forged-author
	// write path around the pen. Its ONE intended reader is OpenScheduler
	// (scheduleopen.go), which lives in this same package — no minter-shaped
	// collaborator sits between it and this field yet because the schedule
	// engine (unlike accessdoor) is the reader, not a caller-facing decision
	// tree.
	timers timerspec.TimerStore

	closer func() error
}

// channelMembershipCheck adapts the channel's actor-membership registry to the
// door's MembershipCheck seam (create's container locus + members-grant late
// binding). It wraps Lookup + Record.IsActive rather than Exists: a deregistered
// actor still has a row (Exists=true) but is NOT a current member, so member
// grants must stop resolving for it the moment it deregisters (late binding, R
// left untouched).
type channelMembershipCheck struct {
	registry storespec.Registry
}

func (c channelMembershipCheck) IsMember(ctx context.Context, id actor.ActorID) (bool, error) {
	rec, ok, err := c.registry.Lookup(ctx, id)
	if err != nil {
		return false, err
	}
	return ok && rec.IsActive(), nil
}

// Lookup adapts storespec.Registry.Lookup to accessdoor.MembershipCheck's
// placement-routing seam (期11 spec §4.3's policy chain ①): Host only, "not
// found" collapsed to found=false regardless of whether the row is merely
// absent or present-but-deregistered (IsActive, mirroring IsMember above —
// a deregistered actor is not a valid creator-affinity source either).
func (c channelMembershipCheck) Lookup(ctx context.Context, id actor.ActorID) (string, bool, error) {
	rec, ok, err := c.registry.Lookup(ctx, id)
	if err != nil {
		return "", false, err
	}
	if !ok || !rec.IsActive() {
		return "", false, nil
	}
	return rec.Host, true, nil
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
	// ReadOnly / SkipDDL skip the idempotent DDL bootstrap (the only migration
	// mechanism) while the schema verifier still runs — they REQUIRE the file to
	// already carry the current schema baseline. A file last touched by an older
	// binary fails fast ("stale channel DB") until one ordinary read-write open
	// self-heals it. Deliberate: admitting a table-short DB would only defer the
	// crash to the first query against the missing table.
	ReadOnly bool
	SkipDDL  bool

	// OnCommit is the post-commit signal source: the store fires it after any
	// durable append commit (request path AND control-plane mirror), so a tap is
	// woken identically regardless of which write path advanced the log. nil =
	// no subscriber. The callback must be non-blocking (a lossy fan-out wake).
	OnCommit func()

	// StorageMounts / StorageControl are file-kind placement's platform-filled
	// injection points (期11 spec §4.3: "注入点契约 runtime 定,实现填充下游
	// 做") — this package DEFINES the accessdoor.Deps shape they land in but
	// never answers them itself (it has no notion of a link/wire or an
	// attach-state table). nil is legal: a channel opened without them (e.g.
	// a kv-only test rig, or before platform's link Acceptor exists —
	// §4.3's own late-bound-injection escape hatch covers that ordering) can
	// resolve authorization and complete kv/kv creates exactly as before;
	// only a file-kind Create fails honestly (§4.3's own reject path).
	StorageMounts accessdoor.StorageMounts
	// StorageControl issues the door's own AllocRequest once placement is
	// chosen (see accessdoor.StorageControl's doc for why this Dep exists
	// beyond spec §4.3's literal placement-CHOICE list).
	StorageControl accessdoor.StorageControl
	// LaneControl mints §5's file byte-route Token — same late-bound,
	// nil-safe injection-point discipline as StorageMounts/StorageControl
	// above (platform fills it over the link Acceptor; nil leaves file
	// OpRead/OpWrite/with_content-create honestly erroring rather than
	// fabricating a route).
	LaneControl accessdoor.LaneControl
}

// OpenChannel opens the per-channel sqlite at dbPath and returns the segregated
// storespec interfaces. This is the public facade over runtime/internal/store
// (which confines the raw *sql.DB). It is the channel-store ASSEMBLY surface —
// the returned ChannelStores.Log/Membership are raw write capabilities that
// bypass the harness gate, so assembly is confined to platform: only the
// platform tree may import this package (enforced by
// archtest.TestRuntimeAssemblyConfinedToPlatform). channelID is the channel
// scope the membership control plane binds to (its mirror events carry this id,
// never a per-call arg).
func OpenChannel(ctx context.Context, channelID channel.ID, dbPath string, opts OpenChannelOptions) (*ChannelStores, error) {
	cs, err := store.OpenChannel(ctx, channelID, dbPath, store.OpenOptions{
		ReadOnly: opts.ReadOnly,
		SkipDDL:  opts.SkipDDL,
	}, opts.OnCommit)
	if err != nil {
		return nil, err
	}

	// Assemble the whole plane-2 door here: this is the dependency confluence
	// point — the R + byte implementations come up from the store, the membership
	// seam wraps the same channel's actor registry. New fail-fasts on an
	// incomplete assembly (missing KindKV driver), so a mis-wired open fails at
	// open, not at first Invoke.
	access, err := accessdoor.New(accessdoor.Deps{
		Registry:       cs.Resources,
		Drivers:        accessdoor.DriverTable{resourcespec.KindKV: cs.KVDriver},
		Membership:     channelMembershipCheck{registry: cs.Registry},
		State:          cs.State,
		ChannelID:      channelID,
		StorageMounts:  opts.StorageMounts,
		StorageControl: opts.StorageControl,
		LaneControl:    opts.LaneControl,
	})
	if err != nil {
		_ = cs.Close()
		return nil, err
	}

	return &ChannelStores{
		Log:        cs.Log,
		Query:      cs.Query,
		Expiry:     cs.Expiry,
		Requests:   cs.Requests,
		Registry:   cs.Registry,
		Principals: cs.Principals,
		Membership: cs.Membership,
		Access:     access,
		Outbox:     cs.Resources,
		timers:     cs.Timers(),
		closer:     cs.Close,
	}, nil
}
