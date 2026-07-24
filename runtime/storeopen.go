package runtime

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// ChannelStores is the channel-local store assembly exposed as segregated
// storespec interfaces. The raw *sql.DB is confined inside runtime/internal/store;
// this public type re-exports only the interface handles.
type ChannelStores struct {
	Log             storespec.MessageLog
	Query           storespec.MessageQuery
	Visible         storespec.VisibleMessageQuery
	Expiry          storespec.ExpiryQuery
	Requests        storespec.RequestLookup
	Declared        storespec.DeclaredControlReader
	DeclAdmission   storespec.DeclAdmissionStore
	Cascade         storespec.CascadeStore
	Routing         storespec.ChannelRouting
	Genesis         storespec.GenesisStore
	SysOps          storespec.SysOpAdmission
	DeclarationSync storespec.DeclarationSyncStore
	Bindings        storespec.DaemonBindingReader
	ResourceRead    storespec.ResourceReadStore

	// Principals is the principal-axis read face (LookupActivePrincipal — the
	// admission path's "which active instance embodies this subject" query),
	// wired EXPLICITLY as its own field even though the same concrete backend
	// serves Registry above. Precedent rule: a consumer needing a capability
	// face gets it declared here at assembly — never recovered by
	// type-asserting a narrower field back to the concrete's wider surface
	// (that assertion is a bypass valve: it voids the interface segregation
	// for every ChannelStores holder at once — 反旁路结构墙).
	Principals storespec.PrincipalRegistry

	// Assembly contains raw leaf-store ports used only by Platform, the
	// channel composition root, to assemble peer Runtime organs. Runtime
	// opens the store organ; it does not assemble Access or Scheduler.
	Assembly AssemblyPorts

	closer func() error
}

type AssemblyPorts struct {
	Resources resourcespec.Registry
	KV        resourcespec.Driver
	State     resourcespec.StateStore
	Timers    timerspec.TimerStore
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
	// ReadOnly / MustExist skip fresh-schema initialization while the
	// verifier still runs. They require an existing DB with the current schema;
	// no released legacy schema is migrated in place.
	ReadOnly  bool
	MustExist bool

	// OnCommit is the post-commit signal source: the store fires it after any
	// durable append commit (request path AND control-plane mirror), so a tap is
	// woken identically regardless of which write path advanced the log. nil =
	// no subscriber. The callback must be non-blocking (a lossy fan-out wake).
	OnCommit func()
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
		ReadOnly:  opts.ReadOnly,
		MustExist: opts.MustExist,
	}, opts.OnCommit)
	if err != nil {
		return nil, err
	}

	return &ChannelStores{
		Log:             cs.Log,
		Query:           cs.Query,
		Visible:         cs.Visible,
		Expiry:          cs.Expiry,
		Requests:        cs.Requests,
		Declared:        cs.Declared,
		DeclAdmission:   cs.DeclAdmission,
		Cascade:         cs.Cascade,
		Routing:         cs.Routing,
		Genesis:         cs.Genesis,
		SysOps:          cs.SysOps,
		DeclarationSync: cs.DeclarationSync,
		Bindings:        cs.Bindings,
		ResourceRead:    cs.ResourceRead,
		Principals:      cs.Principals,
		Assembly: AssemblyPorts{
			Resources: cs.Resources,
			KV:        cs.KVDriver,
			State:     cs.State,
			Timers:    cs.Timers(),
		},
		closer: cs.Close,
	}, nil
}
