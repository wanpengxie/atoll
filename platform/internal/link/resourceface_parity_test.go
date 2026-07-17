package link_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// This file is 期11 spec §3.2's three-avatar parity DoD item (§9.4: "三化身
// parity 测试——同用例本地/live 膜/wire 代理三处同断言"), run against a REAL
// accessdoor door (accessdoor.New over a hand-rolled in-memory
// Registry/Driver/Membership/State — NOT the fakes the other tests in this
// package use, which echo deterministically without running the door's own
// decision tree at all). The local avatar (boundHandle, reached by calling
// the minter directly) and the wire avatar (remoteResourceHandle, reached
// through a real Dial/Accept round trip — which necessarily also exercises
// the middle avatar, liveResourceAccess, on the home side of accessSink) must
// observe the SAME underlying door truth: a resource created through one
// avatar is visible through the other, with identical Stat/List projections.
//
// runtime/internal/store is unreachable here (a Go internal/ package rooted
// at runtime/ — platform/internal/link cannot import it, compiler-enforced),
// so this rig's Registry/Driver/State are minimal in-memory doubles that
// implement the REAL R semantics (union authorize, creator full-rights
// grant, any-grant Stat/List projection) rather than accessdoor's own
// unexported test fakes (package-private, unusable from here) — the point is
// exercising the REAL door logic (query.go/door.go), not a second echo.

// --- minimal in-memory resourcespec.Registry / Driver / MembershipCheck / StateStore ---

type parityRegistry struct {
	mu     sync.Mutex
	rows   map[resource.ResourceID]resourcespec.ResourceMeta
	grants map[resource.ResourceID][]access.Grant
	// bytes is kv's INLINE content — real Registry.Create persists initial
	// bytes in the SAME row/transaction as the meta+grant (resourcespec's own
	// doc: "kv=内联值本身"), so this rig's fake Driver (below) reads/writes
	// THIS map rather than an independent one — otherwise Create's initial
	// bytes would silently vanish from any later Read.
	bytes map[resource.ResourceID][]byte
}

func newParityRegistry() *parityRegistry {
	return &parityRegistry{
		rows:   map[resource.ResourceID]resourcespec.ResourceMeta{},
		grants: map[resource.ResourceID][]access.Grant{},
		bytes:  map[resource.ResourceID][]byte{},
	}
}

func (r *parityRegistry) Resolve(_ context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.rows[id]
	return meta, ok, nil
}

func (r *parityRegistry) Create(_ context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, placementDaemonID, placementCoord string, initial []byte, _ resourcespec.ResourceBirthPlan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.rows[id]; exists {
		return resourcespec.ErrAlreadyExists
	}
	r.rows[id] = resourcespec.ResourceMeta{
		Kind: kind, CreatedAt: time.Now().UnixNano(),
		PlacementDaemonID: placementDaemonID, PlacementCoord: placementCoord,
		CreatedBy: creator,
	}
	r.bytes[id] = initial
	// Birth-time full-rights grant — the ownership predicate (design doc's
	// "出生满权 grant").
	r.grants[id] = []access.Grant{{
		GranteeKind: access.GranteeActor, Grantee: creator,
		Ops: []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete},
	}}
	return nil
}

func (r *parityRegistry) ReserveCreate(context.Context, resource.ResourceID, resourcespec.ResourceKind, actor.ActorID, string, string, bool, resourcespec.ResourceBirthPlan) (string, error) {
	return "", errors.New("parityRegistry: ReserveCreate not exercised by this rig (kv-only)")
}
func (r *parityRegistry) CommitReservation(context.Context, string) (resourcespec.LandedResource, bool, error) {
	return resourcespec.LandedResource{}, false, errors.New("parityRegistry: CommitReservation not exercised by this rig (kv-only)")
}
func (r *parityRegistry) ClearTombstone(context.Context, string) (bool, error) {
	return false, errors.New("parityRegistry: ClearTombstone not exercised by this rig (kv-only)")
}
func (r *parityRegistry) ReservationDaemon(context.Context, string) (string, bool, error) {
	return "", false, errors.New("parityRegistry: ReservationDaemon not exercised by this rig (kv-only)")
}
func (r *parityRegistry) TombstoneDaemon(context.Context, string) (string, bool, error) {
	return "", false, errors.New("parityRegistry: TombstoneDaemon not exercised by this rig (kv-only)")
}
func (r *parityRegistry) ListReservationsByDaemon(context.Context, string) ([]resourcespec.ReservationRow, error) {
	return nil, errors.New("parityRegistry: ListReservationsByDaemon not exercised by this rig (kv-only)")
}
func (r *parityRegistry) ListTombstonesByDaemon(context.Context, string) ([]resourcespec.TombstoneRow, error) {
	return nil, errors.New("parityRegistry: ListTombstonesByDaemon not exercised by this rig (kv-only)")
}
func (r *parityRegistry) ListByPlacementDaemon(context.Context, string) ([]resourcespec.ResourceRow, error) {
	return nil, errors.New("parityRegistry: ListByPlacementDaemon not exercised by this rig (kv-only)")
}
func (r *parityRegistry) SweepExpiredReservations(context.Context, string, int64) ([]resourcespec.ReservationRow, error) {
	return nil, errors.New("parityRegistry: SweepExpiredReservations not exercised by this rig (kv-only)")
}
func (r *parityRegistry) TouchReservationsByCoords(context.Context, string, []string, int64) error {
	return errors.New("parityRegistry: TouchReservationsByCoords not exercised by this rig (kv-only)")
}
func (r *parityRegistry) ActorAllows(_ context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, g := range r.grants[id] {
		if g.GranteeKind == access.GranteeActor && g.Grantee == caller {
			for _, o := range g.Ops {
				if o == op {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (r *parityRegistry) MembersAllow(_ context.Context, id resource.ResourceID, op access.Operation) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, g := range r.grants[id] {
		if g.GranteeKind == access.GranteeMembers {
			for _, o := range g.Ops {
				if o == op {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (r *parityRegistry) SetGrant(_ context.Context, id resource.ResourceID, g access.Grant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.grants[id][:0]
	for _, existing := range r.grants[id] {
		if existing.GranteeKind == g.GranteeKind && existing.Grantee == g.Grantee {
			continue
		}
		kept = append(kept, existing)
	}
	if len(g.Ops) > 0 {
		kept = append(kept, g)
	}
	r.grants[id] = kept
	return nil
}

func (r *parityRegistry) Delete(_ context.Context, id resource.ResourceID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.rows, id)
	delete(r.grants, id)
	return nil
}

func (r *parityRegistry) List(_ context.Context, prefix string, limit int, cursor string) ([]resourcespec.ResourceRow, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []resourcespec.ResourceRow
	for id, meta := range r.rows {
		if prefix != "" && (len(string(id)) < len(prefix) || string(id)[:len(prefix)] != prefix) {
			continue
		}
		out = append(out, resourcespec.ResourceRow{ID: id, Meta: meta, Grants: append([]access.Grant(nil), r.grants[id]...)})
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, "", nil // this rig never exercises pagination beyond one page
}

// parityDriver is the kv byte realizer — it shares parityRegistry's OWN bytes
// map (see that field's doc): a real kv driver and its Registry are the SAME
// DB row, so this fake preserves that co-location rather than drifting into
// two independent byte stores.
type parityDriver struct{ reg *parityRegistry }

func newParityDriver(reg *parityRegistry) *parityDriver { return &parityDriver{reg: reg} }

func (d *parityDriver) Read(_ context.Context, id resource.ResourceID) ([]byte, bool, error) {
	d.reg.mu.Lock()
	defer d.reg.mu.Unlock()
	v, ok := d.reg.bytes[id]
	return v, ok, nil
}
func (d *parityDriver) Write(_ context.Context, id resource.ResourceID, value []byte) error {
	d.reg.mu.Lock()
	defer d.reg.mu.Unlock()
	d.reg.bytes[id] = value
	return nil
}
func (d *parityDriver) Delete(_ context.Context, id resource.ResourceID) error {
	d.reg.mu.Lock()
	defer d.reg.mu.Unlock()
	delete(d.reg.bytes, id)
	return nil
}

// parityMembership treats every id as a channel member — this rig's tests
// are about resource-face parity, not membership decay (accessdoor's own
// decay_test.go owns that matrix over a real store).
type parityMembership struct{}

func (parityMembership) IsMember(context.Context, actor.ActorID) (bool, error) { return true, nil }

// Lookup is not exercised by this kv-only parity rig (file-kind placement
// routing, §4.3).
func (parityMembership) Lookup(context.Context, actor.ActorID) (string, bool, error) {
	return "", false, nil
}

func (parityMembership) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	return storespec.ActorControlRow{ID: id, CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement()}, true, nil
}
func (parityMembership) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (parityMembership) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	return storespec.WorldDurable, true, nil
}
func (parityMembership) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	return storespec.AuthorOK, nil
}

type parityOverlay struct{}

func (parityOverlay) ActorAllows(context.Context, actor.ActorID, resource.ResourceID, access.Operation) (bool, error) {
	return false, nil
}
func (parityOverlay) SetGrant(context.Context, resource.ResourceID, access.Grant) error { return nil }
func (parityOverlay) EndBatch([]actor.ActorID)                                          {}
func (parityOverlay) DeleteResource(resource.ResourceID)                                {}

// parityState is a StateStore no-op stub (Deps requires one; this rig never
// exercises the actor-scoped locus).
type parityState struct{}

func (parityState) Create(context.Context, actor.ActorID, resource.ResourceID, []byte) error {
	return errors.New("parityState: not exercised by this rig")
}
func (parityState) Read(context.Context, actor.ActorID, resource.ResourceID) ([]byte, bool, error) {
	return nil, false, nil
}
func (parityState) Write(context.Context, actor.ActorID, resource.ResourceID, []byte) (bool, error) {
	return false, nil
}
func (parityState) Delete(context.Context, actor.ActorID, resource.ResourceID) (bool, error) {
	return false, nil
}

// newParityDoor builds a REAL accessdoor.AccessMinter over the in-memory
// doubles above.
func newParityDoor(t *testing.T) accessdoor.AccessMinter {
	t.Helper()
	reg := newParityRegistry()
	m, err := accessdoor.New(accessdoor.Deps{
		Registry:  reg,
		Drivers:   accessdoor.DriverTable{accessdoor.KindKV: newParityDriver(reg)},
		Authority: parityMembership{},
		Overlay:   parityOverlay{},
		State:     parityState{},
	})
	if err != nil {
		t.Fatalf("accessdoor.New: %v", err)
	}
	return m
}

// TestResourceFaceThreeAvatarParity is 期11 spec §3.2's three-avatar parity
// DoD proof: create through ONE avatar, read it back through the OTHER — both
// must see the identical door truth, proving boundHandle (local),
// liveResourceAccess (the home-side membrane accessSink wraps every Create/
// Query call in), and remoteResourceHandle (the wire proxy) form one
// consistent capability, not three independently-shaped ones.
func TestResourceFaceThreeAvatarParity(t *testing.T) {
	const callerID = actor.ActorID("tool:parity")
	realMinter := newParityDoor(t)

	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := newTestAcceptor(t, link.Config{
		Minter:    &stubMinter{},
		Access:    realMinter,
		Schedule:  &fakeScheduleMinter{},
		Runtime:   rt,
		ChannelID: testChannelID,
		LeasePing: 5 * time.Second,
		LeaseTTL:  30 * time.Second,
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-parity")
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close(); rt.StopAll() })

	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:],
		[]link.Declaration{{ActorID: callerID, Kind: actor.KindTool, Binding: actor.BindingRuntimeInboundViaRelay}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()
	arms, err := d.OpenStream(context.Background(), callerID, 0, "", func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	d.Start()

	ctx := context.Background()
	local := realMinter.Mint(storespec.AuthorStamp{ID: callerID, BirthVersion: 1}) // LOCAL avatar
	spec := resourcespec.CreateSpec{Kind: resourcespec.KindKV}

	t.Run("create local, read via wire (port avatar sees local avatar's write)", func(t *testing.T) {
		out, err := local.Create(ctx, "r-from-local", spec, []byte("hello"))
		if err != nil || !out.Accepted() {
			t.Fatalf("local create: out=%+v err=%v", out, err)
		}

		statRes, err := arms.Access.Stat(ctx, "r-from-local")
		if err != nil {
			t.Fatalf("port stat: %v", err)
		}
		if statRes.Reject != "" {
			t.Fatalf("port stat reject = %q, want none (the wire avatar must see the local avatar's write)", statRes.Reject)
		}
		if statRes.Meta.Kind != resourcespec.KindKV || statRes.Meta.CreatedBy != callerID {
			t.Fatalf("port stat meta = %+v", statRes.Meta)
		}

		readOut, err := arms.Access.Invoke(ctx, access.OpRead, "r-from-local", nil, nil)
		if err != nil || string(readOut.Value) != "hello" {
			t.Fatalf("port read: out=%+v err=%v, want value=hello", readOut, err)
		}
	})

	t.Run("create via wire, read local (local avatar sees the port avatar's write)", func(t *testing.T) {
		out, err := arms.Access.Create(ctx, "r-from-port", spec, []byte("world"))
		if err != nil || !out.Accepted() {
			t.Fatalf("port create: out=%+v err=%v", out, err)
		}

		statRes, err := local.Stat(ctx, "r-from-port")
		if err != nil {
			t.Fatalf("local stat: %v", err)
		}
		if statRes.Reject != "" {
			t.Fatalf("local stat reject = %q, want none (the local avatar must see the wire avatar's write)", statRes.Reject)
		}

		readOut, err := local.Invoke(ctx, access.OpRead, "r-from-port", nil, nil)
		if err != nil || string(readOut.Value) != "world" {
			t.Fatalf("local read: out=%+v err=%v, want value=world", readOut, err)
		}
	})

	t.Run("List projects both resources identically from either avatar", func(t *testing.T) {
		localPage, err := local.List(ctx, accessdoor.ListQuery{})
		if err != nil {
			t.Fatalf("local list: %v", err)
		}
		portPage, err := arms.Access.List(ctx, accessdoor.ListQuery{})
		if err != nil {
			t.Fatalf("port list: %v", err)
		}
		localIDs := map[resource.ResourceID]bool{}
		for _, e := range localPage.Entries {
			localIDs[e.ID] = true
		}
		portIDs := map[resource.ResourceID]bool{}
		for _, e := range portPage.Entries {
			portIDs[e.ID] = true
		}
		if len(localIDs) != len(portIDs) || !localIDs["r-from-local"] || !localIDs["r-from-port"] ||
			!portIDs["r-from-local"] || !portIDs["r-from-port"] {
			t.Fatalf("List divergence between avatars: local=%v port=%v", localIDs, portIDs)
		}
	})

	t.Run("zero-rights Stat masquerades as not_found identically on both avatars", func(t *testing.T) {
		// A caller with no grant on either resource — mint under a different
		// identity that never created anything and holds no share.
		otherLocal := realMinter.Mint(storespec.AuthorStamp{ID: actor.ActorID("tool:parity-stranger"), BirthVersion: 1})
		res, err := otherLocal.Stat(ctx, "r-from-local")
		if err != nil {
			t.Fatalf("stranger local stat: %v", err)
		}
		if res.Reject != accessdoor.QueryNotFound {
			t.Fatalf("stranger local stat reject = %q, want not_found", res.Reject)
		}
	})
}
