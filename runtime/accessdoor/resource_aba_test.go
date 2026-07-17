package accessdoor_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	store "github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// DoD 10 / S10 ABA 交叉测试：resourceGate 全覆盖后，"判权→执行"全段同一把锁，
// 但锁只保证串行，不保证"旧身份的授权不会挂到同 ID 的新肉身上"——那条不变量要
// 靠 overlay 随 Delete 清理 + CommitReservation 只读已存 plan（不重分类）两条
// 机制共同兑现。本文件用真实 store（runtime/internal/store）驱动 Registry，
// 只有 Authority/Overlay 用最小 fake，专门盯这条 ABA 缝。

type abaAuthority struct {
	mu     sync.Mutex
	rows   map[actor.ActorID]storespec.ActorControlRow
	worlds map[actor.ActorID]storespec.ActorWorld
}

func newAbaAuthority() *abaAuthority {
	return &abaAuthority{rows: map[actor.ActorID]storespec.ActorControlRow{}, worlds: map[actor.ActorID]storespec.ActorWorld{}}
}

func (a *abaAuthority) admit(id actor.ActorID, world storespec.ActorWorld) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rows[id] = storespec.ActorControlRow{ID: id, Kind: actor.KindAgent, CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement()}
	a.worlds[id] = world
}

func (a *abaAuthority) end(id actor.ActorID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.rows, id)
	delete(a.worlds, id)
}

func (a *abaAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.rows[id]
	return r, ok, nil
}
func (a *abaAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (a *abaAuthority) WorldOf(_ context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w, ok := a.worlds[id]
	return w, ok, nil
}
func (a *abaAuthority) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	row, ok, err := a.LookupActive(ctx, stamp.ID)
	if err != nil || !ok {
		return storespec.AuthorNotMember, err
	}
	if row.CurrentDeclVersion != stamp.BirthVersion {
		return storespec.AuthorVersionStale, nil
	}
	return storespec.AuthorOK, nil
}

type abaOverlay struct {
	mu     sync.Mutex
	grants map[resource.ResourceID]map[actor.ActorID]map[access.Operation]bool
}

func newAbaOverlay() *abaOverlay {
	return &abaOverlay{grants: map[resource.ResourceID]map[actor.ActorID]map[access.Operation]bool{}}
}

func (o *abaOverlay) ActorAllows(_ context.Context, id actor.ActorID, rid resource.ResourceID, op access.Operation) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.grants[rid][id][op], nil
}
func (o *abaOverlay) SetGrant(_ context.Context, rid resource.ResourceID, grant access.Grant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.grants[rid] == nil {
		o.grants[rid] = map[actor.ActorID]map[access.Operation]bool{}
	}
	ops := map[access.Operation]bool{}
	for _, op := range grant.Ops {
		ops[op] = true
	}
	o.grants[rid][grant.Grantee] = ops
	return nil
}
func (o *abaOverlay) EndBatch(ids []actor.ActorID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for rid, actors := range o.grants {
		for _, id := range ids {
			delete(actors, id)
		}
		if len(actors) == 0 {
			delete(o.grants, rid)
		}
	}
}
func (o *abaOverlay) DeleteResource(id resource.ResourceID) {
	o.mu.Lock()
	delete(o.grants, id)
	o.mu.Unlock()
}

// hasAnyOverlayGrant reports whether id still carries ANY overlay entry for
// rid — used to assert a stale grantee's row was actually wiped, not merely
// zeroed to an empty-but-present ops set.
func (o *abaOverlay) hasAnyOverlayGrant(rid resource.ResourceID, id actor.ActorID) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	ops, ok := o.grants[rid][id]
	if !ok {
		return false
	}
	for _, present := range ops {
		if present {
			return true
		}
	}
	return false
}

type abaMount struct{ daemon string }

func (m abaMount) ListStorageDaemons(context.Context, channel.ID) ([]accessdoor.StorageMount, error) {
	return []accessdoor.StorageMount{{DaemonID: m.daemon, Online: true}}, nil
}

type abaStorageControl struct{}

func (abaStorageControl) AllocRequest(context.Context, string, accessdoor.StorageAllocSpec) error {
	return nil
}
func (abaStorageControl) ReclaimRequest(context.Context, string, string) error { return nil }

type abaLane struct{}

func (abaLane) OpenTransfer(context.Context, string, string, string, access.Operation, string) (string, error) {
	return "opaque-transfer-token", nil
}

func newAbaAssembly(t *testing.T, authority *abaAuthority, overlay *abaOverlay) (accessdoor.AccessMinter, accessdoor.ResourceCompletion, *store.ChannelStores) {
	t.Helper()
	ctx := context.Background()
	cs, err := store.OpenChannel(ctx, "resource-aba", filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	minter, completion, err := accessdoor.NewAssembly(accessdoor.Deps{
		Registry: cs.Resources, Drivers: accessdoor.DriverTable{accessdoor.KindKV: cs.KVDriver},
		Authority: authority, Overlay: overlay, State: cs.State,
		ChannelID: "resource-aba", StorageMounts: abaMount{daemon: "daemon-aba"},
		StorageControl: abaStorageControl{}, LaneControl: abaLane{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return minter, completion, cs
}

// TestResourceDeleteRecreateSameIDDeniesStaleOverlayGrantee covers ABA bullet
// ①: a forked creator's own-created KV resource picks up a second forked
// grantee via overlay Set; the resource is then deleted and immediately
// recreated under the SAME resource_id by a different creator. The stale
// grantee's OLD overlay row must not silently keep authorizing the NEW
// incarnation — door.invoke's Delete branch already calls
// Overlay.DeleteResource(id) inside the resourceGate, so the ABA window must
// already be closed by the time Create lands the new row under the same gate.
func TestResourceDeleteRecreateSameIDDeniesStaleOverlayGrantee(t *testing.T) {
	authority := newAbaAuthority()
	overlay := newAbaOverlay()
	const (
		creator1 actor.ActorID = "agent:parent/creator-1"
		grantee2 actor.ActorID = "agent:parent/grantee-2"
		creator3 actor.ActorID = "agent:parent/creator-3"
	)
	authority.admit(creator1, storespec.WorldRun)
	authority.admit(grantee2, storespec.WorldRun)
	// creator3 is DURABLE (declared), not forked: its recreate installs a
	// BirthCreatorIdentity grant (creator-exclusive), never a channel-wide
	// GranteeMembers {Read,Write} row. That is deliberate — a forked recreate
	// would legitimately hand every member (including grantee2) fresh
	// members-half read/write, which is NOT the ABA leak this test targets;
	// pinning the recreator to durable isolates "did the OLD overlay row
	// survive" from "does the NEW incarnation's own authority happen to also
	// cover grantee2".
	authority.admit(creator3, storespec.WorldDurable)
	minter, _, cs := newAbaAssembly(t, authority, overlay)
	ctx := context.Background()
	const rid resource.ResourceID = "kv:aba-recreate"

	creator1Handle := minter.Mint(storespec.AuthorStamp{ID: creator1, BirthVersion: 1})
	if out, err := creator1Handle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindKV}, []byte("v1")); err != nil || !out.Accepted() {
		t.Fatalf("initial create=(%+v,%v)", out, err)
	}
	// creator1's own overlay right to Set carries the share to grantee2 — the
	// "ShareActor" overlay-Set half exercised standalone in the third test
	// below; here it is only the setup for the ABA scenario.
	if out, err := creator1Handle.Invoke(ctx, access.OpSet, rid, nil, &access.Grant{
		GranteeKind: access.GranteeActor, Grantee: grantee2, Ops: []access.Operation{access.OpRead, access.OpWrite},
	}); err != nil || !out.Accepted() {
		t.Fatalf("share to grantee2=(%+v,%v)", out, err)
	}
	if allowed, _ := overlay.ActorAllows(ctx, grantee2, rid, access.OpRead); !allowed {
		t.Fatal("setup: grantee2 overlay grant did not land before delete")
	}

	if out, err := creator1Handle.Invoke(ctx, access.OpDelete, rid, nil, nil); err != nil || !out.Accepted() {
		t.Fatalf("delete=(%+v,%v)", out, err)
	}
	if overlay.hasAnyOverlayGrant(rid, grantee2) {
		t.Fatal("stale grantee2 overlay row survived Delete — ABA window open at the delete half")
	}

	creator3Handle := minter.Mint(storespec.AuthorStamp{ID: creator3, BirthVersion: 1})
	if out, err := creator3Handle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindKV}, []byte("v2")); err != nil || !out.Accepted() {
		t.Fatalf("recreate=(%+v,%v)", out, err)
	}

	grantee2Handle := minter.Mint(storespec.AuthorStamp{ID: grantee2, BirthVersion: 1})
	if out, err := grantee2Handle.Invoke(ctx, access.OpRead, rid, nil, nil); err != nil {
		t.Fatalf("stale grantee read after recreate errored: %v", err)
	} else if out.Accepted() {
		t.Fatal("stale overlay grantee read the NEW incarnation of a recreated resource — ABA leak")
	}
	if out, err := grantee2Handle.Invoke(ctx, access.OpWrite, rid, []byte("hijack"), nil); err != nil {
		t.Fatalf("stale grantee write after recreate errored: %v", err)
	} else if out.Accepted() {
		t.Fatal("stale overlay grantee wrote the NEW incarnation of a recreated resource — ABA leak")
	}

	// The new incarnation must be authoritatively creator3's, not a residue of
	// creator1's grant.
	meta, ok, err := cs.Resources.Resolve(ctx, rid)
	if err != nil || !ok || meta.CreatedBy != creator3 {
		t.Fatalf("post-recreate meta=(%+v,%v,%v)", meta, ok, err)
	}
}

// TestDeleteAndAsyncCommitReservationCrossWindowNeverLandsStaleAuthority
// covers ABA bullet ②: a content-bearing file create reserves resourceID X
// under creator1 (R1, still pending — content has not landed yet). Before R1
// completes, a SECOND, unrelated content-less create for the SAME id lands
// under creator2 (this is legal: Resolve only sees committed rows, and R1's
// reservation is a separate table — exists()==false until something
// commits). creator2's landed row is then deleted — Delete's own
// supersedePendingReservationsTx sweeps EVERY still-pending reservation on
// that resource_id (tombstoning its coord and deleting the reservation row)
// specifically so a reservation whose write handle is still open cannot land
// on a resource_id that has moved on underneath it. R1's belated Committed
// therefore MUST observe found=false (a clean no-op per CommitReservation's
// own replay-safety contract), never fabricate a landing under creator1's
// stale plan. This is S10's crash-atomicity claim's live-delete twin: the
// gate serializes create/delete/commit, and the delete half is what actually
// closes the ABA window — this test pins that it does, and that nothing
// downstream (creator1, creator2, or a fresh creator3) ends up holding
// authority over a resource that only ever half-existed.
func TestDeleteAndAsyncCommitReservationCrossWindowNeverLandsStaleAuthority(t *testing.T) {
	authority := newAbaAuthority()
	overlay := newAbaOverlay()
	const (
		creator1 actor.ActorID = "agent:parent/reservation-creator"
		creator2 actor.ActorID = "agent:parent/interim-creator"
		creator3 actor.ActorID = "agent:parent/final-creator"
	)
	authority.admit(creator1, storespec.WorldRun)
	authority.admit(creator2, storespec.WorldRun)
	authority.admit(creator3, storespec.WorldRun)
	minter, completion, cs := newAbaAssembly(t, authority, overlay)
	ctx := context.Background()
	const rid resource.ResourceID = "file:aba-cross-window"

	creator1Handle := minter.Mint(storespec.AuthorStamp{ID: creator1, BirthVersion: 1})
	out, err := creator1Handle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindFile, WithContent: true}, nil)
	if err != nil || !out.Accepted() || out.Route == nil || out.Route.ReservationID == "" {
		t.Fatalf("content-bearing reserve=(%+v,%v)", out, err)
	}
	reservationID := out.Route.ReservationID

	// The reservation is durable but the resource row does not exist yet —
	// Resolve must still report not-found.
	if _, exists, rerr := cs.Resources.Resolve(ctx, rid); rerr != nil || exists {
		t.Fatalf("resource visible before commit: exists=%v err=%v", exists, rerr)
	}

	// An unrelated content-less create for the SAME id lands synchronously
	// under a different creator while R1 is still outstanding.
	creator2Handle := minter.Mint(storespec.AuthorStamp{ID: creator2, BirthVersion: 1})
	interim, err := creator2Handle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindFile}, nil)
	if err != nil || !interim.Accepted() {
		t.Fatalf("interim create=(%+v,%v)", interim, err)
	}
	if meta, ok, merr := cs.Resources.Resolve(ctx, rid); merr != nil || !ok || meta.CreatedBy != creator2 {
		t.Fatalf("interim landing meta=(%+v,%v,%v)", meta, ok, merr)
	}

	// creator2's interim row is deleted. Delete's supersede half sweeps R1
	// (still pending, same resource_id) in the SAME transaction.
	if out, err := creator2Handle.Invoke(ctx, access.OpDelete, rid, nil, nil); err != nil || !out.Accepted() {
		t.Fatalf("delete interim=(%+v,%v)", out, err)
	}

	// R1's belated Committed fires now, through the SAME gate. It must be a
	// clean no-op — landing it would be exactly the ABA leak (stale
	// creator1 authority materializing on a resource_id that moved on to
	// creator2 and back to nothing while R1 sat pending).
	landed, found, cerr := completion.CommitReservation(ctx, reservationID)
	if cerr != nil || found {
		t.Fatalf("stale reservation landed after its resource_id was deleted: landed=%+v found=%v err=%v", landed, found, cerr)
	}
	if _, exists, rerr := cs.Resources.Resolve(ctx, rid); rerr != nil || exists {
		t.Fatalf("resource materialized from a superseded reservation: exists=%v err=%v", exists, rerr)
	}

	// Neither creator1 (the superseded reservation's author) nor creator2
	// (the deleted interim creator) holds anything on this id anymore.
	for _, id := range []actor.ActorID{creator1, creator2} {
		if allowed, _ := cs.Resources.ActorAllows(ctx, id, rid, access.OpRead); allowed {
			t.Fatalf("%s retained a durable grant across the delete/supersede window — ABA leak", id)
		}
		if overlay.hasAnyOverlayGrant(rid, id) {
			t.Fatalf("%s retained an overlay grant across the delete/supersede window — ABA leak", id)
		}
	}

	// A fresh, unrelated creator can now legitimately land the same id with
	// its own exclusive authority — no residue from either earlier actor.
	creator3Handle := minter.Mint(storespec.AuthorStamp{ID: creator3, BirthVersion: 1})
	final, err := creator3Handle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindFile}, nil)
	if err != nil || !final.Accepted() {
		t.Fatalf("final create=(%+v,%v)", final, err)
	}
	meta, ok, merr := cs.Resources.Resolve(ctx, rid)
	if merr != nil || !ok || meta.CreatedBy != creator3 {
		t.Fatalf("post-recreate meta=(%+v,%v,%v)", meta, ok, merr)
	}
	for _, id := range []actor.ActorID{creator1, creator2} {
		if allowed, _ := overlay.ActorAllows(ctx, id, rid, access.OpRead); allowed {
			t.Fatalf("%s can read creator3's fresh resource — ABA leak survived a full recreate cycle", id)
		}
	}
}

// TestForkedCreatorSelfCreatedKVThreeActionsMembersWriteOverlaySetOverlayDelete
// covers ABA/S10 bullet ③: a forked creator's three canonical actions on its
// own self-created KV resource, each pinned to the HALF of the authorization
// surface that actually carries it —
//   - Write lands through the durable GranteeMembers half (birth=ChannelOwned
//     installs {Read,Write} for the whole channel membership, not through the
//     creator's personal overlay row) — proven by having a COMPLETELY
//     UNRELATED member, with no overlay entry at all, perform the write;
//   - ShareActor (OpSet) lands through the creator's overlay Set right (no
//     durable Set grant exists for a forked creator — only overlay carries
//     it);
//   - Delete lands through the creator's overlay Delete right (members only
//     ever get {Read,Write}; a forked creator has no durable Delete anywhere,
//     so a successful Delete is proof the overlay half authorized it).
func TestForkedCreatorSelfCreatedKVThreeActionsMembersWriteOverlaySetOverlayDelete(t *testing.T) {
	authority := newAbaAuthority()
	overlay := newAbaOverlay()
	const (
		creator     actor.ActorID = "agent:parent/kv-creator"
		otherMember actor.ActorID = "human:unrelated-member"
		sharee      actor.ActorID = "agent:parent/sharee"
	)
	authority.admit(creator, storespec.WorldRun)
	authority.admit(otherMember, storespec.WorldDurable)
	authority.admit(sharee, storespec.WorldRun)
	minter, _, cs := newAbaAssembly(t, authority, overlay)
	ctx := context.Background()
	const rid resource.ResourceID = "kv:aba-three-actions"

	creatorHandle := minter.Mint(storespec.AuthorStamp{ID: creator, BirthVersion: 1})
	if out, err := creatorHandle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindKV}, []byte("v0")); err != nil || !out.Accepted() {
		t.Fatalf("create=(%+v,%v)", out, err)
	}
	// members-half sanity: the durable grant is {Read,Write} for the whole
	// channel, and creator itself has no ROW in the overlay table for read/
	// write yet either way — but the assertion that actually PINS "members
	// half" is a different actor with zero overlay presence succeeding.
	if allowed, err := cs.Resources.MembersAllow(ctx, rid, access.OpWrite); err != nil || !allowed {
		t.Fatalf("durable members write grant=(%v,%v)", allowed, err)
	}

	// (1) Write — members half, exercised by an unrelated durable member.
	otherHandle := minter.Mint(storespec.AuthorStamp{ID: otherMember, BirthVersion: 1})
	if out, err := otherHandle.Invoke(ctx, access.OpWrite, rid, []byte("v1"), nil); err != nil || !out.Accepted() {
		t.Fatalf("unrelated member write=(%+v,%v)", out, err)
	}
	if overlay.hasAnyOverlayGrant(rid, otherMember) {
		t.Fatal("unrelated member's write should never have touched the overlay — members half must have carried it alone")
	}

	// (2) ShareActor — overlay Set half, exercised by the creator sharing to
	// a third forked actor. A forked creator holds NO durable Set grant
	// (birth=ChannelOwned only installs {Read,Write} on Registry); if this
	// succeeds it can only be the creator's own overlay Set right.
	if allowed, _ := cs.Resources.ActorAllows(ctx, creator, rid, access.OpSet); allowed {
		t.Fatal("setup invariant broken: forked creator unexpectedly holds a durable Set grant")
	}
	if out, err := creatorHandle.Invoke(ctx, access.OpSet, rid, nil, &access.Grant{
		GranteeKind: access.GranteeActor, Grantee: sharee, Ops: []access.Operation{access.OpRead},
	}); err != nil || !out.Accepted() {
		t.Fatalf("ShareActor via overlay Set=(%+v,%v)", out, err)
	}
	if allowed, _ := overlay.ActorAllows(ctx, sharee, rid, access.OpRead); !allowed {
		t.Fatal("ShareActor's grant did not land in the overlay")
	}

	// (3) Delete — overlay Delete half. Members never get Delete
	// (birth=ChannelOwned's grant is {Read,Write} only), so a forked
	// creator's successful Delete can only be authorized by its own overlay
	// Delete right.
	if allowed, _ := cs.Resources.MembersAllow(ctx, rid, access.OpDelete); allowed {
		t.Fatal("setup invariant broken: durable members grant unexpectedly carries Delete")
	}
	if out, err := creatorHandle.Invoke(ctx, access.OpDelete, rid, nil, nil); err != nil || !out.Accepted() {
		t.Fatalf("Delete via overlay=(%+v,%v)", out, err)
	}
	if _, exists, err := cs.Resources.Resolve(ctx, rid); err != nil || exists {
		t.Fatalf("resource survived overlay-authorized Delete: exists=%v err=%v", exists, err)
	}
}
