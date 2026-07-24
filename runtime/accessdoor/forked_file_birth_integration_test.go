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
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type fileBirthAuthority struct {
	mu     sync.Mutex
	rows   map[actor.ActorID]storespec.ActorControlRow
	worlds map[actor.ActorID]storespec.ActorWorld
}

func (a *fileBirthAuthority) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.rows[id]
	return r, ok, nil
}
func (a *fileBirthAuthority) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]storespec.ActorControlRow, 0, len(a.rows))
	for _, row := range a.rows {
		out = append(out, row)
	}
	return out, nil
}
func (a *fileBirthAuthority) WorldOf(_ context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w, ok := a.worlds[id]
	return w, ok, nil
}
func (a *fileBirthAuthority) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	_, ok, err := a.LookupActive(ctx, stamp.ID)
	if err != nil || !ok {
		return storespec.AuthorNotMember, err
	}
	return storespec.AuthorOK, nil
}
func (a *fileBirthAuthority) end(id actor.ActorID) {
	a.mu.Lock()
	delete(a.rows, id)
	delete(a.worlds, id)
	a.mu.Unlock()
}

type fileBirthOverlay struct {
	mu     sync.Mutex
	grants map[resource.ResourceID]map[actor.ActorID]map[access.Operation]bool
}

func (o *fileBirthOverlay) ActorAllows(_ context.Context, id actor.ActorID, rid resource.ResourceID, op access.Operation) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.grants[rid][id][op], nil
}
func (o *fileBirthOverlay) SetGrant(_ context.Context, rid resource.ResourceID, grant access.Grant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.grants[rid] == nil {
		o.grants[rid] = make(map[actor.ActorID]map[access.Operation]bool)
	}
	ops := make(map[access.Operation]bool)
	for _, op := range grant.Ops {
		ops[op] = true
	}
	o.grants[rid][grant.Grantee] = ops
	return nil
}
func (o *fileBirthOverlay) EndBatch(ids []actor.ActorID) {
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
func (o *fileBirthOverlay) DeleteResource(id resource.ResourceID) {
	o.mu.Lock()
	delete(o.grants, id)
	o.mu.Unlock()
}

type fileBirthMount struct{}

func (fileBirthMount) ListStorageDaemons(context.Context, channel.ID) ([]accessdoor.StorageMount, error) {
	return []accessdoor.StorageMount{{DaemonID: "daemon-file", Online: true}}, nil
}

type fileBirthStorageControl struct{}

func (fileBirthStorageControl) AllocRequest(context.Context, string, accessdoor.StorageAllocSpec) error {
	return nil
}
func (fileBirthStorageControl) ReclaimRequest(context.Context, string, string) error { return nil }

type fileBirthLane struct{}

func (fileBirthLane) OpenTransfer(context.Context, string, string, string, access.Operation, string) (string, error) {
	return "opaque-transfer-token", nil
}

func TestForkedFileCreateLandsChannelOwnedAndSurvivesCreatorEnd(t *testing.T) {
	ctx := context.Background()
	cs, err := store.OpenChannel(ctx, "forked-file-birth", filepath.Join(t.TempDir(), "channel.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	const (
		child  actor.ActorID = "agent:parent/worker-run"
		member actor.ActorID = "human:durable-member"
	)
	daemonPlacement, err := storespec.NewDaemonPlacement("daemon-file")
	if err != nil {
		t.Fatal(err)
	}
	authority := &fileBirthAuthority{
		rows: map[actor.ActorID]storespec.ActorControlRow{
			child:  {ID: child, Kind: actor.KindAgent, CurrentDeclVersion: 1, Placement: daemonPlacement},
			member: {ID: member, Kind: actor.KindHuman, CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement()},
		},
		worlds: map[actor.ActorID]storespec.ActorWorld{child: storespec.WorldRun, member: storespec.WorldDurable},
	}
	overlay := &fileBirthOverlay{grants: make(map[resource.ResourceID]map[actor.ActorID]map[access.Operation]bool)}
	minter, err := accessdoor.New(accessdoor.Deps{
		Registry: cs.Resources, Drivers: accessdoor.DriverTable{accessdoor.KindKV: cs.KVDriver},
		Authority: authority, Overlay: overlay, State: cs.State,
		ChannelID: "forked-file-birth", StorageMounts: fileBirthMount{},
		StorageControl: fileBirthStorageControl{}, LaneControl: fileBirthLane{},
	})
	if err != nil {
		t.Fatal(err)
	}
	const rid resource.ResourceID = "file:forked-output"
	childHandle := minter.Mint(storespec.AuthorStamp{ID: child})
	out, err := childHandle.Create(ctx, rid, accessdoor.CreateSpec{Kind: accessdoor.KindFile}, nil)
	if err != nil || !out.Accepted() {
		t.Fatalf("forked file create=(%+v,%v)", out, err)
	}
	meta, ok, err := cs.Resources.Resolve(ctx, rid)
	if err != nil || !ok || meta.Kind != resourcespec.KindFile || meta.CreatedBy != child || meta.PlacementDaemonID != "daemon-file" {
		t.Fatalf("file meta=(%+v,%v,%v)", meta, ok, err)
	}
	if allowed, _ := cs.Resources.ActorAllows(ctx, child, rid, access.OpRead); allowed {
		t.Fatal("forked creator leaked a durable actor grant")
	}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		if allowed, err := cs.Resources.MembersAllow(ctx, rid, op); err != nil || !allowed {
			t.Fatalf("members %s=(%v,%v)", op, allowed, err)
		}
	}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete} {
		if allowed, err := overlay.ActorAllows(ctx, child, rid, op); err != nil || !allowed {
			t.Fatalf("creator overlay %s=(%v,%v)", op, allowed, err)
		}
	}

	// Creator End removes every run-only authority fact. The resource's closed
	// birth plan was committed in the same transaction, so a durable identity can
	// still obtain both file routes without consulting or reclassifying creator.
	authority.end(child)
	overlay.EndBatch([]actor.ActorID{child})
	if allowed, _ := overlay.ActorAllows(ctx, child, rid, access.OpDelete); allowed {
		t.Fatal("creator overlay survived creator End")
	}
	memberHandle := minter.Mint(storespec.AuthorStamp{ID: member})
	for _, op := range []access.Operation{access.OpRead, access.OpWrite} {
		got, err := memberHandle.Invoke(ctx, op, rid, nil, nil)
		if err != nil || !got.Accepted() || got.Route == nil || got.Route.Token == "" || got.Route.Mode != op {
			t.Fatalf("member %s after creator End=(%+v,%v)", op, got, err)
		}
	}
}
