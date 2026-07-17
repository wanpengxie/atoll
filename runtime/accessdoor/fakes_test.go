package accessdoor

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fakeRegistry is a configurable resourcespec.Registry stub. Every method reads a
// canned result / error field so a test can drive one decision-tree branch at a
// time; the mutating methods record their calls for assertion.
type fakeRegistry struct {
	resolveMeta   resourcespec.ResourceMeta
	resolveExists bool
	resolveErr    error

	createErr   error
	createCalls []createCall

	reserveCreateID         string
	reserveCreateErr        error
	reserveCreateCalls      []createCall
	commitReservationFound  bool
	commitReservationLanded resourcespec.LandedResource
	commitReservationErr    error
	commitReservationCalls  []string
	clearTombstoneFound     bool
	clearTombstoneErr       error

	actorAllows    bool
	actorAllowsErr error

	membersAllow    bool
	membersAllowErr error

	setGrantErr error
	setGrants   []access.Grant

	deleteErr   error
	deleteCalls []resource.ResourceID

	// listRows/listNextCursor/listErr back List — canned per §3.7's door
	// tests (Stat/List projection), which need MULTIPLE rows with distinct
	// grant shapes at once (a single-bool fake cannot express that, same
	// reasoning decay_test.go's real-store rig documents for the set arm).
	listRows       []resourcespec.ResourceRow
	listNextCursor string
	listErr        error
	listCalls      []listCall

	// calls is the total method-invocation count across every method — the
	// actor-scoped negative assertion ("the collapsed branch consults no Registry")
	// asserts this stays zero.
	calls int
}

type listCall struct {
	prefix string
	limit  int
	cursor string
}

type createCall struct {
	id                                resource.ResourceID
	kind                              resourcespec.ResourceKind
	creator                           actor.ActorID
	placementDaemonID, placementCoord string
	initial                           []byte
	birth                             resourcespec.ResourceBirthPlan
}

func (r *fakeRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	r.calls++
	return r.resolveMeta, r.resolveExists, r.resolveErr
}

func (r *fakeRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, initial []byte, birth resourcespec.ResourceBirthPlan) error {
	r.calls++
	r.createCalls = append(r.createCalls, createCall{
		id: id, kind: kind, creator: creator,
		placementDaemonID: placementDaemonID, placementCoord: placementCoord,
		initial: initial, birth: birth,
	})
	return r.createErr
}

// ReserveCreate / CommitReservation / ClearTombstone back §4's placement
// routing (door.create's file-kind branch, query.go) — canned per-call so a
// test can drive the reservation/commit sequence a content-less file create
// runs through.
func (r *fakeRegistry) ReserveCreate(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, dir bool, birth resourcespec.ResourceBirthPlan) (string, error) {
	r.calls++
	r.reserveCreateCalls = append(r.reserveCreateCalls, createCall{
		id: id, kind: kind, creator: creator,
		placementDaemonID: placementDaemonID, placementCoord: placementCoord, birth: birth,
	})
	if r.reserveCreateErr != nil {
		return "", r.reserveCreateErr
	}
	id2 := r.reserveCreateID
	if id2 == "" {
		id2 = "reservation-1"
	}
	return id2, nil
}

func (r *fakeRegistry) CommitReservation(ctx context.Context, reservationID string) (resourcespec.LandedResource, bool, error) {
	r.calls++
	r.commitReservationCalls = append(r.commitReservationCalls, reservationID)
	return r.commitReservationLanded, r.commitReservationFound, r.commitReservationErr
}

func (r *fakeRegistry) ClearTombstone(ctx context.Context, tombstoneID string) (bool, error) {
	r.calls++
	return r.clearTombstoneFound, r.clearTombstoneErr
}

// ReservationDaemon / TombstoneDaemon / ListReservationsByDaemon /
// ListTombstonesByDaemon / ListByPlacementDaemon back §4.7's daemon
// control-RPC handlers (platform, not this door) — no accessdoor caller
// exercises them, stubbed purely for interface compliance.
func (r *fakeRegistry) ReservationDaemon(ctx context.Context, reservationID string) (string, bool, error) {
	r.calls++
	return "", false, errors.New("fakeRegistry: ReservationDaemon not wired (no accessdoor caller)")
}

func (r *fakeRegistry) TombstoneDaemon(ctx context.Context, tombstoneID string) (string, bool, error) {
	r.calls++
	return "", false, errors.New("fakeRegistry: TombstoneDaemon not wired (no accessdoor caller)")
}

func (r *fakeRegistry) ListReservationsByDaemon(ctx context.Context, daemonID string) ([]resourcespec.ReservationRow, error) {
	r.calls++
	return nil, errors.New("fakeRegistry: ListReservationsByDaemon not wired (no accessdoor caller)")
}

func (r *fakeRegistry) ListTombstonesByDaemon(ctx context.Context, daemonID string) ([]resourcespec.TombstoneRow, error) {
	r.calls++
	return nil, errors.New("fakeRegistry: ListTombstonesByDaemon not wired (no accessdoor caller)")
}

func (r *fakeRegistry) ListByPlacementDaemon(ctx context.Context, daemonID string) ([]resourcespec.ResourceRow, error) {
	r.calls++
	return nil, errors.New("fakeRegistry: ListByPlacementDaemon not wired (no accessdoor caller)")
}

func (r *fakeRegistry) SweepExpiredReservations(ctx context.Context, daemonID string, cutoffMs int64) ([]resourcespec.ReservationRow, error) {
	r.calls++
	return nil, errors.New("fakeRegistry: SweepExpiredReservations not wired (no accessdoor caller)")
}

func (r *fakeRegistry) TouchReservationsByCoords(ctx context.Context, daemonID string, coords []string, atMs int64) error {
	r.calls++
	return errors.New("fakeRegistry: TouchReservationsByCoords not wired (no accessdoor caller)")
}

// List is §3.7's door-level consumer (door.list, query.go): canned rows +
// nextCursor let a test drive the any-grant projection over MULTIPLE rows
// with distinct grant shapes in one call — a single-bool fake cannot express
// that.
func (r *fakeRegistry) List(ctx context.Context, prefix string, limit int, cursor string) ([]resourcespec.ResourceRow, string, error) {
	r.calls++
	r.listCalls = append(r.listCalls, listCall{prefix: prefix, limit: limit, cursor: cursor})
	return r.listRows, r.listNextCursor, r.listErr
}

func (r *fakeRegistry) ActorAllows(ctx context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error) {
	r.calls++
	return r.actorAllows, r.actorAllowsErr
}

func (r *fakeRegistry) MembersAllow(ctx context.Context, id resource.ResourceID, op access.Operation) (bool, error) {
	r.calls++
	return r.membersAllow, r.membersAllowErr
}

func (r *fakeRegistry) SetGrant(ctx context.Context, id resource.ResourceID, g access.Grant) error {
	r.calls++
	r.setGrants = append(r.setGrants, g)
	return r.setGrantErr
}

func (r *fakeRegistry) Delete(ctx context.Context, id resource.ResourceID) error {
	r.calls++
	r.deleteCalls = append(r.deleteCalls, id)
	return r.deleteErr
}

// fakeDriver is a configurable resourcespec.Driver stub.
type fakeDriver struct {
	readValue []byte
	readFound bool
	readErr   error

	writeErr   error
	writeCalls [][]byte

	deleteErr   error
	deleteCalls int
}

func (d *fakeDriver) Read(ctx context.Context, id resource.ResourceID) ([]byte, bool, error) {
	return d.readValue, d.readFound, d.readErr
}

func (d *fakeDriver) Write(ctx context.Context, id resource.ResourceID, value []byte) error {
	d.writeCalls = append(d.writeCalls, value)
	return d.writeErr
}

func (d *fakeDriver) Delete(ctx context.Context, id resource.ResourceID) error {
	d.deleteCalls++
	return d.deleteErr
}

// fakeMembership is a configurable ActorAuthority stub. calls counts every
// invocation — the actor-scoped negative assertion ("the collapsed branch checks
// no membership") asserts it stays zero.
type fakeMembership struct {
	isMember bool
	world    storespec.ActorWorld
	err      error
	calls    int

	// lookupHost/lookupFound/lookupErr back Lookup (§4.3 placement chain ①'s
	// creator-affinity read). lookupCalls records every caller id Lookup was
	// asked about.
	lookupHost    string
	lookupFound   bool
	lookupErr     error
	lookupCalls   []actor.ActorID
	authorVerdict storespec.AuthorVerdict
}

type fakeGrantOverlay struct {
	allows  bool
	err     error
	grants  []access.Grant
	deleted []resource.ResourceID
}

func (o *fakeGrantOverlay) ActorAllows(context.Context, actor.ActorID, resource.ResourceID, access.Operation) (bool, error) {
	return o.allows, o.err
}
func (o *fakeGrantOverlay) SetGrant(_ context.Context, _ resource.ResourceID, grant access.Grant) error {
	o.grants = append(o.grants, grant)
	return o.err
}
func (o *fakeGrantOverlay) EndBatch([]actor.ActorID)              {}
func (o *fakeGrantOverlay) DeleteResource(id resource.ResourceID) { o.deleted = append(o.deleted, id) }

func (m *fakeMembership) LookupActive(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	m.calls++
	m.lookupCalls = append(m.lookupCalls, id)
	if m.err != nil {
		return storespec.ActorControlRow{}, false, m.err
	}
	if m.lookupErr != nil {
		return storespec.ActorControlRow{}, false, m.lookupErr
	}
	if !m.isMember && !m.lookupFound {
		return storespec.ActorControlRow{}, false, nil
	}
	p := storespec.NewServerPlacement()
	if m.lookupHost != "" {
		p, _ = storespec.NewDaemonPlacement(m.lookupHost)
	}
	return storespec.ActorControlRow{ID: id, CurrentDeclVersion: 1, Placement: p}, true, nil
}

func (m *fakeMembership) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return nil, nil
}
func (m *fakeMembership) WorldOf(context.Context, actor.ActorID) (storespec.ActorWorld, bool, error) {
	if m.err != nil {
		return 0, false, m.err
	}
	if !m.isMember && !m.lookupFound {
		return 0, false, nil
	}
	world := m.world
	if world == 0 {
		world = storespec.WorldDurable
	}
	return world, true, nil
}
func (m *fakeMembership) CheckAuthor(context.Context, storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	if m.authorVerdict != 0 {
		return m.authorVerdict, nil
	}
	return storespec.AuthorOK, nil
}

func accessStamp(id actor.ActorID) storespec.AuthorStamp {
	return storespec.AuthorStamp{ID: id, BirthVersion: 1}
}

func firstBirth(plans []resourcespec.ResourceBirthPlan) resourcespec.ResourceBirthPlan {
	if len(plans) == 0 {
		return resourcespec.ResourceBirthPlan{}
	}
	return plans[0]
}

func (m *fakeMembership) IsMember(ctx context.Context, id actor.ActorID) (bool, error) {
	m.calls++
	return m.isMember, m.err
}

func (m *fakeMembership) Lookup(ctx context.Context, id actor.ActorID) (string, bool, error) {
	m.calls++
	m.lookupCalls = append(m.lookupCalls, id)
	return m.lookupHost, m.lookupFound, m.lookupErr
}

// fakeStorageMounts is a configurable StorageMounts stub (§4.3 placement
// chain ③④'s mount-table input).
type fakeStorageMounts struct {
	mounts []StorageMount
	err    error
	calls  int
}

func (f *fakeStorageMounts) ListStorageDaemons(ctx context.Context, chID channel.ID) ([]StorageMount, error) {
	f.calls++
	return f.mounts, f.err
}

// fakeStorageControl is a configurable StorageControl stub — records every
// AllocRequest a test's door.create drives so the placement/coord/dir it was
// asked to allocate can be asserted.
type fakeStorageControl struct {
	err          error
	calls        []allocCall
	reclaimErr   error
	reclaimCalls []reclaimCall
}

type allocCall struct {
	daemonID string
	spec     StorageAllocSpec
}

type reclaimCall struct {
	daemonID string
	coord    string
}

func (f *fakeStorageControl) AllocRequest(ctx context.Context, daemonID string, spec StorageAllocSpec) error {
	f.calls = append(f.calls, allocCall{daemonID: daemonID, spec: spec})
	return f.err
}

func (f *fakeStorageControl) ReclaimRequest(ctx context.Context, daemonID string, coord string) error {
	f.reclaimCalls = append(f.reclaimCalls, reclaimCall{daemonID: daemonID, coord: coord})
	return f.reclaimErr
}

// fakeLaneControl is a configurable LaneControl stub (§5 item 0's file
// byte-route Token mint) — records every OpenTransfer call so a test can
// assert the exact (targetDaemonID, requesterDaemonID, coord, mode,
// reservationID) the door routed.
type fakeLaneControl struct {
	token string
	err   error
	calls []openTransferCall
}

type openTransferCall struct {
	targetDaemonID    string
	requesterDaemonID string
	coord             string
	mode              access.Operation
	reservationID     string
}

func (f *fakeLaneControl) OpenTransfer(ctx context.Context, targetDaemonID, requesterDaemonID, coord string, mode access.Operation, reservationID string) (string, error) {
	f.calls = append(f.calls, openTransferCall{targetDaemonID: targetDaemonID, requesterDaemonID: requesterDaemonID, coord: coord, mode: mode, reservationID: reservationID})
	if f.err != nil {
		return "", f.err
	}
	tok := f.token
	if tok == "" {
		tok = "fake-lane-token"
	}
	return tok, nil
}

// fakeStateStore is a configurable resourcespec.StateStore stub. Every method
// reads canned result/error fields so a test can drive one collapsed-branch
// decision at a time; each records its calls (owner/id/bytes) for assertion.
type fakeStateStore struct {
	createErr   error
	createCalls []stateCall

	readValue   []byte
	readPresent bool
	readErr     error
	readCalls   []stateCall

	writePresent bool
	writeErr     error
	writeCalls   []stateCall

	deletePresent bool
	deleteErr     error
	deleteCalls   []stateCall
}

type stateCall struct {
	owner actor.ActorID
	id    resource.ResourceID
	bytes []byte
}

func (s *fakeStateStore) Create(ctx context.Context, owner actor.ActorID, id resource.ResourceID, initial []byte) error {
	s.createCalls = append(s.createCalls, stateCall{owner: owner, id: id, bytes: initial})
	return s.createErr
}

func (s *fakeStateStore) Read(ctx context.Context, owner actor.ActorID, id resource.ResourceID) ([]byte, bool, error) {
	s.readCalls = append(s.readCalls, stateCall{owner: owner, id: id})
	return s.readValue, s.readPresent, s.readErr
}

func (s *fakeStateStore) Write(ctx context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (bool, error) {
	s.writeCalls = append(s.writeCalls, stateCall{owner: owner, id: id, bytes: value})
	return s.writePresent, s.writeErr
}

func (s *fakeStateStore) Delete(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (bool, error) {
	s.deleteCalls = append(s.deleteCalls, stateCall{owner: owner, id: id})
	return s.deletePresent, s.deleteErr
}

// metaKV is the ResourceMeta Resolve returns for the day-1 kind.
func metaKV() resourcespec.ResourceMeta {
	return resourcespec.ResourceMeta{Kind: resourcespec.KindKV, CreatedAt: 1}
}

// newDoor builds a bare door directly (the package's own test may reach past the
// sealed Minter to drive invoke branch-by-branch). The driver is registered under
// KindKV, the day-1 kind Resolve returns.
func newDoor(reg *fakeRegistry, drv *fakeDriver, mem *fakeMembership) *door {
	return &door{deps: Deps{
		Registry:  reg,
		Drivers:   DriverTable{resourcespec.KindKV: drv},
		Authority: mem,
		Overlay:   &fakeGrantOverlay{},
		State:     &fakeStateStore{},
	}}
}

// newFileDoor builds a bare door with the file-kind placement Deps wired
// (§4.3: StorageMounts + StorageControl + ChannelID), on top of newDoor's
// baseline — the constructor query_test.go's placement-chain tests use.
func newFileDoor(reg *fakeRegistry, drv *fakeDriver, mem *fakeMembership, mounts *fakeStorageMounts, ctl *fakeStorageControl, chID channel.ID) *door {
	d := newDoor(reg, drv, mem)
	d.deps.StorageMounts = mounts
	d.deps.StorageControl = ctl
	d.deps.ChannelID = chID
	d.deps.LaneControl = &fakeLaneControl{}
	return d
}

// newStateDoor builds a bare door wired for the actor-scoped branch. The Registry
// and Membership fakes are present so the collapsed branch's negative assertions
// (it consults neither) can inspect their call counts — a wired-but-untouched
// collaborator is the point of the assertion.
func newStateDoor(st *fakeStateStore, reg *fakeRegistry, mem *fakeMembership) *door {
	return &door{deps: Deps{
		Registry:  reg,
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: mem,
		Overlay:   &fakeGrantOverlay{},
		State:     st,
	}}
}
