package accessdoor

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/access"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
	"github.com/wanpengxie/ActOS/runtime/resourcespec"
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

	actorAllows    bool
	actorAllowsErr error

	membersAllow    bool
	membersAllowErr error

	setGrantErr error
	setGrants   []access.Grant

	deleteErr   error
	deleteCalls []resource.ResourceID

	// calls is the total method-invocation count across every method — the
	// actor-scoped negative assertion ("the collapsed branch consults no Registry")
	// asserts this stays zero.
	calls int
}

type createCall struct {
	id      resource.ResourceID
	kind    resourcespec.ResourceKind
	creator actor.ActorID
	initial []byte
}

func (r *fakeRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	r.calls++
	return r.resolveMeta, r.resolveExists, r.resolveErr
}

func (r *fakeRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, initial []byte) error {
	r.calls++
	r.createCalls = append(r.createCalls, createCall{id: id, kind: kind, creator: creator, initial: initial})
	return r.createErr
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

// fakeMembership is a configurable MembershipCheck stub. calls counts every
// invocation — the actor-scoped negative assertion ("the collapsed branch checks
// no membership") asserts it stays zero.
type fakeMembership struct {
	isMember bool
	err      error
	calls    int
}

func (m *fakeMembership) IsMember(ctx context.Context, id actor.ActorID) (bool, error) {
	m.calls++
	return m.isMember, m.err
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
		Registry:   reg,
		Drivers:    DriverTable{resourcespec.KindKV: drv},
		Membership: mem,
		State:      &fakeStateStore{},
	}}
}

// newStateDoor builds a bare door wired for the actor-scoped branch. The Registry
// and Membership fakes are present so the collapsed branch's negative assertions
// (it consults neither) can inspect their call counts — a wired-but-untouched
// collaborator is the point of the assertion.
func newStateDoor(st *fakeStateStore, reg *fakeRegistry, mem *fakeMembership) *door {
	return &door{deps: Deps{
		Registry:   reg,
		Drivers:    DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Membership: mem,
		State:      st,
	}}
}
