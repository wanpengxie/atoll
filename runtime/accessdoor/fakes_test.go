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
}

type createCall struct {
	id      resource.ResourceID
	kind    resourcespec.ResourceKind
	creator actor.ActorID
	initial []byte
}

func (r *fakeRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	return r.resolveMeta, r.resolveExists, r.resolveErr
}

func (r *fakeRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, initial []byte) error {
	r.createCalls = append(r.createCalls, createCall{id: id, kind: kind, creator: creator, initial: initial})
	return r.createErr
}

func (r *fakeRegistry) ActorAllows(ctx context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error) {
	return r.actorAllows, r.actorAllowsErr
}

func (r *fakeRegistry) MembersAllow(ctx context.Context, id resource.ResourceID, op access.Operation) (bool, error) {
	return r.membersAllow, r.membersAllowErr
}

func (r *fakeRegistry) SetGrant(ctx context.Context, id resource.ResourceID, g access.Grant) error {
	r.setGrants = append(r.setGrants, g)
	return r.setGrantErr
}

func (r *fakeRegistry) Delete(ctx context.Context, id resource.ResourceID) error {
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

// fakeMembership is a configurable MembershipCheck stub.
type fakeMembership struct {
	isMember bool
	err      error
}

func (m *fakeMembership) IsMember(ctx context.Context, id actor.ActorID) (bool, error) {
	return m.isMember, m.err
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
	}}
}
