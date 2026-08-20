package accessdoor

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type createCall struct {
	id      resource.ResourceID
	kind    resourcespec.ResourceKind
	creator actor.ActorID
	initial []byte
}

type fakeRegistry struct {
	resolveMeta    resourcespec.ResourceMeta
	resolveExists  bool
	resolveErr     error
	createErr      error
	createCalls    []createCall
	deleteErr      error
	deleteCalls    []resource.ResourceID
	listRows       []resourcespec.ResourceRow
	listNextCursor string
	listErr        error
	calls          int
}

func (r *fakeRegistry) Resolve(context.Context, resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	r.calls++
	return r.resolveMeta, r.resolveExists, r.resolveErr
}
func (r *fakeRegistry) Create(_ context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, initial []byte) error {
	r.calls++
	r.createCalls = append(r.createCalls, createCall{id: id, kind: kind, creator: creator, initial: initial})
	return r.createErr
}
func (r *fakeRegistry) Delete(_ context.Context, id resource.ResourceID) error {
	r.calls++
	r.deleteCalls = append(r.deleteCalls, id)
	return r.deleteErr
}
func (r *fakeRegistry) List(context.Context, string, int, string) ([]resourcespec.ResourceRow, string, error) {
	r.calls++
	return r.listRows, r.listNextCursor, r.listErr
}

type fakeDriver struct {
	readValue   []byte
	readFound   bool
	readErr     error
	writeErr    error
	writeCalls  [][]byte
	deleteErr   error
	deleteCalls int
}

func (d *fakeDriver) Read(context.Context, resource.ResourceID) ([]byte, bool, error) {
	return d.readValue, d.readFound, d.readErr
}
func (d *fakeDriver) Write(_ context.Context, _ resource.ResourceID, value []byte) error {
	d.writeCalls = append(d.writeCalls, value)
	return d.writeErr
}
func (d *fakeDriver) Delete(context.Context, resource.ResourceID) error {
	d.deleteCalls++
	return d.deleteErr
}

type fakeMembership struct {
	isMember    bool
	isOwner     bool
	err         error
	calls       int
	lookupHost  string
	lookupFound bool
	lookupErr   error
	principal   string
	lookupCalls []actor.ActorID
}

func (m *fakeMembership) ResourceActorFacts(_ context.Context, id actor.ActorID) (storespec.ResourceActorFacts, error) {
	m.calls++
	m.lookupCalls = append(m.lookupCalls, id)
	if m.err != nil {
		return storespec.ResourceActorFacts{}, m.err
	}
	if m.lookupErr != nil {
		return storespec.ResourceActorFacts{}, m.lookupErr
	}
	host := ""
	if m.lookupFound {
		host = m.lookupHost
	}
	return storespec.ResourceActorFacts{Active: m.isMember || m.lookupFound, Owner: m.isOwner, Principal: m.principal, PreferredStorageHost: host}, nil
}

func accessAuthority(id actor.ActorID) capauth.Authority { return liveAuthority{id: id} }

type fakeStateStore struct {
	createErr     error
	createCalls   []stateCall
	readValue     []byte
	readPresent   bool
	readErr       error
	readCalls     []stateCall
	writePresent  bool
	writeErr      error
	writeCalls    []stateCall
	deletePresent bool
	deleteErr     error
	deleteCalls   []stateCall
}
type stateCall struct {
	owner actor.ActorID
	id    resource.ResourceID
	bytes []byte
}

func (s *fakeStateStore) Create(_ context.Context, owner actor.ActorID, id resource.ResourceID, b []byte) error {
	s.createCalls = append(s.createCalls, stateCall{owner, id, b})
	return s.createErr
}
func (s *fakeStateStore) Read(_ context.Context, owner actor.ActorID, id resource.ResourceID) ([]byte, bool, error) {
	s.readCalls = append(s.readCalls, stateCall{owner: owner, id: id})
	return s.readValue, s.readPresent, s.readErr
}
func (s *fakeStateStore) Write(_ context.Context, owner actor.ActorID, id resource.ResourceID, b []byte) (bool, error) {
	s.writeCalls = append(s.writeCalls, stateCall{owner, id, b})
	return s.writePresent, s.writeErr
}
func (s *fakeStateStore) Delete(_ context.Context, owner actor.ActorID, id resource.ResourceID) (bool, error) {
	s.deleteCalls = append(s.deleteCalls, stateCall{owner: owner, id: id})
	return s.deletePresent, s.deleteErr
}

func metaKV() resourcespec.ResourceMeta {
	return resourcespec.ResourceMeta{Kind: resourcespec.KindKV, CreatedAt: 1}
}

func newStateDoor(st *fakeStateStore, reg *fakeRegistry, mem *fakeMembership) *door {
	return &door{deps: Deps{Registry: reg, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}}, Authority: mem, State: st}}
}

func mustAccept(t *testing.T, out Outcome, err error) {
	t.Helper()
	if err != nil || !out.Accepted() {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}
func mustVerdict(t *testing.T, out Outcome, err error, want access.FailureReason) {
	t.Helper()
	if err != nil || out.RejectReason != want {
		t.Fatalf("out=%+v err=%v want=%v", out, err, want)
	}
}
