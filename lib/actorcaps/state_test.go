package actorcaps_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// ---------------------------------------------------------------------
// A tiny in-memory resourcespec.StateStore, keyed (owner, id) — enough to
// exercise StateKV's Get/Put/Del against a REAL accessdoor door (no fakes
// standing in for the door itself; only its bottom-most collaborator, which
// package accessdoor already declares an interface for).
// ---------------------------------------------------------------------

type memStateStore struct {
	mu   sync.Mutex
	rows map[actor.ActorID]map[resource.ResourceID][]byte
}

func newMemStateStore() *memStateStore {
	return &memStateStore{rows: make(map[actor.ActorID]map[resource.ResourceID][]byte)}
}

func (s *memStateStore) Create(ctx context.Context, owner actor.ActorID, id resource.ResourceID, initial []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	byOwner, ok := s.rows[owner]
	if !ok {
		byOwner = make(map[resource.ResourceID][]byte)
		s.rows[owner] = byOwner
	}
	if _, exists := byOwner[id]; exists {
		return resourcespec.ErrAlreadyExists
	}
	byOwner[id] = initial
	return nil
}

func (s *memStateStore) Read(ctx context.Context, owner actor.ActorID, id resource.ResourceID) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byOwner, ok := s.rows[owner]
	if !ok {
		return nil, false, nil
	}
	v, ok := byOwner[id]
	return v, ok, nil
}

func (s *memStateStore) Write(ctx context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byOwner, ok := s.rows[owner]
	if !ok {
		return false, nil
	}
	if _, exists := byOwner[id]; !exists {
		return false, nil
	}
	byOwner[id] = value
	return true, nil
}

func (s *memStateStore) Delete(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byOwner, ok := s.rows[owner]
	if !ok {
		return false, nil
	}
	if _, exists := byOwner[id]; !exists {
		return false, nil
	}
	delete(byOwner, id)
	return true, nil
}

var _ resourcespec.StateStore = (*memStateStore)(nil)

// fakeChannelDriver/fakeChannelRegistry/fakeChannelMembership are the
// channel-scoped Deps accessdoor.New still requires (fail-fast on any
// missing collaborator) even though StateKV never reaches them — the
// actor-scoped branch is collapsed and consults neither (state_test.go in
// package accessdoor pins that negative assertion; this file only needs
// SOME non-nil value satisfying each interface).
type fakeChannelDriver struct{}

func (fakeChannelDriver) Read(ctx context.Context, id resource.ResourceID) ([]byte, bool, error) {
	return nil, false, nil
}
func (fakeChannelDriver) Write(ctx context.Context, id resource.ResourceID, value []byte) error {
	return nil
}
func (fakeChannelDriver) Delete(ctx context.Context, id resource.ResourceID) error { return nil }

type fakeChannelRegistry struct{}

func (fakeChannelRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	return resourcespec.ResourceMeta{}, false, nil
}
func (fakeChannelRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, initial []byte) error {
	return nil
}
func (fakeChannelRegistry) ActorAllows(ctx context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error) {
	return false, nil
}
func (fakeChannelRegistry) MembersAllow(ctx context.Context, id resource.ResourceID, op access.Operation) (bool, error) {
	return false, nil
}
func (fakeChannelRegistry) SetGrant(ctx context.Context, id resource.ResourceID, g access.Grant) error {
	return nil
}
func (fakeChannelRegistry) Delete(ctx context.Context, id resource.ResourceID) error { return nil }

type fakeChannelMembership struct{}

func (fakeChannelMembership) IsMember(ctx context.Context, id actor.ActorID) (bool, error) {
	return false, nil
}

// newTestMinter assembles a REAL accessdoor.AccessMinter over an in-memory
// StateStore — the same New() the platform assembly root calls, so StateKV
// is tested against actual door behaviour, not a stand-in for it.
func newTestMinter(t *testing.T, st resourcespec.StateStore) accessdoor.AccessMinter {
	t.Helper()
	m, err := accessdoor.New(accessdoor.Deps{
		Registry:   fakeChannelRegistry{},
		Drivers:    accessdoor.DriverTable{resourcespec.KindKV: fakeChannelDriver{}},
		Membership: fakeChannelMembership{},
		State:      st,
	})
	if err != nil {
		t.Fatalf("accessdoor.New: %v", err)
	}
	return m
}

func TestStateKVRoundtrip(t *testing.T) {
	st := newMemStateStore()
	minter := newTestMinter(t, st)
	kv := actorcaps.StateKV{H: minter.MintState("actor-a")}
	ctx := context.Background()

	// Get on an absent key: not found, no error.
	_, found, err := kv.Get(ctx, "k")
	if err != nil || found {
		t.Fatalf("Get(absent) = found:%v err:%v, want found:false err:nil", found, err)
	}

	// Put on an absent key creates it (Write→not_found→Create fallthrough).
	if err := kv.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("Put(create): %v", err)
	}
	val, found, err := kv.Get(ctx, "k")
	if err != nil || !found || string(val) != "v1" {
		t.Fatalf("Get after create = val:%q found:%v err:%v, want v1/true/nil", val, found, err)
	}

	// Put on an existing key overwrites (Write branch, no Create attempted).
	if err := kv.Put(ctx, "k", []byte("v2")); err != nil {
		t.Fatalf("Put(overwrite): %v", err)
	}
	val, found, err = kv.Get(ctx, "k")
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("Get after overwrite = val:%q found:%v err:%v, want v2/true/nil", val, found, err)
	}

	// Del removes it.
	if err := kv.Del(ctx, "k"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	_, found, err = kv.Get(ctx, "k")
	if err != nil || found {
		t.Fatalf("Get after Del = found:%v err:%v, want false/nil", found, err)
	}

	// Del is idempotent: a not_found verdict is swallowed, never surfaced.
	if err := kv.Del(ctx, "k"); err != nil {
		t.Fatalf("Del(already gone) = %v, want nil (not_found verdict swallowed)", err)
	}
}

// TestStateKVNamespaceIsolation: A writes a key, B (a distinct owner welded
// at MintState) reads the SAME key string and finds nothing — "reject
// another actor's key" is not expressible at this collapsed locus (there is
// no R, no membership check to deny with); isolation is structural, keyed
// entirely by the welded owner coordinate, and this asserts exactly that.
func TestStateKVNamespaceIsolation(t *testing.T) {
	st := newMemStateStore()
	minter := newTestMinter(t, st)
	ctx := context.Background()

	a := actorcaps.StateKV{H: minter.MintState("actor-a")}
	b := actorcaps.StateKV{H: minter.MintState("actor-b")}

	if err := a.Put(ctx, "shared-key", []byte("a's secret")); err != nil {
		t.Fatalf("a.Put: %v", err)
	}

	val, found, err := b.Get(ctx, "shared-key")
	if err != nil {
		t.Fatalf("b.Get: unexpected err %v", err)
	}
	if found || val != nil {
		t.Fatalf("b.Get(a's key) = val:%q found:%v, want not-found (namespace isolation)", val, found)
	}

	// B may freely create the SAME key string under its own namespace — the
	// two rows never collide (keyed (owner, id), not id alone).
	if err := b.Put(ctx, "shared-key", []byte("b's secret")); err != nil {
		t.Fatalf("b.Put: %v", err)
	}
	val, found, err = a.Get(ctx, "shared-key")
	if err != nil || !found || string(val) != "a's secret" {
		t.Fatalf("a.Get after b.Put = val:%q found:%v err:%v, want a's secret/true/nil (b's write did not clobber a)", val, found, err)
	}
}

// TestStateKVPutNotFoundConvertsToCreate pins the Put verdict-branch
// mechanically: an absent row's first Write comes back resource_not_found,
// and Put falls through to Create rather than surfacing that verdict as an
// error to the caller.
func TestStateKVPutNotFoundConvertsToCreate(t *testing.T) {
	st := newMemStateStore()
	minter := newTestMinter(t, st)
	kv := actorcaps.StateKV{H: minter.MintState("actor-a")}
	ctx := context.Background()

	if _, exists := st.rows["actor-a"]["fresh"]; exists {
		t.Fatal("row must not pre-exist")
	}
	if err := kv.Put(ctx, "fresh", []byte("born")); err != nil {
		t.Fatalf("Put on absent key: %v", err)
	}
	got, ok := st.rows["actor-a"]["fresh"]
	if !ok || string(got) != "born" {
		t.Fatalf("underlying store row = %q ok:%v, want born/true (Create fallthrough landed)", got, ok)
	}
}

// TestStateKVDriverErrorSurfaces: a genuine store fault (not a not_found
// verdict) is NOT swallowed — Get/Put/Del must still surface it.
func TestStateKVDriverErrorSurfaces(t *testing.T) {
	st := &brokenStateStore{err: errors.New("disk on fire")}
	minter := newTestMinter(t, st)
	kv := actorcaps.StateKV{H: minter.MintState("actor-a")}
	ctx := context.Background()

	if _, _, err := kv.Get(ctx, "k"); err == nil {
		t.Fatal("Get over a broken store returned nil error, want surfaced driver_error")
	}
	if err := kv.Put(ctx, "k", []byte("v")); err == nil {
		t.Fatal("Put over a broken store returned nil error, want surfaced driver_error")
	}
	if err := kv.Del(ctx, "k"); err == nil {
		t.Fatal("Del over a broken store returned nil error, want surfaced driver_error")
	}
}

type brokenStateStore struct{ err error }

func (s *brokenStateStore) Create(ctx context.Context, owner actor.ActorID, id resource.ResourceID, initial []byte) error {
	return s.err
}
func (s *brokenStateStore) Read(ctx context.Context, owner actor.ActorID, id resource.ResourceID) ([]byte, bool, error) {
	return nil, false, s.err
}
func (s *brokenStateStore) Write(ctx context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (bool, error) {
	return false, s.err
}
func (s *brokenStateStore) Delete(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (bool, error) {
	return false, s.err
}

var _ resourcespec.StateStore = (*brokenStateStore)(nil)
