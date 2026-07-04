package link

import (
	"context"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fakeRaceRegistry is a hand-scripted storespec.Registry for reconcileHost's
// successor-takeover race test: ListActive answers a FIXED snapshot (frozen at
// construction, exactly as a real point-in-time ListActive read is), while
// Lookup answers a SEPARATE, independently-set view — modelling the row having
// moved on (re-registered under a successor compute) in the gap between
// reconcileHost's ListActive snapshot and its post-tx re-Lookup. No fake
// Registry existed in this package prior to this test; ListActive/Lookup here
// deliberately do NOT share state, so a test can construct the exact two-step
// staleness reconcileHost must tolerate without needing real goroutine timing.
type fakeRaceRegistry struct {
	mu       sync.Mutex
	snapshot []storespec.Record
	lookup   map[actor.ActorID]storespec.Record
}

func (f *fakeRaceRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]storespec.Record, len(f.snapshot))
	copy(cp, f.snapshot)
	return cp, nil
}

func (f *fakeRaceRegistry) Lookup(_ context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.lookup[id]
	return rec, ok, nil
}

func (f *fakeRaceRegistry) Exists(ctx context.Context, id actor.ActorID) (bool, error) {
	_, ok, err := f.Lookup(ctx, id)
	return ok, err
}

// fakeRaceMembership mirrors the real store's ExpectedHost guard (§10.12): a
// remove whose ExpectedHost no longer matches the id's current authoritative
// host is a 0-rows-affected no-op — exactly the mechanism that lets a
// successor's row survive a stale reconcile's removal attempt.
type fakeRaceMembership struct {
	mu      sync.Mutex
	hosts   map[actor.ActorID]string // current authoritative host per id
	removed []actor.ActorID          // ids actually removed (ExpectedHost matched)
}

func (f *fakeRaceMembership) Insert(context.Context, storespec.Record) error { return nil }
func (f *fakeRaceMembership) Deregister(context.Context, actor.ActorID, int64) error {
	return nil
}
func (f *fakeRaceMembership) ApplyMemberTransitions(_ context.Context, _ []storespec.MemberActorAdd, removes []storespec.MemberActorRemove) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rm := range removes {
		if f.hosts[rm.ID] != rm.ExpectedHost {
			continue // stale ExpectedHost: host already flipped to a successor — no-op
		}
		delete(f.hosts, rm.ID)
		f.removed = append(f.removed, rm.ID)
	}
	return nil
}

// raceObsWatcher is a no-op ObsWatcher: reconcileHost requires a non-nil
// ObsWatcher to enter its obs cleanup arm at all — the race itself is observed
// through obsReg state, not through this watcher receiving anything.
type raceObsWatcher struct{}

func (w *raceObsWatcher) OnObs(context.Context, actor.ActorID, actorrt.ObsKind, actorrt.ObsValue) {
}

// TestReconcileHost_SuccessorTakeoverRace_ObsNotMisclearedOnStaleRow is the P1-3
// codex review fix regression test: reconcileHost used to UnwatchObs+clear
// obsReg for a falling-out candidate INSIDE the same loop that built the
// removes batch — BEFORE ApplyMemberTransitions ever ran. If, between
// reconcileHost's ListActive snapshot and that tx, a successor compute takes
// over the same actor id (re-attaches, re-registering it under a NEW host),
// the ExpectedHost guard correctly no-ops the stale remove — but the obs
// cleanup had ALREADY run unconditionally, ripping out the successor's live
// obs registration on the strength of a snapshot that was stale by the time
// the tx committed. The fix moves obs cleanup to AFTER the tx commits and
// re-Looks-up each row: only a row CONFIRMED gone/inactive is cleaned; a row
// found still active (moved to a successor) is left untouched.
func TestReconcileHost_SuccessorTakeoverRace_ObsNotMisclearedOnStaleRow(t *testing.T) {
	ctx := context.Background()
	const victim = actor.ActorID("victim")
	const staleHost = "compute-a"
	const successorHost = "compute-b"

	// The ListActive snapshot reconcileHost reads at its top: as of THIS
	// snapshot, victim is still hosted on staleHost (compute-a) — the reconcile
	// that is about to run believes it owns victim's removal.
	reg := &fakeRaceRegistry{
		snapshot: []storespec.Record{
			{ID: victim, Kind: actor.KindTool, Host: staleHost, CreatedAt: 1},
		},
		lookup: map[actor.ActorID]storespec.Record{
			// The post-tx re-Lookup (the fix's new read) sees the CURRENT truth:
			// victim has ALREADY moved to successorHost and is still active —
			// this is what makes the snapshot stale relative to the tx.
			victim: {ID: victim, Kind: actor.KindTool, Host: successorHost, CreatedAt: 2},
		},
	}
	mem := &fakeRaceMembership{
		// The membership's authoritative host view already reflects the
		// successor's takeover — so ApplyMemberTransitions's ExpectedHost guard
		// (ExpectedHost=staleHost) will correctly no-op victim's removal.
		hosts: map[actor.ActorID]string{victim: successorHost},
	}

	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	obs := &raceObsWatcher{}
	acc := NewAcceptor(Config{
		Runtime:    rt,
		Membership: mem,
		Registry:   reg,
		ObsWatcher: obs,
		ChannelID:  channel.ID("test-reconcile-race"),
	})

	// Simulate victim's obs registration already being live (as it would be
	// after the successor's own handleAttach ran WatchObs for it) — this is the
	// registration the bug used to rip out.
	acc.obsMu.Lock()
	acc.obsReg[victim] = true
	acc.obsMu.Unlock()

	var portMu sync.Mutex
	ports := map[actor.ActorID]actorrt.Incarnation{} // no local port retained — not the focus here

	// staleHost's reconcile runs: its own declaration set no longer includes
	// victim (it fell out from staleHost's point of view), so newAllowed is
	// empty — victim is a removal candidate exactly as it was pre-takeover.
	acc.reconcileHost(ctx, staleHost, map[actor.ActorID]bool{}, &portMu, ports)

	if len(mem.removed) != 0 {
		t.Fatalf("ApplyMemberTransitions removed %v, want none (ExpectedHost guard must no-op a stale remove)", mem.removed)
	}

	acc.obsMu.Lock()
	stillReg := acc.obsReg[victim]
	acc.obsMu.Unlock()
	if !stillReg {
		t.Fatal("obsReg[victim] was cleared by a stale reconcile — the successor's live obs registration was misclearer, exactly the race this fix closes")
	}
}

// TestReconcileHost_ConfirmedGone_ObsIsCleaned is the companion case: when the
// removal genuinely lands (no successor takeover — ExpectedHost matches), obs
// cleanup must still fire, just now from the post-tx re-Lookup path rather than
// the old pre-tx inline clear. Without this the fix could regress into "obs
// cleanup never runs" (a different bug in the opposite direction: an obsReg
// leak that would silently blunt future de-duplication and skip UnwatchObs on
// a genuinely dead row).
func TestReconcileHost_ConfirmedGone_ObsIsCleaned(t *testing.T) {
	ctx := context.Background()
	const victim = actor.ActorID("victim")
	const host = "compute-a"

	reg := &fakeRaceRegistry{
		snapshot: []storespec.Record{
			{ID: victim, Kind: actor.KindTool, Host: host, CreatedAt: 1},
		},
		lookup: map[actor.ActorID]storespec.Record{
			// Post-tx re-Lookup confirms the row is now deregistered (no
			// takeover — genuinely gone).
			victim: {ID: victim, Kind: actor.KindTool, Host: host, CreatedAt: 1, DeregisteredAt: 2},
		},
	}
	mem := &fakeRaceMembership{hosts: map[actor.ActorID]string{victim: host}}

	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	obs := &raceObsWatcher{}
	acc := NewAcceptor(Config{
		Runtime:    rt,
		Membership: mem,
		Registry:   reg,
		ObsWatcher: obs,
		ChannelID:  channel.ID("test-reconcile-confirmed"),
	})
	acc.obsMu.Lock()
	acc.obsReg[victim] = true
	acc.obsMu.Unlock()

	var portMu sync.Mutex
	ports := map[actor.ActorID]actorrt.Incarnation{}

	acc.reconcileHost(ctx, host, map[actor.ActorID]bool{}, &portMu, ports)

	if len(mem.removed) != 1 || mem.removed[0] != victim {
		t.Fatalf("ApplyMemberTransitions removed %v, want [%q]", mem.removed, victim)
	}
	acc.obsMu.Lock()
	stillReg := acc.obsReg[victim]
	acc.obsMu.Unlock()
	if stillReg {
		t.Fatal("obsReg[victim] was NOT cleared for a confirmed-gone row — obs cleanup regressed to a no-op")
	}
}
