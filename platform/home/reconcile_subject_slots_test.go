package home

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type subjectSlotAuthorityStub struct {
	list   func(context.Context) ([]storespec.ActorRecord, error)
	lookup func(context.Context, actor.ActorID) (storespec.ActorRecord, bool, error)
}

func (s subjectSlotAuthorityStub) ListActive(ctx context.Context) ([]storespec.ActorRecord, error) {
	return s.list(ctx)
}

func (s subjectSlotAuthorityStub) LookupActive(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ActorRecord, bool, error) {
	return s.lookup(ctx, id)
}

func testSubjectSlotLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSweepSubjectSlotsConvergesBothDirections(t *testing.T) {
	t.Parallel()
	slots := subjectgate.NewRegistry()
	slots.EnsureSlot("human:stale")
	authority := subjectSlotAuthorityStub{
		list: func(context.Context) ([]storespec.ActorRecord, error) {
			return []storespec.ActorRecord{
				{ID: "human:current", Kind: actor.KindHuman},
				{ID: "tool:current", Kind: actor.KindTool},
			}, nil
		},
		lookup: func(_ context.Context, id actor.ActorID) (storespec.ActorRecord, bool, error) {
			if id == "human:stale" {
				return storespec.ActorRecord{}, false, nil
			}
			t.Fatalf("unexpected terminal lookup for %q", id)
			return storespec.ActorRecord{}, false, nil
		},
	}

	sweepSubjectSlots(context.Background(), testSubjectSlotLogger(), authority, slots)

	if _, ok := slots.Slot("human:current"); !ok {
		t.Fatal("active human slot was not ensured")
	}
	if _, ok := slots.Slot("tool:current"); ok {
		t.Fatal("non-human received a subject slot")
	}
	if _, ok := slots.Slot("human:stale"); ok {
		t.Fatal("confirmed inactive slot was not removed")
	}
}

func TestSweepSubjectSlotsListFailureRetainsEverything(t *testing.T) {
	t.Parallel()
	slots := subjectgate.NewRegistry()
	slots.EnsureSlot("human:existing")
	authority := subjectSlotAuthorityStub{
		list: func(context.Context) ([]storespec.ActorRecord, error) {
			return nil, errors.New("list unavailable")
		},
		lookup: func(context.Context, actor.ActorID) (storespec.ActorRecord, bool, error) {
			t.Fatal("lookup must not run after list failure")
			return storespec.ActorRecord{}, false, nil
		},
	}

	sweepSubjectSlots(context.Background(), testSubjectSlotLogger(), authority, slots)

	if _, ok := slots.Slot("human:existing"); !ok {
		t.Fatal("list failure was treated as an empty desired set")
	}
}

func TestSweepSubjectSlotsLookupFailureRetainsOnlyUncertainCandidate(t *testing.T) {
	t.Parallel()
	slots := subjectgate.NewRegistry()
	slots.EnsureSlot("human:uncertain")
	slots.EnsureSlot("human:dead")
	authority := subjectSlotAuthorityStub{
		list: func(context.Context) ([]storespec.ActorRecord, error) {
			return nil, nil
		},
		lookup: func(_ context.Context, id actor.ActorID) (storespec.ActorRecord, bool, error) {
			if id == "human:uncertain" {
				return storespec.ActorRecord{}, false, errors.New("lookup unavailable")
			}
			return storespec.ActorRecord{}, false, nil
		},
	}

	sweepSubjectSlots(context.Background(), testSubjectSlotLogger(), authority, slots)

	if _, ok := slots.Slot("human:uncertain"); !ok {
		t.Fatal("uncertain candidate was deleted")
	}
	if _, ok := slots.Slot("human:dead"); ok {
		t.Fatal("confirmed dead candidate was retained")
	}
}

func TestSweepSubjectSlotsTerminalReadProtectsConcurrentAdmit(t *testing.T) {
	t.Parallel()
	const id actor.ActorID = "human:admitted-after-snapshot"
	slots := subjectgate.NewRegistry()
	slots.EnsureSlot(id)
	authority := subjectSlotAuthorityStub{
		list: func(context.Context) ([]storespec.ActorRecord, error) {
			return nil, nil
		},
		lookup: func(_ context.Context, got actor.ActorID) (storespec.ActorRecord, bool, error) {
			if got != id {
				t.Fatalf("lookup id=%q, want %q", got, id)
			}
			// Models Admit publishing active truth and its inline Ensure after
			// the sweep's initial snapshot but before the delete edge.
			slots.EnsureSlot(id)
			return storespec.ActorRecord{ID: id, Kind: actor.KindHuman}, true, nil
		},
	}

	sweepSubjectSlots(context.Background(), testSubjectSlotLogger(), authority, slots)

	if _, ok := slots.Slot(id); !ok {
		t.Fatal("terminal read failed to protect concurrently admitted human")
	}
}

func TestSweepSubjectSlotsRemovesStaleEnsureOnNextPass(t *testing.T) {
	t.Parallel()
	const id actor.ActorID = "human:stale-ensure"
	slots := subjectgate.NewRegistry()
	authority := subjectSlotAuthorityStub{
		list: func(context.Context) ([]storespec.ActorRecord, error) {
			return nil, nil
		},
		lookup: func(context.Context, actor.ActorID) (storespec.ActorRecord, bool, error) {
			return storespec.ActorRecord{}, false, nil
		},
	}

	// A stale inline fast path recreates a dead actor's slot after a prior
	// pass. No edge cleanup is needed; the next level pass removes it.
	slots.EnsureSlot(id)
	sweepSubjectSlots(context.Background(), testSubjectSlotLogger(), authority, slots)

	if _, ok := slots.Slot(id); ok {
		t.Fatal("stale ensured slot survived the next level pass")
	}
}
