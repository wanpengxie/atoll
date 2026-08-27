package schedule

import (
	"context"
	"testing"
	"time"
)

// A caller asking "what alarms do I have" must get ONE answer, not one per
// storage home: durable and memory are a storage choice the author already made
// and should not have to re-ask about. The merge lives on the handle because it
// is the only place that holds both halves — the store cannot see this
// instance's in-memory set.
func TestListMergesBothHomesForTheWeldedAuthorOnly(t *testing.T) {
	store := newFakeStore()
	clock := newFakeClock(time.UnixMilli(1_000))
	minter, _ := newTestEngine(t, store, &fakeFireSink{}, clock)

	mine := minter.MintAuthority(testAuthority{id: "agent:mine:1"})
	theirs := minter.MintAuthority(testAuthority{id: "agent:theirs:1"})

	ctx := context.Background()
	durable, err := mine.Schedule(ctx, ScheduleReq{Home: TimerHomeDurable, FireAt: 9_000, Type: "late"})
	if err != nil {
		t.Fatal(err)
	}
	memory, err := mine.Schedule(ctx, ScheduleReq{Home: TimerHomeMemory, FireAt: 3_000, Type: "early"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := theirs.Schedule(ctx, ScheduleReq{Home: TimerHomeDurable, FireAt: 1_000, Type: "not-mine"}); err != nil {
		t.Fatal(err)
	}

	got, err := mine.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("list=%+v, want both of this author's alarms and nobody else's", got)
	}
	// Earliest first, regardless of which home each one lives in.
	if got[0].ID != memory || got[0].Home != TimerHomeMemory || got[0].FireAt != 3_000 {
		t.Fatalf("first=%+v, want the memory-home alarm at 3000", got[0])
	}
	if got[1].ID != durable || got[1].Home != TimerHomeDurable || got[1].FireAt != 9_000 {
		t.Fatalf("second=%+v, want the durable alarm at 9000", got[1])
	}
	if got[0].Type != "early" || got[1].Type != "late" {
		t.Fatalf("types=%q/%q", got[0].Type, got[1].Type)
	}
}

// An author with nothing pending gets an empty list, never an error — "I have
// no alarms" is an answer, not a fault.
func TestListOfAnAuthorWithNoAlarmsIsEmptyNotAnError(t *testing.T) {
	store := newFakeStore()
	minter, _ := newTestEngine(t, store, &fakeFireSink{}, newFakeClock(time.UnixMilli(1_000)))

	got, err := minter.MintAuthority(testAuthority{id: "agent:idle:1"}).List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("list=%+v, want empty", got)
	}
}

// Cancelling removes the alarm from the answer — the list reflects what is
// still pending, not what was ever asked for.
func TestListDropsACancelledAlarm(t *testing.T) {
	store := newFakeStore()
	minter, _ := newTestEngine(t, store, &fakeFireSink{}, newFakeClock(time.UnixMilli(1_000)))

	ctx := context.Background()
	handle := minter.MintAuthority(testAuthority{id: "agent:mine:1"})
	id, err := handle.Schedule(ctx, ScheduleReq{Home: TimerHomeDurable, FireAt: 9_000, Type: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	got, err := handle.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("list=%+v, want the cancelled alarm gone", got)
	}
}
