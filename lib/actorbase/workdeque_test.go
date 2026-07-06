package actorbase

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func reqEnv(id string) *message.Envelope {
	return &message.Envelope{ID: message.ID(id), Kind: message.KindRequest}
}
func evEnv(id string) *message.Envelope {
	return &message.Envelope{ID: message.ID(id), Kind: message.KindEvent}
}

// TestWorkDeque_FIFO: pop returns items in push order.
func TestWorkDeque_FIFO(t *testing.T) {
	t.Parallel()
	q := newWorkDeque(8)
	for _, id := range []string{"a", "b", "c"} {
		if d := q.push(evEnv(id)); d != nil {
			t.Fatalf("push %s dropped %v unexpectedly", id, d.ID)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		got, ok := q.pop()
		if !ok || string(got.ID) != want {
			t.Fatalf("pop = %v,%v want %s", got, ok, want)
		}
	}
	if _, ok := q.pop(); ok {
		t.Fatal("pop on empty deque returned ok")
	}
}

// TestWorkDeque_OverflowEvictsOldestEvent_KeepsRequests (DoD⑥ + P0-4=A): a full
// deque under an event flood evicts the OLDEST EVENT, never an admitted request —
// the request stays and remains poppable (its account never orphaned), and
// cross-kind FIFO holds.
func TestWorkDeque_OverflowEvictsOldestEvent_KeepsRequests(t *testing.T) {
	t.Parallel()
	q := newWorkDeque(3)
	q.push(reqEnv("R1")) // an admitted request at the front
	q.push(evEnv("E1"))
	q.push(evEnv("E2")) // full: [R1, E1, E2]

	// A fresh event overflows → evicts the OLDEST NON-request (E1), keeps R1.
	if d := q.push(evEnv("E3")); d == nil || string(d.ID) != "E1" {
		t.Fatalf("overflow dropped %v, want E1 (oldest event)", d)
	}
	// Deque is now [R1, E2, E3]; another event evicts E2.
	if d := q.push(evEnv("E4")); d == nil || string(d.ID) != "E2" {
		t.Fatalf("overflow dropped %v, want E2", d)
	}
	// The request survived the whole flood and pops first (front-FIFO).
	got, ok := q.pop()
	if !ok || string(got.ID) != "R1" {
		t.Fatalf("first pop = %v,%v want R1 (request never evicted)", got, ok)
	}
	got, _ = q.pop()
	if string(got.ID) != "E3" {
		t.Fatalf("second pop = %v want E3 (FIFO among survivors)", got.ID)
	}
	got, _ = q.pop()
	if string(got.ID) != "E4" {
		t.Fatalf("third pop = %v want E4", got.ID)
	}
}

// TestWorkDeque_AllRequestsFull_DropsNewEvent: when every queued item is a
// request and the deque is full, a new EVENT is refused (dropping the newcomer is
// honest; evicting a request to seat an event would orphan an open account).
func TestWorkDeque_AllRequestsFull_DropsNewEvent(t *testing.T) {
	t.Parallel()
	q := newWorkDeque(2)
	q.push(reqEnv("R1"))
	q.push(reqEnv("R2")) // full, all requests

	if d := q.push(evEnv("E1")); d == nil || string(d.ID) != "E1" {
		t.Fatalf("all-request-full push dropped %v, want the newcomer E1", d)
	}
	// Both requests are intact and FIFO.
	a, _ := q.pop()
	b, _ := q.pop()
	if string(a.ID) != "R1" || string(b.ID) != "R2" {
		t.Fatalf("survivors = %s,%s want R1,R2", a.ID, b.ID)
	}
}
