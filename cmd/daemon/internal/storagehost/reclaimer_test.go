package storagehost

import "testing"

func TestReclaimer_AxisAllocatedRemovesBytes(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "coord1", false); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	var r Reclaimer
	if err := r.Reclaim(cr, "coord1", provenanceAxisAllocated); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if _, err := (Streamer{}).OpenRead(cr, "coord1"); err == nil {
		t.Fatal("bytes must be gone after Reclaim(axis-allocated)")
	}
}

func TestReclaimer_RegisteredNeverTouchesDisk(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "coord2", false); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	var r Reclaimer
	if err := r.Reclaim(cr, "coord2", "registered"); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if _, err := (Streamer{}).OpenRead(cr, "coord2"); err != nil {
		t.Fatalf("registered provenance must never touch disk, but bytes are gone: %v", err)
	}
}

func TestReclaimer_UnknownCoordIsNoop(t *testing.T) {
	cr := newTestChannelRoot(t)
	var r Reclaimer
	if err := r.Reclaim(cr, "never-existed", provenanceAxisAllocated); err != nil {
		t.Fatalf("Reclaim on a never-existed coord must be a clean no-op: %v", err)
	}
}
