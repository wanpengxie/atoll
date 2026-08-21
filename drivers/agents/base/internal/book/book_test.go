package book

import "testing"

func TestRemoveRequestLeavesNoTombstoneAndCleansProjection(t *testing.T) {
	s := New()
	row := &Request{ID: "r", Bytes: 3, Location: Buffered}
	s.Requests[row.ID] = row
	s.Buffer = append(s.Buffer, row.ID)
	s.BufferBytes = 3
	if got := s.RemoveRequest(row.ID); got != row {
		t.Fatalf("removed=%p want %p", got, row)
	}
	if s.Requests[row.ID] != nil || len(s.Buffer) != 0 || s.BufferBytes != 0 {
		t.Fatalf("state retained tombstone/projection: %+v", s)
	}
}

func TestInsertAtAndIndexInBufferPreserveOrder(t *testing.T) {
	s := New()
	s.Buffer = []RequestID{"r1", "r3"}
	s.InsertAt(1, "r2")
	if got := s.IndexInBuffer("r2"); got != 1 {
		t.Fatalf("index=%d buffer=%v", got, s.Buffer)
	}
	if got := s.IndexInBuffer("missing"); got != -1 {
		t.Fatalf("missing index=%d", got)
	}
}
