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
