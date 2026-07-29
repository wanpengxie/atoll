package link

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
)

// lanecontrol_review_test.go covers 期11 review #F/#G/#H — the byte route's own
// mechanical hardening: header-read deadline, transfer TTL GC, and
// authorize-before-mutate + idempotent ResolveCoord.

// --- #H: ResolveCoord validates before mutating, and is replay-safe ----------

func TestHandleResolveCoord_AuthorizeBeforeMutate_AndIdempotent(t *testing.T) {
	a := &Acceptor{lane: newLaneState()}
	ticket, err := a.OpenTransfer(context.Background(), "target-d", "coord-x", access.OpRead, "res-1")
	if err != nil {
		t.Fatalf("OpenTransfer: %v", err)
	}

	// A WRONG sender must not burn the token (the pre-review delete-then-check
	// form destroyed a valid transfer on an unauthorized probe).
	bad := a.handleResolveCoord("wrong-d", &ResolveCoordRequest{RequestID: "r1", Token: ticket})
	if bad.OK {
		t.Fatal("wrong sender resolved the token")
	}

	// The legitimate target can STILL resolve it — proof the token was not
	// burned by the unauthorized probe above.
	ok1 := a.handleResolveCoord("target-d", &ResolveCoordRequest{RequestID: "r2", Token: ticket})
	if !ok1.OK || ok1.Coord != "coord-x" || ok1.Mode != access.OpRead || ok1.ReservationID != "res-1" {
		t.Fatalf("target resolve = %+v, want OK coord-x/read/res-1", ok1)
	}

	// A retry by the same authorized target succeeds AGAIN (replay-safe: a
	// dropped reply / re-dialed handle re-resolves the same route.Token).
	ok2 := a.handleResolveCoord("target-d", &ResolveCoordRequest{RequestID: "r3", Token: ticket})
	if !ok2.OK || ok2.Coord != "coord-x" {
		t.Fatalf("retry resolve = %+v, want the same OK result (idempotent)", ok2)
	}
}

// --- 期11 review残余#3: TTL is enforced AT USE, not only opportunistically
//     at the NEXT mint --------------------------------------------------------

// TestHandleResolveCoord_ExpiredTransferRejected proves a transfer aged past
// laneTransferTTL is rejected by handleResolveCoord even for the legitimate
// target — and deleted right there, not left for some future mint's
// opportunistic sweep to eventually notice. Before this fix, TTL was only
// checked opportunistically inside OpenTransfer; a stale token nobody
// happened to mint again after would still resolve successfully forever.
func TestHandleResolveCoord_ExpiredTransferRejected(t *testing.T) {
	a := &Acceptor{lane: newLaneState()}
	ticket, err := a.OpenTransfer(context.Background(), "target-d", "coord-x", access.OpRead, "res-1")
	if err != nil {
		t.Fatalf("OpenTransfer: %v", err)
	}
	a.lane.mu.Lock()
	tr := a.lane.resolutions[ticket]
	tr.mintedAt = time.Now().Add(-2 * laneTransferTTL)
	a.lane.resolutions[ticket] = tr
	a.lane.mu.Unlock()

	reply := a.handleResolveCoord("target-d", &ResolveCoordRequest{RequestID: "r1", Token: ticket})
	if reply.OK {
		t.Fatal("an expired transfer must not resolve, even for the legitimate target")
	}

	a.lane.mu.Lock()
	_, stillThere := a.lane.resolutions[ticket]
	a.lane.mu.Unlock()
	if stillThere {
		t.Fatal("an expired transfer must be deleted AT USE (handleResolveCoord), not merely rejected")
	}
}

// --- #G: abandoned transfers are TTL-GC'd, never leaked to Acceptor death ----

func TestOpenTransfer_TTLReclaimsAbandonedTokens(t *testing.T) {
	a := &Acceptor{lane: newLaneState()}
	old, err := a.OpenTransfer(context.Background(), "t", "c", access.OpRead, "res")
	if err != nil {
		t.Fatalf("OpenTransfer: %v", err)
	}
	// Age the minted-but-never-resolved token past the TTL.
	a.lane.mu.Lock()
	tr := a.lane.resolutions[old]
	tr.mintedAt = time.Now().Add(-2 * laneTransferTTL)
	a.lane.resolutions[old] = tr
	a.lane.mu.Unlock()

	// A fresh mint triggers the opportunistic sweep.
	if _, err := a.OpenTransfer(context.Background(), "t2", "c2", access.OpRead, "res2"); err != nil {
		t.Fatalf("OpenTransfer 2: %v", err)
	}

	a.lane.mu.Lock()
	_, stillThere := a.lane.resolutions[old]
	n := len(a.lane.resolutions)
	a.lane.mu.Unlock()
	if stillThere {
		t.Fatal("abandoned (open-no-resolve) token was not GC'd past its TTL")
	}
	if n != 1 {
		t.Fatalf("transfers map size = %d, want 1 (only the fresh token survives)", n)
	}
}

// --- #F: readStreamJSON bounds the header read with a deadline, then clears it

// deadlineConn is an io.Reader that also records every SetReadDeadline call —
// the deadline API readStreamJSON type-asserts for.
type deadlineConn struct {
	r         *bytes.Reader
	deadlines []time.Time
}

func (c *deadlineConn) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *deadlineConn) SetReadDeadline(t time.Time) error {
	c.deadlines = append(c.deadlines, t)
	return nil
}

func TestReadStreamJSON_BoundsHeaderReadThenClears(t *testing.T) {
	c := &deadlineConn{r: bytes.NewReader([]byte("{\"kind\":\"actor\"}\n"))}
	var hdr streamHeader
	if err := readStreamJSON(c, &hdr); err != nil {
		t.Fatalf("readStreamJSON: %v", err)
	}
	if hdr.Kind != streamActor {
		t.Fatalf("header kind = %q, want actor", hdr.Kind)
	}
	// A deadline was SET on entry (non-zero) and CLEARED on return (zero), so
	// whatever follows on the same stream inherits no bound.
	if len(c.deadlines) < 2 {
		t.Fatalf("SetReadDeadline calls = %d, want >= 2 (set then clear)", len(c.deadlines))
	}
	if c.deadlines[0].IsZero() {
		t.Fatal("first SetReadDeadline must be a non-zero header bound")
	}
	if last := c.deadlines[len(c.deadlines)-1]; !last.IsZero() {
		t.Fatalf("last SetReadDeadline = %v, want the zero-time clear", last)
	}
}

// A plain io.Reader (no deadline API) must still work — the deadline is simply
// skipped, never a panic.
func TestReadStreamJSON_PlainReaderNoDeadline(t *testing.T) {
	var hdr streamHeader
	if err := readStreamJSON(io.Reader(bytes.NewReader([]byte("{\"kind\":\"control\"}\n"))), &hdr); err != nil {
		t.Fatalf("readStreamJSON(plain): %v", err)
	}
	if hdr.Kind != streamControl {
		t.Fatalf("kind = %q, want control", hdr.Kind)
	}
}
