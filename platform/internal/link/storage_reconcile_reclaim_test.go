package link

// The two storage control-RPC round trips whose failure mode is SILENT rather
// than loud: ReconcilePull (daemon→home, the daemon's ONLY source of "what
// should exist" after a reconnect — §4.7's fourth frame) and ReclaimRequest
// (home→daemon, the GC arm — 期11 review §2.5 #B).
//
// storagecontrol_test.go pins their happy paths (a frame reaches its real
// handler, its verdict rides back). What lives here is the other half: that
// the pull arm survives a real reconnect, that a daemon only ever sees rows
// the home filed under ITS OWN authenticated identity, and — on both arms —
// that a failure is reported as a failure rather than as an empty/clean
// answer.
//
// Why the split matters. A ReconcilePull answered with zero rows and no reason
// reads as "you own nothing", which the Scrubber turns into "sweep everything
// on disk as an orphan"; a ReclaimRequest answered OK when nothing was
// collected leaves the home believing bytes are gone that are still on disk.
// Neither shows up as an error anywhere — they show up as missing or duplicated
// data much later.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// --- doubles ---------------------------------------------------------------

// reconcileLedgerPull is one recorded home-side arrival: the coords the daemon
// declared active plus the sender the HOME attributed the frame to. The sender
// is the whole point — ReconcilePull carries no sender field, so a home that
// took one from the payload would be trivially spoofable.
type reconcileLedgerPull struct {
	sender       string
	activeCoords []string
}

// reconcileLedgerStorage is a home StorageHostControl that files rows per
// daemon, exactly as the real one does (§4.7's sender-auth read discipline).
type reconcileLedgerStorage struct {
	mu    sync.Mutex
	pulls []reconcileLedgerPull
	rows  map[string]ReconcilePullReply
	err   error
}

func (s *reconcileLedgerStorage) Committed(context.Context, string, string) (bool, bool, error) {
	return true, false, nil
}

func (s *reconcileLedgerStorage) ReclaimAck(context.Context, string, string) (bool, error) {
	return true, nil
}

func (s *reconcileLedgerStorage) ReconcilePull(
	_ context.Context,
	sender string,
	activeCoords []string,
) ([]ReconcileResource, []ReconcileReservation, []ReconcileTombstone, error) {
	s.mu.Lock()
	s.pulls = append(s.pulls, reconcileLedgerPull{
		sender: sender, activeCoords: append([]string(nil), activeCoords...),
	})
	rows := s.rows[sender]
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return nil, nil, nil, err
	}
	return rows.Resources, rows.PendingReservations, rows.PendingTombstones, nil
}

func (s *reconcileLedgerStorage) recordedPulls() []reconcileLedgerPull {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]reconcileLedgerPull(nil), s.pulls...)
}

// reclaimVerdictOpener is a daemon LocalFileOpener whose ReclaimCoord verdict
// is a test's own choice — the only arm ReclaimRequest drives. Every other arm
// stays inert so an accidental use is loud.
type reclaimVerdictOpener struct {
	mu      sync.Mutex
	coords  []string
	verdict func(coord string) error
}

func (o *reclaimVerdictOpener) OpenRead(string) (io.ReadSeekCloser, error) {
	return nil, errors.New("reclaimVerdictOpener: OpenRead unexercised")
}

func (o *reclaimVerdictOpener) OpenWrite(string) (accessdoor.LocalWriteHandle, error) {
	return nil, errors.New("reclaimVerdictOpener: OpenWrite unexercised")
}

func (o *reclaimVerdictOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("reclaimVerdictOpener: OpenDir unexercised")
}

func (o *reclaimVerdictOpener) ReclaimCoord(coord string) error {
	o.mu.Lock()
	o.coords = append(o.coords, coord)
	verdict := o.verdict
	o.mu.Unlock()
	if verdict == nil {
		return nil
	}
	return verdict(coord)
}

func (o *reclaimVerdictOpener) reclaimed() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.coords...)
}

var _ LocalFileOpener = (*reclaimVerdictOpener)(nil)

// dialReconcileDaemon dials one named daemon against rig with an explicit
// opener, mirroring the production Dial path (real websocket, real attach).
func dialReconcileDaemon(t *testing.T, rig *acceptorRig, daemonID string, opener LocalFileOpener) *Dialer {
	t.Helper()
	dialer, err := Dial(t.Context(), rig.wsURL()+"?daemon="+daemonID, DialConfig{
		SessionLedger:   NewRemoteSessionLedger(nil),
		LocalFileOpener: opener,
	}, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", daemonID, err)
	}
	t.Cleanup(func() { _ = dialer.Close() })
	return dialer
}

func byDaemonAcceptorRig(t *testing.T, storage StorageHostControl) *acceptorRig {
	t.Helper()
	return newAcceptorRig(t, acceptorRigConfig{
		storage:  storage,
		daemonID: func(req *http.Request) string { return req.URL.Query().Get("daemon") },
	})
}

// --- ReconcilePull: the round trip across a reconnect ----------------------

// A daemon holds no local truth, so after a reconnect its whole ledger view
// has to come back over this one arm — which first requires the arm to still
// WORK on the new session. That is what this pins, and only that: a real
// disconnect (the carrier is torn down and the home retires the row) followed
// by a fresh production dial under the SAME daemon identity, after which the
// pull still routes to the home's handler, is attributed to the reconnected
// identity, and its reply rides back over the new carrier intact.
//
// Two things it deliberately does NOT claim:
//
//   - "the rows are restored" is a statement about codec + routing here, not
//     about recovery behaviour: the reply is whatever this file's own fake
//     filed under the daemon key, so the assertion is that the reply survives
//     the round trip whole, not that any real ledger rebuilt anything;
//   - the activeCoords the second pull carries are FRESH only in the sense
//     that the caller passed a different literal. This layer makes no
//     snapshot decision at all — SendReconcilePull forwards the caller's
//     slice verbatim (dial.go). Computing the live active-coord set is
//     platform/compute's Scrubber, and freshness has to be pinned there.
func TestReconcilePullStillRoundTripsAfterReconnectUnderTheSameIdentity(t *testing.T) {
	storage := &reconcileLedgerStorage{rows: map[string]ReconcilePullReply{
		"daemon-restart": {
			Resources:           []ReconcileResource{{Coord: "landed-1"}},
			PendingReservations: []ReconcileReservation{{ReservationID: "rsv-1", Coord: "staging-1"}},
			PendingTombstones:   []ReconcileTombstone{{TombstoneID: "tmb-1", Coord: "dead-1"}},
		},
	}}
	rig := byDaemonAcceptorRig(t, storage)

	first := dialReconcileDaemon(t, rig, "daemon-restart", nil)
	before, err := first.SendReconcilePull(t.Context(), []string{"open-before"})
	if err != nil {
		t.Fatalf("first SendReconcilePull: %v", err)
	}
	if len(before.Resources) != 1 || len(before.PendingReservations) != 1 ||
		len(before.PendingTombstones) != 1 {
		t.Fatalf("pre-disconnect reply lost rows: %+v", before)
	}

	// Real disconnect: the carrier goes, and the home retires the row.
	if err := first.Close(); err != nil {
		t.Fatalf("close first session: %v", err)
	}
	if !sessRaceEventually(30*time.Second, func() bool {
		return !rig.acc.IsAttached("daemon-restart")
	}) {
		t.Fatal("the home kept the daemon attached after the carrier was closed")
	}

	// Reconnect under the same identity and reconcile again.
	second := dialReconcileDaemon(t, rig, "daemon-restart", nil)
	after, err := second.SendReconcilePull(t.Context(), []string{"open-after"})
	if err != nil {
		t.Fatalf("post-reconnect SendReconcilePull: %v", err)
	}
	if len(after.Resources) != 1 || after.Resources[0].Coord != "landed-1" ||
		len(after.PendingReservations) != 1 || after.PendingReservations[0].ReservationID != "rsv-1" ||
		len(after.PendingTombstones) != 1 || after.PendingTombstones[0].TombstoneID != "tmb-1" {
		t.Fatalf("the reply lost rows on the way back over the new carrier: %+v", after)
	}
	if after.Reason != "" {
		t.Fatalf("post-reconnect reply carried a reason: %q", after.Reason)
	}

	pulls := storage.recordedPulls()
	if len(pulls) != 2 {
		t.Fatalf("home saw %d pulls, want 2: %+v", len(pulls), pulls)
	}
	for i, pull := range pulls {
		if pull.sender != "daemon-restart" {
			t.Fatalf("pull %d attributed to %q, want the authenticated daemon-restart", i, pull.sender)
		}
	}
	// The caller's argument reaches the home unaltered on the new session (the
	// arm is a pass-through, so this is a wire-fidelity check, not a freshness
	// one — the second call simply passed a different literal than the first).
	if len(pulls[1].activeCoords) != 1 || pulls[1].activeCoords[0] != "open-after" {
		t.Fatalf("the post-reconnect pull arrived carrying %v, want the caller's own [open-after]",
			pulls[1].activeCoords)
	}
}

// The reply is scoped to the sender the HOME authenticated, and the request
// frame has no field through which a daemon could claim to be someone else —
// otherwise one daemon's pull would enumerate another's coord list (§4.7's
// "不泄他 daemon 的 coord 清单") and, worse, could sweep it.
func TestReconcilePullRowsAreScopedToTheAuthenticatedSender(t *testing.T) {
	storage := &reconcileLedgerStorage{rows: map[string]ReconcilePullReply{
		"daemon-a": {Resources: []ReconcileResource{{Coord: "a-only"}}},
		"daemon-b": {Resources: []ReconcileResource{{Coord: "b-only"}}},
	}}
	rig := byDaemonAcceptorRig(t, storage)
	a := dialReconcileDaemon(t, rig, "daemon-a", nil)
	b := dialReconcileDaemon(t, rig, "daemon-b", nil)

	replyA, err := a.SendReconcilePull(t.Context(), []string{"a-live"})
	if err != nil {
		t.Fatalf("daemon-a pull: %v", err)
	}
	replyB, err := b.SendReconcilePull(t.Context(), []string{"b-live"})
	if err != nil {
		t.Fatalf("daemon-b pull: %v", err)
	}
	if len(replyA.Resources) != 1 || replyA.Resources[0].Coord != "a-only" {
		t.Fatalf("daemon-a saw %+v", replyA.Resources)
	}
	if len(replyB.Resources) != 1 || replyB.Resources[0].Coord != "b-only" {
		t.Fatalf("daemon-b saw %+v", replyB.Resources)
	}

	pulls := storage.recordedPulls()
	seen := map[string][]string{}
	for _, pull := range pulls {
		seen[pull.sender] = pull.activeCoords
	}
	if len(seen) != 2 || len(seen["daemon-a"]) != 1 || seen["daemon-a"][0] != "a-live" ||
		len(seen["daemon-b"]) != 1 || seen["daemon-b"][0] != "b-live" {
		t.Fatalf("home attributed pulls as %+v", seen)
	}

	// Structural half: the frame itself offers no sender slot to forge.
	raw, err := json.Marshal(ReconcilePull{RequestID: "rq", ActiveCoords: []string{"c"}})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"sender", "daemon_id", "daemon", "channel_id"} {
		if _, found := fields[forbidden]; found {
			t.Fatalf("reconcile_pull carries a self-reported %q field", forbidden)
		}
	}
}

// A home-side failure must arrive as a REASON on an otherwise empty reply, not
// as a clean empty ledger. "No rows, no reason" is what a daemon reads as
// "nothing here is yours" — and acts on by sweeping its whole directory.
func TestReconcilePullHomeFailureIsAReasonNotAnEmptyLedger(t *testing.T) {
	storage := &reconcileLedgerStorage{
		rows: map[string]ReconcilePullReply{
			"daemon-broken": {Resources: []ReconcileResource{{Coord: "landed-1"}}},
		},
		err: errors.New("resource registry unavailable"),
	}
	rig := byDaemonAcceptorRig(t, storage)
	daemon := dialReconcileDaemon(t, rig, "daemon-broken", nil)

	reply, err := daemon.SendReconcilePull(t.Context(), []string{"open-1"})
	if err != nil {
		t.Fatalf("the round trip itself failed: %v", err)
	}
	if reply.Reason == "" {
		t.Fatal("a home-side reconcile failure came back as a clean empty ledger (silent data loss)")
	}
	if !strings.Contains(reply.Reason, "resource registry unavailable") {
		t.Fatalf("reason = %q, want the home's own failure named", reply.Reason)
	}
	if len(reply.Resources) != 0 || len(reply.PendingReservations) != 0 ||
		len(reply.PendingTombstones) != 0 {
		t.Fatalf("a failed pull still carried rows: %+v", reply)
	}
}

// --- ReclaimRequest: the GC arm ---------------------------------------------

// A daemon that could not collect the bytes must fail the home's call. The
// alternative — reporting OK — leaves the home believing an orphan is gone
// while it is still on disk, and nothing ever revisits it.
func TestReclaimRequestFailureIsNotDressedAsSuccess(t *testing.T) {
	rig := byDaemonAcceptorRig(t, &reconcileLedgerStorage{})
	opener := &reclaimVerdictOpener{
		verdict: func(string) error { return errors.New("live root is read-only") },
	}
	dialReconcileDaemon(t, rig, "daemon-gc", opener)

	err := rig.acc.SendReclaimRequest(t.Context(), "daemon-gc", "coord-orphan")
	if err == nil {
		t.Fatal("a failed reclaim was reported to the home as success")
	}
	if !strings.Contains(err.Error(), "live root is read-only") {
		t.Fatalf("err = %v, want the daemon's own reason relayed", err)
	}
	if got := opener.reclaimed(); len(got) != 1 || got[0] != "coord-orphan" {
		t.Fatalf("daemon attempted %v, want exactly [coord-orphan]", got)
	}
}

// A compute with no storage host wired cannot collect anything; that has to be
// an honest refusal, never an OK the home would record as "collected".
func TestReclaimRequestWithoutStorageHostIsRefused(t *testing.T) {
	rig := byDaemonAcceptorRig(t, &reconcileLedgerStorage{})
	dialReconcileDaemon(t, rig, "daemon-bare", nil)

	err := rig.acc.SendReclaimRequest(t.Context(), "daemon-bare", "coord-orphan")
	if err == nil {
		t.Fatal("a compute with no storage host claimed the coord was reclaimed")
	}
	if !strings.Contains(err.Error(), "no storage host wired") {
		t.Fatalf("err = %v, want the honest not-wired reason", err)
	}
}

// Reclaim is replay-safe and precisely targeted: repeating it on a coord is a
// clean no-op (the delete path retries freely), and only the coord the home
// names is ever touched — the arm carries exactly one coord, so a wrong or
// widened target here is deletion of live data.
func TestReclaimRequestRepeatsAreCleanAndTouchOnlyTheNamedCoord(t *testing.T) {
	rig := byDaemonAcceptorRig(t, &reconcileLedgerStorage{})
	opener := &reclaimVerdictOpener{}
	dialReconcileDaemon(t, rig, "daemon-gc", opener)

	for _, coord := range []string{"coord-x", "coord-x", "coord-y"} {
		if err := rig.acc.SendReclaimRequest(t.Context(), "daemon-gc", coord); err != nil {
			t.Fatalf("reclaim %s: %v", coord, err)
		}
	}
	got := opener.reclaimed()
	want := []string{"coord-x", "coord-x", "coord-y"}
	if len(got) != len(want) {
		t.Fatalf("daemon reclaimed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("daemon reclaimed %v, want %v", got, want)
		}
	}
}
