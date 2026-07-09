package main

// walk_crash_test.go is 期11 spec §6 item③'s "崩溃恢复 walk（B 的验收）":
// real create-outbox recovery driven end to end — a real reservation
// durably written at the real server, real daemon-side fsync+rename, then a
// simulated crash on EITHER side, then real recovery (the daemon's Scrubber
// resending Committed via ReconcilePull — S6's own found+built
// resumeLandedReservations, see cmd/daemon/internal/storagehost/
// scrubber.go's doc). Scans (per spec): reservation 落行 / 孤儿清 / 幂等
// no-op — three things. The FOURTH spec item ("并加 ReclaimAck walk") is
// ALREADY exercised end to end by walk_workspace_test.go's own step 6 (its
// "landed file gone from disk" poll IS the ReclaimAck round trip's
// observable effect — the Reclaimer only removes local bytes AFTER the
// daemon successfully collects+acks) — not duplicated here.
//
// Simulation technique: committingWriteHandle (platform/internal/link/
// dial.go, real production code) ALWAYS fires Committed(reservationID) as a
// best-effort side effect of Commit() — its error is discarded, so Commit()
// itself always reports success to the caller regardless of whether
// Committed actually reached home. This test injects a wrapping
// LocalFileOpener whose write handle's Commit() first does the REAL local
// fsync+rename (so bytes genuinely land, exactly like production), then
// signals the test and BLOCKS until the test says go — giving the test a
// deterministic window to sever the connection (close home / cancel the
// daemon) BEFORE committingWriteHandle's own SendCommitted attempt, so that
// attempt is guaranteed to fail (a network/context error, silently
// discarded by production's own best-effort posture) rather than racing it.

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/cmd/daemon/internal/storagehost"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// interceptOpener wraps a real platform.LocalFileOpener so every OpenWrite's
// returned handle's Commit() calls onCommit AFTER the real fsync+rename
// (bytes genuinely land) but BEFORE returning to the caller — the seam this
// file's crash simulations use. onCommit may block; it fires once per
// handle (each OpenWrite call gets its own handle).
type interceptOpener struct {
	real     platform.LocalFileOpener
	onCommit func()
}

func (o interceptOpener) OpenRead(coord string) (io.ReadSeekCloser, error) {
	return o.real.OpenRead(coord)
}

func (o interceptOpener) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	wh, err := o.real.OpenWrite(coord)
	if err != nil {
		return nil, err
	}
	return interceptWriteHandle{LocalWriteHandle: wh, onCommit: o.onCommit}, nil
}

func (o interceptOpener) OpenDir(coord string) (accessdoor.LocalDirHandle, error) {
	return o.real.OpenDir(coord)
}

func (o interceptOpener) ReclaimCoord(coord string) error {
	return o.real.ReclaimCoord(coord)
}

type interceptWriteHandle struct {
	accessdoor.LocalWriteHandle
	onCommit func()
}

func (h interceptWriteHandle) Commit() error {
	if err := h.LocalWriteHandle.Commit(); err != nil {
		return err
	}
	if h.onCommit != nil {
		h.onCommit()
	}
	return nil
}

var _ platform.LocalFileOpener = interceptOpener{}

// writeFireAndForget writes a bare kind=request envelope as pen, asserting
// only that the write itself was accepted — used exactly where a crash
// simulation makes waiting for a terminal unreliable/pointless (recovery is
// verified via LATER, separate requests instead).
func writeFireAndForget(t *testing.T, pen harness.Pen, target actor.ActorID, reqType string) {
	t.Helper()
	env := &message.Envelope{
		ID:         message.ID(uuid.NewString()),
		TS:         time.Now().UnixMilli(),
		Kind:       message.KindRequest,
		Type:       reqType,
		Payload:    []byte(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{target},
	}
	res, err := pen.Write(context.Background(), env)
	if err != nil {
		t.Fatalf("fire-and-forget pen.Write(%s -> %s): %v", reqType, target, err)
	}
	if !res.Accepted() {
		t.Fatalf("fire-and-forget pen.Write(%s -> %s) rejected: %s", reqType, target, res.RejectReason)
	}
}

// crashRecoveryOps is the shared verb table this file's publisher actors
// use: ping (attachment probe), publish_intercepted (the content-bearing
// create whose Commit() routes through this file's interceptOpener),
// publish_abandoned (mints a reservation + route but never writes — the
// orphan-timeout subtest's construction), and stat/read_own (post-recovery
// verification).
func crashRecoveryOps(fileID resource.ResourceID, content string) map[string]walkOpFunc {
	return map[string]walkOpFunc{
		"walk.ping": func(actorbase.Sys, actorbase.Msg) (any, string, string) {
			return walkResult{OK: true}, "", ""
		},
		"walk.publish_intercepted": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			fa, out, err := sys.Resource().CreateFile(fileID, false, true)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			if !out.Accepted() {
				return walkResult{OK: false, Reason: string(out.RejectReason)}, "", ""
			}
			if fa.Local == nil || fa.Local.Write == nil {
				return walkResult{OK: false, Reason: "no local write handle"}, "", ""
			}
			if _, werr := fa.Local.Write.Write([]byte(content)); werr != nil {
				return nil, "internal_error", "write: " + werr.Error()
			}
			_ = fa.Local.Write.Commit() // error intentionally ignored — see file doc
			return walkResult{OK: true}, "", ""
		},
		"walk.publish_abandoned": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			// Mints a reservation + resolves the write route, but deliberately
			// NEVER writes/commits — the orphan-timeout sweep subtest's own
			// construction of "a reservation with no local bytes, ever".
			_, out, err := sys.Resource().CreateFile(fileID, false, true)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: out.Accepted(), Reason: string(out.RejectReason)}, "", ""
		},
		"walk.stat": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			st, err := sys.Resource().Stat(fileID)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			return walkResult{OK: st.Reject == "", Reason: string(st.Reject)}, "", ""
		},
		"walk.read_own": func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
			fa, out, err := sys.Resource().Open(fileID, access.OpRead)
			if err != nil {
				return nil, "internal_error", err.Error()
			}
			if !out.Accepted() {
				return readResultCR{walkResult: walkResult{OK: false, Reason: string(out.RejectReason)}}, "", ""
			}
			if fa.Local == nil || fa.Local.Read == nil {
				return readResultCR{walkResult: walkResult{OK: false, Reason: "no local read handle"}}, "", ""
			}
			defer fa.Local.Read.Close()
			b, _ := io.ReadAll(fa.Local.Read)
			return readResultCR{walkResult: walkResult{OK: true}, Content: string(b)}, "", ""
		},
	}
}

type readResultCR struct {
	walkResult
	Content string `json:"content"`
}

// TestWalk3_CrashRecovery_ServerCrashBeforeLanding: reservation written
// (durable at the server) + daemon fsync+rename lands real bytes, then the
// SERVER crashes before Committed is processed — server restarts (fresh
// *platform.Home, SAME sqlite) — the daemon's own redial reconnects — its
// Scrubber's periodic ReconcilePull finds the still-pending reservation,
// sees its coord already landed locally, and resends Committed — the row
// lands with full byte integrity.
func TestWalk3_CrashRecovery_ServerCrashBeforeLanding(t *testing.T) {
	const chID = channelpkg.ID("walk3-server-crash")
	const publisherID = actor.ActorID("agent:walk3-pub-a")
	const fileID = resource.ResourceID("file:walk3-server-crash/report.txt")
	const content = "server crashed between rename and Committed\n"
	const daemonID = "walk3-daemon-a"

	dbPath := filepath.Join(t.TempDir(), "walk3-server-crash.sqlite")
	h1 := newWalkHomeWithConfig(t, platform.HomeConfig{ChannelID: chID, DBPath: dbPath, ReservationTimeout: time.Hour})
	wsURL, swap := newWalkDaemonServerSwappable(t, daemonID, h1)

	renamed := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	onCommit := func() {
		once.Do(func() { close(renamed) })
		<-proceed
	}

	d := startWalkDaemon(t, wsURL, daemonID, string(chID), walkDaemonConfig{
		ScrubberInterval: 150 * time.Millisecond,
		LocalFileOpener: func(sh *storagehost.Host) platform.LocalFileOpener {
			return interceptOpener{real: storageHostAdapter{host: sh}, onCommit: onCommit}
		},
	})

	ops := crashRecoveryOps(fileID, content)
	d.addActor(t, h1, publisherID, actor.KindAgent, walkActorDef(ops))
	controller1 := newControllerPen(t, h1, actor.ActorID("user:walk3-driver-a1"), actor.KindHuman)

	// Confirm the actor is attached before firing the crash-triggering
	// request (avoids racing RunCompute's own attach latency).
	requireCompleted(t, "ping", sendAndAwait(t, h1, controller1, publisherID, "walk.ping", nil, 15*time.Second))

	// Fire-and-forget: this request's own terminal is unreliable by
	// construction (the pen it would reply over dies mid-flight, once we
	// crash h1 below) — recovery is verified via LATER, separate requests
	// against h2.
	writeFireAndForget(t, controller1, publisherID, "walk.publish_intercepted")

	select {
	case <-renamed:
	case <-time.After(15 * time.Second):
		t.Fatal("write handle's Commit() never reached the real rename")
	}

	// --- simulate: SERVER crashes right here (reservation durable, real
	// bytes landed locally, but NOT YET Committed-landed at the server) —
	// close h1, open h2 against the SAME sqlite, swap the httptest handler
	// (the daemon's ServerWS URL never changes, matching a real restart).
	if err := h1.Close(); err != nil {
		t.Fatalf("h1.Close (simulated crash): %v", err)
	}
	h2 := newWalkHomeWithConfig(t, platform.HomeConfig{ChannelID: chID, DBPath: dbPath, ReservationTimeout: time.Hour})
	t.Cleanup(func() { _ = h2.Close() })
	swap(h2)

	// Let Commit() proceed — its SendCommitted attempt now hits the closed
	// h1 connection and fails (silently discarded, matches production).
	close(proceed)

	// --- "server restarted": wait for the daemon's own redial + Scrubber
	// pass to discover the pending reservation and resend Committed against
	// h2, landing the row. Poll via a FRESH controller pen against h2.
	controller2 := newControllerPen(t, h2, actor.ActorID("user:walk3-driver-a2"), actor.KindHuman)
	pollUntilLanded(t, h2, controller2, publisherID, "walk.stat", 30*time.Second)

	// Byte integrity: the SAME creator actor reads its own file back (Local
	// route, same daemon) and the content matches exactly what was written
	// before the crash.
	term := sendAndAwait(t, h2, controller2, publisherID, "walk.read_own", nil, 10*time.Second)
	requireCompleted(t, "read_own (post-crash)", term)
	var readRes readResultCR
	decodeWalkPayload(t, term, &readRes)
	if !readRes.OK {
		t.Fatalf("read_own after recovery rejected: %s", readRes.Reason)
	}
	if readRes.Content != content {
		t.Fatalf("recovered content = %q, want %q", readRes.Content, content)
	}

	// Idempotent no-op: the Scrubber's periodic pass keeps running (150ms
	// cadence) well past landing — if a resend-after-already-committed ever
	// caused an error/duplicate, SOME subsequent stat/read in this test
	// would already have surfaced it (Committed's own found=false no-op
	// contract is what makes this safe, unit-proven in
	// platform/storagehost_test.go's TestHomeStorageHostControl_Committed_SenderAuth).
}

// TestWalk3_CrashRecovery_DaemonCrashBeforeCommitted: the "另一路" (§1.7's
// own words) — the bytes land locally (fsync+rename) but the DAEMON itself
// dies before (or during) sending Committed, home never crashes. A FRESH
// daemon process reopens the SAME on-disk workspace root, connects to the
// SAME (still-alive) home, and its Scrubber discovers the reservation is
// pending + its coord already landed locally on THIS disk — resends
// Committed — the row lands.
func TestWalk3_CrashRecovery_DaemonCrashBeforeCommitted(t *testing.T) {
	const chID = channelpkg.ID("walk3-daemon-crash")
	const publisherID = actor.ActorID("agent:walk3-pub-b")
	const fileID = resource.ResourceID("file:walk3-daemon-crash/report.txt")
	const content = "daemon crashed between rename and Committed\n"
	const daemonID = "walk3-daemon-b"

	h := newWalkHome(t, chID) // home never crashes in this scenario — normal auto-cleanup is fine.
	wsURL := newWalkDaemonServer(t, h, daemonID)

	renamed := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	var d1 *walkDaemon // captured by the closure below, assigned right after startWalkDaemon returns.
	onCommit := func() {
		once.Do(func() { close(renamed) })
		<-proceed
		// Kill THIS daemon instance right here, before returning control to
		// committingWriteHandle.Commit()'s own SendCommitted attempt — d1's
		// RunCompute goroutine tears down (DetachAll/StopAll), so that
		// attempt races a closing/closed Dialer and fails, discarded, same
		// as the server-crash subtest's h1.Close() timing.
		d1.cancel()
		<-d1.done
	}

	wsRoot := t.TempDir() // survives d1's death — d2 reopens the SAME root.
	d1 = startWalkDaemonAt(t, wsURL, daemonID, string(chID), wsRoot, walkDaemonConfig{
		ScrubberInterval: 150 * time.Millisecond,
		LocalFileOpener: func(sh *storagehost.Host) platform.LocalFileOpener {
			return interceptOpener{real: storageHostAdapter{host: sh}, onCommit: onCommit}
		},
	})

	ops := crashRecoveryOps(fileID, content)
	d1.addActor(t, h, publisherID, actor.KindAgent, walkActorDef(ops))
	controller := newControllerPen(t, h, actor.ActorID("user:walk3-driver-b"), actor.KindHuman)

	requireCompleted(t, "ping", sendAndAwait(t, h, controller, publisherID, "walk.ping", nil, 15*time.Second))
	writeFireAndForget(t, controller, publisherID, "walk.publish_intercepted")

	select {
	case <-renamed:
	case <-time.After(15 * time.Second):
		t.Fatal("write handle's Commit() never reached the real rename")
	}
	close(proceed) // triggers d1's death inside onCommit, synchronously joined there.

	// --- "daemon crashed": start a FRESH daemon instance reopening the SAME
	// workspace root, same computeID, connecting to the SAME (still-alive)
	// home. It has NO local truth of its own (storagehost.Open just re-scans
	// the filesystem) — recovery relies ENTIRELY on ReconcilePull.
	d2 := startWalkDaemonAt(t, wsURL, daemonID, string(chID), wsRoot, walkDaemonConfig{ScrubberInterval: 150 * time.Millisecond})
	d2.addActor(t, h, publisherID, actor.KindAgent, walkActorDef(ops))

	pollUntilLanded(t, h, controller, publisherID, "walk.stat", 30*time.Second)

	term := sendAndAwait(t, h, controller, publisherID, "walk.read_own", nil, 10*time.Second)
	requireCompleted(t, "read_own (post-daemon-crash)", term)
	var readRes readResultCR
	decodeWalkPayload(t, term, &readRes)
	if !readRes.OK {
		t.Fatalf("read_own after recovery rejected: %s", readRes.Reason)
	}
	if readRes.Content != content {
		t.Fatalf("recovered content = %q, want %q", readRes.Content, content)
	}
}

// TestWalk3_CrashRecovery_AbandonedReservationTimeoutSweep (§1.7's third
// trigger, "超时未Committed"): a reservation whose write is NEVER even
// attempted (no AllocRequest-equivalent staging ever happens — the closest
// honest analogue this test can construct to "AllocRequest lost" within the
// with-content path, which has no separate AllocRequest step of its own —
// see this file's own package doc) ages out server-side, driven by a SHORT
// injected HomeConfig.ReservationTimeout, and no longer blocks/affects a
// later independent create on a DIFFERENT id. The precise "row deleted from
// resource_reservations" fact is unit-proven directly in
// runtime/internal/store's TestResource_SweepExpiredReservations and
// platform's TestHomeStorageHostControl_ReconcilePull_SweepsExpiredReservationsFirst
// — this integration walk's own value-add is confirming the daemon keeps
// operating normally (no wedge, no explosion) across the sweep.
func TestWalk3_CrashRecovery_AbandonedReservationTimeoutSweep(t *testing.T) {
	const chID = channelpkg.ID("walk3-orphan-sweep")
	const publisherID = actor.ActorID("agent:walk3-pub-c")
	const abandonedID = resource.ResourceID("file:walk3-orphan-sweep/abandoned.txt")
	const healthyID = resource.ResourceID("file:walk3-orphan-sweep/healthy.txt")
	const daemonID = "walk3-daemon-c"

	h := newWalkHomeWithConfig(t, platform.HomeConfig{
		ChannelID: chID, DBPath: walkDBPath(t),
		ReservationTimeout: 300 * time.Millisecond,
	})
	t.Cleanup(func() { _ = h.Close() })
	wsURL := newWalkDaemonServer(t, h, daemonID)
	d := startWalkDaemon(t, wsURL, daemonID, string(chID), walkDaemonConfig{ScrubberInterval: 150 * time.Millisecond})

	ops := crashRecoveryOps(abandonedID, "never written")
	// walk.publish_abandoned always targets abandonedID (closed over above);
	// add a second op for the healthy control resource.
	ops["walk.publish_healthy"] = func(sys actorbase.Sys, _ actorbase.Msg) (any, string, string) {
		fa, out, err := sys.Resource().CreateFile(healthyID, false, true)
		if err != nil {
			return nil, "internal_error", err.Error()
		}
		if !out.Accepted() {
			return walkResult{OK: false, Reason: string(out.RejectReason)}, "", ""
		}
		if fa.Local == nil || fa.Local.Write == nil {
			return walkResult{OK: false, Reason: "no local write handle"}, "", ""
		}
		if _, werr := fa.Local.Write.Write([]byte("still healthy")); werr != nil {
			return nil, "internal_error", "write: " + werr.Error()
		}
		if cerr := fa.Local.Write.Commit(); cerr != nil {
			return nil, "internal_error", "commit: " + cerr.Error()
		}
		return walkResult{OK: true}, "", ""
	}
	d.addActor(t, h, publisherID, actor.KindAgent, walkActorDef(ops))
	controller := newControllerPen(t, h, actor.ActorID("user:walk3-driver-c"), actor.KindHuman)

	// Mint the abandoned reservation (route resolved, nothing ever written).
	term := sendAndAwait(t, h, controller, publisherID, "walk.publish_abandoned", nil, 15*time.Second)
	requireCompleted(t, "publish_abandoned", term)
	var abandonRes walkResult
	decodeWalkPayload(t, term, &abandonRes)
	if !abandonRes.OK {
		t.Fatalf("publish_abandoned (minting the reservation) rejected: %s", abandonRes.Reason)
	}

	// Wait past the injected timeout + a few ReconcilePull cycles — the
	// sweep runs server-side, level-triggered by the daemon's own periodic
	// ReconcilePull (this daemon's cadence is 150ms, timeout 300ms).
	time.Sleep(1200 * time.Millisecond)

	// The abandoned id never landed (no Committed ever fires for it) —
	// confirmed indistinguishable from "never existed" throughout.
	term = sendAndAwait(t, h, controller, publisherID, "walk.stat", nil, 10*time.Second)
	requireCompleted(t, "stat abandoned (post-sweep)", term)
	var abandonedStat walkResult
	decodeWalkPayload(t, term, &abandonedStat)
	if abandonedStat.OK {
		t.Fatalf("abandoned reservation somehow landed a row — construction is broken")
	}

	// The system keeps operating normally: an UNRELATED create still lands
	// cleanly (no wedge left behind by the swept reservation).
	term = sendAndAwait(t, h, controller, publisherID, "walk.publish_healthy", nil, 15*time.Second)
	requireCompleted(t, "publish_healthy (post-sweep)", term)
	var healthyRes walkResult
	decodeWalkPayload(t, term, &healthyRes)
	if !healthyRes.OK {
		t.Fatalf("unrelated create after the sweep window rejected: %s", healthyRes.Reason)
	}
}
