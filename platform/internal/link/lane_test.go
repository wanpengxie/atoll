package link_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// lane_test.go is 期11 spec §5's own DoD proof (§9 item 5'): a REAL door
// (accessdoor.New, reusing resourceface_parity_test.go's in-memory
// Registry/Driver/State doubles) wired to a REAL Acceptor+Dialer pair over
// an actual WS round trip (httptest), exercising the file byte route
// end-to-end — same-daemon Local (zerocopy: no lane byte-hop, one small
// ResolveCoord RPC then a direct local handle) and cross-daemon Stream
// (home relays between two daemons' links via top-level lane substreams) — never
// the fakes runtime/accessdoor's own package-private test doubles use.

// laneMembership is a configurable accessdoor.MembershipCheck: IsMember
// always true (this rig is not about membership decay), Lookup answers a
// per-caller host from a plain map — the file route's own same-daemon-vs-
// cross-host decision input.
type laneMembership struct{ hosts map[actor.ActorID]string }

func (laneMembership) IsMember(context.Context, actor.ActorID) (bool, error) { return true, nil }
func (m laneMembership) Lookup(_ context.Context, id actor.ActorID) (string, bool, error) {
	host, ok := m.hosts[id]
	if !ok {
		return "", false, nil
	}
	return host, true, nil
}

// laneLocalFile is an in-memory link.LocalFileOpener — the daemon-side
// same-machine byte store this test drives instead of a real
// cmd/daemon/internal/storagehost.Host (which platform/internal/link
// cannot import, §8.2's server-zero-storage boundary applies to production
// code, not this test double).
type laneLocalFile struct {
	mu    sync.Mutex
	bytes map[string][]byte
}

func newLaneLocalFile() *laneLocalFile { return &laneLocalFile{bytes: map[string][]byte{}} }

func (f *laneLocalFile) OpenRead(coord string) (io.ReadSeekCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.bytes[coord]
	if !ok {
		return nil, errors.New("laneLocalFile: no bytes for coord " + coord)
	}
	return laneReadHandle{bytes.NewReader(append([]byte(nil), b...))}, nil
}

func (f *laneLocalFile) OpenWrite(coord string) (accessdoor.LocalWriteHandle, error) {
	return &laneWriteHandle{f: f, coord: coord}, nil
}

// OpenDir is unexercised by the lane rig — a directory lease never crosses the
// lane (resolveFileRoute rejects dir && !Local, 期11 丁12); present only to
// satisfy the link.LocalFileOpener interface.
func (f *laneLocalFile) OpenDir(coord string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("laneLocalFile: OpenDir not supported (dir lease never crosses the lane)")
}

// ReclaimCoord is 期11 S2's daemon-side "非-land 终态回收" seam — deletes
// coord's in-memory bytes, mirroring storagehost.Host.ReclaimCoord's real
// removal of live/<coord>. Unexercised by this rig's own tests (parityRegistry
// does not support reservation semantics, so no lane test here can DRIVE a
// real Lost outcome); present only to satisfy link.LocalFileOpener — S2's own
// dedicated coverage is dial_reclaim_test.go (package link) + door_test.go's
// verdict-mapping test + scrubber_test.go's crash-resume reclaim test.
func (f *laneLocalFile) ReclaimCoord(coord string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.bytes, coord)
	return nil
}

type laneReadHandle struct{ *bytes.Reader }

func (laneReadHandle) Close() error { return nil }

type laneWriteHandle struct {
	f     *laneLocalFile
	coord string
	buf   bytes.Buffer
	done  bool
}

func (h *laneWriteHandle) Write(p []byte) (int, error) { return h.buf.Write(p) }
func (h *laneWriteHandle) Commit() error {
	if h.done {
		return errors.New("laneWriteHandle: already closed")
	}
	h.done = true
	h.f.mu.Lock()
	h.f.bytes[h.coord] = append([]byte(nil), h.buf.Bytes()...)
	h.f.mu.Unlock()
	return nil
}
func (h *laneWriteHandle) Abort() error { h.done = true; return nil }

var _ link.LocalFileOpener = (*laneLocalFile)(nil)

// lateAccRef is this test's own late-binding seam (mirrors production's
// platform/storagehost.go lateAcceptor): accessdoor.New needs a LaneControl
// before the Acceptor it will route through can exist (the Acceptor's own
// Config.Access needs the minter first) — a pointer set AFTER both are
// built closes the cycle, exactly like production's own ordering constraint
// (§4.3's own "late-bound" escape hatch, reused here for the identical
// reason).
type lateAccRef struct{ acc *link.Acceptor }

func (l *lateAccRef) OpenTransfer(ctx context.Context, targetDaemonID, requesterDaemonID, coord string, mode access.Operation, reservationID string) (string, error) {
	return l.acc.OpenLaneTransfer(ctx, targetDaemonID, requesterDaemonID, coord, mode, reservationID)
}

// newLaneDoor builds a REAL accessdoor.AccessMinter over parity's in-memory
// Registry/Driver/State doubles, wired for file-kind byte access: Membership
// answers per-caller hosts, LaneControl routes through ref (bound to the
// Acceptor once it exists, see lateAccRef's doc).
func newLaneDoor(t *testing.T, ref *lateAccRef, hosts map[actor.ActorID]string) (accessdoor.AccessMinter, *parityRegistry) {
	t.Helper()
	reg := newParityRegistry()
	m, err := accessdoor.New(accessdoor.Deps{
		Registry:    reg,
		Drivers:     accessdoor.DriverTable{accessdoor.KindKV: newParityDriver(reg)},
		Membership:  laneMembership{hosts: hosts},
		State:       parityState{},
		LaneControl: ref,
	})
	if err != nil {
		t.Fatalf("accessdoor.New: %v", err)
	}
	return m, reg
}

// dialLaneDaemon dials one daemon onto srv, declares actorID, opens its
// resource-face stream, opens the lane carrier, and wires opener as its
// LocalFileOpener. Returns the Dialer and the actor's CellArms.
func dialLaneDaemon(t *testing.T, srv *httptest.Server, daemonID string, actorID actor.ActorID, opener link.LocalFileOpener) (*link.Dialer, link.CellArms) {
	t.Helper()
	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], daemonID,
		[]link.Declaration{{ActorID: actorID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{LocalFileOpener: opener}, nil)
	if err != nil {
		t.Fatalf("Dial(%s): %v", daemonID, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	arms, err := d.OpenStream(context.Background(), actorID, 0, func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatalf("OpenStream(%s): %v", daemonID, err)
	}
	d.Start()
	// Flattened lane (片③): no per-link carrier to open. The daemon accepts
	// home-relayed inbound lane substreams via its always-running accept loop
	// (onLane → handleLaneInbound), and opens its own redeem substreams on
	// demand — both need only a live Dial, already done above.
	return d, arms
}

// TestLaneSameDaemonLocalRoute is §5's Local-route DoD proof: a caller
// whose Membership.Lookup Host matches the file's PlacementDaemonID gets
// Route.Local=true and redeems it via ONE ResolveCoord control-RPC (never
// the lane byte-hop) — reading back the exact bytes the daemon's
// LocalFileOpener holds, and writing new bytes through the same route.
func TestLaneSameDaemonLocalRoute(t *testing.T) {
	const readerID = actor.ActorID("tool:local-reader")
	const daemonID = "daemon-local"
	const coord = "coord-local-1"
	const rid = resource.ResourceID("file-local-1")

	ref := &lateAccRef{}
	minter, reg := newLaneDoor(t, ref, map[actor.ActorID]string{readerID: daemonID})

	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := link.NewAcceptor(link.Config{
		Minter: &stubMinter{}, Access: minter, Schedule: &fakeScheduleMinter{}, Runtime: rt,
		Membership: &stubMembership{}, ChannelID: testChannelID,
		LeasePing: 5 * time.Second, LeaseTTL: 30 * time.Second,
	})
	ref.acc = acc

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { acc.Serve(w, req, daemonID) }))
	t.Cleanup(func() { _ = acc.Close(); srv.Close(); rt.StopAll() })

	opener := newLaneLocalFile()
	opener.bytes[coord] = []byte("hello local")
	_, arms := dialLaneDaemon(t, srv, daemonID, readerID, opener)

	// Seed the file row directly (bypassing door.create's placement policy
	// chain, which §5 does not need to re-exercise — door_test.go/
	// query_test.go already cover it) with PlacementDaemonID == daemonID so
	// the door's resolveFileRoute picks Local.
	ctx := context.Background()
	if err := reg.Create(ctx, rid, resourcespec.KindFile, readerID, daemonID, coord, resourcespec.ProvenanceAxisAllocated, nil); err != nil {
		t.Fatalf("seed file row: %v", err)
	}

	fo, ok := arms.Access.(accessdoor.FileOpener)
	if !ok {
		t.Fatal("remoteResourceHandle must implement accessdoor.FileOpener")
	}

	t.Run("read redeems a Local handle with the daemon's own bytes", func(t *testing.T) {
		fa, out, err := fo.Open(ctx, rid, access.OpRead)
		if err != nil || !out.Accepted() {
			t.Fatalf("Open(read): out=%+v err=%v", out, err)
		}
		if out.Route == nil || !out.Route.Local {
			t.Fatalf("want Local route, got %+v", out.Route)
		}
		if fa.Local == nil || fa.Local.Read == nil {
			t.Fatalf("want a populated Local.Read handle, got %+v", fa)
		}
		defer fa.Local.Read.Close()
		got, err := io.ReadAll(fa.Local.Read)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != "hello local" {
			t.Fatalf("bytes = %q, want %q", got, "hello local")
		}
	})

	t.Run("write redeems a Local write handle, Commit lands the bytes", func(t *testing.T) {
		fa, out, err := fo.Open(ctx, rid, access.OpWrite)
		if err != nil || !out.Accepted() {
			t.Fatalf("Open(write): out=%+v err=%v", out, err)
		}
		if fa.Local == nil || fa.Local.Write == nil {
			t.Fatalf("want a populated Local.Write handle, got %+v", fa)
		}
		if _, err := fa.Local.Write.Write([]byte("updated bytes")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := fa.Local.Write.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		opener.mu.Lock()
		got := string(opener.bytes[coord])
		opener.mu.Unlock()
		if got != "updated bytes" {
			t.Fatalf("daemon-local bytes = %q, want %q", got, "updated bytes")
		}
	})
}

// TestLaneCrossDaemonStreamRoute is §5's Stream-route DoD proof (item ⑩'s
// "consumer 在 B、字节在 A"): a caller on daemon B, reading a file placed on
// daemon A, gets Route.Local=false and a redeemed lane Stream whose bytes
// are relayed home↔A↔home↔B — proving the full "consumer→server→A" /
// "server 居中转发" byte path, not just the control-plane decision.
func TestLaneCrossDaemonStreamRoute(t *testing.T) {
	const daemonA = "daemon-A"
	const daemonB = "daemon-B"
	const readerB = actor.ActorID("tool:reader-B")
	const coord = "coord-cross-1"
	const rid = resource.ResourceID("file-cross-1")

	ref := &lateAccRef{}
	minter, reg := newLaneDoor(t, ref, map[actor.ActorID]string{readerB: daemonB})

	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := link.NewAcceptor(link.Config{
		Minter: &stubMinter{}, Access: minter, Schedule: &fakeScheduleMinter{}, Runtime: rt,
		Membership: &stubMembership{}, ChannelID: testChannelID,
		LeasePing: 5 * time.Second, LeaseTTL: 30 * time.Second,
	})
	ref.acc = acc

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		daemonID := req.URL.Query().Get("daemon")
		acc.Serve(w, req, daemonID)
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close(); rt.StopAll() })

	openerA := newLaneLocalFile()
	openerA.bytes[coord] = []byte("hello cross daemon")

	// daemon A hosts the bytes but declares no reader actor of interest here
	// (it only needs its lane carrier open to SERVE the transfer).
	dialLaneDaemonAt(t, srv, daemonA, "tool:placeholder-A", openerA)
	_, armsB := dialLaneDaemonAt(t, srv, daemonB, readerB, nil)

	ctx := context.Background()
	if err := reg.Create(ctx, rid, resourcespec.KindFile, readerB, daemonA, coord, resourcespec.ProvenanceAxisAllocated, nil); err != nil {
		t.Fatalf("seed file row: %v", err)
	}

	fo, ok := armsB.Access.(accessdoor.FileOpener)
	if !ok {
		t.Fatal("remoteResourceHandle must implement accessdoor.FileOpener")
	}
	fa, out, err := fo.Open(ctx, rid, access.OpRead)
	if err != nil || !out.Accepted() {
		t.Fatalf("Open(read) on B: out=%+v err=%v", out, err)
	}
	if out.Route == nil || out.Route.Local {
		t.Fatalf("want a non-Local (Stream) route from B, got %+v", out.Route)
	}
	if fa.Stream == nil {
		t.Fatalf("want a populated Stream, got %+v", fa)
	}
	defer fa.Stream.Close()
	got, err := io.ReadAll(fa.Stream)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(got) != "hello cross daemon" {
		t.Fatalf("bytes = %q, want %q", got, "hello cross daemon")
	}

	t.Run("write from B relays to A's local bytes", func(t *testing.T) {
		fa, out, err := fo.Open(ctx, rid, access.OpWrite)
		if err != nil || !out.Accepted() {
			t.Fatalf("Open(write) on B: out=%+v err=%v", out, err)
		}
		if out.Route == nil || out.Route.Local {
			t.Fatalf("want a non-Local (Stream) write route from B, got %+v", out.Route)
		}
		if fa.Stream == nil {
			t.Fatalf("want a populated Stream, got %+v", fa)
		}
		if _, err := fa.Stream.Write([]byte("written from B")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := fa.Stream.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		// The target daemon's inbound handler commits asynchronously to this
		// call returning (Close only signals "no more bytes coming", the
		// daemon's own io.Copy/Commit runs on its own goroutine) — poll
		// briefly rather than assume a specific commit latency.
		deadline := time.Now().Add(2 * time.Second)
		for {
			openerA.mu.Lock()
			got := string(openerA.bytes[coord])
			openerA.mu.Unlock()
			if got == "written from B" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("daemon-A bytes = %q, want %q (commit did not land in time)", got, "written from B")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// TestLaneCrossDaemonLargeTransfer is §9 DoD item 5's explicit ">16MiB
// 全链路成功" proof: the resource lane's byte path has NO frame-cap bound
// (unlike kv's Outcome.Value, which is capped by the 16MiB ipc frame —
// §3.7's own List-limit binding references that SAME cap) — a file read
// larger than that cap must complete cleanly through the full
// requester→home→target relay.
func TestLaneCrossDaemonLargeTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("short: ~1.4s large cross-daemon byte transfer — full gate (make test-full) runs it")
	}
	const daemonA = "daemon-A-large"
	const daemonB = "daemon-B-large"
	const readerB = actor.ActorID("tool:reader-B-large")
	const coord = "coord-large-1"
	const rid = resource.ResourceID("file-large-1")
	const size = 17 << 20 // 17 MiB, > the 16MiB ipc frame cap

	ref := &lateAccRef{}
	minter, reg := newLaneDoor(t, ref, map[actor.ActorID]string{readerB: daemonB})

	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := link.NewAcceptor(link.Config{
		Minter: &stubMinter{}, Access: minter, Schedule: &fakeScheduleMinter{}, Runtime: rt,
		Membership: &stubMembership{}, ChannelID: testChannelID,
		LeasePing: 5 * time.Second, LeaseTTL: 30 * time.Second,
	})
	ref.acc = acc

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, req.URL.Query().Get("daemon"))
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close(); rt.StopAll() })

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	openerA := newLaneLocalFile()
	openerA.bytes[coord] = payload

	dialLaneDaemonAt(t, srv, daemonA, "tool:placeholder-A-large", openerA)
	_, armsB := dialLaneDaemonAt(t, srv, daemonB, readerB, nil)

	ctx := context.Background()
	if err := reg.Create(ctx, rid, resourcespec.KindFile, readerB, daemonA, coord, resourcespec.ProvenanceAxisAllocated, nil); err != nil {
		t.Fatalf("seed file row: %v", err)
	}

	fo := armsB.Access.(accessdoor.FileOpener)
	fa, out, err := fo.Open(ctx, rid, access.OpRead)
	if err != nil || !out.Accepted() || fa.Stream == nil {
		t.Fatalf("Open(read): out=%+v err=%v fa=%+v", out, err, fa)
	}
	defer fa.Stream.Close()
	got, err := io.ReadAll(fa.Stream)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(got) != size {
		t.Fatalf("len(got) = %d, want %d", len(got), size)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("large transfer content mismatch")
	}
}

// dialLaneDaemonAt is dialLaneDaemon's twin for a multi-daemon rig whose
// httptest handler dispatches on a ?daemon= query param (Acceptor.Serve's
// daemonID is otherwise fixed per-handler in the single-daemon tests above).
func dialLaneDaemonAt(t *testing.T, srv *httptest.Server, daemonID string, actorID actor.ActorID, opener link.LocalFileOpener) (*link.Dialer, link.CellArms) {
	t.Helper()
	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:]+"?daemon="+daemonID, daemonID,
		[]link.Declaration{{ActorID: actorID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{LocalFileOpener: opener}, nil)
	if err != nil {
		t.Fatalf("Dial(%s): %v", daemonID, err)
	}
	t.Cleanup(func() { _ = d.Close() })
	arms, err := d.OpenStream(context.Background(), actorID, 0, func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatalf("OpenStream(%s): %v", daemonID, err)
	}
	d.Start()
	// Flattened lane (片③): no per-link carrier to open — see dialLaneDaemon.
	return d, arms
}
