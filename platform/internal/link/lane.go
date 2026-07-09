package link

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// laneHeaderReadTimeout bounds the ONE header/ack read at the head of every
// lane substream (期11 review #F). Without it, a peer that opens a lane
// substream and then never writes its header (a half-open connection, a peer
// bug) wedges the dispatch goroutine reading it forever — the Lease pings only
// watch the control stream, so a stuck lane substream leaks undetected.
// Generous relative to leasePing/leaseTTL (10s/30s): a header is tens of bytes,
// so any healthy peer sends it near-instantly; this only ever fires on a
// genuinely stuck/half-open stream. The deadline is CLEARED the moment the
// header is read (readLaneJSON's defer), so it never bounds the raw byte pump
// that follows on the same stream — a long transfer is unaffected.
const laneHeaderReadTimeout = 30 * time.Second

// lane.go is 期11 spec §5's resource lane transport. 片③ FLATTENED it: a lane
// redeem is now a PLAIN top-level substream of the link session (linksession.go),
// opened with streamHeader{Kind:lane} and immediately followed by the
// laneRedeemHeader below — NOT a second yamux session nested inside a carrier
// substream (the retired 片② design). The top-level session already gives
// bidirectional Open/Accept (§5 item 0's "server 可发起，非现状仅 daemon 侧"):
// the requester daemon Opens a lane substream toward the home (a redeem), the
// home Opens a lane substream toward the transfer's TARGET daemon (the relay),
// and each end's accept loop dispatches tag=lane by its own role (home →
// handleLaneRedeem, daemon → handleLaneInbound). Not a second WS connection,
// not a second HTTP endpoint — one session, many self-describing substreams.

// --- lane data-stream header wire shapes -----------------------------------

// laneRedeemHeader is the FIRST thing a requester writes on a stream it
// opens on its OWN lane session to redeem a Token (§5 item 0: "consumer 拿
// 到的是字节流,不是可离线兑现的票据" — redemption always happens on the
// requester's own already-authenticated connection, never a bare token
// handed to a third party). Sent only on the cross-host (Stream) route; the
// Local route never touches the lane at all (it resolves via the
// control-RPC ResolveCoord frame instead, see storagecontrol.go's doc).
type laneRedeemHeader struct {
	Token string `json:"token"`
}

// laneAck is the small accept/reject reply either side of a freshly-opened
// lane stream sends before raw bytes start flowing — a malformed/unknown
// token, an unreachable target daemon, or a local open failure all surface
// here rather than as a bare stream close a caller cannot distinguish from
// "zero bytes, all good".
type laneAck struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

// writeLaneJSON / readLaneJSON are the lane's own tiny framing: a single
// newline-terminated JSON value per header/ack, exactly once per stream,
// before any raw byte flow begins. Newline-terminated (not length-prefixed)
// is sufficient because every message here is sent by exactly one side at a
// fixed protocol step — no interleaving, no need for a byte-exact framer.
func writeLaneJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

// readLaneJSON reads EXACTLY through the terminating '\n' and no further —
// deliberately NOT a bufio.Reader/json.Decoder wrapping r directly: both
// read AHEAD into their own internal buffer past the delimiter (a decoder
// has no length prefix to bound its read to), silently swallowing the
// raw byte payload that immediately follows on the SAME stream (every lane
// data stream's header is followed by raw bytes with no further framing).
// A byte-at-a-time scan is the simplest way to guarantee zero over-read;
// headers here are tens of bytes, so the per-byte Read call cost is noise.
func readLaneJSON(r io.Reader, v any) error {
	// Bound the header read against a half-open / never-writing peer (#F). Set
	// on entry, CLEARED on return (defer) so the subsequent raw byte pump on the
	// SAME stream inherits no deadline. Only streams that carry a deadline API
	// (net.Conn / yamux stream) are bounded; a plain io.Reader (test buffer)
	// simply skips it.
	if dl, ok := r.(interface{ SetReadDeadline(t time.Time) error }); ok {
		_ = dl.SetReadDeadline(time.Now().Add(laneHeaderReadTimeout))
		defer func() { _ = dl.SetReadDeadline(time.Time{}) }()
	}
	var buf []byte
	one := make([]byte, 1)
	for {
		n, err := r.Read(one)
		if n == 1 {
			if one[0] == '\n' {
				return json.Unmarshal(buf, v)
			}
			buf = append(buf, one[0])
		}
		if err != nil {
			if err == io.EOF && len(buf) > 0 {
				return json.Unmarshal(buf, v)
			}
			return fmt.Errorf("link: lane: read header: %w", err)
		}
	}
}

// pumpBidirectional copies bytes in both directions between a and b until
// EITHER side's copy ends (EOF or error), then closes both — the lane's
// whole "中途取消/关闭" contract (§5.1④): either party closing its own end
// tears the WHOLE transfer down, never leaves the other half dangling. Not
// true independent half-close (a protocol where one direction legitimately
// outlives the other after the first EOF would need CloseWrite semantics);
// every lane data transfer this section builds is one-directional per
// request (a read moves target→requester bytes only, a write moves
// requester→target bytes only, and neither redefines mid-stream), so full
// bidirectional teardown on first EOF is the correct, simplest closure for
// day-1 — flagged in the handoff as a scope note, not a partial build.
func pumpBidirectional(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(a, b); done <- struct{}{} }()
	go func() { _, _ = io.Copy(b, a); done <- struct{}{} }()
	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}

// LocalFileOpener is the daemon-side same-machine byte-access capability
// (期11 spec §3.4's "daemon 本地颁 os.Root 子句柄") — the injection-point
// contract implemented (via a platform-layer bridge mirroring StorageHost)
// by cmd/daemon/internal/storagehost.Host, and consulted from TWO call
// sites this section wires: (1) a same-daemon caller's Local route
// (remoteResourceHandle.Redeem, dial.go) and (2) this daemon acting as a
// lane transfer's TARGET (the inbound lane-stream handler, dial.go). Both
// resolve coord via the SAME control-RPC ResolveCoord round trip first —
// this interface itself never sees a coord it did not already receive from
// home over that channel (§1.3's "daemon 无 truth": nothing here is
// derived locally).
type LocalFileOpener interface {
	OpenRead(coord string) (io.ReadSeekCloser, error)
	// OpenWrite's return type reuses accessdoor.LocalWriteHandle directly —
	// this package already imports accessdoor (relaywire.go's pre-existing
	// edge), so unlike platform/compute.go's StorageHost (whose implementor,
	// cmd/daemon/internal/storagehost, sits OUTSIDE what platform can
	// import, forcing a mirror type) there is no visibility boundary here
	// to mirror across.
	OpenWrite(coord string) (accessdoor.LocalWriteHandle, error)
	// OpenDir is the directory-shaped resource's subtree-lease redemption (期11
	// 丁12): an os.Root confined to live/<coord> behind accessdoor.
	// LocalDirHandle. Consulted only on the same-daemon Local route (a dir
	// lease never crosses the lane — resolveFileRoute rejects dir && !Local).
	OpenDir(coord string) (accessdoor.LocalDirHandle, error)
	// ReclaimCoord removes coord's already-landed local bytes (期11 S2,
	// transfer-lifecycle-spec.md §2/§3's #2's "非-land 终态回收"):
	// committingWriteHandle's Commit calls this when the home's
	// CommittedReply comes back Lost=true — this daemon's own fsync+rename
	// won LOCALLY (§3.5) but lost the same-resource_id race at the home, so
	// its bytes at coord are now orphaned and must be collected, never
	// retried. Idempotent — a coord with nothing there is a clean no-op
	// (cmd/daemon/internal/storagehost.Host.ReclaimCoord's own doc, which
	// reuses the SAME Reclaimer a tombstone's delete already collects
	// through).
	ReclaimCoord(coord string) error
}

func laneErr(reason string, args ...any) error {
	return fmt.Errorf("link: lane: "+reason, args...)
}
