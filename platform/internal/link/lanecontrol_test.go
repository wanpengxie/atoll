package link

// Lane transfer ticket tests: the consume-once redeem capability and the
// retryable resolve capability, including their real local/cross-host walks.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// The redeem ticket is single-use: consumed at its first valid redemption, so
// a replay within the TTL finds nothing. The paired resolve ticket remains.
func TestLaneRedeemTicketIsSingleUse(t *testing.T) {
	acc := &Acceptor{lane: newLaneState(), sessions: newSessionRegistry(nil), ctx: context.Background()}
	tickets, err := acc.OpenLaneTransfer(
		context.Background(), "target-daemon", "req-daemon", "coord-1", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if tickets.Redeem == tickets.Resolve {
		t.Fatal("redeem and resolve tickets collapsed to one capability")
	}
	// A wrong holder is rejected without consuming the legitimate requester's
	// redeem ticket.
	home, wrong := net.Pipe()
	go acc.handleLaneRedeem("wrong-daemon", home)
	if err := writeLaneJSON(wrong, laneRedeemHeader{Token: tickets.Redeem}); err != nil {
		t.Fatal(err)
	}
	var wrongAck laneAck
	if err := readLaneJSON(wrong, &wrongAck); err != nil {
		t.Fatal(err)
	}
	_ = wrong.Close()
	if wrongAck.OK {
		t.Fatal("wrong holder redeemed ticket")
	}
	redeem := func() laneAck {
		home, daemon := net.Pipe()
		defer daemon.Close()
		go acc.handleLaneRedeem("req-daemon", home)
		if err := writeLaneJSON(daemon, laneRedeemHeader{Token: tickets.Redeem}); err != nil {
			t.Fatal(err)
		}
		var ack laneAck
		if err := readLaneJSON(daemon, &ack); err != nil {
			t.Fatal(err)
		}
		return ack
	}
	first := redeem()
	if first.OK || !strings.Contains(first.Reason, "no live link") {
		t.Fatalf("first redemption ack=%+v want target-unreachable", first)
	}
	second := redeem()
	if second.OK || !strings.Contains(second.Reason, "unknown or mismatched") {
		t.Fatalf("replayed ticket was honored: ack=%+v", second)
	}
	fresh, err := acc.OpenLaneTransfer(
		context.Background(), "target-daemon", "req-daemon", "coord-1", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Redeem == tickets.Redeem || fresh.Resolve == tickets.Resolve {
		t.Fatal("failed redemption was rolled back instead of requiring fresh tickets")
	}
}

type staticReadHandle struct{ *bytes.Reader }

func (*staticReadHandle) Close() error { return nil }

type staticLaneOpener struct {
	coord string
	data  []byte
}

func (o *staticLaneOpener) OpenRead(coord string) (io.ReadSeekCloser, error) {
	if coord != o.coord {
		return nil, errors.New("unexpected coord " + coord)
	}
	return &staticReadHandle{Reader: bytes.NewReader(o.data)}, nil
}

func (*staticLaneOpener) OpenWrite(string) (accessdoor.LocalWriteHandle, error) {
	return nil, errors.New("write unexercised")
}

func (*staticLaneOpener) OpenDir(string) (accessdoor.LocalDirHandle, error) {
	return nil, errors.New("directory unexercised")
}

func (*staticLaneOpener) ReclaimCoord(string) error { return nil }

func dialLaneTestDaemon(t *testing.T, rig *acceptorRig, daemonID string, opener LocalFileOpener) *Dialer {
	t.Helper()
	dialer, err := Dial(
		t.Context(), rig.wsURL()+"?daemon="+daemonID,
		DialConfig{
			SessionLedger:   NewRemoteSessionLedger(nil),
			LocalFileOpener: opener,
		},
		nil,
	)
	if err != nil {
		t.Fatalf("dial %s: %v", daemonID, err)
	}
	t.Cleanup(func() { _ = dialer.Close() })
	return dialer
}

// The cross-host topology is requester(redeem ticket) -> home -> target(resolve
// ticket). Both peers below are real Dialers on distinct authenticated
// connections; the target resolves over the real control table before bytes
// make a complete trip back to the requester.
func TestLaneTicketsCrossHostFullRoundTrip(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		daemonID: func(req *http.Request) string { return req.URL.Query().Get("daemon") },
	})
	dialLaneTestDaemon(t, rig, "target-daemon", &staticLaneOpener{
		coord: "coord-cross", data: []byte("cross-host-bytes"),
	})
	requester := dialLaneTestDaemon(t, rig, "requester-daemon", nil)
	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "target-daemon", "requester-daemon",
		"coord-cross", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := requester.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Local: false, Token: tickets.Redeem, Mode: access.OpRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	if file.Stream == nil {
		t.Fatal("cross-host redemption did not return a byte stream")
	}
	got, err := io.ReadAll(file.Stream)
	_ = file.Stream.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "cross-host-bytes" {
		t.Fatalf("payload = %q", got)
	}
}

func TestLaneTicketsLocalRouteResolvesDirectly(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		daemonID: func(req *http.Request) string { return req.URL.Query().Get("daemon") },
	})
	daemon := dialLaneTestDaemon(t, rig, "local-daemon", &staticLaneOpener{
		coord: "coord-local", data: []byte("local-bytes"),
	})
	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "local-daemon", "local-daemon",
		"coord-local", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := daemon.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Local: true, Token: tickets.Resolve, Mode: access.OpRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	if file.Local == nil || file.Local.Read == nil || file.Stream != nil {
		t.Fatalf("local redemption returned wrong access shape: %+v", file)
	}
	got, err := io.ReadAll(file.Local.Read)
	_ = file.Local.Read.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "local-bytes" {
		t.Fatalf("payload = %q", got)
	}
}

func TestResolveTicketSurvivesDroppedReplyOverControlWire(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	tickets, err := rig.acc.OpenLaneTransfer(
		t.Context(), "daemon-1", "requester-daemon",
		"coord-retry", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	sendResolve := func(requestID string) ResolveCoordReply {
		t.Helper()
		raw, encodeErr := encodeLaneControl(laneControlFrame{
			Kind: ctrlResolveCoord,
			ResolveCoord: &ResolveCoordRequest{
				RequestID: requestID, Token: tickets.Resolve,
			},
		})
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		daemon.send(raw)
		return daemon.waitResolveReply()
	}
	first := sendResolve("resolve-dropped")
	if !first.OK {
		t.Fatalf("first resolve rejected: %s", first.Reason)
	}
	// Treat the first reply as lost at the target and retry the same resolve
	// capability through the control table with a fresh correlation id.
	second := sendResolve("resolve-retry")
	if !second.OK || second.Coord != first.Coord || second.Mode != first.Mode {
		t.Fatalf("retry=%+v first=%+v", second, first)
	}
}

func TestConcurrentRedeemOpensOnlyOneTargetChannel(t *testing.T) {
	acc := &Acceptor{lane: newLaneState(), sessions: newSessionRegistry(nil), ctx: context.Background()}
	tickets, err := acc.OpenLaneTransfer(
		context.Background(), "target-daemon", "requester-daemon",
		"coord-cross", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	record, err := acc.sessions.mint("target-daemon")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acc.sessions.activate(record); err != nil {
		t.Fatal(err)
	}
	var opened atomic.Int64
	record.setHandle(&linkHandle{openLane: func(context.Context) (net.Conn, error) {
		opened.Add(1)
		homeSide, targetSide := net.Pipe()
		go func() {
			defer targetSide.Close()
			var header laneRedeemHeader
			_ = readLaneJSON(targetSide, &header)
			_ = writeLaneJSON(targetSide, laneAck{Reason: "test stop"})
		}()
		return homeSide, nil
	}})

	start := make(chan struct{})
	acks := make(chan laneAck, 2)
	var group sync.WaitGroup
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			homeSide, requesterSide := net.Pipe()
			defer requesterSide.Close()
			go acc.handleLaneRedeem("requester-daemon", homeSide)
			<-start
			_ = writeLaneJSON(requesterSide, laneRedeemHeader{Token: tickets.Redeem})
			var ack laneAck
			_ = readLaneJSON(requesterSide, &ack)
			acks <- ack
		}()
	}
	close(start)
	group.Wait()
	close(acks)
	if opened.Load() != 1 {
		t.Fatalf("target channels opened = %d, want exactly 1", opened.Load())
	}
}
