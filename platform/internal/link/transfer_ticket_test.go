package link

// File byte-route ticket tests: one retryable capability, resolved by the
// daemon that owns the bytes, walked over a real authenticated connection.
// Byte access is same-daemon only, so a ticket never travels between two
// daemons and the home never stands between anyone's bytes.

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

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

// The read direction end to end: the daemon resolves its ticket over the real
// control table and opens the handle locally. The bytes never cross a wire.
func TestFileRouteResolvesLocallyAndReadsBytes(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{
		daemonID: func(req *http.Request) string { return req.URL.Query().Get("daemon") },
	})
	daemon := dialLaneTestDaemon(t, rig, "local-daemon", &staticLaneOpener{
		coord: "coord-local", data: []byte("local-bytes"),
	})
	ticket, err := rig.acc.OpenTransfer(
		t.Context(), "local-daemon", "coord-local", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	file, err := daemon.redeemFileRoute(t.Context(), accessdoor.FileRoute{
		Token: ticket, Mode: access.OpRead,
	})
	if err != nil {
		t.Fatal(err)
	}
	if file.Local == nil || file.Local.Read == nil {
		t.Fatalf("redemption returned wrong access shape: %+v", file)
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

// The ticket is read-only until it expires: a target whose reply was lost can
// ask again and get the same answer. Nothing consumes it, so nothing can make
// a retry look like a replay.
func TestResolveTicketSurvivesDroppedReplyOverControlWire(t *testing.T) {
	rig := newAcceptorRig(t, acceptorRigConfig{})
	daemon := dialRawDaemon(t, rig.wsURL(), true)
	ticket, err := rig.acc.OpenTransfer(
		t.Context(), "daemon-1", "coord-retry", access.OpRead, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	sendResolve := func(requestID string) ResolveCoordReply {
		t.Helper()
		raw, encodeErr := encodeLaneControl(laneControlFrame{
			Kind: ctrlResolveCoord,
			ResolveCoord: &ResolveCoordRequest{
				RequestID: requestID, Token: ticket,
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
	// Treat the first reply as lost at the target and retry the same ticket
	// through the control table with a fresh correlation id.
	second := sendResolve("resolve-retry")
	if !second.OK || second.Coord != first.Coord || second.Mode != first.Mode {
		t.Fatalf("retry=%+v first=%+v", second, first)
	}
}
