package home

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type mountRoutes struct{}

func (mountRoutes) PokePlan(string, string) {}
func (mountRoutes) FileCreate(context.Context, string, string, string, accessdoor.FileNodeType) error {
	return nil
}
func (mountRoutes) FileDelete(context.Context, string, string, string) error { return nil }
func (mountRoutes) FileStat(context.Context, string, string, string) (platform.DaemonFileInfo, bool, error) {
	return platform.DaemonFileInfo{}, false, nil
}
func (mountRoutes) FileList(context.Context, string, string, string, int, string) ([]platform.DaemonFileInfo, string, error) {
	return nil, "", nil
}
func (mountRoutes) AttachedDaemons(string) []string  { return nil }
func (mountRoutes) LaneAttached(string, string) bool { return true }
func (mountRoutes) LaneWorkspace(context.Context, string, string) (string, bool, error) {
	return "/workspace", true, nil
}

type mountBindings struct{}

func (mountBindings) IsBound(context.Context, channel.ID, string) (bool, error) { return true, nil }
func (mountBindings) ListBoundDeviceIDs(context.Context, channel.ID) ([]string, error) {
	return []string{"retired-id", "live-id"}, nil
}
func (mountBindings) ChannelDesired(context.Context, channel.ID) (channelspec.ChannelDesiredFacts, bool, error) {
	return channelspec.ChannelDesiredFacts{}, false, nil
}

type mountDirectory struct{}

func (mountDirectory) ResolveDeviceID(_ context.Context, id string) (string, bool, bool, error) {
	switch id {
	case "retired-id":
		return "old-device", false, true, nil
	case "live-id":
		return "retired-id", true, true, nil
	default:
		return "", false, false, nil
	}
}
func (mountDirectory) ResolveDeviceName(_ context.Context, name string) (string, bool, bool, error) {
	if name == "retired-id" || name == "legacy-name" {
		return "live-id", true, true, nil
	}
	return "", false, false, nil
}

func TestCanonicalRetiredDeviceIDNeverFallsThroughToLegacyNameLookup(t *testing.T) {
	mounts := daemonStorageMounts{routes: mountRoutes{}, bindings: mountBindings{}, directory: mountDirectory{}, chID: "c0"}
	if _, found, err := mounts.ResolveStorageDaemon(t.Context(), "c0", "retired-id"); err != nil || found {
		t.Fatalf("retired canonical id resolved through another device name: found=%v err=%v", found, err)
	}
	legacy, found, err := mounts.ResolveStorageDaemon(t.Context(), "c0", "legacy-name")
	if err != nil || !found || legacy.DaemonID != "live-id" {
		t.Fatalf("legacy display name compatibility mount=%+v found=%v err=%v", legacy, found, err)
	}
}

// fileHost stands in for the machine at the far end of a lane: it speaks the
// exchange protocol, so what this exercises is the real framing and the real
// stream, not a stub that agrees with the caller.
type fileHost struct {
	online  bool
	content string
	written chan string
}

func (h *fileHost) Online(string, channel.ID) bool { return h.online }

// OpenHost hands back a stream whose host-leg header is already written, which
// is the contract the real daemon host follows.
func (h *fileHost) OpenHost(_ context.Context, ticket dataplane.Ticket) (io.ReadWriteCloser, error) {
	near, far := net.Pipe()
	go h.serve(far, ticket.Mode)
	if err := link.WriteExchangeControl(near, link.ExchangeHostHeader{Path: ticket.Path, Mode: ticket.Mode}); err != nil {
		_ = near.Close()
		return nil, err
	}
	return near, nil
}

func (h *fileHost) serve(conn io.ReadWriteCloser, mode access.Operation) {
	defer conn.Close()
	var head link.ExchangeHostHeader
	if err := link.ReadExchangeControl(conn, &head); err != nil {
		return
	}
	if mode == access.OpRead {
		if err := link.WriteExchangeBytes(conn, io.NopCloser(newStringReader(h.content))); err != nil {
			return
		}
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
		return
	}
	var got stringWriter
	if err := link.ReadExchangeBytes(&got, conn); err != nil {
		return
	}
	h.written <- got.String()
	_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
}

func openPlane(t *testing.T, host *fileHost) (dataplane.Issuer, dataplane.Redeemer) {
	t.Helper()
	issue, redeem, bind, closePlane := dataplane.New()
	if err := bind.BindHostStreamOpener(host); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bind.UnbindHostStreamOpener()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = closePlane(ctx)
	})
	return issue, redeem
}

func issueFor(t *testing.T, issue dataplane.Issuer, mode access.Operation) string {
	t.Helper()
	grant, err := issue.Issue(t.Context(), dataplane.IssueSpec{
		Address: "daemon://host/c0/docs/a.txt", Path: "docs/a.txt",
		ChannelID: "c0", Mode: mode, HostID: "daemon-a", HostName: "host",
		Caller: "agent:steward:9",
	})
	if err != nil {
		t.Fatal(err)
	}
	return grant.Ticket
}

// An actor living in this process can move bytes both ways. The server has
// always been the side that opens these streams — for browsers and between
// machines — but had no way to keep one for an actor of its own, so a
// server-resident actor could be granted a file route and then get
// capability_unavailable when it tried to act on it.
func TestAnActorInThisProcessMovesBytesBothWays(t *testing.T) {
	host := &fileHost{online: true, content: "bytes from the machine", written: make(chan string, 1)}
	issue, redeem := openPlane(t, host)
	seam := daemonTransferRedeem{redeemer: redeem, chID: "c0"}

	read, err := seam.RedeemTransfer(t.Context(), "agent:steward:9", accessdoor.FileRoute{
		Token: issueFor(t, issue, access.OpRead), Mode: access.OpRead, Redeem: accessdoor.FileRedeemRemote,
	})
	if err != nil {
		t.Fatalf("redeem read: %v", err)
	}
	reader, ok := read.Reader()
	if !ok {
		t.Fatal("read redemption produced no reader")
	}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != host.content {
		t.Fatalf("read = (%q, %v)", got, err)
	}
	_ = reader.Close()

	write, err := seam.RedeemTransfer(t.Context(), "agent:steward:9", accessdoor.FileRoute{
		Token: issueFor(t, issue, access.OpWrite), Mode: access.OpWrite, Redeem: accessdoor.FileRedeemRemote,
	})
	if err != nil {
		t.Fatalf("redeem write: %v", err)
	}
	writer, ok := write.Writer()
	if !ok {
		t.Fatal("write redemption produced no writer")
	}
	if _, err := writer.Write([]byte("bytes from the server")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	select {
	case landed := <-host.written:
		if landed != "bytes from the server" {
			t.Fatalf("machine received %q", landed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bytes never reached the machine")
	}
}

// The redemption is scoped like every other: an actor this ticket was not
// issued to gets nothing, even though it is asking from inside the process.
func TestInProcessRedemptionIsScopedToItsActor(t *testing.T) {
	host := &fileHost{online: true, content: "x", written: make(chan string, 1)}
	issue, redeem := openPlane(t, host)
	seam := daemonTransferRedeem{redeemer: redeem, chID: "c0"}
	ticket := issueFor(t, issue, access.OpRead)

	if _, err := seam.RedeemTransfer(t.Context(), "agent:other:1", accessdoor.FileRoute{
		Token: ticket, Mode: access.OpRead, Redeem: accessdoor.FileRedeemRemote,
	}); !errors.Is(err, dataplane.ErrInvalidTicket) {
		t.Fatalf("another actor err=%v, want ErrInvalidTicket", err)
	}

	// A local route says the caller's own machine holds the bytes. Nothing here
	// is such a machine, so honouring one would mean reading a path off some
	// disk this process does not own.
	if _, err := seam.RedeemTransfer(t.Context(), "agent:steward:9", accessdoor.FileRoute{
		Path: "docs/a.txt", Mode: access.OpRead, Redeem: accessdoor.FileRedeemLocal,
	}); err == nil {
		t.Fatal("a local route was honoured by a process that owns no channel disk")
	}
}

type stringWriter struct{ b []byte }

func (w *stringWriter) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }
func (w *stringWriter) String() string              { return string(w.b) }

func newStringReader(s string) io.Reader { return &stringReader{s: s} }

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
