package link

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type testDeviceLaneControl struct {
	resolveReply ResolveCoordReply
	resolveErr   error
	commitReply  CommittedReply
	commitErr    error
	resolveToken string
	committedID  string
	commits      int
}

func (c *testDeviceLaneControl) ResolveCoord(_ context.Context, token string) (ResolveCoordReply, error) {
	c.resolveToken = token
	return c.resolveReply, c.resolveErr
}

func (c *testDeviceLaneControl) SendCommitted(_ context.Context, id, ticket string) (CommittedReply, error) {
	c.commits++
	c.committedID = id
	return c.commitReply, c.commitErr
}

type testWriteHandle struct {
	commitErr error
	commits   int
}

func (*testWriteHandle) Write(p []byte) (int, error) { return len(p), nil }
func (h *testWriteHandle) Commit() error {
	h.commits++
	return h.commitErr
}
func (*testWriteHandle) Abort() error { return nil }

type testReadSeekCloser struct{ *strings.Reader }

func (*testReadSeekCloser) Close() error { return nil }

type testLocalDir struct{}

func (*testLocalDir) Create(string) (*os.File, error)                     { return nil, nil }
func (*testLocalDir) Open(string) (*os.File, error)                       { return nil, nil }
func (*testLocalDir) OpenFile(string, int, os.FileMode) (*os.File, error) { return nil, nil }
func (*testLocalDir) Mkdir(string, os.FileMode) error                     { return nil }
func (*testLocalDir) MkdirAll(string, os.FileMode) error                  { return nil }
func (*testLocalDir) Remove(string) error                                 { return nil }
func (*testLocalDir) RemoveAll(string) error                              { return nil }
func (*testLocalDir) Stat(string) (os.FileInfo, error)                    { return nil, nil }
func (*testLocalDir) ReadFile(string) ([]byte, error)                     { return nil, nil }
func (*testLocalDir) WriteFile(string, []byte, os.FileMode) error         { return nil }
func (*testLocalDir) Close() error                                        { return nil }

type testLocalFileOpener struct {
	read      io.ReadSeekCloser
	write     accessdoor.WriteHandle
	dir       accessdoor.LocalDirHandle
	openErr   error
	opened    string
	openKind  string
	reclaimed []string
}

func (o *testLocalFileOpener) OpenRead(coord string) (io.ReadSeekCloser, error) {
	o.opened, o.openKind = coord, "read"
	return o.read, o.openErr
}
func (o *testLocalFileOpener) OpenWrite(coord string) (accessdoor.WriteHandle, error) {
	o.opened, o.openKind = coord, "write"
	return o.write, o.openErr
}
func (o *testLocalFileOpener) OpenDir(coord string) (accessdoor.LocalDirHandle, error) {
	o.opened, o.openKind = coord, "dir"
	return o.dir, o.openErr
}
func (o *testLocalFileOpener) ReclaimCoord(coord string) error {
	o.reclaimed = append(o.reclaimed, coord)
	return nil
}

func TestDeviceCommittingWritePreservesCommitOutcomeSemantics(t *testing.T) {
	localFailure := errors.New("local commit failed")
	transportFailure := errors.New("lane broke")
	homeFailure := errors.New("reservation rejected")
	tests := []struct {
		name          string
		localErr      error
		reply         CommittedReply
		sendErr       error
		wantErr       error
		wantContains  string
		wantSend      bool
		wantReclaimed bool
	}{
		{name: "local failure sends nothing", localErr: localFailure, wantErr: localFailure},
		{
			name: "transport failure is outcome unknown", sendErr: transportFailure,
			wantContains: "outcome unknown", wantSend: true,
		},
		{name: "found succeeds", reply: CommittedReply{Found: true}, wantSend: true},
		{
			name: "not found fails honestly", reply: CommittedReply{},
			wantContains: "completion identity not found", wantSend: true,
		},
		{
			name:  "home rejection fails without reclaim",
			reply: CommittedReply{Reason: homeFailure.Error()}, wantContains: homeFailure.Error(),
			wantSend: true,
		},
		{
			name:  "lost fails and reclaims exact coord",
			reply: CommittedReply{Found: true, Lost: true}, wantContains: "reservation lost",
			wantSend: true, wantReclaimed: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local := &testWriteHandle{commitErr: test.localErr}
			control := &testDeviceLaneControl{commitReply: test.reply, commitErr: test.sendErr}
			files := &testLocalFileOpener{}
			handle := &deviceCommittingWrite{
				WriteHandle: local, control: control, files: files,
				reservationID: "reservation-a", coord: "coord-a",
			}

			err := handle.Commit()
			switch {
			case test.wantErr != nil && !errors.Is(err, test.wantErr):
				t.Fatalf("Commit error = %v, want %v", err, test.wantErr)
			case test.wantContains != "" && (err == nil || !strings.Contains(err.Error(), test.wantContains)):
				t.Fatalf("Commit error = %v, want substring %q", err, test.wantContains)
			case test.wantErr == nil && test.wantContains == "" && err != nil:
				t.Fatalf("Commit error = %v, want success", err)
			}
			if got := control.commits; got != boolInt(test.wantSend) {
				t.Fatalf("SendCommitted calls = %d, want %d", got, boolInt(test.wantSend))
			}
			if test.wantSend && control.committedID != "reservation-a" {
				t.Fatalf("committed reservation = %q", control.committedID)
			}
			if test.wantReclaimed {
				if len(files.reclaimed) != 1 || files.reclaimed[0] != "coord-a" {
					t.Fatalf("reclaimed coords = %v", files.reclaimed)
				}
			} else if len(files.reclaimed) != 0 {
				t.Fatalf("unexpected reclaim = %v", files.reclaimed)
			}
		})
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestDeviceFileRedeemerRoutesOnlyAfterAuthorizedResolution(t *testing.T) {
	t.Run("both capabilities required", func(t *testing.T) {
		for _, redeemer := range []*deviceFileRedeemer{
			{files: &testLocalFileOpener{}},
			{control: &testDeviceLaneControl{}},
		} {
			if _, err := redeemer.redeemFileRoute(
				t.Context(), accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "token-a", Mode: access.OpRead},
			); err == nil {
				t.Fatal("redeemer accepted a missing capability")
			}
		}
	})

	t.Run("resolve rejection never opens local bytes", func(t *testing.T) {
		control := &testDeviceLaneControl{
			resolveReply: ResolveCoordReply{Reason: "unauthorized"},
		}
		files := &testLocalFileOpener{}
		_, err := (&deviceFileRedeemer{control: control, files: files}).redeemFileRoute(
			t.Context(), accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "token-a", Mode: access.OpRead},
		)
		if err == nil || files.openKind != "" {
			t.Fatalf("rejected route error=%v local open=%q", err, files.openKind)
		}
	})

	read := &testReadSeekCloser{Reader: strings.NewReader("bytes")}
	write := &testWriteHandle{}
	dir := &testLocalDir{}
	for _, test := range []struct {
		name       string
		route      accessdoor.FileRoute
		reply      ResolveCoordReply
		wantKind   string
		wantCommit bool
	}{
		{
			name: "read", route: accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "read-token", Mode: access.OpRead},
			reply:    ResolveCoordReply{OK: true, Coord: "read-coord", Mode: access.OpRead},
			wantKind: "read",
		},
		{
			name: "plain write", route: accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "write-token", Mode: access.OpWrite},
			reply:    ResolveCoordReply{OK: true, Coord: "write-coord", Mode: access.OpWrite},
			wantKind: "write",
		},
		{
			name:  "committing write",
			route: accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "create-token", Mode: access.OpWrite},
			reply: ResolveCoordReply{
				OK: true, Coord: "create-coord", Mode: access.OpWrite,
				ReservationID: "reservation-a",
			},
			wantKind: "write", wantCommit: true,
		},
		{
			name: "directory", route: accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "dir-token", Mode: access.OpWrite, Dir: true},
			reply:    ResolveCoordReply{OK: true, Coord: "dir-coord", Mode: access.OpWrite, Dir: true},
			wantKind: "dir",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			control := &testDeviceLaneControl{resolveReply: test.reply}
			files := &testLocalFileOpener{read: read, write: write, dir: dir}
			access, err := (&deviceFileRedeemer{control: control, files: files}).redeemFileRoute(
				t.Context(), test.route,
			)
			if err != nil {
				t.Fatal(err)
			}
			if control.resolveToken != test.route.Token || files.opened != test.reply.Coord ||
				files.openKind != test.wantKind {
				t.Fatalf("resolve=%q open=(%q,%q)", control.resolveToken, files.openKind, files.opened)
			}
			if access.Local == nil {
				t.Fatal("successful redemption returned no local access")
			}
			_, committing := access.Local.Write.(*deviceCommittingWrite)
			if committing != test.wantCommit {
				t.Fatalf("committing wrapper = %v, want %v", committing, test.wantCommit)
			}
		})
	}

	t.Run("ledger reply mode overrides the route display field", func(t *testing.T) {
		control := &testDeviceLaneControl{
			resolveReply: ResolveCoordReply{OK: true, Coord: "coord-a", Mode: access.OpRead},
		}
		files := &testLocalFileOpener{}
		fa, err := (&deviceFileRedeemer{control: control, files: files}).redeemFileRoute(
			t.Context(), accessdoor.FileRoute{Redeem: accessdoor.FileRedeemLocal, Token: "token-a", Mode: access.Operation("execute")},
		)
		if err != nil || fa.Local == nil || files.openKind != "read" {
			t.Fatalf("ledger-authoritative read = %+v kind=%q, %v", fa, files.openKind, err)
		}
	})
}
