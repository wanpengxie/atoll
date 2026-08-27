package accessdoor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func TestCreateKVRejectsFileAddressBeforeWritingRegistry(t *testing.T) {
	reg := &fakeRegistry{}
	minter, err := New(Deps{
		Registry: reg,
		Drivers: DriverTable{
			resourcespec.KindKV: &fakeDriver{},
		},
		Authority: &fakeMembership{isMember: true},
		State:     &fakeStateStore{},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = minter.MintAuthority(accessAuthority("agent:a")).Create(
		t.Context(), "daemon://laptop-a/c0.channel-a/docs/report.txt",
		resourcespec.CreateSpec{Kind: resourcespec.KindKV}, []byte("value"),
	)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("Create error = %v, want ErrMalformed", err)
	}
	if len(reg.createCalls) != 0 {
		t.Fatalf("registry writes = %d, want 0", len(reg.createCalls))
	}
}

type listFiles struct {
	rows       []FileInfo
	daemonID   string
	pathPrefix string
	limit      int
	cursor     string
	next       string
	err        error
	created    FileInfo
}

func (f *listFiles) Create(_ context.Context, daemonID, path string, nodeType FileNodeType) error {
	f.daemonID, f.created = daemonID, FileInfo{Path: path, NodeType: nodeType}
	return nil
}
func (*listFiles) Delete(context.Context, string, string) error { return nil }
func (*listFiles) Stat(context.Context, string, string) (FileInfo, bool, error) {
	return FileInfo{}, false, nil
}

func TestCreateDirectoryUsesFileCreateWithDirectoryNode(t *testing.T) {
	files := &listFiles{}
	d := &door{deps: Deps{
		Authority: &fakeMembership{isMember: true}, ChannelID: "channel-a", ChannelName: "c0.channel-a",
		StorageMounts: directMounts{}, Files: files,
	}}
	out, err := d.create(t.Context(), actor.ActorID("agent:a"),
		"daemon://laptop-a/c0.channel-a/docs", resourcespec.CreateSpec{
			Kind: resourcespec.KindFile, NodeType: resourcespec.FileNodeDirectory,
		}, nil)
	if err != nil || !out.Accepted() {
		t.Fatalf("create directory = (%+v, %v)", out, err)
	}
	if files.daemonID != "daemon-a" || files.created.Path != "docs" || files.created.NodeType != FileNodeDirectory {
		t.Fatalf("file create = daemon %q info %+v", files.daemonID, files.created)
	}
}

func TestCreateDirectoryRejectsContent(t *testing.T) {
	err := ingressCreate("daemon://laptop-a/c0.channel-a/docs", resourcespec.CreateSpec{
		Kind: resourcespec.KindFile, NodeType: resourcespec.FileNodeDirectory, WithContent: true,
	}, nil)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("directory with content error = %v, want malformed", err)
	}
}
func (f *listFiles) List(_ context.Context, daemonID, prefix string, limit int, cursor string) ([]FileInfo, string, error) {
	f.daemonID, f.pathPrefix, f.limit, f.cursor = daemonID, prefix, limit, cursor
	return f.rows, f.next, f.err
}

// Listing takes the same route as a single address: the door serves one
// channel, so a prefix naming any other channel is refused before membership is
// even consulted. Nothing upstream re-derives the channel from the address —
// the frame says which channel the request is made in, and this is where the
// two are held to agree.
func TestFileListRejectsPrefixForAnotherChannel(t *testing.T) {
	membership := &fakeMembership{isMember: true}
	d := &door{deps: Deps{
		Authority:     membership,
		ChannelID:     "channel-a",
		ChannelName:   "c0.channel-a",
		StorageMounts: directMounts{},
		Files:         &listFiles{},
	}}
	if _, err := d.list(t.Context(), actor.ActorID("agent:a"), ListQuery{
		Prefix: "daemon://laptop-a/c0.channel-b/docs/",
	}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("cross-channel list error = %v, want ErrMalformed", err)
	}
	if membership.calls != 0 {
		t.Fatalf("membership consulted %d times before rejection", membership.calls)
	}
}

func TestFileListPassesPaginationToDiskAndReturnsNext(t *testing.T) {
	files := &listFiles{next: "next-page"}
	for i := 0; i < 7; i++ {
		files.rows = append(files.rows, FileInfo{Path: fmt.Sprintf("docs/%03d.txt", i), NodeType: FileNodeRegular})
	}
	files.rows = append(files.rows, FileInfo{Path: "docs/archive", NodeType: FileNodeDirectory})
	d := &door{deps: Deps{
		Authority:     &fakeMembership{isMember: true},
		ChannelID:     "channel-a",
		ChannelName:   "c0.channel-a",
		StorageMounts: directMounts{},
		Files:         files,
	}}

	page, err := d.list(t.Context(), actor.ActorID("agent:a"), ListQuery{
		Prefix: "daemon://laptop-a/c0.channel-a/docs/", Limit: 8, Cursor: "prior-page",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(page.Entries), 8; got != want {
		t.Fatalf("file list entries = %d, want %d", got, want)
	}
	if page.Next != "next-page" || page.Reject != "" {
		t.Fatalf("file list page = %+v", page)
	}
	if files.daemonID != "daemon-a" || files.pathPrefix != "docs/" || files.limit != 8 || files.cursor != "prior-page" {
		t.Fatalf("disk list call = (%q, %q, %d, %q)", files.daemonID, files.pathPrefix, files.limit, files.cursor)
	}
	for i, entry := range page.Entries[:7] {
		want := fmt.Sprintf("daemon://laptop-a/c0.channel-a/docs/%03d.txt", i)
		if string(entry.ID) != want || entry.Kind != resourcespec.KindFile {
			t.Fatalf("entry[%d] = %+v, want id %q kind file", i, entry, want)
		}
		if entry.NodeType != FileNodeRegular {
			t.Fatalf("entry[%d] node type = %q", i, entry.NodeType)
		}
		if len(entry.Ops) != 3 || entry.Ops[0] != access.OpRead {
			t.Fatalf("entry[%d] ops = %v", i, entry.Ops)
		}
	}
	directory := page.Entries[7]
	if directory.NodeType != FileNodeDirectory || len(directory.Ops) != 1 || directory.Ops[0] != access.OpDelete {
		t.Fatalf("directory entry ops = %+v", directory)
	}
}

func TestFileListMapsMalformedDeviceCursor(t *testing.T) {
	files := &listFiles{err: ErrMalformedFileCursor}
	d := &door{deps: Deps{
		Authority: &fakeMembership{isMember: true}, ChannelID: "channel-a", ChannelName: "c0.channel-a",
		StorageMounts: directMounts{}, Files: files,
	}}
	page, err := d.list(t.Context(), actor.ActorID("agent:a"), ListQuery{
		Prefix: "daemon://laptop-a/c0.channel-a/docs/", Cursor: "bad",
	})
	if err != nil || page.Reject != QueryBadCursor {
		t.Fatalf("file list = (%+v, %v), want bad_cursor", page, err)
	}
}
