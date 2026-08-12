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
		t.Context(), "daemon://laptop-a/docs/report.txt",
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
}

func (*listFiles) Create(context.Context, string, string) error { return nil }
func (*listFiles) Delete(context.Context, string, string) error { return nil }
func (*listFiles) Stat(context.Context, string, string) (FileInfo, bool, error) {
	return FileInfo{}, false, nil
}
func (f *listFiles) List(_ context.Context, daemonID, prefix string) ([]FileInfo, error) {
	f.daemonID, f.pathPrefix = daemonID, prefix
	return f.rows, nil
}

func TestFileListReturnsEveryDiskEntryBeyondDefaultLimit(t *testing.T) {
	files := &listFiles{}
	for i := 0; i < defaultListLimit+7; i++ {
		files.rows = append(files.rows, FileInfo{Path: fmt.Sprintf("docs/%03d.txt", i)})
	}
	d := &door{deps: Deps{
		Authority:     &fakeMembership{isMember: true},
		ChannelID:     "channel-a",
		StorageMounts: directMounts{},
		Files:         files,
	}}

	page, err := d.list(t.Context(), actor.ActorID("agent:a"), ListQuery{
		Prefix: "daemon://laptop-a/docs/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(page.Entries), len(files.rows); got != want {
		t.Fatalf("file list entries = %d, want all %d disk entries", got, want)
	}
	if page.Next != "" || page.Reject != "" {
		t.Fatalf("file list page = %+v, want complete unpaged result", page)
	}
	if files.daemonID != "daemon-a" || files.pathPrefix != "docs/" {
		t.Fatalf("disk list call = (%q, %q)", files.daemonID, files.pathPrefix)
	}
	for i, entry := range page.Entries {
		want := fmt.Sprintf("daemon://laptop-a/docs/%03d.txt", i)
		if string(entry.ID) != want || entry.Kind != resourcespec.KindFile {
			t.Fatalf("entry[%d] = %+v, want id %q kind file", i, entry, want)
		}
		if len(entry.Ops) != 3 || entry.Ops[0] != access.OpRead {
			t.Fatalf("entry[%d] ops = %v", i, entry.Ops)
		}
	}
}
