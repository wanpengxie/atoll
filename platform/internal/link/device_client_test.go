package link

import (
	"bytes"
	"io"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type localFiles struct {
	opened string
	data   []byte
}

func (f *localFiles) OpenRead(path string) (io.ReadSeekCloser, error) {
	f.opened = path
	return &readSeekCloser{Reader: bytes.NewReader(f.data)}, nil
}
func (f *localFiles) OpenWrite(path string) (accessdoor.WriteHandle, error) {
	f.opened = path
	return &memoryWrite{}, nil
}

type readSeekCloser struct{ *bytes.Reader }

func (*readSeekCloser) Close() error { return nil }

type memoryWrite struct{ bytes.Buffer }

func (*memoryWrite) Commit() error { return nil }
func (*memoryWrite) Abort() error  { return nil }

func TestLocalRedemptionOpensLogicalPathWithoutTicket(t *testing.T) {
	files := &localFiles{data: []byte("ok")}
	r := &deviceFileRedeemer{files: files}
	accessed, err := r.redeemFileRoute(t.Context(), accessdoor.FileRoute{Path: "docs/a.txt", Mode: access.OpRead, Redeem: accessdoor.FileRedeemLocal})
	if err != nil {
		t.Fatal(err)
	}
	reader, ok := accessed.Reader()
	if !ok {
		t.Fatal("missing reader")
	}
	defer reader.Close()
	got, _ := io.ReadAll(reader)
	if string(got) != "ok" || files.opened != "docs/a.txt" {
		t.Fatalf("opened=%q bytes=%q", files.opened, got)
	}
}
