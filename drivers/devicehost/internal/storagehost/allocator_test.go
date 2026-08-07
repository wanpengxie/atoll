package storagehost

import (
	"io"
	"os"
	"testing"
)

func newTestChannelRoot(t *testing.T) *channelRoot {
	t.Helper()
	cr, err := openChannelRoot(t.TempDir(), "ch1")
	if err != nil {
		t.Fatalf("openChannelRoot: %v", err)
	}
	t.Cleanup(func() { _ = cr.Close() })
	return cr
}

func TestAllocator_TouchCreatesEmptyFile(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "coord1", false); err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	p, _ := livePath("coord1")
	fi, err := cr.root.Stat(p)
	if err != nil {
		t.Fatalf("Stat after Alloc: %v", err)
	}
	if fi.IsDir() {
		t.Fatal("expected a regular file, got a directory")
	}
	if fi.Size() != 0 {
		t.Errorf("expected an empty file, size=%d", fi.Size())
	}
}

func TestAllocator_MkdirCreatesDirectory(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "coord2", true); err != nil {
		t.Fatalf("Alloc: %v", err)
	}
	p, _ := livePath("coord2")
	fi, err := cr.root.Stat(p)
	if err != nil {
		t.Fatalf("Stat after Alloc: %v", err)
	}
	if !fi.IsDir() {
		t.Fatal("expected a directory")
	}
}

func TestAllocator_IdempotentReplay(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "coord3", false); err != nil {
		t.Fatalf("first Alloc: %v", err)
	}
	// Write some content, then replay Alloc — a repeat AllocRequest must not
	// truncate content that already landed (§4.7's "mkdir 已存在=成功"
	// extended to touch's idempotency).
	p, _ := livePath("coord3")
	f, err := cr.root.OpenFile(p, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	if err := a.Alloc(cr, "coord3", false); err != nil {
		t.Fatalf("replay Alloc: %v", err)
	}
	rf, err := cr.root.Open(p)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer rf.Close()
	got, err := io.ReadAll(rf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("content after replay = %q, want %q (replay must not truncate)", got, "hello")
	}

	// A dir replay over an existing directory must also be a clean no-op.
	if err := a.Alloc(cr, "coord2-dir", true); err != nil {
		t.Fatalf("first mkdir: %v", err)
	}
	if err := a.Alloc(cr, "coord2-dir", true); err != nil {
		t.Fatalf("replay mkdir: %v", err)
	}
}

func TestAllocator_RejectsBadCoord(t *testing.T) {
	cr := newTestChannelRoot(t)
	var a Allocator
	if err := a.Alloc(cr, "../escape", false); err == nil {
		t.Fatal("expected an error for a path-traversal coord")
	}
}
