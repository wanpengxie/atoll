package storagehost

import (
	"io"
	"testing"
)

func TestStreamer_WriteCommitThenRead(t *testing.T) {
	cr := newTestChannelRoot(t)
	var s Streamer

	wh, err := s.OpenWrite(cr, "coord1")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if _, err := wh.Write([]byte("hello world")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Not yet visible at the live coord — Commit is the sole visibility edge.
	if _, err := s.OpenRead(cr, "coord1"); err == nil {
		t.Fatal("coord must not be readable before Commit")
	}

	if err := wh.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rh, err := s.OpenRead(cr, "coord1")
	if err != nil {
		t.Fatalf("OpenRead after commit: %v", err)
	}
	defer rh.Close()
	got, err := io.ReadAll(rh)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("content = %q, want %q", got, "hello world")
	}
}

func TestStreamer_AbortDiscardsStaging(t *testing.T) {
	cr := newTestChannelRoot(t)
	var s Streamer

	wh, err := s.OpenWrite(cr, "coord2")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if _, err := wh.Write([]byte("abandoned")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wh.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	if _, err := s.OpenRead(cr, "coord2"); err == nil {
		t.Fatal("coord must not exist after Abort")
	}

	entries := listStagingEntries(t, cr)
	if len(entries) != 0 {
		t.Errorf("staging entries after Abort = %v, want none", entries)
	}
}

func TestStreamer_ConcurrentWritesGetIndependentStagingFiles(t *testing.T) {
	cr := newTestChannelRoot(t)
	var s Streamer

	wh1, err := s.OpenWrite(cr, "coord3")
	if err != nil {
		t.Fatalf("OpenWrite 1: %v", err)
	}
	wh2, err := s.OpenWrite(cr, "coord3")
	if err != nil {
		t.Fatalf("OpenWrite 2: %v", err)
	}
	if wh1.stagingRelPath == wh2.stagingRelPath {
		t.Fatalf("two concurrent writes to the same coord must get distinct staging paths, both = %q", wh1.stagingRelPath)
	}
	_ = wh1.Abort()
	_ = wh2.Abort()
}

func TestStreamer_RejectsBadCoord(t *testing.T) {
	cr := newTestChannelRoot(t)
	var s Streamer
	if _, err := s.OpenRead(cr, "../escape"); err == nil {
		t.Fatal("OpenRead must reject a traversal coord")
	}
	if _, err := s.OpenWrite(cr, "../escape"); err == nil {
		t.Fatal("OpenWrite must reject a traversal coord")
	}
}

func listStagingEntries(t *testing.T, cr *channelRoot) []string {
	t.Helper()
	f, err := cr.root.Open(stagingDir)
	if err != nil {
		t.Fatalf("open staging dir: %v", err)
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Fatalf("readdirnames: %v", err)
	}
	return names
}
