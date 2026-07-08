package storagehost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAssertPathSegment(t *testing.T) {
	ok := []string{"a", "abc123", "A-b_c", "0123456789abcdef", "channel-1"}
	for _, v := range ok {
		if err := assertPathSegment("test", v); err != nil {
			t.Errorf("assertPathSegment(%q) = %v, want nil", v, err)
		}
	}
	bad := []string{"", ".", "..", "a/b", "a/../b", "/etc/passwd", "a.b", "a b", "a\x00b"}
	for _, v := range bad {
		if err := assertPathSegment("test", v); err == nil {
			t.Errorf("assertPathSegment(%q) = nil, want an error", v)
		}
	}
}

func TestOpenChannelRootRejectsBadChannelID(t *testing.T) {
	if _, err := openChannelRoot(t.TempDir(), "../escape"); err == nil {
		t.Fatal("expected an error for a path-traversal channelID")
	}
}

func TestOpenChannelRootCreatesSiblingLayout(t *testing.T) {
	root := t.TempDir()
	cr, err := openChannelRoot(root, "ch1")
	if err != nil {
		t.Fatalf("openChannelRoot: %v", err)
	}
	defer cr.Close()

	// live/ and staging/ both created under resources/<channelID>/ — a
	// SIBLING of <root>/<channelID>/ (the device workspace tree), never
	// nested under it (§4.2).
	for _, want := range []string{"resources/ch1/live", "resources/ch1/staging"} {
		fi, err := os.Stat(filepath.Join(root, want))
		if err != nil || !fi.IsDir() {
			t.Errorf("expected directory %q to exist: err=%v", want, err)
		}
	}
}

func TestLivePathAndStagingPathRejectBadCoord(t *testing.T) {
	if _, err := livePath("../escape"); err == nil {
		t.Fatal("livePath must reject a traversal coord")
	}
	if _, err := stagingPath("../escape", "suffix"); err == nil {
		t.Fatal("stagingPath must reject a traversal coord")
	}
	if _, err := stagingPath("coord", "../escape"); err == nil {
		t.Fatal("stagingPath must reject a traversal suffix")
	}
}
