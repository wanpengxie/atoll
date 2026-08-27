package storagehost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostUsesChannelDirectoryDirectly(t *testing.T) {
	root := t.TempDir()
	h, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	w, err := h.OpenWrite("docs/report.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("from api")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "docs", "report.txt"))
	if err != nil || string(got) != "from api" {
		t.Fatalf("disk=%q err=%v", got, err)
	}
	if err := os.WriteFile(filepath.Join(root, "reverse.txt"), []byte("from bash"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := h.OpenRead("reverse.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, 9)
	n, err := r.Read(buf)
	if err != nil || string(buf[:n]) != "from bash" {
		t.Fatalf("api=%q err=%v", buf[:n], err)
	}
}

func TestHostReportsPhysicalNodeType(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "note.txt"), []byte("note"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	rows, next, err := h.List("", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if next != "" {
		t.Fatalf("unexpected next cursor %q", next)
	}
	got := make(map[string]NodeType, len(rows))
	for _, row := range rows {
		got[row.Path] = row.NodeType
	}
	if got["empty"] != NodeDirectory || got["note.txt"] != NodeRegular {
		t.Fatalf("node types = %v", got)
	}
	info, found, err := h.Stat("empty")
	if err != nil || !found || info.NodeType != NodeDirectory {
		t.Fatalf("stat empty = %+v, found=%v err=%v", info, found, err)
	}
}

func TestHostListsDirectoryFirstWithOpaqueCursor(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"z-dir", "a-dir"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"b.txt", "a.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	first, next, err := h.List("", 3, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{first[0].Path, first[1].Path, first[2].Path}; got[0] != "a-dir" || got[1] != "z-dir" || got[2] != "a.txt" || next == "" {
		t.Fatalf("first page = %v next=%q", got, next)
	}
	second, last, err := h.List("", 3, next)
	if err != nil || len(second) != 1 || second[0].Path != "b.txt" || last != "" {
		t.Fatalf("second page = %+v next=%q err=%v", second, last, err)
	}
	if _, _, err := h.List("", 3, "not-a-cursor"); err != ErrMalformedCursor {
		t.Fatalf("bad cursor error = %v", err)
	}
	if _, _, err := h.List("other/", 3, next); err != ErrMalformedCursor {
		t.Fatalf("cross-directory cursor error = %v", err)
	}
}

func TestHostCreatesDirectoryNode(t *testing.T) {
	root := t.TempDir()
	h, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if err := h.Create("new-folder", NodeDirectory); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "new-folder"))
	if err != nil || !info.IsDir() {
		t.Fatalf("created node = %+v err=%v", info, err)
	}
}
