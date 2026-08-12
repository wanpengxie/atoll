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
