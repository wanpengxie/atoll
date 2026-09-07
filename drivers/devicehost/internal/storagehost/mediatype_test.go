package storagehost

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHostReportsMediaTypeFromNameThenBytes(t *testing.T) {
	root := t.TempDir()
	h, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	write := func(name string, body []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("notes.md", []byte("# hi"))
	write("typed.ts", []byte("export const x = 1"))
	write("report", []byte("%PDF-1.4\n1 0 obj\n<< >>\nendobj\n"))              // no extension: magic decides
	write("picture", append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 64)...)) // PNG magic
	write("plain-notes", []byte("key = value\nname = atoll\n"))                // no extension, UTF-8 text
	write("blob", []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0x00, 0x00, 0x00})      // binary, no signature
	write("empty", nil)
	write("Makefile", []byte("all:\n\tgo build ./...\n"))
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"notes.md":    "text/markdown",
		"typed.ts":    "text/plain",
		"report":      "application/pdf",
		"picture":     "image/png",
		"plain-notes": "text/plain",
		"blob":        "application/octet-stream",
		"empty":       "text/plain",
		"Makefile":    "text/plain",
		"sub":         "",
	}
	for name, expected := range want {
		info, found, err := h.Stat(name)
		if err != nil || !found {
			t.Fatalf("stat %s: found=%v err=%v", name, found, err)
		}
		if info.MediaType != expected {
			t.Errorf("stat %s: media type %q, want %q", name, info.MediaType, expected)
		}
	}
	rows, _, err := h.List("./", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, row := range rows {
		seen[row.Path] = row.MediaType
	}
	for name, expected := range want {
		if seen[name] != expected {
			t.Errorf("list %s: media type %q, want %q", name, seen[name], expected)
		}
	}
}
