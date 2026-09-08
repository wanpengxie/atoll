package piworkspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDecodeArgsEnforcesAdvertisedClosedSchemaBeforePi(t *testing.T) {
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"path":"x","content":"ok","misspelled_option":true}`),
		json.RawMessage(`{"path":7,"content":"ok"}`),
	} {
		if _, _, err := decodeArgs("workspace.write", raw); err == nil {
			t.Fatalf("invalid write args accepted: %s", raw)
		}
	}
	if path, got, err := decodeArgs("workspace.write", json.RawMessage(`{"path":"x","content":"ok"}`)); err != nil || path != "x" || string(got) != `{"path":"x","content":"ok"}` {
		t.Fatalf("path=%q raw=%s err=%v", path, got, err)
	}
}

func TestSafeWorkspacePathRejectsLexicalAndSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "outside-link")); err != nil {
		t.Fatal(err)
	}
	if err := safeWorkspacePath(root, "nested/new.txt"); err != nil {
		t.Fatalf("safe new path rejected: %v", err)
	}
	for _, path := range []string{"../escape", filepath.Join(root, "absolute"), "outside-link/file.txt"} {
		if err := safeWorkspacePath(root, path); err == nil {
			t.Fatalf("unsafe path %q accepted", path)
		}
	}
}
