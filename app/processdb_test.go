package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenProcessDBModeTruthAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.sqlite")
	if _, err := OpenProcessDB(path, false); err == nil {
		t.Fatal("upgrade mode created a missing database")
	}
	p, err := OpenProcessDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenProcessDB(path, true); err == nil {
		t.Fatal("--init accepted an existing database")
	}
	if _, err := OpenProcessDB(path, false); err == nil {
		t.Fatal("second process opener acquired a live lock")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenProcessDB(path, false)
	if err != nil {
		t.Fatalf("upgrade reopen after Close: %v", err)
	}
	_ = reopened.Close()
}

func TestOpenProcessDBRejectsSymlinkAndHardlinkAliases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.sqlite")
	p, err := OpenProcessDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	symlink := filepath.Join(dir, "alias.sqlite")
	if err := os.Symlink(path, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenProcessDB(symlink, false); err == nil {
		t.Fatal("symlink alias bypassed process exclusion")
	}
	hardlink := filepath.Join(dir, "hard.sqlite")
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenProcessDB(hardlink, false); err == nil {
		t.Fatal("hardlink alias bypassed inode exclusion")
	}
}
