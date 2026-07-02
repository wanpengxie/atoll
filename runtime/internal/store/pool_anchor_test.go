package store

import (
	"context"
	"path/filepath"
	"testing"
)

// TestChannelDBPoolPinnedToOneConnection anchors a cross-file coupling that
// was previously prose-only: messages.go's in-tx provisional-after-final
// re-check (finalExistsTx) is ATOMIC only because sqlite.go pins the pool to
// a single serialized connection — on >1 connections, a final committing on
// connection B between the re-check's SELECT and the INSERT on connection A
// would land a zombie provisional after a final. The final-after-final facet
// has a UNIQUE index; THIS facet's geometry is the pool pin, so the pin gets
// a test: an innocent-looking SetMaxOpenConns bump turns this red instead of
// silently reopening the window.
func TestChannelDBPoolPinnedToOneConnection(t *testing.T) {
	cs, err := OpenChannel(context.Background(), "C-pool",
		filepath.Join(t.TempDir(), "ch.sqlite"), OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = cs.Close() }()

	if got := cs.db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("channel db MaxOpenConnections = %d, want 1 — the in-tx provisional-after-final re-check is only atomic on a single serialized connection", got)
	}
}
