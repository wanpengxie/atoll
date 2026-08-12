package gateway

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAutomationTokenPublishFailureRevokesSession(t *testing.T) {
	store := NewSessionStore()
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := MintAutomationToken(store, "root", filepath.Join(blocker, "token")); err == nil {
		t.Fatal("publication unexpectedly succeeded")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.entries) != 0 {
		t.Fatalf("publish failure left %d live sessions", len(store.entries))
	}
}

func TestSessionStoreIsMemoryOnlyAndExpires(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewSessionStore()
	store.now = func() time.Time { return now }
	token := store.Mint("alice", time.Minute)
	if principal, ok := store.Verify(token); !ok || principal != "alice" {
		t.Fatalf("verify=(%q,%v)", principal, ok)
	}
	now = now.Add(time.Minute)
	if _, ok := store.Verify(token); ok {
		t.Fatal("expired token remained live")
	}
	if _, ok := NewSessionStore().Verify(token); ok {
		t.Fatal("a new process store recovered an old token")
	}
}

func TestMintSweepsAllExpiredSessions(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewSessionStore()
	store.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		store.Mint("expired", time.Minute)
	}
	now = now.Add(time.Minute)
	live := store.Mint("live", time.Hour)
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.entries) != 1 {
		t.Fatalf("Mint left %d entries, want only the new live session", len(store.entries))
	}
	if _, ok := store.entries[live]; !ok {
		t.Fatal("Mint sweep removed the newly minted session")
	}
}
