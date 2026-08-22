package gateway

import (
	"path/filepath"
	"testing"
	"time"
)

// 会话跨进程重启存活：落盘的 token 在新开的 store 里仍然可验；
// 撤销与过期也随文件走。重装（文件消失）= 全部撤销。
func TestSessionStoreSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")

	first := OpenSessionStore(path)
	token := first.Mint("root", time.Hour)
	if principal, ok := first.Verify(token); !ok || principal != "root" {
		t.Fatalf("fresh token must verify, got %q %v", principal, ok)
	}

	reopened := OpenSessionStore(path)
	if principal, ok := reopened.Verify(token); !ok || principal != "root" {
		t.Fatalf("token must survive reopen, got %q %v", principal, ok)
	}

	reopened.Revoke(token)
	third := OpenSessionStore(path)
	if _, ok := third.Verify(token); ok {
		t.Fatal("revoked token must stay revoked across reopen")
	}
}

func TestSessionStoreDropsExpiredOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	store := OpenSessionStore(path)
	token := store.Mint("root", time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	reopened := OpenSessionStore(path)
	if _, ok := reopened.Verify(token); ok {
		t.Fatal("expired token must not survive reopen")
	}
}
