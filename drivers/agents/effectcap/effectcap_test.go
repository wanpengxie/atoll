package effectcap

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestResolveOpenRacesWithCuts(t *testing.T) {
	vault := NewVault()
	scope := vault.Mint(message.Anchored("parent", "correlation").WithContext(message.Context{Session: "S", Caller: &message.Caller{Channel: "c", Actor: "human:a:1"}}))
	if got, ok := vault.ResolveOpen(scope); !ok || got.ParentID != "parent" || got.CorrelationID != "correlation" {
		t.Fatalf("open resolve=%+v ok=%v", got, ok)
	}
	snapshot, _ := vault.ResolveOpen(scope)
	app, err := snapshot.Cause.Context()
	if err != nil || app.Session != "S" || app.Caller == nil || app.Caller.Actor != "human:a:1" {
		t.Fatalf("tool scope lost request context: %+v %v", app, err)
	}
	vault.Revoke(scope)
	if len(vault.rows) != 0 {
		t.Fatal("revoked request metadata was retained")
	}
	if _, ok := vault.ResolveOpen(scope); ok {
		t.Fatal("revoked scope admitted")
	}
	other := vault.Mint(message.Anchored(message.ID("other"), message.ID("other")).WithContext(message.Context{}))
	vault.Seal()
	if _, ok := vault.ResolveOpen(other); ok {
		t.Fatal("sealed vault admitted")
	}
	if got := vault.Mint(message.Anchored(message.ID("late"), message.ID("late")).WithContext(message.Context{})); got.id != 0 {
		t.Fatal("mint after seal succeeded")
	}
}
