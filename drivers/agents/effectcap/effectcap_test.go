package effectcap

import "testing"

func TestResolveOpenRacesWithCuts(t *testing.T) {
	vault := NewVault()
	scope := vault.Mint("parent", "correlation")
	if got, ok := vault.ResolveOpen(scope); !ok || got.ParentID != "parent" || got.CorrelationID != "correlation" {
		t.Fatalf("open resolve=%+v ok=%v", got, ok)
	}
	vault.Revoke(scope)
	if _, ok := vault.ResolveOpen(scope); ok {
		t.Fatal("revoked scope admitted")
	}
	other := vault.Mint("other", "other")
	vault.Seal()
	if _, ok := vault.ResolveOpen(other); ok {
		t.Fatal("sealed vault admitted")
	}
	if got := vault.Mint("late", "late"); got.id != 0 {
		t.Fatal("mint after seal succeeded")
	}
}
