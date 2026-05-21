package framework

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
)

func TestRedactKeepsEdges(t *testing.T) {
	got := Redact("abcdef1234567890")
	if got != "ab***90" {
		t.Fatalf("Redact long: got %q want %q", got, "ab***90")
	}
}

func TestRedactShortMasksFully(t *testing.T) {
	if got := Redact("abcd"); got != "***" {
		t.Fatalf("Redact short: got %q want %q", got, "***")
	}
	if got := Redact(""); got != "" {
		t.Fatalf("Redact empty: got %q want %q", got, "")
	}
}

func TestRedactSubstringsScrubsAll(t *testing.T) {
	in := "leaked secret=abcdef1234567890 token=zxcvbn0987654321 done"
	out := RedactSubstrings(in, "abcdef1234567890", "zxcvbn0987654321")
	if strings.Contains(out, "abcdef1234567890") {
		t.Fatalf("RedactSubstrings did not scrub secret: %q", out)
	}
	if strings.Contains(out, "zxcvbn0987654321") {
		t.Fatalf("RedactSubstrings did not scrub token: %q", out)
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("RedactSubstrings did not insert redaction: %q", out)
	}
}

func TestRedactErrorPreservesUnwrap(t *testing.T) {
	inner := errors.New("secret=abcdef1234567890 oops")
	red := RedactError(inner, "abcdef1234567890")
	if red.Error() == inner.Error() {
		t.Fatalf("RedactError did not change Error() text: %q", red.Error())
	}
	if !errors.Is(red, inner) {
		t.Fatalf("RedactError broke errors.Is chain")
	}
}

func TestMemoryCredentialStoreRoundTrip(t *testing.T) {
	s := NewMemoryCredentialStore()
	ctx := context.Background()
	if err := s.Put(ctx, "k1", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := s.Get(ctx, "k1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("Get: v=%q ok=%v err=%v", v, ok, err)
	}
	if err := s.Delete(ctx, "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ = s.Get(ctx, "k1")
	if ok {
		t.Fatalf("Get after Delete returned ok=true")
	}
}

func TestMemoryCredentialStorePutRejectsEmptyKey(t *testing.T) {
	s := NewMemoryCredentialStore()
	if err := s.Put(context.Background(), "", "v"); err == nil {
		t.Fatalf("Put empty key: expected error")
	}
}

func TestScopedCredentialStoreScopesKeys(t *testing.T) {
	ctx := context.Background()
	inner := NewMemoryCredentialStore()
	a := NewScopedCredentialStoreForDeclaration(inner, adapter.Declaration{
		Name:    "adapter-a",
		ActorID: actor.ActorID("tool:a"),
	})
	b := NewScopedCredentialStoreForDeclaration(inner, adapter.Declaration{
		Name:    "adapter-b",
		ActorID: actor.ActorID("tool:b"),
	})
	if err := a.Put(ctx, "shared.secret", "secret-a"); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := b.Put(ctx, "shared.secret", "secret-b"); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if got, ok, err := a.Get(ctx, "shared.secret"); err != nil || !ok || got != "secret-a" {
		t.Fatalf("Get a = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := b.Get(ctx, "shared.secret"); err != nil || !ok || got != "secret-b" {
		t.Fatalf("Get b = %q ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := inner.Get(ctx, "shared.secret"); err != nil || ok {
		t.Fatalf("unscoped Get ok=%v err=%v", ok, err)
	}
	if err := a.Delete(ctx, "shared.secret"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	if _, ok, err := a.Get(ctx, "shared.secret"); err != nil || ok {
		t.Fatalf("Get deleted a ok=%v err=%v", ok, err)
	}
	if got, ok, err := b.Get(ctx, "shared.secret"); err != nil || !ok || got != "secret-b" {
		t.Fatalf("Delete a affected b: got=%q ok=%v err=%v", got, ok, err)
	}
}

func TestMissingCredentialErrorWraps(t *testing.T) {
	err := MissingCredentialError("feishu.app_secret")
	if !errors.Is(err, ErrCredentialMissing) {
		t.Fatalf("MissingCredentialError did not wrap ErrCredentialMissing")
	}
	if !strings.Contains(err.Error(), "feishu.app_secret") {
		t.Fatalf("MissingCredentialError text missing key: %q", err.Error())
	}
}
