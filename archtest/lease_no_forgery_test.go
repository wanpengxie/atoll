package archtest

import (
	"fmt"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// leaseNoForgeryScanDirs are the resource-access packages 期11 spec §8.9's
// red line binds: "本期绝不为 file 访问建密码学票据（签名/nonce/过期/一次性/
// 绑定）" — the door (runtime/accessdoor, where handles/reservations are
// minted and verified) and the lane/storage half of the link transport
// (where a Token crosses the wire, platform/internal/link's storagecontrol.go
// + lanecontrol.go + lane.go). A local handle is domain-internal capability
// consumed in place (§8.9: "consumer 拿它当场用，不跨信任域旅行"), never a
// self-verifying, offline-redeemable credential — v0.4's own P0 was exactly
// this line crossed once already ("v0.4 正栽于此").
var leaseNoForgeryScanDirs = []string{"../runtime/accessdoor"}

// leaseNoForgeryScanFiles are the specific platform/internal/link files that
// carry the lane/storage control-RPC wire shapes (§4.7/§5) — NOT the whole
// link package (most of link is the general actor-message mux, out of this
// axis). These are exactly the three files 期11 S5/S6 built the resource
// lane in; the rest of link (attach, cancel-forward, the actor stream mux)
// is a different concern with its own existing purity tests
// (lane_purity_test.go covers the yamux-confinement half of this same
// slice).
var leaseNoForgeryScanFiles = []string{
	"../platform/internal/link/lane.go",
	"../platform/internal/link/lanecontrol.go",
	"../platform/internal/link/storagecontrol.go",
}

// forgeryImports is the closed set of cryptographic SIGNING/MAC primitives
// this axis's red line bans — the mechanism a self-verifying, offline-
// redeemable ticket would be built from (HMAC/Ed25519/RSA/ECDSA signatures,
// or golang.org/x/crypto's own signature-shaped packages). Deliberately NOT
// on this list: crypto/rand (an opaque RANDOM SEED source — used by
// runtime/resourcespec.GenerateCoord for a salted-hash storage coordinate,
// which is a random opaque handle, not a signed/verifiable credential) and
// crypto/sha256 (a plain digest, used by runtime/accessdoor/query.go for a
// cursor hash and by GenerateCoord for the same salted-hash — hashing alone
// proves nothing about WHO issued a value, which is exactly why it is not a
// signature primitive). Signing something is the one operation this axis's
// design deliberately never performs (§8.9's "v1 对称原理执法形").
var forgeryImports = map[string]bool{
	"crypto/hmac":    true,
	"crypto/ed25519": true,
	"crypto/rsa":     true,
	"crypto/ecdsa":   true,
}

// xCryptoPrefix additionally bans golang.org/x/crypto's whole tree: every
// package under it is either a signature scheme (golang.org/x/crypto/ed25519
// pre-stdlib-inclusion, golang.org/x/crypto/ssh's own signers, etc.) or
// adjacent enough to a signing primitive that its appearance in a
// resource-access package is itself the tripwire this test exists to catch
// — a legitimate day-1 need here has zero precedent in the spec.
const xCryptoPrefix = "golang.org/x/crypto/"

// TestLeaseNoForgery pins 期11 spec §8.9 ("本地句柄无防伪") + §9 DoD#6
// ("lease 无防伪 archtest：资源包内禁密码学 token 签发形") mechanically: the
// resource-access packages named above may never import a cryptographic
// signing/MAC primitive. A local handle/Token is an opaque, server-tracked,
// single-use value (see lanecontrol.go's OpenLaneTransfer: uuid.NewString(),
// looked up server-side on redeem/resolve — never a value a holder can
// verify or redeem WITHOUT that server-side round trip); the moment a
// signature primitive shows up here, someone is building the offline-
// verifiable ticket §8.9 forbids for this phase (deferred whole to "防伪
// defer 联邦 B 档").
//
// This is the nail that keeps ResolveCoord (lanecontrol.go) REPLAY-SAFE: a
// coord is resolved by a server-side, single-redeem lookup of an opaque uuid,
// never by verifying a self-contained signed ticket a holder could replay
// offline. The instant a signing/MAC primitive appears in these files, that
// server-tracked single-use property is being swapped for an offline-
// redeemable credential — exactly the replay window §8.9 forbids.
func TestLeaseNoForgery(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	check := func(path string) {
		for _, imp := range importsOf(t, fset, path) {
			if forgeryImports[imp] || strings.HasPrefix(imp, xCryptoPrefix) {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — a cryptographic signing/MAC primitive in a resource-access package would build the offline-redeemable, self-verifying ticket 期11 spec §8.9 explicitly forbids for this phase (local handles are opaque + server-tracked, never signed)", path, imp))
			}
		}
	}

	for _, dir := range leaseNoForgeryScanDirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if skipDirs[d.Name()] {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			check(path)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	for _, path := range leaseNoForgeryScanFiles {
		check(path)
	}

	if len(violations) > 0 {
		t.Fatalf("lease/handle forgery red line violated (期11 spec §8.9, §9 DoD#6):\n  %s", strings.Join(violations, "\n  "))
	}
}
