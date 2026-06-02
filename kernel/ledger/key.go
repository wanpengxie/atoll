// Package ledger owns the idempotency Key type + the LedgerKey derivation
// (SHA-256 over canonical JSON of {turn_id, semantic_action_key}) that
// backs L2 §1.4.10.1 Turn Replay idempotency. Pure proto: just the key
// type and a pure derivation function.
//
// The stateful action_ledger reserve/commit contract (the Ledger
// interface + Entry/Status rows) is an engine seam and lives in runtime
// (implemented over sqlite). kernel only owns the key derivation.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// Key is the action_ledger primary key — caller-derived idempotency key.
// String form is hex-lowercase SHA-256 (64 chars) per L2 §1.4.10.2.
type Key string

// String returns the wire form.
func (k Key) String() string { return string(k) }

// DeriveKey computes the ledger key per L2 §1.4.10.1:
//
//	ledger_key = SHA-256_hex(RFC8785_canonicalize({
//	  turn_id, semantic_action_key
//	}))
//
// Stable across daemon / caller as long as both pass the same turnID +
// semanticActionKey strings (caller's responsibility — see §1.4.10.1
// Semantic_action_key per-type 推导示例 for type-specific guidance).
//
// Returns an error only if the canonicalize layer rejects the input
// (degenerate / invalid Number range — extremely unlikely for the two
// string inputs used here).
func DeriveKey(turnID, semanticActionKey string) (Key, error) {
	// Reuse the canonical-JSON impl from kernel/message so daemon /
	// caller / adapter all hash through one normalized path.
	raw := []byte(`{"semantic_action_key":` + jsonString(semanticActionKey) +
		`,"turn_id":` + jsonString(turnID) + `}`)
	canon, err := message.CanonicalizeJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return Key(hex.EncodeToString(sum[:])), nil
}

// jsonString returns the JSON-encoded form of s (with quotes + escapes
// per RFC 8259). Inlined to avoid pulling encoding/json for two
// strings; we only escape the four canonical-required runes.
func jsonString(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			out = append(out, '\\', '"')
		case '\\':
			out = append(out, '\\', '\\')
		case '\b':
			out = append(out, '\\', 'b')
		case '\t':
			out = append(out, '\\', 't')
		case '\n':
			out = append(out, '\\', 'n')
		case '\f':
			out = append(out, '\\', 'f')
		case '\r':
			out = append(out, '\\', 'r')
		default:
			if c < 0x20 {
				const hexd = "0123456789abcdef"
				out = append(out, '\\', 'u', '0', '0', hexd[(c>>4)&0xF], hexd[c&0xF])
			} else {
				out = append(out, c)
			}
		}
	}
	out = append(out, '"')
	return string(out)
}
