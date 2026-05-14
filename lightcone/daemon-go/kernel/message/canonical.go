package message

// CanonicalHash signature — implementation lands in T1.
//
// Algorithm: RFC 8785 (JCS) canonical JSON serialization + SHA-256
// (hex, lowercase). Spec source: L2 §1.4.10.2.
//
// Three call sites share this hash function (L1 §10.2.2):
//
//  1. Message-Write Harness step 0.5 / step 8 catch — content compare.
//  2. action_ledger.ledger_key — caller-side idempotency key derivation
//     (L2 §1.4.10.1).
//  3. Adapter deterministic response id — `response:<request_id>:<hash>`
//     (L2 §1.4.10.2 + L2 §8.5).
//
// The implementation today lives in pkg/canonical. T1 migrates the
// algorithm body here and pkg/canonical re-exports.
//
// TODO(T1): move pkg/canonical.CanonicalHash into this file.
type CanonicalHasher interface {
	// CanonicalHash returns SHA-256 (hex, lowercase) over the
	// RFC 8785 canonicalization of the 14 hash-input fields of e
	// (L2 §1.4.10.2).
	CanonicalHash(e Envelope) (string, error)

	// CanonicalHashPayload returns SHA-256 (hex, lowercase) over the
	// RFC 8785 canonicalization of payload bytes. Used by the adapter
	// deterministic response id derivation per L2 §1.4.10.2.
	CanonicalHashPayload(payload []byte) (string, error)
}
