// Package ledger holds the ActionLedger reserve/commit contract +
// ledger_key derivation rule.
//
// kernel/ledger is IO-free. The channel-local sqlite-backed
// action_ledger table lives in runtime/store per T3.
package ledger

// Key is the ledger_key scalar derived per L2 §1.4.10.1:
//
//	ledger_key = canonical_hash(caller_actor || ":" || envelope)
//
// where envelope is the post-normalize, pre-harness 14-field hash
// input. T1 wires the canonical_hash call site here; this file is the
// T2 skeleton holding the scalar type.
type Key string

// String returns the wire form of k.
func (k Key) String() string { return string(k) }
