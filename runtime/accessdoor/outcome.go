package accessdoor

import "github.com/wanpengxie/atoll/protocol/access"

// Outcome is the per-invocation verdict — the access-plane dual of
// harness.WriteResult. RejectReason draws directly on the proto FailureReason
// closed set, so outcome_unknown's slot is present day-1 (only the port arm
// produces it; in-proc is synchronous and never does — reason.go). An accepted
// outcome carries Value/Found for reads; the other ops leave them zero.
type Outcome struct {
	// Value is the bytes returned by a read (nil otherwise).
	Value []byte
	// Found is a read's resolved-but-empty signal: false with an accepted outcome
	// means the resource resolved but holds no bytes (a legal state, not a
	// failure). Zero for non-read ops.
	Found bool
	// RejectReason is the failure verdict, or "" for an accepted outcome.
	RejectReason access.FailureReason
	// Route is OpRead/OpWrite(file)'s and Create(file, with_content=true)'s
	// authorization product (期11 spec §5 item 0/§3.4) — set ONLY on an
	// accepted outcome for a file-kind byte-access request, nil otherwise
	// (kv ops never populate it; Value stays kv's carrier, unchanged). It is
	// NEVER bytes and NEVER an internal file handle — see FileRoute's
	// own doc for exactly what it carries and why.
	Route *FileRoute
}

// Accepted reports whether the invocation succeeded (no reject verdict).
func (o Outcome) Accepted() bool { return o.RejectReason == "" }
