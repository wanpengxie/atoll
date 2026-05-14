// Package log holds the append-only MessageLog contract and the seq
// cursor scalar.
//
// kernel/log is IO-free. The channel-local sqlite messages table lives
// in runtime/store per T3.
package log

// Seq is the AUTO_INCREMENT message sequence number (L2 §1.4.1).
//
// Monotonic per channel-local store. seq is a STORE-DERIVED column and
// is excluded from the canonical hash input (L1 §10.2.2).
type Seq int64

// Cursor is the typed pair (Seq, MessageID) used by view sync push and
// resync to address a specific row in the append-only log.
type Cursor struct {
	Seq       Seq
	MessageID string
}
