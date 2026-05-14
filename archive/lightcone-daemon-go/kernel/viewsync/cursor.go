// Package viewsync holds the daemon→server view sync contract:
//
//   - PushFrame / AckFrame / Resync request+response types
//   - Pusher (daemon side), Receiver (server side), Resyncer (RPC)
//     interfaces
//   - Cursor triplet (last_pushed_seq / last_acked_seq / last_received_seq)
//
// At-least-once protocol per L1 §8 (upgraded by T1.1).
//
// kernel/viewsync is IO-free. Persistent outbox and WS client live in
// runtime/transit per T3.
package viewsync

import "github.com/coagent-ai/daemon-go/kernel/log"

// LastPushedSeq is the daemon-side high watermark of frames pushed to
// server (may still be unacknowledged).
type LastPushedSeq log.Seq

// LastAckedSeq is the daemon-side high watermark of frames the server
// has durably applied (per server ack). outbox rows ≤ LastAckedSeq are
// GC candidates.
type LastAckedSeq log.Seq

// LastReceivedSeq is the server-side high watermark of CONTIGUOUS
// applied frames. Gaps below this watermark are filled by Resync; live
// frames above it are buffered until the gap closes.
type LastReceivedSeq log.Seq
