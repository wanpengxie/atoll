// Package harness defines the Message-Write Harness contract — the
// 9-step normalize+validate chain that every channel-truth write goes
// through (L1 §0.1 / §10.2). It does not implement the chain (that
// lives in runtime/harness — T3); it only declares the interfaces +
// re-exports the harness-reject closed reason set.
package harness

import "github.com/wanpengxie/ActOS/kernel/message"

// RejectReason is the harness-facing name for kernel/message.HarnessRejectReason.
type RejectReason = message.HarnessRejectReason
