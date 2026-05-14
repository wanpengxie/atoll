// Package harness holds the Message-Write Harness abstractions:
//
//   - 9-step validation Chain (L1 §10)
//   - Step interface (composable validation unit)
//   - HarnessRejectReason mirror (sourced from kernel/message/reason.go)
//
// kernel/harness owns the CONTRACT; the concrete 9-step body lives in
// runtime per T3.
package harness

import "github.com/coagent-ai/daemon-go/kernel/message"

// HarnessRejectReason re-exports message.HarnessRejectReason so harness
// step authors only need to import the kernel/harness package. Closed
// set per L1 §10.3.1.
type HarnessRejectReason = message.HarnessRejectReason
