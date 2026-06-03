// Package harness is the runtime-side concrete implementation of the
// Message-Write Harness contract declared in kernel/harness (L1 §10.2).
//
// The harness owns the 9-step validation + dispatch chain every channel-
// truth write MUST go through, no matter the entry path:
//
//   - worker IPC (runtime/workerhost.Host.handleWrite)
//   - daemon-side control.write_message (FIX-T2)
//   - adapter framework respond / publish (adapters/framework)
//
// Concrete steps (Step 0 normalize + 1..9 validation, plus the
// out-of-band Step 0.5 dedupe) each implement kernel/harness.Step; the
// Chain composes them in stable StepID order with a short-circuit on
// the first reject. Implementations live in step_*.go siblings of
// chain.go.
//
// Authoritative spec:
//
//   - L1 §10.2  Harness 校验链
//   - L1 §10.2.1 Authoritative pseudocode
//   - L1 §10.3.1 Closed harness_reject_reason set
//   - L2 §1.4.2  type_registry table (read at step 4/5/6/8)
//   - L2 §1.4.5  Engine append ACL (harness is the only principal)
//   - L2 §1.4.10.2 Canonical hash (Step 0.5 + Step 8 dedupe)
//
// **Not in scope of this package**:
//
//   - Post-append delivery to actor mailboxes. The harness only validates +
//     appends truth; delivering an appended envelope into the audience's
//     mailboxes (and reporting per-audience Outcome / collapsing an
//     undeliverable request to receiver_unavailable) is runtime/actorrt's
//     Deliver — invoked by the server write-loop, not a separate package.
//   - Long-pending scheduler tie-in (runtime/scheduler).
//   - Adapter binding tri-class transport (kernel/adapter +
//     adapters/framework).
//   - Channel-local fencing token validation (FIX-T6 — runtime/store
//     channel_lock guard). The harness exposes a hook on Deps so the
//     check can be layered in without rewriting the chain.
package harness
