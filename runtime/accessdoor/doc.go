// Package accessdoor is the runtime-side implementation of the access plane's
// door — the single gate every second-plane invocation (read/write/set/delete/
// create on a resource) MUST pass through, dual to runtime/harness on the
// message plane.
//
// The door does three things message writes never bundle into one place, so it
// is a short decision tree rather than a step chain (access has no id/ts/seq/
// audience to validate independently):
//
//   - ingress shape check — a named cluster of pure functions (checkOperation,
//     checkArgs, checkGrant presence, ValidateGrant) run before RESOLVE. A
//     structurally malformed invocation is a protocol error (ErrMalformed, a Go
//     error), NEVER a FailureReason verdict — proto reason.go deliberately omits
//     a "malformed" value.
//   - the decision tree (door.invoke) — RESOLVE, then the two-locus authorization
//     (create via channel membership; object ops via R, unioning the actor entry
//     with a members entry gated by a check-time membership lookup), then EXECUTE.
//   - the welded-caller capability — AccessHandle is welded to ONE caller at
//     construction (never self-reported), the plane-2 dual of harness.Pen.
//
// The package exports only the AccessHandle capability + a Minter (the door's one
// outward face); the bare door never leaves the package (New hides it inside a
// minter, mirroring harness). It imports the resourcespec seam — the Registry (R
// + existence) and Driver (bytes) contracts — never their implementation.
//
// Not in scope of this package:
//
//   - The driver/Registry implementations (runtime/internal/store) and the
//     door's assembly (runtime.OpenChannel). Downstream sees a Mint'd
//     AccessHandle, never the bare Registry/Driver.
//   - The port (cross-wire) arm of AccessHandle and the caps-injection /
//     liveness wrapping — same interface, a second implementation, landing when
//     caps are wired.
package accessdoor
