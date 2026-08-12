// Package accessdoor is the runtime-side implementation of the access plane's
// door — the single gate every second-plane invocation (read/write/set/delete/
// create/stat/list on a resource) MUST pass through, dual to runtime/harness on
// the message plane.
//
// The door does three things message writes never bundle into one place, so it
// is a short decision tree rather than a step chain (access has no id/ts/seq/
// audience to validate independently):
//
//   - ingress shape check — a named cluster of pure functions (checkOperation,
//     checkArgs, checkGrant presence, ValidateGrant, ingressCreate) run before
//     RESOLVE. A structurally malformed invocation is a protocol error
//     (ErrMalformed, a Go error), NEVER a FailureReason verdict — proto
//     reason.go deliberately omits a "malformed" value.
//   - the decision tree (door.invoke / door.create / door.stat / door.list) —
//     RESOLVE, then the two-locus authorization (create via channel
//     membership; object ops via the channel-owner root or R, unioning the
//     actor entry with a members entry gated by a check-time membership lookup;
//     Stat/List via the SAME union plus the owner root as a visibility
//     projection), then EXECUTE. One tree,
//     several entry methods — the scope split below is a vocabulary split,
//     not a second tree (期11 spec §3.1).
//   - the welded-caller capability, split into TWO faces along the scope axis
//     (期11 spec §3.1's "scope 劈面"): AccessHandle (Invoke only — the
//     actor-scoped/state locus, which structurally has no kind/R/membership
//     for Create/Stat/List to mean anything) and ResourceAccessHandle
//     (Invoke+Create+Stat+List — the channel-scoped/resource locus). Both are
//     welded to ONE caller/owner at construction (never self-reported), the
//     plane-2 dual of harness.Pen.
//
// The package exports the two capability faces + a Minter (the door's one
// outward face, Mint→ResourceAccessHandle / MintState→AccessHandle); the bare
// door never leaves the package (New hides it inside a minter, mirroring
// harness). It imports the resourcespec seam — the Registry (R + existence)
// and Driver (bytes) contracts — never their implementation, and RE-EXPORTS
// the resourcespec types downstream needs by shape (CreateSpec, KindKV) as
// type aliases / consts, so nothing outside the runtime tree ever imports
// resourcespec directly.
//
// Not in scope of this package:
//
//   - The driver/Registry implementations (runtime/internal/store) and the
//     door's assembly (runtime.OpenChannel). Downstream sees a Mint'd handle,
//     never the bare Registry/Driver.
//   - The port (cross-wire) proxies and the caps-injection / liveness
//     wrapping (platform/internal/link's remoteAccessHandle/
//     remoteResourceHandle, liveAccess/liveResourceAccess) — same two
//     interfaces, second implementations, one layer up.
//   - file kind's byte route (Open/FileAccess) — the door DECIDES it
//     (Outcome.Route: FileRoute{Token, Mode, ReservationID}, minted
//     via Deps.TransferControl) but never REDEEMS it: FileOpener (fileaccess.go)
//     is the redemption capability, implemented one layer up by whichever
//     avatar can actually reach live bytes (platform/internal/link's
//     remoteResourceHandle, the daemon-hosted wire proxy — day-1's only
//     implementor; the portal separately redeems browser tickets over HTTP).
//     Invoke's
//     file read/write branch and Create's with_content=true branch both
//     funnel through door.resolveFileRoute (door.go) — the ONE decision point
//     both share; its ticket selects local redemption or the authenticated
//     cross-daemon exchange without exposing the placement coordinate.
//     No file BYTE ever touches this package — Deps.TransferControl mints an
//     opaque Token, never a coord, never a live handle.
package accessdoor
