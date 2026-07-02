// Package tap is the physical downstream of commit: a lossy wake Signal plus a
// cursor Pump. The commit path's only post-commit duty is signal.Notify() (no
// business effect, no dependency); every consumer of committed truth is a Pump
// subscriber that reads forward from its own seq cursor. Correctness lives in
// the cursor read, never in the signal — the signal is lossy-by-design, so a
// coalesced or dropped wake costs nothing (the next read still sees every new
// seq). This is the one CDC-style seam that serves all downstreams: cell
// delivery, client push, and (future, need-driven) projection/bridge/mobile taps.
//
// tap is pure mechanism with zero business semantics: it moves rows by seq, it
// does not read envelope/type/kind. Its vocabulary is seq / cursor / signal.
package tap
