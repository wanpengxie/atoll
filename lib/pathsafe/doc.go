// Package pathsafe maps opaque ids (actor/channel ids that may carry ':' like
// "agent:boost", or '/'/'\' path separators) to a single filesystem-safe path
// segment. A plain stdlib string util, no layer of its own.
//
// NOT a storage-name generator (期11 S6 account item ⑦, judged during resource
// axis完备化 construction): Segment's character-replace is lossy, not
// collision-free — ":"->"-" and a literal "-" collide; "/" and "\\" both
// ->"_" and collide. The resource axis's channelID/coord path segments
// (cmd/daemon/internal/storagehost) instead use an ALLOW-LIST charset assert
// (assertPathSegment) that rejects an illegal id outright rather than
// rewriting it — collision-free by construction. archtest/pathsafe_purity_test.go
// pins that this package is never imported there.
package pathsafe
