// Package storagehost is the device carrier's storage host (期11 spec §4):
// the physical, disk-touching half of the resource axis's file kind. It
// exists ONLY here — Go's own internal/ visibility rule confines every
// importer to the drivers/devicehost subtree (the device-carrier driver both
// packagings — cmd/daemon and cmd/atoll's in-process device — run through;
// the human-ingress peer is drivers/gateway), mechanically enforcing §8.2's
// red line ("server=纯协调组件...file落盘代码只存在于daemon运行时包")
// stronger than an archtest scan could: the server assembly (engineboot)
// never imports devicehost, and no package outside devicehost can import
// this one — full stop, not merely "does not currently".
//
// Four components (§4.1), all daemon-runtime, all daemon-same-shape:
//
//   - Allocator — mkdir/touch under the per-channel resource root when the
//     home's door authorizes a create (AllocRequest, §4.7's first frame).
//   - Streamer — the daemon half of the symmetric data path: local os.Root
//     handles for a same-machine consumer, and (§5's lane, now landed) bytes
//     forwarded through the server for a cross-daemon consumer. devicehost's
//     storageadapter.go wraps this Host as compute.LocalFileOpener, which
//     platform/internal/link's lane.go consults on both call sites §5 wires:
//     a same-daemon caller's Local route and this daemon acting as a lane
//     transfer's target.
//   - Reclaimer — collects a deleted file's bytes once the home's registry
//     has tombstoned them (ReconcilePullReply's "待收 tombstone"),
//     confirming via ReclaimAck (§4.7's third frame).
//   - Scrubber — the ONLY source of truth this daemon has for what it
//     should hold (it keeps none itself, §1.3: "daemon 无 truth"): a
//     periodic (ticker-driven, level-triggered) ReconcilePull (§4.7's
//     fourth frame) followed by a registry↔directory reconciliation pass.
//
// Layout (§4.2, first-version-is-final — "改布局=迁移"):
//
//	<workspaceRoot>/resources/<channelID>/live/<coord>     landed bytes
//	<workspaceRoot>/resources/<channelID>/staging/<coord>-<suffix>  in-flight writes
//
// This is a SIBLING of the device workspace tree (<workspaceRoot>/<channelID>/...),
// not nested under it — separate os.Root trees, so an agent's `rm -rf` of its
// own workspace can never reach the resource tree (misc-cast protection, not
// confidentiality — §4.2's own words: "误铲防护非机密性执法").
package storagehost
