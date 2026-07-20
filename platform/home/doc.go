// Package home assembles the complete membrane for one channel. It owns the
// channel-local store, admission harness, actor runtime, reconciliation rings,
// subject and daemon links, and the private structural operation executor.
//
// Home is intentionally not a realm-facing service. Its only exported method is
// View; platform/channelhost is the sole package-to-package owner and projects a
// generation-fenced Bundle with four narrow faces: Gateway, Daemon, SysOp, and
// View. Bootstrap and shutdown are package bridges used only by channelhost.
//
// Serving-time structural changes converge through the private opEntry. Both
// member operate frames and channelhost SysOp calls are adapters to that one
// component. It commits idempotency anchor, system audit event pair, and durable
// structure in one SQLite transaction; runtime effects are post-commit hints and
// reconciliation remains the correctness backstop.
//
// The package also resolves empty audiences after a write crosses the membrane,
// pumps committed messages according to Audience, and exposes reader-scoped
// visible history and resource projections through View. Realm policy and storage
// shape never enter its requirement interfaces.
package home
