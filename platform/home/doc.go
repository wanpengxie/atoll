// Package home assembles the complete membrane for one channel. It owns the
// channel-local store, admission harness, actor runtime, reconciliation rings,
// subject and daemon links, and the private structural operation executor.
//
// Home is intentionally not a space-facing service. Its only exported method is
// View; platform/channelhost is the sole package-to-package owner and projects a
// generation-fenced Bundle with Gateway, Daemon, registrar-call, and View faces.
// View. Bootstrap and shutdown are package bridges used only by channelhost.
//
// Serving-time structural changes converge through the private opEntry. Both
// member operate frames are the sole composition-mutation adapter to that one
// component. It commits the system audit event pair and durable structure in
// one SQLite transaction; runtime effects are post-commit hints and
// reconciliation remains the correctness backstop.
//
// The package pumps committed messages according to Audience and exposes
// member/observer history plus resource projections through View. Empty event
// audiences are valid pure-log writes; requests require an explicit audience.
// Space policy and storage shape never enter its requirement interfaces.
package home
