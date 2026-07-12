// Package device is the generic device actor every daemon ships by default:
// the physical hands of the daemon's machine, exposed to the channel as a
// kind=tool actor. It serves exactly four types — device.exec (computation)
// and device.file.read/write/edit (file manipulation) — all confined to a
// per-channel workspace directory. Everything else (grep, ls, find, git, …)
// goes through device.exec: a dedicated type exists only where the shell
// round-trip is unreliable for a model (write/edit).
//
// The actor id is device:<name> — one per physical device, so device identity
// rides in the id. Credentials live in the daemon process (env/keychain),
// never in workspace files.
package device
