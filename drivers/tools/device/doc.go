// Package device is the generic device actor every daemon ships by default:
// the physical hands of the daemon's machine, exposed to the channel as a
// kind=tool actor. It serves exactly four types — device.exec (computation)
// and device.file.read/write/edit (file manipulation) — all confined to a
// per-channel workspace directory. Everything else (grep, ls, find, git, …)
// goes through device.exec: a dedicated type exists only where the shell
// round-trip is unreliable for a model (write/edit).
//
// The actor id is the channel seat named by the plan. The physical device is
// the seat's desired_host DeviceID; it is never re-derived by this actor.
// Human-readable file addresses use the device's canonical registry name,
// while placement and authority continue to join by DeviceID. Credentials
// live in the daemon process (env/keychain), never in workspace files.
package device
