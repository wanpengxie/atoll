// Package ipc declares the daemon ↔ worker IPC wire format.
//
// It is the smallest possible shared surface: both runtime/workerhost
// (daemon side, may import sqlite) and runtime/worker (subprocess side,
// MUST NOT import sqlite or runtime/store) reference this package to
// agree on frame kinds and payload schemas without dragging in
// disallowed transitive imports.
package ipc
