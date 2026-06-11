// Package host is the daemon-side holder of business cells (tool/agent actors)
// on an attached compute. Each hosted cell's PEN is an out-of-process writer
// (ipc.RemoteWriter over the actor's link stream): the daemon holds NO local
// truth, so behavior.Respond / behavior.EmitEvent flow UP the wire to the home
// harness and observe the home's authoritative write verdict. Dispatch routes an
// inbound envelope into a hosted cell's mailbox; on a cell's abnormal death the
// host closes that actor's stream so the home port reads EOF (the presence-down
// edge that materialises receiver_unavailable) — death is the stream EOF, never
// a translated frame.
package host
