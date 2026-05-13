// Package worker will host the v4 worker runtime (go-kimi Agent +
// v4 ABI adapter + in_worker_bus harness wrapper) introduced by tickets
// T10 and T11 (.dalek/pm/m1.3-tickets.md §T10/§T11).
//
// T0 only seeds the directory and a smoke entrypoint (cmd/worker/main.go
// + pkg/kimismoke). The real wire emitter / turn-ctx injection / tool
// actor wrappers land in T10–T11.
package worker
