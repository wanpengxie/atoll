// Package presence is the home-side volatile presence fold: it folds actor-source
// obs PUSH edges (introspect.ObsPresence) into a "latest per actor" level, and
// decays an actor to unknown on its presence-down edge (link death cascade). It is
// the home's L3 device-presence view — in-memory ONLY, never persisted (presence
// is volatile; a stored "online" would lie after restart). Pure mechanism: it
// folds opaque snapshots and never interprets them (守结构不守词汇 — online/offline
// is the consumer's read via introspect.ParsePresence).
package presence
