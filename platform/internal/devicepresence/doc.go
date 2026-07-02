// Package devicepresence is the home-side volatile L3 device-presence fold: it
// folds actor-source obs PUSH edges (introspect.ObsDevicePresence) into a "latest
// per actor" level, and decays an actor to unknown on its down edge — a
// down-edge decay backstop (abnormal death only; a clean deactivation publishes
// no edge, its entry is superseded by the next fold or cleared on home restart).
// This is the home's L3 device-presence view — in-memory ONLY, never persisted
// (device presence is volatile; a stored "online" would lie after restart). Pure
// mechanism: it folds opaque snapshots and never interprets them (守结构不守词汇
// — online/offline is the consumer's read via introspect.ParseDevicePresence).
package devicepresence
