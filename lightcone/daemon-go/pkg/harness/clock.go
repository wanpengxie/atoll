package harness

import "time"

// nowMillis returns the current wall-clock time in milliseconds since
// the Unix epoch, matching L0 §2.1's `ts` / `ts_received` unit.
//
// Centralised here so tests can monkey-patch Deps.Clock and the
// production code path uses a single function — easier to audit when
// debugging timestamp drift.
func nowMillis() int64 {
	return time.Now().UnixMilli()
}
