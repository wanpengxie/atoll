package viewcache

import "time"

// timeNowFn is the package-level clock. Tests swap it before calling
// Apply to make received_at deterministic.
var timeNowFn = time.Now
