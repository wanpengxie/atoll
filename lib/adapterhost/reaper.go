package adapterhost

// reapExpired drops in-flight requests whose deadline has passed (actor-local
// bounded reaper). The cell self-schedules this on a slow tick — it runs on the
// cell goroutine so it touches a.inflight with no lock. The terminal itself is
// the caller's caller-scoped responsibility (substrate author #2); this only
// bounds the cell's memory for requests it never answered. A request with no
// deadline (env.ExpiresAt nil) stays until its terminal is written.
func (a *adapterActor) reapExpired(nowMs int64) {
	for k, env := range a.inflight {
		if env.ExpiresAt != nil && *env.ExpiresAt > 0 && nowMs >= *env.ExpiresAt {
			delete(a.inflight, k)
		}
	}
}
