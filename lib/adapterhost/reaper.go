package adapterhost

// reapExpired drops in-flight requests whose deadline has passed (actor-local
// bounded reaper). Called LAZILY — swept opportunistically from remember on each
// new request, NOT on a self-scheduled timer (Redis-style: bounding this cache
// is pure memory hygiene, nothing must fire AT the deadline). Runs on the cell
// goroutine so it touches a.inflight with no lock. The terminal itself is the
// caller's caller-scoped responsibility (substrate author #2); this only bounds
// the cell's memory for requests it never answered. A request with no deadline
// (env.ExpiresAt nil) stays until its terminal is written.
func (a *adapterActor) reapExpired(nowMs int64) {
	for k, env := range a.inflight {
		if env.ExpiresAt != nil && *env.ExpiresAt > 0 && nowMs >= *env.ExpiresAt {
			delete(a.inflight, k)
		}
	}
}
