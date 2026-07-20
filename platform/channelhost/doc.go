// Package channelhost owns local per-channel database paths and serving Home
// lifecycles. Realm code sees only the five lifecycle verbs and acquired
// capability bundles; it never receives a database path or *home.Home.
package channelhost
