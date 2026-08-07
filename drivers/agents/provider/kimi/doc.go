// Package kimi adapts go-kimi's agent and wire stream to the asynchronous
// base.Engine contract. Streaming deltas remain internal; only complete tool
// phases and a turn terminal cross EventPort.
package kimi
