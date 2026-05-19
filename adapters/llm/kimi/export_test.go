package kimi

import (
	gokimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"
)

// This file is in package `kimi` (not `kimi_test`) so the test helpers
// below can reach private bridge state. Tests in bridge_test.go import
// these as exported names (the suffix `_test.go` tells go that they are
// only built for the test binary).

// Agent is the public alias for the bridge's kimiAgent interface so
// tests can declare scripted doubles.
type Agent = kimiAgent

// AgentConfig is re-exported so tests can write the SetAgentFactory
// signature without importing go-kimi directly.
type AgentConfig = gokimi.AgentConfig

// SetAgentFactory swaps the bridge's agent factory hook. Test-only.
func SetAgentFactory(b *Bridge, fn func(AgentConfig) (Agent, error)) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.agentNew = fn
	b.mu.Unlock()
}

// BridgeWireEmitter returns a fresh wire.Emitter that talks to the
// bridge's internal wire channel. Scripted agents use this to drive
// the wire stream from inside their Run() implementation.
//
// Returns a no-op emitter until the bridge's Run() loop has built a
// wire channel; tests must call this from inside the scripted agent's
// emit hook, which fires only after Bridge.Run has installed the
// channel.
func BridgeWireEmitter(b *Bridge) wire.Emitter {
	if b == nil {
		return wire.NoopEmitter{}
	}
	b.mu.Lock()
	emitter := b.testWireEmitter
	b.mu.Unlock()
	if emitter == nil {
		return wire.NoopEmitter{}
	}
	return emitter
}

// EnvelopeIDForTest exposes the private id generator for collision tests.
func EnvelopeIDForTest(b *Bridge, ipc IPCFacade, nowMs int64) string {
	return b.envelopeID(ipc, nowMs).String()
}

// ClassifyLLMError is the exported alias for the internal classifier so
// the table test in bridge_test.go can pin the reason buckets.
func ClassifyLLMError(err error) string { return classifyLLMError(err) }
