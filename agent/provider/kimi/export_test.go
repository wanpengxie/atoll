package kimi

import (
	"context"

	gokimi "github.com/wanpengxie/go-kimi/pkg/kimi"
	"github.com/wanpengxie/go-kimi/pkg/kimi/wire"

	"github.com/wanpengxie/atoll/protocol/message"
)

// This file is in package `agent` (not `agent_test`) so the test helpers
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
// Must be called before Start.
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
// Returns a no-op emitter until Start has built the wire channel; tests
// must call this from inside the scripted agent's emit hook, which fires
// only after Start has installed the channel.
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
func EnvelopeIDForTest(b *Bridge, nowMs int64) string {
	return b.envelopeID(nowMs).String()
}

// ClassifyLLMError is the exported alias for the internal classifier so
// the table test in bridge_test.go can pin the reason buckets.
func ClassifyLLMError(err error) string { return classifyLLMError(err) }

// WithTurnContext attaches a turn (trigger envelope) to ctx so external
// tests can invoke the channel tools as if inside a live turn.
func WithTurnContext(ctx context.Context, env message.Envelope) context.Context {
	return context.WithValue(ctx, channelToolRuntimeKey{}, turnItem{env: env})
}

// ChannelToolsForTest exposes the go-kimi tool surface for direct
// invocation in tests.
func ChannelToolsForTest(b *Bridge) []interface {
	Name() string
} {
	tools := b.channelTools()
	out := make([]interface{ Name() string }, 0, len(tools))
	for _, t := range tools {
		out = append(out, t)
	}
	return out
}
