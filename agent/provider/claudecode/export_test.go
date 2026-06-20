package claudecode

// This file is in package `claudeagent` (not `_test`) so the helpers can reach
// private bridge state. bridge_test.go (package claudeagent_test) uses these.

// ClaudeClient is the public alias for the engine interface so tests can declare
// scripted doubles without importing the SDK's concrete client.
type ClaudeClient = claudeClient

// SetClientFactory swaps the bridge's client factory hook. Test-only; call
// before Start.
func SetClientFactory(b *Bridge, fn func() (ClaudeClient, error)) {
	if b == nil {
		return
	}
	b.clientNew = func() (claudeClient, error) { return fn() }
}

// MCPToolNames returns the tool names the bridge wires into its in-process MCP
// server — proves the meta-tool surface is bridged to the claude engine.
func MCPToolNames(b *Bridge) []string {
	cfg := b.buildMCPServer()
	if cfg == nil || cfg.Instance == nil {
		return nil
	}
	names := make([]string, 0, len(cfg.Instance.Tools))
	for _, t := range cfg.Instance.Tools {
		names = append(names, t.Name)
	}
	return names
}
