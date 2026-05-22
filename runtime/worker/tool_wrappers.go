package worker

// ToolWrappers are in_worker_bus tool actor placeholders. Each tool the
// agent loop can call routes through one of these, which in turn
// converts the call into a v4 envelope + dispatches via IPCClient.
//
// launch ships only the wiring shape — concrete tool implementations land
// in adapters/ (T4 / T5).
type ToolWrapper interface {
	// Name returns the canonical actor id, e.g. "tool:feishu-adapter".
	Name() string
}

// (Reserved for adapter wrappers in T4 / T5.)
