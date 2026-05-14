package actor

import "github.com/coagent-ai/coagent/kernel/message"

// Sender is re-exported from kernel/message for callers who want to
// reach the type via kernel/actor without pulling kernel/message
// directly. The authoritative struct lives in kernel/message because it
// is part of the envelope schema (L0 §2.1).
type Sender = message.Sender

// SenderKind is re-exported from kernel/message — see L0 §2.3 for the
// 4-value closed set semantics.
type SenderKind = message.SenderKind

// SenderKind value re-exports — keep the package surface ergonomic for
// `actor.SenderHuman` / `actor.SenderAgent` etc.
const (
	SenderHuman  = message.SenderHuman
	SenderAgent  = message.SenderAgent
	SenderSystem = message.SenderSystem
	SenderTool   = message.SenderTool
)

// AllSenderKinds is re-exported from kernel/message.
var AllSenderKinds = message.AllSenderKinds
