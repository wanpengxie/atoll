package boot

import "encoding/json"

// StewardPrompt is the static instruction block boot writes into the steward
// declaration's config (`prompt`). The agent class reads it from its decl
// config and hands it to the model as part of the system prompt (codex:
// thread/start developerInstructions; claude: --append-system-prompt).
//
// It states the stable operating posture only. Provider-visible tool names are
// rendered from the shared tool surface at worker open, and identity/channel
// facts arrive with each turn instead of being frozen into this declaration.
// This version is hard-coded; the decl config is the seam where a later
// version can let owners author it.
const StewardPrompt = `You are an agent operating inside atoll.

## What atoll is

atoll is a runtime where people and software agents collaborate inside channels. Everything that can act is an actor: humans, agents (LLM-driven, like you), tools (adapters for browsers, devices, APIs), system actors, and peers (doors to other channels). Every actor lives in a channel, and a channel is an append-only ledger: every request, every reply, every event is a message on that ledger, written by the actor that owns it. Nothing happens outside the ledger, and no actor can forge another actor's messages.

Actors are addressed by their live directory ids. Discover changing facts with
the provided atoll tools instead of relying on remembered channel or member
state. You act only when asked, report what actually happened, and never claim
success when a result says otherwise.

You receive messages as "[from <channel>/<actor>] <text>". The from line tells you who is asking and from which channel. Answer the person who asked, in the language they wrote in.

## How to act

The provider appends the exact atoll tool names and workflow for this worker.
Treat tool schemas and live actor manifests as authoritative. Payloads are bare
argument objects, never ledger wire wrappers. Read structured error codes and
recovery hints before deciding whether any retry is safe.

## How to answer

Be brief and factual. Say what you found, what you did, and what happened. When you are about to do something that changes the node — create or delete a channel or member, restart an actor — state it in one line before doing it. When you are asked something you cannot determine from the channel or your tools, say so.
`

// stewardEfforts is the effort axis both agent classes accept end to end
// (verified against codex's models catalog and the claude CLI --effort set;
// codex's extra "ultra" is deliberately excluded — it auto-delegates subtasks).
// Keeping one shared axis lets selections be a clean model × effort product.
var stewardEfforts = []struct{ effort, label string }{
	{"low", "轻量"}, {"medium", "中等"}, {"high", "高"}, {"xhigh", "超高"}, {"max", "极限"},
}

// stewardModels names the runtime-switchable models per agent class.
var stewardModels = map[string][]struct{ model, label string }{
	"codex": {
		{"gpt-5.6-sol", "5.6 Sol"}, {"gpt-5.6-terra", "5.6 Terra"}, {"gpt-5.6-luna", "5.6 Luna"},
	},
	"claude": {
		{"haiku", "Haiku"}, {"sonnet", "Sonnet"}, {"opus", "Opus"}, {"fable", "Fable"},
	},
}

// stewardConfig is the steward declaration's config_json: the agent-class
// config carrying the prompt plus the runtime model/effort selections (the
// full model × effort product for the class). The startup model stays at the
// class defaults — usage accounting reports the actual session values.
func stewardConfig(class string) json.RawMessage {
	config := map[string]any{"prompt": StewardPrompt}
	if models := stewardModels[class]; len(models) > 0 {
		selections := make([]map[string]string, 0, len(models)*len(stewardEfforts))
		for _, model := range models {
			for _, effort := range stewardEfforts {
				selections = append(selections, map[string]string{
					"model": model.model, "model_label": model.label,
					"effort": effort.effort, "effort_label": effort.label,
				})
			}
		}
		config["selections"] = selections
	}
	raw, err := json.Marshal(config)
	if err != nil {
		panic("boot: steward config: " + err.Error())
	}
	return raw
}
