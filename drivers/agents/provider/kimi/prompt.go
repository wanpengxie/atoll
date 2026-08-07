package kimi

import (
	"fmt"
	"strings"
)

// Situation carries the FACTS of one agent instance's circumstances — and
// only facts. No role labels: "bootloader guide" and "working principal"
// are behaviours the ONE prompt skeleton derives from these facts (an actor
// is never boxed into a fixed role); the same instance changes behaviour
// the moment its facts change (a device attaches, a workspace appears).
type Situation struct {
	// HasWorkspace: a private device workspace (bash/files of its own)
	// exists for this instance.
	HasWorkspace bool
	// WorkspaceDir is the workspace path when HasWorkspace.
	WorkspaceDir string
	// Host is where this instance runs ("server" / "daemon") — purely for
	// situational honesty when talking to users.
	Host string
}

// BuildSystemPrompt assembles the prompt-cache friendly stable prefix:
//
//	[L0-L2 platform teaching]        — stable skeleton, byte-identical
//	[situation block]                 — facts + the behaviour they imply
//	[L4 domain prompt]                — channel-type template
//
// The prompt carries NO frozen actor/type snapshot. The concrete set of
// actors and request-callable types is dynamic channel state, discovered
// live at call time via the list_actors / describe_* meta tools — baking
// it into the cached prefix would (a) go stale the instant an actor joins
// after spawn and (b) churn the cache prefix. channelType is purely
// informational. Empty domainPrompt yields skeleton + situation alone.
func BuildSystemPrompt(sit Situation, channelType, domainPrompt string) string {
	var b strings.Builder
	b.WriteString(platformTeachingPrompt)
	b.WriteString("\n\n")
	b.WriteString(situationPrompt(sit))

	domain := strings.TrimSpace(domainPrompt)
	if domain != "" {
		b.WriteString("\n\n")
		if channelType != "" {
			b.WriteString("# Channel template: ")
			b.WriteString(channelType)
			b.WriteString("\n\n")
		}
		b.WriteString(domain)
	}
	return b.String()
}

// situationPrompt renders the situation FACTS plus the behaviour the
// skeleton derives from them. The no-workspace branch IS the bootstrap
// guide behaviour; the workspace branch IS the working-principal
// discipline — same skeleton, different facts.
func situationPrompt(sit Situation) string {
	var b strings.Builder
	b.WriteString("# Your situation\n\n")
	host := sit.Host
	if host == "" {
		host = "unknown"
	}
	fmt.Fprintf(&b, "- You run on: %s.\n", host)
	if sit.HasWorkspace {
		fmt.Fprintf(&b, "- You HAVE a private workspace at %s — your home. ", sit.WorkspaceDir)
		b.WriteString("Persist durable working rules, plans and notes there " +
			"(via the channel's device file tools); the channel's public " +
			"messages are the shared memory of record — important outcomes " +
			"belong there, not only in your private files.\n")
	} else {
		b.WriteString("- You have NO private workspace: no bash or files of " +
			"your own. You can still converse and call ANY actor the " +
			"channel's daemons provide (call_actor).\n")
		b.WriteString("- Be honest about this limit. When a task needs " +
			"compute or files and list_actors shows no device actor, tell " +
			"the user plainly and guide them to attach one (install the " +
			"daemon / run the connect command they have). After they act, " +
			"check list_actors again and confirm what arrived before " +
			"proceeding.\n")
	}
	return b.String()
}

// platformTeachingPrompt is the L0-L2 stable prefix every atoll
// agent carries. Intentionally short — the goal is to anchor the
// agent on the atoll envelope protocol without exploding the cache
// surface. Future ticket can extend with concrete examples.
const platformTeachingPrompt = `You are a atoll agent — an LLM-backed actor inside a channel-scoped runtime.

Protocol contract (do not violate):
- You receive turn triggers that carry one user-visible message plus channel context.
- You produce one complete terminal answer. The runtime writes it as the response to the triggering request.
- Tool and turn phase events are runtime-owned activity telemetry; do not imitate or fabricate them in text.
- When you have nothing useful to add, exit the turn promptly — a terse "ack" beats a verbose filler.
- Tool calls (xhs publish, search, get-note, etc.) flow through the channel's adapter actors via call_actor. Reference them by their declared type; the harness routes the request.
- call_actor is fast-path: short calls return their result inline; long calls return an ack and the result comes back later (await_result to block, or react to it as a new message). Fan out with wait=false, then await_result/cancel. See "Tool invocation" in the channel context for the full pattern.

Stay grounded in the trigger payload and the channel's domain template below.`
