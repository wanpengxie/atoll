// Package script provides the deterministic scripted engine class for the C1
// minimal-loop e2e harness (c1-minimal-loop-build-spec.md §3): a non-LLM
// assistant whose behaviour is a fixed function of its input, so the loop's
// "做/回" legs are CI-assertable. It is registered as a flat agent class
// ("script", kind=agent) beside claude/go-kimi and stays after the LLM looper
// lands — demoted to the permanent regression line, never retired.
//
// Structural determinism (spec red line 3): this class calls no LLM, opens no
// network connection, and derives nothing from wall-clock or randomness — the
// resource id is keyed off the request message id, and every reply is a pure
// function of (request payload, tool terminal, resource bytes). It speaks only
// the L1 verb table (Recv/Reply/Fail/Call/Resource) and never touches pen/caps
// (spec red line 4).
package script

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
)

const (
	// TypeChat: call the configured tool with the request payload, persist the
	// payload bytes as a file resource, reply {ok, echoed, resource_id}.
	TypeChat = "loop.chat"
	// TypeVerify: read the named resource's bytes back from the daemon disk and
	// reply {exists, size, content} — the loop's "真读字节" verification leg.
	TypeVerify = "loop.verify"

	// toolSayType is the echo tool's one verb (actors/echo TypeSay). Spelled
	// locally so an engine class never imports a tool actor package.
	toolSayType = "echo.say"

	// callWait bounds one echo call's terminal wait — under the harness's 30s
	// per-attempt budget so a wedged call surfaces as tool_call_failed, not a
	// silently-expired request.
	callWait = 25 * time.Second

	// statPoll{Interval,Timeout} bound loop.verify's Stat-until-visible poll
	// (same mechanism as the walk harness's pollUntilLanded: a just-committed
	// create is visible only after the registry lands it; not_found → retry).
	statPollInterval = 100 * time.Millisecond
	statPollTimeout  = 15 * time.Second
)

const actorDoc = "Deterministic scripted assistant for the C1 minimal loop: " +
	"loop.chat calls the configured tool (echo.say), writes the request payload " +
	"as a file resource and replies {ok, echoed, resource_id}; loop.verify reads " +
	"a resource's bytes back and replies {exists, size, content}; any other type " +
	"fails type_unsupported."

// config is the class's per-instance config shape (rides
// actor_decls.config_json → InstanceSpec.Config): tool_id names the declared
// tool instance loop.chat calls.
type config struct {
	ToolID string `json:"tool_id"`
}

func init() {
	registry.Register("script", registry.ClassDecl{Kind: actor.KindAgent, New: construct})
}

// construct closes the parsed config into the Proc (Constructor(spec,deps) →
// Def → New() per incarnation). A missing tool_id is a hard build error — an
// unconfigured script cell would fail every loop.chat, so it must not build.
func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, fmt.Errorf("script: instance id required (no class default)")
	}
	var cfg config
	if len(spec.Config) > 0 {
		if err := json.Unmarshal(spec.Config, &cfg); err != nil {
			return platform.ActorDecl{}, fmt.Errorf("script: parse config: %w", err)
		}
	}
	toolID := actor.ActorID(strings.TrimSpace(cfg.ToolID))
	if toolID == "" {
		return platform.ActorDecl{}, fmt.Errorf("script: config.tool_id required")
	}
	def, err := base.Def(actorDoc, base.Config{NewEngine: func(sys actorbase.Sys, _ []byte, events base.EventPort) (base.Engine, error) {
		return newEngine(sys, toolID, events), nil
	}})
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{
		ID:      spec.ID,
		Kind:    actor.KindAgent,
		Factory: platform.ActorFactory{Proc: def},
	}, nil
}

// isJSONObject mirrors the write-face guard: the chat payload must be a JSON
// object because the tool terminal merges protocol fields (status) into the
// echoed payload — a non-object payload cannot round-trip that merge.
func isJSONObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	return m != nil
}

// handleChat: ① call the tool with the request payload; ② strip the protocol
// fields (status/reason) off the terminal to recover the echoed value; ③ write
// the ORIGINAL request payload bytes as a new file resource (id keyed off the
// message id: deterministic, and a harness retry under a fresh message id never
// collides into already_exists); ④ reply.
func handleChat(sys actorbase.Sys, msg actorbase.Msg, toolID actor.ActorID) {
	if !isJSONObject(msg.Payload) {
		_, _ = sys.Fail(msg, "bad_payload", "loop.chat payload must be a JSON object")
		return
	}
	pending, err := sys.Call(toolID, toolSayType, msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "tool_call_failed", "call "+string(toolID)+": "+err.Error())
		return
	}
	term, err := pending.Wait(msg.Ctx(), callWait)
	if err != nil {
		_, _ = sys.Fail(msg, "tool_call_failed", "await tool terminal: "+err.Error())
		return
	}
	var echoed map[string]any
	if err := json.Unmarshal(term.Payload, &echoed); err != nil {
		_, _ = sys.Fail(msg, "tool_call_failed", "parse tool terminal: "+err.Error())
		return
	}
	if status, _ := echoed["status"].(string); status != message.StatusCompleted {
		code, _ := echoed["error_code"].(string)
		detail, _ := echoed["detail"].(string)
		_, _ = sys.Fail(msg, "tool_call_failed", fmt.Sprintf("tool failed: %s %s", code, detail))
		return
	}
	// Strip the protocol fields the terminal write merged in (RespondJSON always
	// carries status; failed terminals carry reason) — what remains is the tool's
	// own reply value, i.e. the echo of the request payload.
	delete(echoed, "status")
	delete(echoed, "reason")

	rid := resource.ResourceID("file:loop/" + string(msg.ID))
	fa, out, err := sys.Resource().CreateFile(rid, false /*dir*/, true /*withContent*/)
	if err != nil {
		_, _ = sys.Fail(msg, "resource_failed", "create "+string(rid)+": "+err.Error())
		return
	}
	if !out.Accepted() {
		_, _ = sys.Fail(msg, "resource_failed", "create "+string(rid)+" rejected: "+string(out.RejectReason))
		return
	}
	if fa.Local == nil || fa.Local.Write == nil {
		_, _ = sys.Fail(msg, "resource_failed", "no local write handle (script cell expected same-daemon placement)")
		return
	}
	// The file content is the ORIGINAL payload bytes verbatim (no map re-encode:
	// the harness byte-compares against what it sent).
	if _, werr := fa.Local.Write.Write([]byte(msg.Payload)); werr != nil {
		_ = fa.Local.Write.Abort()
		_, _ = sys.Fail(msg, "resource_failed", "write "+string(rid)+": "+werr.Error())
		return
	}
	if cerr := fa.Local.Write.Commit(); cerr != nil {
		_, _ = sys.Fail(msg, "resource_failed", "commit "+string(rid)+": "+cerr.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{
		"ok": true, "echoed": echoed, "resource_id": string(rid),
	})
}

type verifyReq struct {
	ResourceID string `json:"resource_id"`
}

// handleVerify: poll Stat until the resource is visible (a just-committed
// create lands asynchronously; not_found → retry), then Open(read) and read the
// bytes back off the real disk — Stat alone is伪覆盖: it never touches the
// daemon's filesystem, so an empty-disk restart would still "exist".
func handleVerify(sys actorbase.Sys, msg actorbase.Msg) {
	var req verifyReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil || strings.TrimSpace(req.ResourceID) == "" {
		_, _ = sys.Fail(msg, "bad_payload", "loop.verify requires {resource_id}")
		return
	}
	rid := resource.ResourceID(req.ResourceID)

	deadline := time.Now().Add(statPollTimeout)
	for {
		st, err := sys.Resource().Stat(rid)
		if err != nil {
			_, _ = sys.Fail(msg, "resource_failed", "stat "+string(rid)+": "+err.Error())
			return
		}
		if st.Reject == "" {
			break
		}
		// Only not-found is the transient "committed but not yet landed" state
		// worth polling; any other reject (denied, ...) is a permanent verdict —
		// burning the poll budget on it would only delay the same failure.
		// Compared through the proto vocabulary (access.ResourceNotFound shares
		// the wire word with the query layer's not-found verdict) — a plugin dir
		// must not import runtime/accessdoor (archtest 逃生门纪律).
		if string(st.Reject) != string(access.ResourceNotFound) {
			_, _ = sys.Fail(msg, "resource_failed", "stat "+string(rid)+" rejected: "+string(st.Reject))
			return
		}
		if time.Now().After(deadline) {
			_, _ = sys.Fail(msg, "resource_failed",
				"stat "+string(rid)+" not visible before deadline: "+string(st.Reject))
			return
		}
		select {
		case <-msg.Ctx().Done():
			_, _ = sys.Fail(msg, "resource_failed", "stat poll cancelled: "+msg.Ctx().Err().Error())
			return
		case <-time.After(statPollInterval):
		}
	}

	fa, out, err := sys.Resource().Open(rid, access.OpRead)
	if err != nil {
		_, _ = sys.Fail(msg, "resource_failed", "open "+string(rid)+": "+err.Error())
		return
	}
	if !out.Accepted() {
		_, _ = sys.Fail(msg, "resource_failed", "open "+string(rid)+" rejected: "+string(out.RejectReason))
		return
	}
	if fa.Local == nil || fa.Local.Read == nil {
		_, _ = sys.Fail(msg, "resource_failed", "no local read handle (script cell expected same-daemon placement)")
		return
	}
	defer fa.Local.Read.Close()
	b, rerr := io.ReadAll(fa.Local.Read)
	if rerr != nil {
		_, _ = sys.Fail(msg, "resource_failed", "read "+string(rid)+": "+rerr.Error())
		return
	}
	_, _ = sys.Reply(msg, map[string]any{
		"exists": true, "size": len(b), "content": string(b),
	})
}
