package base

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type metatoolBridge struct {
	exec    *metatool.Exec
	catalog []metatool.MetaTool
	vault   *effectcap.Vault
}

func newToolBridge(sys actorbase.Sys, vault *effectcap.Vault) runtimeproto.ToolBridge {
	return &metatoolBridge{exec: ExecFace(sys, 15*time.Second), catalog: metatool.MetaTools(), vault: vault}
}
func (b *metatoolBridge) Catalog() []runtimeproto.ToolSpec {
	out := make([]runtimeproto.ToolSpec, len(b.catalog))
	for i, v := range b.catalog {
		out[i] = runtimeproto.ToolSpec{Name: v.Spec.Name, Description: v.Spec.Description, Schema: append(json.RawMessage(nil), v.Spec.Schema...)}
	}
	return out
}
func (b *metatoolBridge) Invoke(ctx context.Context, scope effectcap.Scope, in runtimeproto.ToolInvocation) runtimeproto.ToolResult {
	snapshot, ok := b.vault.ResolveOpen(scope)
	if !ok {
		return runtimeproto.ToolResult{Text: "effect scope is not open", IsError: true}
	}
	for _, tool := range b.catalog {
		if tool.Spec.Name != in.Name {
			continue
		}
		rv := tool.Execute(ctx, in.Params, b.exec, metatool.RuntimeContext{Trigger: metatool.Trigger{Envelope: message.Envelope{ID: message.ID(snapshot.ParentID)}, CorrelationID: message.ID(snapshot.CorrelationID)}})
		raw, err := json.Marshal(rv.Value)
		if err != nil {
			return runtimeproto.ToolResult{Text: err.Error(), IsError: true}
		}
		return runtimeproto.ToolResult{Text: string(raw), IsError: rv.IsError}
	}
	return runtimeproto.ToolResult{Text: "unknown tool: " + in.Name, IsError: true}
}

type channelResourceBridge struct {
	sys   actorbase.Sys
	vault *effectcap.Vault
}

func newResourceBridge(sys actorbase.Sys, vault *effectcap.Vault) runtimeproto.ResourceBridge {
	return &channelResourceBridge{sys: sys, vault: vault}
}
func (b *channelResourceBridge) Invoke(ctx context.Context, scope effectcap.Scope, in runtimeproto.ResourceInvocation) runtimeproto.ResourceResult {
	if _, ok := b.vault.ResolveOpen(scope); !ok {
		return runtimeproto.ResourceResult{Error: "effect scope is not open"}
	}
	rid := resource.ResourceID(in.ResourceID)
	switch in.Operation {
	case "write_file":
		fa, out, err := b.sys.Resource().Open(rid, access.OpWrite)
		if err != nil {
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		if !out.Accepted() {
			return runtimeproto.ResourceResult{Error: string(out.RejectReason)}
		}
		writer, ok := fa.Writer()
		if !ok {
			return runtimeproto.ResourceResult{Error: "file write unavailable"}
		}
		if _, err = writer.Write(in.Payload); err != nil {
			_ = writer.Abort()
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		if err = writer.Commit(); err != nil {
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		return runtimeproto.ResourceResult{Payload: json.RawMessage(`{"ok":true}`)}
	case "read_file":
		// Resource visibility is decided once by accessdoor. Bridge never polls
		// for a not_found decision to change underneath it.
		st, err := b.sys.Resource().Stat(rid)
		if err != nil {
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		if st.Reject != "" {
			return runtimeproto.ResourceResult{Error: string(st.Reject)}
		}
		fa, out, err := b.sys.Resource().Open(rid, access.OpRead)
		if err != nil {
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		if !out.Accepted() {
			return runtimeproto.ResourceResult{Error: string(out.RejectReason)}
		}
		reader, ok := fa.Reader()
		if !ok {
			return runtimeproto.ResourceResult{Error: "file read unavailable"}
		}
		defer reader.Close()
		raw, err := io.ReadAll(reader)
		if err != nil {
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		payload, _ := json.Marshal(map[string]any{"content": string(raw), "size": len(raw)})
		return runtimeproto.ResourceResult{Payload: payload}
	case "stat":
		st, err := b.sys.Resource().Stat(rid)
		if err != nil {
			return runtimeproto.ResourceResult{Error: err.Error()}
		}
		if st.Reject != "" {
			return runtimeproto.ResourceResult{Error: string(st.Reject)}
		}
		return runtimeproto.ResourceResult{Payload: json.RawMessage(`{"exists":true}`)}
	default:
		return runtimeproto.ResourceResult{Error: fmt.Sprintf("unsupported resource operation %q", in.Operation)}
	}
}
