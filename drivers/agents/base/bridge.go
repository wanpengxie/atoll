package base

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type scopeLease struct{ anchor, correlation message.ID }

func acquireScope(scope EffectScope) (EffectLease, bool) {
	if scope.gate == nil {
		return EffectLease{}, false
	}
	scope.gate.mu.Lock()
	defer scope.gate.mu.Unlock()
	if !scope.gate.open {
		return EffectLease{}, false
	}
	return NewEffectLease(scopeLease{anchor: message.ID(scope.gate.anchor), correlation: message.ID(scope.gate.correl)}), true
}

type metatoolBridge struct {
	exec    *metatool.Exec
	catalog []metatool.MetaTool
}

func newToolBridge(sys actorbase.Sys) ToolBridge {
	return &metatoolBridge{exec: ExecFace(sys, 15*time.Second), catalog: metatool.MetaTools()}
}
func (b *metatoolBridge) Catalog() []ToolSpec {
	out := make([]ToolSpec, len(b.catalog))
	for i, v := range b.catalog {
		out[i] = ToolSpec{Name: v.Spec.Name, Description: v.Spec.Description, Schema: append(json.RawMessage(nil), v.Spec.Schema...)}
	}
	return out
}
func (*metatoolBridge) Acquire(s EffectScope) (EffectLease, bool) { return acquireScope(s) }
func (b *metatoolBridge) Invoke(ctx context.Context, lease EffectLease, in ToolInvocation) ToolResult {
	s, ok := lease.value.(scopeLease)
	if !ok {
		return ToolResult{Text: "invalid effect lease", IsError: true}
	}
	for _, tool := range b.catalog {
		if tool.Spec.Name == in.Name {
			rv := tool.Execute(ctx, in.Params, b.exec, metatool.RuntimeContext{Trigger: metatool.Trigger{Envelope: message.Envelope{ID: s.anchor}, CorrelationID: s.correlation}})
			raw, err := json.Marshal(rv.Value)
			if err != nil {
				return ToolResult{Text: err.Error(), IsError: true}
			}
			return ToolResult{Text: string(raw), IsError: rv.IsError}
		}
	}
	return ToolResult{Text: "unknown tool: " + in.Name, IsError: true}
}

type channelResourceBridge struct{ sys actorbase.Sys }

func newResourceBridge(sys actorbase.Sys) ResourceBridge                 { return &channelResourceBridge{sys: sys} }
func (*channelResourceBridge) Acquire(s EffectScope) (EffectLease, bool) { return acquireScope(s) }
func (b *channelResourceBridge) Invoke(ctx context.Context, _ EffectLease, in ResourceInvocation) ResourceResult {
	rid := resource.ResourceID(in.ResourceID)
	switch in.Operation {
	case "write_file":
		fa, out, err := b.sys.Resource().CreateFile(rid, false, true)
		if err != nil {
			return ResourceResult{Error: err.Error()}
		}
		if !out.Accepted() {
			return ResourceResult{Error: string(out.RejectReason)}
		}
		if fa.Local == nil || fa.Local.Write == nil {
			return ResourceResult{Error: "local write unavailable"}
		}
		if _, err = fa.Local.Write.Write(in.Payload); err != nil {
			_ = fa.Local.Write.Abort()
			return ResourceResult{Error: err.Error()}
		}
		if err = fa.Local.Write.Commit(); err != nil {
			return ResourceResult{Error: err.Error()}
		}
		return ResourceResult{Payload: json.RawMessage(`{"ok":true}`)}
	case "read_file":
		deadline := time.NewTimer(15 * time.Second)
		defer deadline.Stop()
		for {
			st, err := b.sys.Resource().Stat(rid)
			if err != nil {
				return ResourceResult{Error: err.Error()}
			}
			if st.Reject == "" {
				break
			}
			if string(st.Reject) != string(access.ResourceNotFound) {
				return ResourceResult{Error: string(st.Reject)}
			}
			select {
			case <-ctx.Done():
				return ResourceResult{Error: ctx.Err().Error()}
			case <-deadline.C:
				return ResourceResult{Error: "resource not visible before deadline"}
			case <-time.After(100 * time.Millisecond):
			}
		}
		fa, out, err := b.sys.Resource().Open(rid, access.OpRead)
		if err != nil {
			return ResourceResult{Error: err.Error()}
		}
		if !out.Accepted() {
			return ResourceResult{Error: string(out.RejectReason)}
		}
		if fa.Local == nil || fa.Local.Read == nil {
			return ResourceResult{Error: "local read unavailable"}
		}
		defer fa.Local.Read.Close()
		raw, err := io.ReadAll(fa.Local.Read)
		if err != nil {
			return ResourceResult{Error: err.Error()}
		}
		payload, _ := json.Marshal(map[string]any{"content": string(raw), "size": len(raw)})
		return ResourceResult{Payload: payload}
	case "stat":
		st, err := b.sys.Resource().Stat(rid)
		if err != nil {
			return ResourceResult{Error: err.Error()}
		}
		if st.Reject != "" {
			return ResourceResult{Error: string(st.Reject)}
		}
		return ResourceResult{Payload: json.RawMessage(`{"exists":true}`)}
	default:
		return ResourceResult{Error: fmt.Sprintf("unsupported resource operation %q", in.Operation)}
	}
}
