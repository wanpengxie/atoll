package agentcontext

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	contextproto "github.com/wanpengxie/atoll/drivers/tools/agentcontext/api"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
)

const Class = "agent-context"

type Config struct {
	SystemPrompt string `json:"system_prompt,omitempty"`
}

func init() {
	registry.Register(Class, registry.ClassDecl{Kind: actor.KindTool, Placement: channelspec.PlacementDaemon, Manifest: manifest(), New: construct,
		ValidateConfig: func(raw json.RawMessage) error {
			var c Config
			if len(raw) == 0 {
				return nil
			}
			return actorbase.DecodeStrict(raw, &c)
		},
		ConfigSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"system_prompt":{"type":"string"}}}`)})
}

func construct(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	var cfg Config
	if len(spec.Config) > 0 {
		if err := actorbase.DecodeStrict(spec.Config, &cfg); err != nil {
			return platform.ActorDecl{}, err
		}
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: manifest(), New: func() (actorbase.Proc, error) { return proc(cfg, deps), nil }}}}, nil
}

func manifest() introspect.Manifest {
	return introspect.Manifest{Class: Class, Interfaces: []string{"actor", "context"}, Words: map[string]introspect.WordSpec{
		contextproto.TypeBuild: {Description: "Build and persist one immutable, work-scoped model context artifact. Prior Pi messages are continued and only the explicitly supplied new inputs are appended; the Channel ledger is not copied wholesale.", InputSchema: json.RawMessage(`{"type":"object","required":["work_id","assignment_id","inputs"],"properties":{"work_id":{"type":"string"},"assignment_id":{"type":"string"},"inputs":{"type":"array","minItems":1},"prior":{"type":"array"}},"additionalProperties":false}`), OutputSchema: json.RawMessage(`{"type":"object","required":["artifact_id","work_id","assignment_id","input_through","system_prompt","messages","codec","created_at"],"properties":{"artifact_id":{"type":"string"},"work_id":{"type":"string"},"assignment_id":{"type":"string"},"input_through":{"type":"integer"},"system_prompt":{"type":"string"},"messages":{"type":"array"},"codec":{"type":"string"},"created_at":{"type":"integer"}},"additionalProperties":false}`), ErrorCodes: []string{"invalid_args", "context_limit", "state_unavailable"}},
	}}
}

func proc(cfg Config, deps registry.Deps) actorbase.Proc {
	return func(sys actorbase.Sys) error {
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			if msg.Type != contextproto.TypeBuild {
				_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("context actor does not answer %q", msg.Type))
				continue
			}
			var req contextproto.BuildRequest
			if err := actorbase.DecodeStrict(msg.Payload, &req); err != nil {
				_, _ = sys.Fail(msg, "invalid_args", err.Error())
				continue
			}
			if req.WorkID == "" || req.AssignmentID == "" || len(req.Inputs) == 0 {
				_, _ = sys.Fail(msg, "invalid_args", "work_id, assignment_id, and inputs are required")
				continue
			}
			sort.SliceStable(req.Inputs, func(i, j int) bool { return req.Inputs[i].Seq < req.Inputs[j].Seq })
			messages := append([]json.RawMessage(nil), req.Prior...)
			var through int64
			for _, in := range req.Inputs {
				if in.Seq > through {
					through = in.Seq
				}
				raw, _ := json.Marshal(buildUserMessage(in, deps, time.Now().UnixMilli()))
				messages = append(messages, raw)
			}
			if contextSize(messages) > agentloop.MaxHistoryBytes {
				_, _ = sys.Fail(msg, "context_limit", "Pi context exceeds the phase-one recovery limit")
				continue
			}
			prompt := cfg.SystemPrompt
			if prompt == "" {
				prompt = "You are an Atoll Agent. Complete the requested work using the available tools. Keep tool calls precise and report a clear final result."
			}
			artifact := contextproto.Artifact{ArtifactID: "ctx-" + uuid.NewString(), WorkID: req.WorkID, AssignmentID: req.AssignmentID, InputThrough: through, SystemPrompt: prompt, Messages: messages, Codec: "pi-context-v1", CreatedAt: time.Now().UnixMilli()}
			raw, _ := json.Marshal(artifact)
			out, err := sys.State().Put(resource.ResourceID("artifact."+artifact.ArtifactID), raw)
			if err != nil || !out.Accepted() {
				detail := "state write rejected"
				if err != nil {
					detail = err.Error()
				}
				_, _ = sys.Fail(msg, "state_unavailable", detail)
				continue
			}
			_, _ = sys.Reply(msg, artifact)
		}
	}
}

func contextSize(messages []json.RawMessage) int {
	total := 0
	for _, item := range messages {
		total += len(item)
	}
	return total
}

func buildUserMessage(in agentloop.Input, deps registry.Deps, timestamp int64) map[string]any {
	text := in.Text
	if line := callerLine(in); line != "" {
		text = line + "\n" + text
	}
	if in.Origin != nil && in.Origin.Session != "" {
		line := "[origin session=" + in.Origin.Session
		if in.Origin.Label != "" {
			line += " " + in.Origin.Label
		}
		text = line + "]\n" + text
	}
	if lines := attachmentLines(in.Attachments, deps); lines != "" {
		text += "\n" + lines
	}
	metadata := map[string]any{"input_id": in.ID, "input_seq": in.Seq}
	if len(in.Attachments) > 0 {
		// Keep the original structured descriptors as provenance while exposing
		// usable local paths in the text the model consumes.
		metadata["attachments"] = in.Attachments
	}
	return map[string]any{"role": "user", "content": text, "timestamp": timestamp, "metadata": metadata}
}

func callerLine(in agentloop.Input) string {
	fields := make([]string, 0, 4)
	if in.CallerChannel != "" {
		fields = append(fields, "channel="+string(in.CallerChannel))
	}
	if in.CallerActor != "" {
		parts := strings.Split(string(in.CallerActor), ":")
		if len(parts) == 3 && parts[1] != "" && parts[2] != "" {
			fields = append(fields, "kind="+parts[0])
			label := "declaration"
			if parts[0] == "human" {
				label = "principal"
			}
			fields = append(fields, label+"="+parts[1])
		}
		fields = append(fields, "actor="+string(in.CallerActor))
	}
	if len(fields) == 0 {
		return ""
	}
	return "[from " + strings.Join(fields, " ") + "]"
}

type attachment struct {
	Address   string `json:"address"`
	Resource  string `json:"resource_id"`
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
}

func attachmentLines(raws []json.RawMessage, deps registry.Deps) string {
	lines := make([]string, 0, len(raws))
	for _, raw := range raws {
		var a attachment
		if json.Unmarshal(raw, &a) != nil {
			lines = append(lines, "[attachment descriptor="+strconv.Quote(string(raw))+"]")
			continue
		}
		address := a.Address
		if address == "" {
			address = a.Resource
		}
		var line strings.Builder
		line.WriteString("[attachment")
		if a.Name != "" {
			line.WriteString(" name=")
			line.WriteString(strconv.Quote(a.Name))
		}
		if a.MediaType != "" {
			line.WriteString(" media_type=")
			line.WriteString(strconv.Quote(a.MediaType))
		}
		line.WriteString(" path=")
		if local, ok := localAttachmentPath(address, deps); ok {
			line.WriteString(strconv.Quote(local))
		} else {
			line.WriteString(strconv.Quote(address))
			line.WriteString(` note="not in this workspace"`)
		}
		line.WriteByte(']')
		lines = append(lines, line.String())
	}
	return strings.Join(lines, "\n")
}

func localAttachmentPath(address string, deps registry.Deps) (string, bool) {
	u, err := url.Parse(address)
	if err != nil || strings.Contains(strings.ToLower(address), "%2f") || u.Scheme != "daemon" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Host != deps.DeviceName {
		return "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(parts) != 2 || parts[0] != filepath.Base(filepath.Clean(deps.WorkspaceDir)) {
		return "", false
	}
	path := parts[1]
	if path == "" || filepath.IsAbs(path) {
		return "", false
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == "" || part == "." || part == ".." {
			return "", false
		}
	}
	return filepath.ToSlash(path), true
}
