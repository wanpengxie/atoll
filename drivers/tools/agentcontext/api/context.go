package api

import (
	"encoding/json"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
)

const TypeBuild = "context.build"

type BuildRequest struct {
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id"`
	Inputs       []agentloop.Input `json:"inputs"`
	Prior        []json.RawMessage `json:"prior,omitempty"`
}

type Artifact struct {
	ArtifactID   string            `json:"artifact_id"`
	WorkID       agentproto.WorkID `json:"work_id"`
	AssignmentID string            `json:"assignment_id"`
	InputThrough int64             `json:"input_through"`
	SystemPrompt string            `json:"system_prompt"`
	Messages     []json.RawMessage `json:"messages"`
	Codec        string            `json:"codec"`
	CreatedAt    int64             `json:"created_at"`
}
