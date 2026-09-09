package native

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const snapshotVersion = 1

type inputRecord struct {
	agentloop.Input
	Disposition string `json:"disposition"`
}

type operationRecord struct {
	Kind     string          `json:"kind"`
	Hash     string          `json:"hash"`
	Response json.RawMessage `json:"response"`
}

type workRecord struct {
	SessionID       string                     `json:"session_id,omitempty"`
	Resumed         bool                       `json:"resumed,omitempty"`
	ID              agentproto.WorkID          `json:"work_id"`
	Owner           harness.Caller             `json:"owner"`
	SourceRequest   string                     `json:"source_request_id"`
	SubmissionKey   string                     `json:"submission_key,omitempty"`
	SubmissionHash  string                     `json:"submission_hash,omitempty"`
	RelatedWorkID   agentproto.WorkID          `json:"related_work_id,omitempty"`
	Delivery        agentproto.Delivery        `json:"delivery"`
	State           agentproto.WorkState       `json:"state"`
	Stage           string                     `json:"stage,omitempty"`
	Outcome         agentproto.Outcome         `json:"outcome,omitempty"`
	Inputs          []inputRecord              `json:"inputs"`
	AssignmentID    string                     `json:"assignment_id,omitempty"`
	AssignedThrough int64                      `json:"assigned_through,omitempty"`
	Looper          string                     `json:"looper,omitempty"`
	ExecutionState  string                     `json:"execution_state,omitempty"`
	BoundaryID      string                     `json:"boundary_id,omitempty"`
	Continuation    bool                       `json:"continuation,omitempty"`
	Operations      map[string]operationRecord `json:"operations,omitempty"`
	Result          json.RawMessage            `json:"result,omitempty"`
	CreatedAt       int64                      `json:"created_at"`
	UpdatedAt       int64                      `json:"updated_at"`
}

type snapshot struct {
	Version    int                    `json:"version"`
	Works      map[string]*workRecord `json:"works"`
	Order      []string               `json:"order"`
	NextLooper int                    `json:"next_looper"`
}

func newSnapshot() snapshot {
	return snapshot{Version: snapshotVersion, Works: map[string]*workRecord{}}
}

func newWorkID() agentproto.WorkID { return agentproto.WorkID("w-" + uuid.NewString()) }
func newAssignmentID() string      { return "a-" + uuid.NewString() }
func newInputID() string           { return "i-" + uuid.NewString() }

func submissionHash(req agentproto.AskRequest) string {
	copy := req
	copy.SubmissionKey = ""
	raw, _ := json.Marshal(copy)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func requestHash(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func operationIndexKey(c harness.Caller, key string) string {
	return string(c.Channel) + "\x00" + string(c.Actor) + "\x00" + key
}

func workScopeKey(c harness.Caller) string { return string(c.Channel) }
func submissionIndexKey(c harness.Caller, key string) string {
	return string(c.Channel) + "\x00" + string(c.Actor) + "\x00" + key
}

func (s *snapshot) findSubmission(c harness.Caller, key string) *workRecord {
	if key == "" {
		return nil
	}
	want := submissionIndexKey(c, key)
	for _, w := range s.Works {
		if submissionIndexKey(w.Owner, w.SubmissionKey) == want {
			return w
		}
	}
	return nil
}

func (s *snapshot) openCount() int {
	n := 0
	for _, w := range s.Works {
		if w.State == agentproto.WorkOpen {
			n++
		}
	}
	return n
}

func (s *snapshot) visible(c harness.Caller) []*workRecord {
	out := make([]*workRecord, 0)
	for _, id := range s.Order {
		w := s.Works[id]
		if w != nil && workScopeKey(w.Owner) == workScopeKey(c) {
			out = append(out, w)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt == out[j].CreatedAt {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt > out[j].CreatedAt
	})
	return out
}

func publicWork(w *workRecord) agentproto.Work {
	return agentproto.Work{WorkID: w.ID, SessionID: w.SessionID, TurnID: w.AssignmentID, State: w.State, Stage: w.Stage, Outcome: w.Outcome,
		SourceRequest: w.SourceRequest, SubmissionKey: w.SubmissionKey, RelatedWorkID: w.RelatedWorkID, ExecutionState: w.ExecutionState,
		CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt}
}

func detailedWork(w *workRecord) agentproto.Work {
	out := publicWork(w)
	out.Inputs = make([]agentproto.WorkInput, 0, len(w.Inputs))
	for _, input := range w.Inputs {
		out.Inputs = append(out.Inputs, agentproto.WorkInput{InputID: input.ID, Seq: input.Seq, Disposition: input.Disposition})
	}
	return out
}

func nowMillis() int64 { return time.Now().UnixMilli() }

func validateSnapshot(s snapshot) error {
	if s.Version != snapshotVersion {
		return fmt.Errorf("unsupported snapshot version %d", s.Version)
	}
	if s.Works == nil {
		return fmt.Errorf("snapshot works missing")
	}
	for id, w := range s.Works {
		if w == nil || string(w.ID) != id {
			return fmt.Errorf("invalid work entry %q", id)
		}
	}
	return nil
}
