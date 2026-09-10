package native

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/google/uuid"
	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

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
	SourceCause   message.Cause     `json:"-"`
	SourceContext harness.Context   `json:"-"`
	SessionID     string            `json:"session_id,omitempty"`
	Resumed       bool              `json:"resumed,omitempty"`
	ID            agentproto.WorkID `json:"work_id"`
	// Submitter is the authenticated envelope sender and owns isolation and
	// control checks. Owner is application attribution only.
	Submitter       actor.ActorID              `json:"-"`
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
	Operations      map[string]operationRecord `json:"operations,omitempty"`
	Result          json.RawMessage            `json:"result,omitempty"`
	CreatedAt       int64                      `json:"created_at"`
	UpdatedAt       int64                      `json:"updated_at"`
}

// workTable belongs to this process only. Work receipts, queues, operations and
// waiter associations are never persisted or reconstructed after restart.
type workTable struct {
	Works      map[string]*workRecord
	Order      []string
	NextLooper int
}

func newWorkTable() workTable {
	return workTable{Works: map[string]*workRecord{}}
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

func operationIndexKey(sender actor.ActorID, key string) string {
	return string(sender) + "\x00" + key
}

func submissionIndexKey(sender actor.ActorID, key string) string {
	return string(sender) + "\x00" + key
}

func (s *workTable) findSubmission(sender actor.ActorID, key string) *workRecord {
	if key == "" {
		return nil
	}
	want := submissionIndexKey(sender, key)
	for _, w := range s.Works {
		if submissionIndexKey(w.Submitter, w.SubmissionKey) == want {
			return w
		}
	}
	return nil
}

func (s *workTable) openCount() int {
	n := 0
	for _, w := range s.Works {
		if w.State == agentproto.WorkOpen {
			n++
		}
	}
	return n
}

func (s *workTable) orderedWorks() []*workRecord {
	out := make([]*workRecord, 0)
	for _, id := range s.Order {
		if w := s.Works[id]; w != nil {
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
