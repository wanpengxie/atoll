package codex

import (
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/base"
)

type eventRecord struct {
	kind, turn, text, detail string
	op                       base.OpID
	verdict                  base.ControlVerdict
	status                   base.TurnStatus
	cause                    base.LostCause
}

type recordingEvents struct {
	mu      sync.Mutex
	records []eventRecord
	persist map[string][]byte
}

func (r *recordingEvents) add(v eventRecord) {
	r.mu.Lock()
	r.records = append(r.records, v)
	r.mu.Unlock()
}
func (r *recordingEvents) TurnStarted(op base.OpID, turn string) {
	r.add(eventRecord{kind: "started", op: op, turn: turn})
}
func (r *recordingEvents) TurnRejected(op base.OpID, code, detail string) {
	r.add(eventRecord{kind: "rejected", op: op, text: code, detail: detail})
}
func (r *recordingEvents) Tool(turn, call, phase, name, status, detail string) {
	r.add(eventRecord{kind: "tool:" + phase, turn: turn, text: call + ":" + name + ":" + status, detail: detail})
}
func (r *recordingEvents) TurnEnded(turn string, status base.TurnStatus, text, detail string) {
	r.add(eventRecord{kind: "ended", turn: turn, status: status, text: text, detail: detail})
}
func (r *recordingEvents) ControlDone(op base.OpID, verdict base.ControlVerdict, turn, detail string) {
	r.add(eventRecord{kind: "control", op: op, verdict: verdict, turn: turn, detail: detail})
}
func (r *recordingEvents) ProviderLost(cause base.LostCause, detail string) {
	r.add(eventRecord{kind: "lost", cause: cause, detail: detail})
}
func (r *recordingEvents) Persist(key string, value []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.persist == nil {
		r.persist = map[string][]byte{}
	}
	r.persist[key] = append([]byte(nil), value...)
}

func (r *recordingEvents) snapshot() []eventRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]eventRecord(nil), r.records...)
}

var _ base.EventPort = (*recordingEvents)(nil)
var _ base.BootPort = (*recordingEvents)(nil)
