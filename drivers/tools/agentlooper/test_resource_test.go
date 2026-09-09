package agentlooper

import (
	"encoding/json"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

func testContextMessages(session string) []json.RawMessage {
	sharedLooperResources.mu.Lock()
	defer sharedLooperResources.mu.Unlock()
	raw := sharedLooperResources.values[resource.ResourceID("ctx/"+session)]
	var object struct {
		Messages []json.RawMessage `json:"messages"`
	}
	_ = json.Unmarshal(raw, &object)
	return object.Messages
}

type looperTestBase struct{ actorbase.Sys }

func (looperTestBase) Resource() actorbase.ResourceHandle { return sharedLooperResources }

var sharedLooperResources = &looperTestResource{values: map[resource.ResourceID][]byte{}}

type looperTestResource struct {
	mu     sync.Mutex
	values map[resource.ResourceID][]byte
}

func (r *looperTestResource) Create(id resource.ResourceID, b []byte) (accessdoor.Outcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[id] = append([]byte(nil), b...)
	return accessdoor.Outcome{}, nil
}
func (r *looperTestResource) Read(id resource.ResourceID) (accessdoor.Outcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.values[id]
	if !ok {
		return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	return accessdoor.Outcome{Found: true, Value: append([]byte(nil), b...)}, nil
}
func (r *looperTestResource) Write(id resource.ResourceID, b []byte) (accessdoor.Outcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.values[id]; !ok {
		return accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	r.values[id] = append([]byte(nil), b...)
	return accessdoor.Outcome{}, nil
}
func (r *looperTestResource) Delete(id resource.ResourceID) (accessdoor.Outcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.values, id)
	return accessdoor.Outcome{}, nil
}
func (*looperTestResource) Stat(resource.ResourceID) (accessdoor.StatResult, error) {
	return accessdoor.StatResult{}, nil
}
func (*looperTestResource) List(accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return accessdoor.ListPage{}, nil
}
func (*looperTestResource) Open(resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}
func (*looperTestResource) CreateFile(resource.ResourceID, bool) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}
func (*looperTestResource) CreateFileDecided(resource.ResourceID, bool) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (*looperTestResource) CreateDirectory(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
