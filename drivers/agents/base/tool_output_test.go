package base

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type outputWriteHandle struct {
	bytes.Buffer
	committed bool
	aborted   bool
}

func (w *outputWriteHandle) Commit() error { w.committed = true; return nil }
func (w *outputWriteHandle) Abort() error  { w.aborted = true; return nil }

type outputResources struct {
	directories []resource.ResourceID
	file        resource.ResourceID
	writer      *outputWriteHandle
	fail        bool
}

func (r *outputResources) Create(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (r *outputResources) Read(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (r *outputResources) Write(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (r *outputResources) Delete(resource.ResourceID) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (r *outputResources) Stat(resource.ResourceID) (accessdoor.StatResult, error) {
	return accessdoor.StatResult{}, nil
}
func (r *outputResources) List(accessdoor.ListQuery) (accessdoor.ListPage, error) {
	return accessdoor.ListPage{}, nil
}
func (r *outputResources) Open(resource.ResourceID, access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, nil
}
func (r *outputResources) CreateFile(id resource.ResourceID, _ bool) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	if r.fail {
		return accessdoor.FileAccess{}, accessdoor.Outcome{RejectReason: access.DriverError}, nil
	}
	r.file = id
	r.writer = &outputWriteHandle{}
	return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Write: r.writer}}, accessdoor.Outcome{}, nil
}
func (r *outputResources) CreateFileDecided(resource.ResourceID, bool) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, nil
}
func (r *outputResources) CreateDirectory(id resource.ResourceID) (accessdoor.Outcome, error) {
	r.directories = append(r.directories, id)
	return accessdoor.Outcome{}, nil
}

func TestPrepareOversizedToolOutputWritesExactJSONAndReturnsPointer(t *testing.T) {
	raw := json.RawMessage(`{"status":"completed","name":"image","result":"` + strings.Repeat("a", 4000) + `"}`)
	resources := &outputResources{}
	value, err := prepareOversizedToolOutput(resources, "local-device", "dev-channel", "/srv/channels/c0.dev", raw, 1024)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		External externalJSONRecord `json:"external_json"`
		Project  json.RawMessage    `json:"projection"`
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !got.External.Stored || got.External.Path == "" || !strings.HasPrefix(got.External.ResourceID, "daemon://local-device/dev-channel/.atoll/outputs/") {
		t.Fatalf("external record = %+v", got.External)
	}
	if got.External.OriginalBytes != len(raw) || len(got.External.SHA256) != 64 {
		t.Fatalf("external metadata = %+v", got.External)
	}
	if resources.writer == nil || !resources.writer.committed || resources.writer.aborted || !bytes.Equal(resources.writer.Bytes(), raw) {
		t.Fatal("channel file did not commit the exact original JSON")
	}
	if len(got.Project) > 1024 || !json.Valid(got.Project) || !strings.Contains(string(got.Project), `"status":"completed"`) {
		t.Fatalf("projection = %s", got.Project)
	}
	if len(resources.directories) != 2 {
		t.Fatalf("directories = %v", resources.directories)
	}
}

func TestPrepareOversizedToolOutputStorageFailureStillReturnsBoundedProjection(t *testing.T) {
	raw := json.RawMessage(`{"status":"completed","result":"` + strings.Repeat("a", 4000) + `"}`)
	value, storageErr := prepareOversizedToolOutput(&outputResources{fail: true}, "local-device", "dev-channel", "/srv/channels/c0.dev", raw, 1024)
	if storageErr == nil {
		t.Fatal("storage failure was swallowed")
	}
	encoded, _ := json.Marshal(value)
	var got struct {
		External externalJSONRecord `json:"external_json"`
		Project  json.RawMessage    `json:"projection"`
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.External.Stored || got.External.Reason != "channel_file_write_failed" {
		t.Fatalf("external record = %+v", got.External)
	}
	if len(got.Project) > 1024 || !json.Valid(got.Project) {
		t.Fatalf("projection = %s", got.Project)
	}
}

func TestPrepareToolOutputKeepsTenMiBAndBelowInline(t *testing.T) {
	raw := json.RawMessage(`{"status":"completed"}`)
	l := &agentLoop{}
	got, ok := l.prepareToolOutput(raw).(json.RawMessage)
	if !ok || !bytes.Equal(got, raw) {
		t.Fatalf("inline output = %#v", got)
	}
}
