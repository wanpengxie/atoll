package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/coagent-ai/coagent/adapters/device/xhs/cli/internal/xhs"
)

func TestWriteOK(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteOK(&buf, map[string]any{"correlation_id": "01H"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	var got Envelope
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.OK {
		t.Fatal("expected ok=true")
	}
	if got.Error != nil {
		t.Fatalf("error should be nil: %+v", got.Error)
	}
}

func TestWriteErr(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteErr(&buf, "bad", "boom"); err != nil {
		t.Fatalf("write: %v", err)
	}
	var got Envelope
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.OK {
		t.Fatal("expected ok=false")
	}
	if got.Error == nil || got.Error.Code != "bad" || got.Error.Message != "boom" {
		t.Fatalf("envelope error mismatch: %+v", got.Error)
	}
}

func TestWriteErrFrom_CLIError(t *testing.T) {
	var buf bytes.Buffer
	err := NewCLIError("invalid_argument", "title is %s", "required")
	_ = WriteErrFrom(&buf, err)
	if !strings.Contains(buf.String(), `"code":"invalid_argument"`) {
		t.Fatalf("missing CLI code: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "title is required") {
		t.Fatalf("missing message: %s", buf.String())
	}
}

func TestWriteErrFrom_XhsCodeError(t *testing.T) {
	var buf bytes.Buffer
	err := &xhs.CodeError{Code: "daemon_unreachable", Msg: "off"}
	_ = WriteErrFrom(&buf, err)
	if !strings.Contains(buf.String(), `"code":"daemon_unreachable"`) {
		t.Fatalf("missing xhs code: %s", buf.String())
	}
}

func TestWriteErrFrom_PlainError(t *testing.T) {
	var buf bytes.Buffer
	_ = WriteErrFrom(&buf, errors.New("kaboom"))
	if !strings.Contains(buf.String(), `"code":"internal_error"`) {
		t.Fatalf("expected internal_error code: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "kaboom") {
		t.Fatalf("missing wrapped message: %s", buf.String())
	}
}
