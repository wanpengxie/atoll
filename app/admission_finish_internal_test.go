package app

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestAdmissionFinishWarnsWhenPendingStateWasLost(t *testing.T) {
	a, _ := newLifecycleTestApp(t, 0)
	var logs bytes.Buffer
	a.logger = slog.New(slog.NewTextHandler(&logs, nil))
	svc := newAdmissionService(a)

	err := svc.finish(context.Background(), admissionRecord{OperationID: "missing-operation"}, "done", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "admission finish lost pending state") {
		t.Fatalf("zero-row finish did not emit warning: %s", logs.String())
	}
}
