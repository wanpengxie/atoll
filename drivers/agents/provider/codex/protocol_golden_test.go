package codex

import (
	"reflect"
	"slices"
	"testing"
)

func TestInitializeOptsOutAllDeltaNotificationMethods(t *testing.T) {
	want := []string{"item/agentMessage/delta", "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/plan/delta", "item/reasoning/summaryTextDelta", "item/reasoning/textDelta", "command/exec/outputDelta", "process/outputDelta", "thread/realtime/outputAudio/delta", "thread/realtime/transcript/delta"}
	if got := deltaNotificationMethods(); !reflect.DeepEqual(got, want) {
		t.Fatalf("methods=%v", got)
	}
}
func TestRequiredMethodsAndFieldsGolden(t *testing.T) {
	want := map[string][]string{
		"initialize":                            {"capabilities.optOutNotificationMethods", "clientInfo.name", "clientInfo.title", "clientInfo.version", "result.userAgent"},
		"initialized":                           {},
		"thread/start":                          {"approvalPolicy", "cwd", "result.thread.id", "sandbox"},
		"thread/resume":                         {"excludeTurns", "result.thread.id", "threadId"},
		"turn/start":                            {"input", "threadId"},
		"turn/steer":                            {"expectedTurnId", "input", "threadId"},
		"turn/interrupt":                        {"threadId", "turnId"},
		"turn/started":                          {"threadId", "turn.id"},
		"turn/completed":                        {"threadId", "turn.error.additionalDetails", "turn.error.message", "turn.id", "turn.status"},
		"item/started":                          {"item.id", "item.status", "item.tool", "item.type", "threadId", "turnId"},
		"item/completed":                        {"item.aggregatedOutput", "item.command", "item.id", "item.status", "item.text", "item.tool", "item.type", "threadId", "turnId"},
		"error":                                 {"error.message", "threadId", "turnId", "willRetry"},
		"currentTime/read":                      {},
		"item/commandExecution/requestApproval": {"result.decision"},
		"item/fileChange/requestApproval":       {"result.decision"},
		"item/permissions/requestApproval":      {"error.code", "error.message"},
		"execCommandApproval":                   {"result.decision"},
		"applyPatchApproval":                    {"result.decision"},
	}
	got := requiredProtocolSurface()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("protocol surface=\n%#v\nwant=\n%#v", got, want)
	}
}

func requiredProtocolSurface() map[string][]string {
	surface := map[string][]string{
		"initialize":                            {"clientInfo.name", "clientInfo.title", "clientInfo.version", "capabilities.optOutNotificationMethods", "result.userAgent"},
		"initialized":                           {},
		"thread/start":                          {"approvalPolicy", "sandbox", "cwd", "result.thread.id"},
		"thread/resume":                         {"threadId", "excludeTurns", "result.thread.id"},
		"turn/start":                            {"threadId", "input"},
		"turn/steer":                            {"threadId", "expectedTurnId", "input"},
		"turn/interrupt":                        {"threadId", "turnId"},
		"turn/started":                          {"threadId", "turn.id"},
		"turn/completed":                        {"threadId", "turn.id", "turn.status", "turn.error.message", "turn.error.additionalDetails"},
		"item/started":                          {"threadId", "turnId", "item.id", "item.type", "item.tool", "item.status"},
		"item/completed":                        {"threadId", "turnId", "item.id", "item.type", "item.text", "item.tool", "item.command", "item.status", "item.aggregatedOutput"},
		"error":                                 {"threadId", "turnId", "willRetry", "error.message"},
		"currentTime/read":                      {},
		"item/commandExecution/requestApproval": {"result.decision"},
		"item/fileChange/requestApproval":       {"result.decision"},
		"item/permissions/requestApproval":      {"error.code", "error.message"},
		"execCommandApproval":                   {"result.decision"},
		"applyPatchApproval":                    {"result.decision"},
	}
	for _, fields := range surface {
		slices.Sort(fields)
	}
	return surface
}
