package driverproto

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/runtime/harness"
)

// The from line is the model's only statement of who is asking, and the seed
// segment means two different things by kind. Labelling an agent's declaration
// "principal" would tell the model a declaration is a login account; labelling
// a human's principal "declaration" hides the one fact that is stable across
// channels. Either way the model reasons about the sender from a false premise.
func TestCallerLineNamesSeedByKind(t *testing.T) {
	human := CallerLine(harness.Caller{Channel: "c0", Actor: "human:root:1787128257816"})
	for _, want := range []string{"channel=c0", "kind=human", "principal=root", "actor=human:root:1787128257816"} {
		if !strings.Contains(human, want) {
			t.Errorf("human from line missing %q: %s", want, human)
		}
	}
	if strings.Contains(human, "declaration=") {
		t.Errorf("human principal labelled as a declaration: %s", human)
	}

	agent := CallerLine(harness.Caller{Channel: "c0", Actor: "agent:steward-decl:1787128257816"})
	if !strings.Contains(agent, "declaration=steward-decl") {
		t.Errorf("agent declaration not labelled: %s", agent)
	}
	if strings.Contains(agent, "principal=") {
		t.Errorf("agent declaration labelled as a principal: %s", agent)
	}

	// The fixed system actor has no segments; it must still be nameable.
	system := CallerLine(harness.Caller{Channel: "c0", Actor: "system"})
	if !strings.Contains(system, "actor=system") || strings.Contains(system, "kind=") {
		t.Errorf("system caller line invented segments it does not have: %s", system)
	}
}

func TestResolveAttachmentRequiresThisDeviceAndWorkspace(t *testing.T) {
	self := Situation{DeviceID: "device-id", DeviceName: "local-device", Channel: "project-id", WorkspaceDir: "/var/atoll/channels/c0.proj"}
	local := Attachment{Address: "daemon://local-device/c0.proj/uploads/%E7%A0%94%E7%A9%B6-%E6%96%87%E6%A1%A3.md", Name: "研究 文档.md"}
	if got := ResolveAttachment(local, self); got.LocalPath != "uploads/研究-文档.md" {
		t.Fatalf("local path=%q, want cwd-relative path", got.LocalPath)
	}
	foreign := local
	foreign.Address = "daemon://other-device/c0.proj/uploads/研究-文档.md"
	if got := ResolveAttachment(foreign, self); got.LocalPath != "" {
		t.Fatalf("foreign local path=%q, want empty", got.LocalPath)
	}
	wrongChannel := local
	wrongChannel.Address = "daemon://local-device/c0.other/uploads/研究-文档.md"
	if got := ResolveAttachment(wrongChannel, self); got.LocalPath != "" {
		t.Fatalf("wrong-channel local path=%q, want empty", got.LocalPath)
	}
}

func TestAttachmentLinesPreserveOriginalNameAndReportForeignAddress(t *testing.T) {
	self := Situation{DeviceID: "device-id", DeviceName: "local-device", Channel: "project-id", WorkspaceDir: "/var/atoll/channels/c0.proj"}
	atts := []Attachment{
		{Address: "daemon://local-device/c0.proj/uploads/%E7%A0%94%E7%A9%B6-%E6%96%87%E6%A1%A3.md", Name: "研究 文档.md"},
		{Address: "daemon://other-device/c0.proj/uploads/draft.pdf", Name: `draft "one".pdf`},
	}
	want := "[附件 name=\"研究 文档.md\" path=\"uploads/研究-文档.md\"]\n" +
		`[附件 name="draft \"one\".pdf" path="daemon://other-device/c0.proj/uploads/draft.pdf" note="not on this device"]`
	if got := AttachmentLines(atts, self); got != want {
		t.Fatalf("attachment lines:\n%s\nwant:\n%s", got, want)
	}
	if got := AttachmentLines(nil, self); got != "" {
		t.Fatalf("empty attachment lines=%q", got)
	}
	unnamed := AttachmentLines([]Attachment{{Address: "daemon://local-device/c0.proj/uploads/note.txt"}}, self)
	if unnamed != `[附件 path="uploads/note.txt"]` {
		t.Fatalf("unnamed attachment line=%q", unnamed)
	}
}
