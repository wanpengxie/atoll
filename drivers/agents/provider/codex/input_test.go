package codex

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestInputHasNoProviderCharacterCeiling(t *testing.T) {
	text := strings.Repeat("🙂", (1<<20)+1)
	got := buildInput([]driverproto.DriverMessage{{Text: text}}, nil, driverproto.Situation{})
	if len(got) != 1 || got[0]["text"] != text {
		t.Fatal("large input was not preserved")
	}
}

func TestBuildInputAppendsAttachmentLineToTextBlock(t *testing.T) {
	self := driverproto.Situation{DeviceID: "device-id", DeviceName: "local-device", Channel: "project-id", WorkspaceDir: "/var/atoll/channels/c0.proj"}
	message := driverproto.DriverMessage{
		Text: "please read",
		Attachments: []driverproto.Attachment{{
			Address: "daemon://local-device/c0.proj/uploads/research.md",
			Name:    "研究 文档.md",
		}},
	}
	got := buildInput([]driverproto.DriverMessage{message}, nil, self)
	want := "please read\n[附件 name=\"研究 文档.md\" path=\"uploads/research.md\"]"
	if len(got) != 1 || got[0]["type"] != "text" || got[0]["text"] != want {
		t.Fatalf("input=%+v, want attachment in text block", got)
	}
}
