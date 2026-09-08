package agentcontext

import (
	"encoding/json"
	"strings"
	"testing"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/registry"
)

func TestBuildUserMessagePreservesSpeakerOriginAndAttachments(t *testing.T) {
	in := agentloop.Input{
		ID: "i-1", Seq: 7, Text: "read this",
		CallerChannel: "calling-channel", CallerActor: "human:root:123",
		Origin:      &agentproto.Origin{Session: "browser-1", Label: "Mac Chrome"},
		Attachments: []json.RawMessage{json.RawMessage(`{"resource_id":"resource-1","address":"daemon://local-device/c0.agent/uploads/%E7%A0%94%E7%A9%B6.md","name":"研究.md","media_type":"text/markdown"}`)},
	}
	message := buildUserMessage(in, registry.Deps{DeviceName: "local-device", WorkspaceDir: "/tmp/c0.agent"}, 99)
	content, _ := message["content"].(string)
	for _, want := range []string{
		"[origin session=browser-1 Mac Chrome]",
		"[from channel=calling-channel kind=human principal=root actor=human:root:123]",
		"read this",
		`[attachment name="研究.md" media_type="text/markdown" path="uploads/研究.md"]`,
	} {
		if !strings.Contains(content, want) {
			t.Errorf("content missing %q:\n%s", want, content)
		}
	}
	metadata := message["metadata"].(map[string]any)
	if got := metadata["input_seq"]; got != int64(7) {
		t.Fatalf("input_seq=%v", got)
	}
	if got := metadata["attachments"].([]json.RawMessage); len(got) != 1 || string(got[0]) != string(in.Attachments[0]) {
		t.Fatalf("attachments=%s", got)
	}
}

func TestAttachmentLineDoesNotTurnForeignOrEscapingAddressIntoLocalPath(t *testing.T) {
	deps := registry.Deps{DeviceName: "local-device", WorkspaceDir: "/tmp/c0.agent"}
	for _, address := range []string{
		"daemon://other-device/c0.agent/uploads/a.md",
		"daemon://local-device/c0.other/uploads/a.md",
		"daemon://local-device/c0.agent/uploads/%2Fetc/passwd",
		"daemon://local-device/c0.agent/uploads/../secret",
	} {
		if path, ok := localAttachmentPath(address, deps); ok {
			t.Errorf("address %q resolved to %q", address, path)
		}
	}
}
