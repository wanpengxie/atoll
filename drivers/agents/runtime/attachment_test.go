package runtime

import (
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

func TestToDriverAttachmentsCarriesAddressAndName(t *testing.T) {
	got := toDriverAttachments([]runtimeproto.Attachment{{Address: "daemon://local/c0/uploads/report.md", Name: "Report Draft.md"}})
	if len(got) != 1 || got[0].Address != "daemon://local/c0/uploads/report.md" || got[0].Name != "Report Draft.md" {
		t.Fatalf("attachments=%+v", got)
	}
	if got[0].LocalPath != "" {
		t.Fatalf("runtime parsed local path %q; resolution belongs with Situation", got[0].LocalPath)
	}
}
