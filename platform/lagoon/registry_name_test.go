package lagoon

import (
	"testing"

	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestQualifiedChannelNamesUseOneDottedSpelling(t *testing.T) {
	rows, err := qualifyChannelRows([]regspec.ChannelRow{
		{ID: "backend-id", ParentID: "ops-id", Name: "backend"},
		{ID: "c0", Name: "c0"},
		{ID: "ops-id", ParentID: "c0", Name: "ops"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[channel.ID]string{"c0": "c0", "ops-id": "c0.ops", "backend-id": "c0.ops.backend"}
	for _, row := range rows {
		if row.QualifiedName != want[row.ID] {
			t.Errorf("channel %s qualified name=%q, want %q", row.ID, row.QualifiedName, want[row.ID])
		}
	}
}
