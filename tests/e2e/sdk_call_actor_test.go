//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/pkg/coagentsdk"
	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

func TestE2E_SDKCallActor_XHSPublishScaffold(t *testing.T) {
	s := harness.Start(t, harness.Options{UseScaffoldXHS: true})

	email := "sdkcall+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-sdkcall-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-sdkcall-"+uniqSuffix(), "xhs-creator")
	s.BindChannel(wsID, chID)

	client := &coagentsdk.Client{
		BaseURL:      s.ServerURLBase(),
		SessionToken: s.SessionToken(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
		ChannelID: chID,
		ActorID:   "tool:xhs-adapter",
		Type:      "xhs.publish",
		Payload: json.RawMessage(`{
			"title": "sdk hello",
			"content": "sdk world"
		}`),
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if !res.OK {
		t.Fatalf("CallActor OK=false: %+v", res.Error)
	}
	var data struct {
		NoteID string `json:"note_id"`
		URL    string `json:"url"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatalf("decode result data: %v raw=%s", err, string(res.Data))
	}
	if data.NoteID != "mock-note-1" {
		t.Fatalf("note_id=%q want mock-note-1 (data=%s)", data.NoteID, string(res.Data))
	}
	if data.URL == "" {
		t.Fatalf("url empty (data=%s)", string(res.Data))
	}
	if data.Status != "" {
		t.Fatalf("status leaked into Data: %s", string(res.Data))
	}
}
