package engineboot

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

func terminalValue(t *testing.T, raw json.RawMessage, out any) {
	t.Helper()
	var terminal struct {
		Status    string          `json:"status"`
		ErrorCode string          `json:"error_code"`
		Detail    string          `json:"detail"`
		Value     json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &terminal); err != nil {
		t.Fatal(err)
	}
	if terminal.Status != message.StatusCompleted {
		t.Fatalf("terminal failed code=%s detail=%s raw=%s", terminal.ErrorCode, terminal.Detail, raw)
	}
	if out != nil {
		if err := json.Unmarshal(terminal.Value, out); err != nil {
			t.Fatalf("decode value: %v raw=%s", err, raw)
		}
	}
}

func TestBootPublishesC0AndLobbyOnly(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password", OpenRegistration: true}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, core := eng.host.Acquire(channelspec.C0ChannelID)
		_, lobby := eng.host.Acquire(channelspec.LobbyChannelID)
		if core && lobby {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("core/lobby not serving")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range []channel.ID{channelspec.C0ChannelID, channelspec.LobbyChannelID} {
		row, ok, err := eng.registry.GetChannelDesired(context.Background(), id)
		if err != nil || !ok {
			t.Fatalf("channel %s row ok=%v err=%v", id, ok, err)
		}
		var spec lagoon.GenesisSpec
		if err := json.Unmarshal(row.Spec, &spec); err != nil || spec.Profile.Description == nil || *spec.Profile.Description != row.Description || spec.Profile.Serving == nil || *spec.Profile.Serving != row.Serving {
			t.Fatalf("channel %s frozen profile=%+v row=(%q,%d) err=%v", id, spec.Profile, row.Description, row.Serving, err)
		}
		if len(spec.Profile.Endpoints) != 0 {
			t.Fatalf("channel %s unexpectedly froze endpoints=%d", id, len(spec.Profile.Endpoints))
		}
		bundle, serving := eng.host.Acquire(id)
		if !serving {
			t.Fatalf("channel %s stopped serving", id)
		}
		roster, err := bundle.View().Roster(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, member := range roster {
			seen[string(member.ID)] = true
			if member.ID == actor.ActorID(string(actor.KindPeer)+":c0") || member.DeclID == "c0" {
				t.Fatalf("channel %s contains forbidden c0 peer: %+v", id, member)
			}
		}
		if id == channelspec.C0ChannelID {
			for _, prefix := range []string{"system:registrar:", "peer:svcactor:", "human:root:", "agent:"} {
				found := false
				for member := range seen {
					found = found || strings.HasPrefix(member, prefix)
				}
				if !found {
					t.Fatalf("c0 roster omitted %q: %v", prefix, seen)
				}
			}
		} else {
			for _, prefix := range []string{"peer:svcactor:", "human:guest:"} {
				found := false
				for member := range seen {
					found = found || strings.HasPrefix(member, prefix)
				}
				if !found {
					t.Fatalf("lobby roster omitted %q: %v", prefix, seen)
				}
			}
			for member := range seen {
				if strings.HasPrefix(member, "human:root:") {
					t.Fatalf("lobby contains root: %v", seen)
				}
			}
		}
	}
}

func callMember(t *testing.T, ch channel.ID, bundle channelhost.Bundle, principal string, target actor.ActorID, word string, payload any) json.RawMessage {
	t.Helper()
	if message.IsSpaceWord(word) {
		target = actor.SystemActorID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sender, ok, err := bundle.View().ResolvePrincipal(ctx, principal)
	if err != nil || !ok {
		t.Fatalf("resolve %s: %v %v", principal, ok, err)
	}
	slot, ok := bundle.Gateway().SubjectSlotFor(sender)
	if !ok {
		t.Fatal("subject slot missing")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	id := message.ID(uuid.NewString())
	frame, err := subjectgate.NewFrame(subjectgate.FrameSubmit, uuid.NewString(), subjectgate.SubmitPayload{ChannelID: string(ch), ID: string(id), MsgType: word, Kind: string(message.KindRequest), Audience: []string{string(target)}, Visibility: string(message.VisibilityPublic), Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slot.Deliver(ctx, frame); err != nil {
		t.Fatal(err)
	}
	var cursor int64
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, next, err := bundle.View().ReadVisibleAfterSeq(ctx, cursor, 256)
		if err != nil {
			t.Fatal(err)
		}
		cursor = next
		for _, row := range rows {
			if row.IsTerminal && row.Envelope.ParentID == id {
				return row.Envelope.Payload
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func onlyDecl(t *testing.T, bundle channelhost.Bundle, id string) actor.ActorID {
	t.Helper()
	roster, err := bundle.View().Roster(context.Background())
	ids := make([]actor.ActorID, 0, 1)
	for _, row := range roster {
		if row.DeclID == id {
			ids = append(ids, row.ID)
		}
	}
	if err != nil || len(ids) != 1 {
		t.Fatalf("decl %s ids=%v err=%v", id, ids, err)
	}
	return ids[0]
}
