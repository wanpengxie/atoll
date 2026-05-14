package catalog_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/coagent/server/catalog"
	"github.com/coagent-ai/coagent/server/store"
)

func newTestService(t *testing.T) *catalog.Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "c.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Insert a placeholder user row so FK constraints pass.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO users (id, email, password_hash, display_name, email_verified, created_at)
		 VALUES ('u1','u1@example.com','hash','U1',1,0),
		        ('u2','u2@example.com','hash','U2',1,0),
		        ('u3','u3@example.com','hash','U3',1,0)`,
	); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	return catalog.NewService(db)
}

func TestCreateWorkspaceAndChannel(t *testing.T) {
	t.Parallel()
	svc := newTestService(t)
	ctx := context.Background()

	ws, err := svc.CreateWorkspace(ctx, catalog.CreateWorkspaceInput{Name: "Demo", OwnerID: "u1"})
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	if ws.Name != "Demo" {
		t.Errorf("name=%q want Demo", ws.Name)
	}

	got, err := svc.GetWorkspace(ctx, ws.ID, "u1")
	if err != nil {
		t.Fatalf("GetWorkspace: %v", err)
	}
	if got.ID != ws.ID {
		t.Errorf("got.ID=%q want %q", got.ID, ws.ID)
	}

	if _, err := svc.GetWorkspace(ctx, ws.ID, "u2"); err != catalog.ErrNotWorkspaceMember {
		t.Errorf("GetWorkspace as outsider err=%v want ErrNotWorkspaceMember", err)
	}

	ch, members, err := svc.CreateChannel(ctx, catalog.CreateChannelInput{
		WorkspaceID: ws.ID,
		Name:        "general",
		Type:        "group",
		CreatorID:   "u1",
		Members:     []catalog.NewMember{{UserID: "u2"}},
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if got, want := len(members), 2; got != want {
		t.Errorf("members count=%d want %d", got, want)
	}
	// creator first
	if members[0].UserID != "u1" || members[0].Role != "owner" {
		t.Errorf("owner row %+v", members[0])
	}
	if members[1].UserID != "u2" || members[1].Role != "member" {
		t.Errorf("member row %+v", members[1])
	}

	// outsider can't list channels
	if _, err := svc.ListChannels(ctx, ws.ID, "u3"); err != catalog.ErrNotWorkspaceMember {
		t.Errorf("outsider list err=%v want ErrNotWorkspaceMember", err)
	}

	// add u3 as workspace member via direct insert (catalog doesn't
	// expose AddWorkspaceMember in T6 minimal API — sufficient to add
	// user as channel-only via daemonbus flow). Skip the workspace-
	// add path here; just ensure u3 isn't a channel member.
	if _, err := svc.GetChannelMember(ctx, ch.ID, "u3"); err != catalog.ErrNotChannelMember {
		t.Errorf("u3 channel-member err=%v want ErrNotChannelMember", err)
	}

	// Add u2 again — should hit ErrMemberExists.
	if _, err := svc.AddChannelMember(ctx, ch.ID, catalog.NewMember{UserID: "u2"}); err != catalog.ErrMemberExists {
		t.Errorf("dup add err=%v want ErrMemberExists", err)
	}

	// Remove + re-add.
	if err := svc.RemoveChannelMember(ctx, ch.ID, "u2"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := svc.GetChannelMember(ctx, ch.ID, "u2"); err != catalog.ErrNotChannelMember {
		t.Errorf("post-remove err=%v", err)
	}
}

func TestInitialMembersFor(t *testing.T) {
	t.Parallel()
	members := []catalog.ChannelMember{
		{ChannelID: "c1", UserID: "u1", ActorIDInChannel: "user:u1", Role: "owner"},
		{ChannelID: "c1", UserID: "u2", ActorIDInChannel: "user:u2", Role: "member"},
	}
	out := catalog.InitialMembersFor(members, func(uid string) string { return "Display:" + uid })
	if len(out) != 2 {
		t.Fatalf("len=%d want 2", len(out))
	}
	if out[0].Kind != "human" {
		t.Errorf("kind=%q want human", out[0].Kind)
	}
	if out[0].DisplayName != "Display:u1" {
		t.Errorf("display=%q", out[0].DisplayName)
	}
}
