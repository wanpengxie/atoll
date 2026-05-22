package catalog_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/server/catalog"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/store"
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
		{ChannelID: "c1", UserID: "u1", MemberActorID: "user:u1", Role: "owner"},
		{ChannelID: "c1", UserID: "u2", MemberActorID: "user:u2", Role: "member"},
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

func TestPlacementHook_OnChannelMembersChanged_FiresOnRouteWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newTestService(t)
	ctx := context.Background()
	ws, err := svc.CreateWorkspace(ctx, catalog.CreateWorkspaceInput{Name: "Demo", OwnerID: "u1"})
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	ch, _, err := svc.CreateChannel(ctx, catalog.CreateChannelInput{
		WorkspaceID: ws.ID,
		Name:        "general",
		CreatorID:   "u1",
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	hook := &recordingPlacementHook{}
	svc.SetPlacementHook(hook)

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("coagent.user", identity.User{ID: "u1"})
		c.Next()
	})
	svc.RegisterRoutes(r.Group("/api"), nil)

	req := httptest.NewRequest(http.MethodPost, "/api/channels/"+ch.ID+"/members", strings.NewReader(`{"user_id":"u2","member_actor_id":"user:u2","role":"member"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST member status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err != nil {
		t.Fatalf("process add transition: %v", err)
	}
	if len(hook.adds) != 1 || hook.adds[0].MemberActorID != "user:u2" || len(hook.removes) != 0 {
		t.Fatalf("hook after add adds=%+v removes=%+v", hook.adds, hook.removes)
	}

	req = httptest.NewRequest(http.MethodDelete, "/api/channels/"+ch.ID+"/members/u2", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE member status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err != nil {
		t.Fatalf("process remove transition: %v", err)
	}
	if len(hook.removes) != 1 || hook.removes[0] != "user:u2" {
		t.Fatalf("hook removes=%+v", hook.removes)
	}
}

func TestCatalogMember_DaemonOffline_QueuesTransitionOutbox(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, ch := setupMemberRouteTest(t)
	hook := &recordingPlacementHook{fail: true}
	svc.SetPlacementHook(hook)

	r := routeWithUser(svc, "u1")
	req := httptest.NewRequest(http.MethodPost, "/api/channels/"+ch.ID+"/members", strings.NewReader(`{"user_id":"u2","member_actor_id":"user:u2"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST member status=%d body=%s", rec.Code, rec.Body.String())
	}
	if n, err := svc.PendingMemberTransitionCount(context.Background()); err != nil || n != 1 {
		t.Fatalf("pending transitions=%d err=%v want 1", n, err)
	}
	if _, err := svc.ProcessDueMemberTransitions(context.Background(), 10); err == nil {
		t.Fatal("ProcessDueMemberTransitions err=nil want offline mirror failure")
	}
	if n, err := svc.PendingMemberTransitionCount(context.Background()); err != nil || n != 1 {
		t.Fatalf("pending transitions after failure=%d err=%v want 1", n, err)
	}
}

func TestMemberTransitionOutbox_RetryAfterDaemonReconnect(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	base := time.UnixMilli(1_000)
	now := base
	svc.WithClock(func() time.Time { return now })
	ws, err := svc.CreateWorkspace(ctx, catalog.CreateWorkspaceInput{Name: "Demo", OwnerID: "u1"})
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	ch, _, err := svc.CreateChannel(ctx, catalog.CreateChannelInput{WorkspaceID: ws.ID, Name: "general", CreatorID: "u1"})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	hook := &recordingPlacementHook{fail: true}
	svc.SetPlacementHook(hook)
	if _, err := svc.AddChannelMember(ctx, ch.ID, catalog.NewMember{UserID: "u2", MemberActorID: "user:u2"}); err != nil {
		t.Fatalf("AddChannelMember: %v", err)
	}
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err == nil {
		t.Fatal("first process err=nil want offline failure")
	}
	hook.fail = false
	now = base.Add(3 * time.Minute)
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err != nil {
		t.Fatalf("retry process: %v", err)
	}
	if n, err := svc.PendingMemberTransitionCount(ctx); err != nil || n != 0 {
		t.Fatalf("pending after retry=%d err=%v want 0", n, err)
	}
	if len(hook.adds) != 1 || hook.adds[0].MemberActorID != "user:u2" {
		t.Fatalf("hook adds=%+v want retried mirror", hook.adds)
	}
}

func TestCatalogMember_RetrySafe_DoesNotDuplicateMirror(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	ws, err := svc.CreateWorkspace(ctx, catalog.CreateWorkspaceInput{Name: "Demo", OwnerID: "u1"})
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	ch, _, err := svc.CreateChannel(ctx, catalog.CreateChannelInput{WorkspaceID: ws.ID, Name: "general", CreatorID: "u1"})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	hook := &recordingPlacementHook{}
	svc.SetPlacementHook(hook)
	if _, err := svc.AddChannelMember(ctx, ch.ID, catalog.NewMember{UserID: "u2", MemberActorID: "user:u2"}); err != nil {
		t.Fatalf("AddChannelMember: %v", err)
	}
	if _, err := svc.AddChannelMember(ctx, ch.ID, catalog.NewMember{UserID: "u2", MemberActorID: "user:u2"}); !errors.Is(err, catalog.ErrMemberExists) {
		t.Fatalf("duplicate AddChannelMember err=%v want ErrMemberExists", err)
	}
	if n, err := svc.PendingMemberTransitionCount(ctx); err != nil || n != 1 {
		t.Fatalf("pending transitions=%d err=%v want 1", n, err)
	}
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err != nil {
		t.Fatalf("process transitions: %v", err)
	}
	if len(hook.adds) != 1 {
		t.Fatalf("hook adds=%d want exactly one mirror", len(hook.adds))
	}
}

func TestCatalogMember_RemoveSubscriptionRevokedEvenIfDaemonOffline(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	ws, err := svc.CreateWorkspace(ctx, catalog.CreateWorkspaceInput{Name: "Demo", OwnerID: "u1"})
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	ch, _, err := svc.CreateChannel(ctx, catalog.CreateChannelInput{
		WorkspaceID: ws.ID,
		Name:        "general",
		CreatorID:   "u1",
		Members:     []catalog.NewMember{{UserID: "u2", MemberActorID: "user:u2"}},
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	revoker := &recordingRevoker{}
	hook := &recordingPlacementHook{fail: true}
	svc.SetSubscriptionRevoker(revoker)
	svc.SetPlacementHook(hook)
	if err := svc.RemoveChannelMember(ctx, ch.ID, "u2"); err != nil {
		t.Fatalf("RemoveChannelMember: %v", err)
	}
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err == nil {
		t.Fatal("process remove err=nil want daemon mirror failure")
	}
	if revoker.calls != 1 || revoker.channelID != channel.ID(ch.ID) || revoker.userID != "u2" {
		t.Fatalf("revoker=%+v want one u2 revoke", revoker)
	}
	if n, err := svc.PendingMemberTransitionCount(ctx); err != nil || n != 1 {
		t.Fatalf("pending after failed remove=%d err=%v want 1", n, err)
	}
}

type recordingPlacementHook struct {
	adds    []catalog.ChannelMember
	removes []string
	fail    bool
}

func (h *recordingPlacementHook) OnChannelCreated(ctx context.Context, ch catalog.Channel, members []catalog.ChannelMember) error {
	return nil
}

func (h *recordingPlacementHook) OnChannelMembersChanged(ctx context.Context, channelID string, adds []catalog.ChannelMember, removes []string) error {
	if h.fail {
		return errors.New("daemon offline")
	}
	h.adds = append(h.adds, adds...)
	h.removes = append(h.removes, removes...)
	return nil
}

type recordingRevoker struct {
	calls     int
	channelID channel.ID
	userID    string
}

func (r *recordingRevoker) RevokeChannelUser(channelID channel.ID, userID string) {
	r.calls++
	r.channelID = channelID
	r.userID = userID
}

func setupMemberRouteTest(t *testing.T) (*catalog.Service, catalog.Channel) {
	t.Helper()
	svc := newTestService(t)
	ctx := context.Background()
	ws, err := svc.CreateWorkspace(ctx, catalog.CreateWorkspaceInput{Name: "Demo", OwnerID: "u1"})
	if err != nil {
		t.Fatalf("CreateWorkspace: %v", err)
	}
	ch, _, err := svc.CreateChannel(ctx, catalog.CreateChannelInput{WorkspaceID: ws.ID, Name: "general", CreatorID: "u1"})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	return svc, ch
}

func routeWithUser(svc *catalog.Service, userID string) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("coagent.user", identity.User{ID: userID})
		c.Next()
	})
	svc.RegisterRoutes(r.Group("/api"), nil)
	return r
}
