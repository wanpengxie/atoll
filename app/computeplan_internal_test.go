package app

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const planValidClass = "test-plan-valid-class"

func init() {
	registry.Register(planValidClass, registry.ClassDecl{Kind: actor.KindAgent})
}

func TestAppPlanProvider_InvalidCompositionRowFailsWholePlan(t *testing.T) {
	cases := []struct {
		name     string
		declID   string
		class    string
		seedDecl bool
	}{
		{name: "missing declaration", declID: "missing", class: planValidClass},
		{name: "unknown class", declID: "decl", class: "test-plan-unknown-class", seedDecl: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if tc.seedDecl {
				if _, err := db.Exec(`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@x','x',1)`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO actor_decls(id,name,owner,default_class,created_at,updated_at) VALUES (?,?,?,?,1,1)`, tc.declID, "d", "u", tc.class); err != nil {
					t.Fatal(err)
				}
			}

			chID := channel.ID("plan-channel")
			a := &App{db: db, homes: map[channel.ID]*home.Home{}}
			h, err := home.Open(home.Config{
				ChannelID: chID, DBPath: filepath.Join(dir, "channel.sqlite"),
				CompositionResolver: compositionResolver{app: a},
				DaemonAuthority:     appDaemonAuthority{app: a},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			if _, _, _, err := h.IntroduceComposition(ctx, storespec.CompositionIntroduce{
				DeclID: tc.declID, Principal: "principal", Class: tc.class,
				Placement: storespec.PlacementDaemon, DesiredHost: "daemon-1",
				Kind: actor.KindAgent, At: 1,
			}); err != nil {
				t.Fatal(err)
			}

			a.homes[chID] = h
			if _, err := (appPlanProvider{app: a}).Plan(ctx, chID, "daemon-1"); err == nil {
				t.Fatal("invalid composition row produced a successful partial plan")
			}
		})
	}
}
