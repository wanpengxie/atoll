package archtest

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestLinkSeamRetiredSymbolsAbsent(t *testing.T) {
	retired := []string{
		"ComputeID", "BoundID", "PlanSink", "PrepareHandshakeObserved",
		"CommitWhile", "Caps" + "Factory(", "full" + "Caps", "reconcile" + "Host",
		"func OpenDB(", "CompositionResolver != nil", "cfg.Desired", "cfg.Builder",
		"submitControlThroughDoor", "controlRequestTimeout", "handleListActors",
		"handleActorStatus", "handleChannelPresenceDrops", "handleCursor",
		"handleListMessages", "handleRestartDecl",
		"handleRemoveActor", "handleSetDefaultAgent", "type daemonAssignment struct",
		`"/actor-decls/:declID/restart"`,
	}
	for _, root := range []string{"../app", "../cmd", "../platform", "../runtime"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, err := readFile(path)
			if err != nil {
				return err
			}
			for _, symbol := range retired {
				if strings.Contains(string(b), symbol) {
					t.Errorf("%s contains retired link-seam symbol %q", filepath.ToSlash(path), symbol)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinkSeamRetiredSymbolGuardFixtures(t *testing.T) {
	retired := []struct {
		pattern string
		fixture string
	}{
		{"Compute" + "ID", "type request struct { ComputeID string }"},
		{"Bound" + "ID", "type pull struct { BoundID string }"},
		{"Plan" + "Sink", "var sink PlanSink"},
		{"PrepareHandshake" + "Observed", "runtime.PrepareHandshakeObserved()"},
		{"Commit" + "While", "prepared.CommitWhile()"},
		{"Caps" + "Factory(", "CapsFactory(func() {})"},
		{"full" + "Caps", "factory.fullCaps"},
		{"reconcile" + "Host", "acceptor.reconcileHost()"},
		{"func Open" + "DB(", "func OpenDB(path string) {}"},
		{"CompositionResolver != nil", "if cfg.CompositionResolver != nil {}"},
		{"cfg.Desired", "use(cfg.Desired)"},
		{"cfg.Builder", "use(cfg.Builder)"},
		{"submitControlThroughDoor", "submitControlThroughDoor(ctx)"},
		{"controlRequestTimeout", "app.controlRequestTimeout"},
		{"handleListActors", "app.handleListActors(ctx)"},
		{"type daemonAssignment struct", "type daemonAssignment struct{}"},
		{`"/actor-decls/:declID/restart"`, `router.POST("/actor-decls/:declID/restart", handler)`},
	}
	for _, tc := range retired {
		if !strings.Contains(tc.fixture, tc.pattern) {
			t.Errorf("guard pattern %q does not trip its fixture %q", tc.pattern, tc.fixture)
		}
	}
}
