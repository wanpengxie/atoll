package claudecode

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// TestNewDecl_ResumeContract pins claude's daemon-side build/resume contract
// (daemon-composition spec §2·5; codex's "don't ASSUME the SDK auto-resumes from
// workdir" concern). The parts we OWN and therefore test:
//
//   - build is SEED-OPTIONAL: NewDecl succeeds with NO state (fresh start) AND
//     with a state seed (resume). Resume is driven by the EXPLICIT State.Seed the
//     daemon persists locally (cmd/daemon localStateSlot) → State.Seed wired to
//     cfg.ResumeSeed, State.Store to cfg.Checkpoint — NOT implicit-workdir magic.
//   - build is CREDS-INDEPENDENT: no ANTHROPIC_API_KEY / login is read at BUILD
//     (the claude CLI carries its own auth at RUN time, see agent.go EnvKeys), so
//     a daemon builds the assigned claude with only the user's LOCAL login.
func TestNewDecl_ResumeContract(t *testing.T) {
	base := registry.Deps{ChannelID: "c1", WorkspaceDir: t.TempDir()}
	spec := registry.InstanceSpec{ID: actor.ActorID("agent:rev")}

	// (a) no state / no seed → fresh start, builds fine (no creds in env).
	decl, err := NewDecl(spec, base)
	if err != nil {
		t.Fatalf("NewDecl with no state should build (fresh start): %v", err)
	}
	if decl.Kind != actor.KindAgent {
		t.Fatalf("claude decl Kind = %v, want agent", decl.Kind)
	}
	if decl.ID != spec.ID {
		t.Fatalf("claude decl ID = %v, want %v", decl.ID, spec.ID)
	}

	// (b) with a local state seed → resume path also builds fine.
	seeded := base
	seeded.State = registry.StateSlot{
		Dir:   t.TempDir(),
		Seed:  json.RawMessage(`"claude-session-xyz"`),
		Store: func(json.RawMessage) error { return nil },
	}
	if _, err := NewDecl(spec, seeded); err != nil {
		t.Fatalf("NewDecl with a resume seed should build: %v", err)
	}
}

// TestNewDecl_RequiresChannelAndID pins WHY the daemon must build from an
// EXPLICIT assignment (not a blind-build of registry.Classes() with empty specs):
// claude refuses an empty id — exactly the fatal the old "one of each" loop would
// hit once agents were compiled into the daemon (daemon-composition spec §1).
func TestNewDecl_RequiresChannelAndID(t *testing.T) {
	if _, err := NewDecl(registry.InstanceSpec{ID: "agent:x"}, registry.Deps{}); err == nil {
		t.Fatal("NewDecl with empty ChannelID should error")
	}
	if _, err := NewDecl(registry.InstanceSpec{}, registry.Deps{ChannelID: "c1"}); err == nil {
		t.Fatal("NewDecl with empty instance id should error (no blind-build)")
	}
}
