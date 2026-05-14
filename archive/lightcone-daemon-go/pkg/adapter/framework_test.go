package adapter

import (
	"strings"
	"testing"
)

// TestDeclarationValidate spans the structural checks Validate enforces.
func TestDeclarationValidate(t *testing.T) {
	good := Declaration{
		Name:         "demo",
		ActorID:      "tool:demo",
		Types:        []string{"demo.echo", "demo.ping"},
		Binding:      "daemon_rpc",
		MaxPendingMs: map[string]int64{"demo.echo": 1, "demo.ping": 1},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate(good) = %v, want nil", err)
	}

	cases := []struct {
		name string
		mut  func(d *Declaration)
		want string
	}{
		{"missing name", func(d *Declaration) { d.Name = "" }, "Declaration.Name is required"},
		{"missing actor", func(d *Declaration) { d.ActorID = "" }, "ActorID is required"},
		{"empty types", func(d *Declaration) { d.Types = nil }, "Types must be non-empty"},
		{"bad binding", func(d *Declaration) { d.Binding = "bus" }, "Binding"},
		{"missing maxpending", func(d *Declaration) { d.MaxPendingMs = nil }, "MaxPendingMs is required"},
		{"missing entry", func(d *Declaration) {
			d.MaxPendingMs = map[string]int64{"demo.echo": 1}
		}, "MaxPendingMs missing for type \"demo.ping\""},
		{"zero entry", func(d *Declaration) {
			d.MaxPendingMs = map[string]int64{"demo.echo": 0, "demo.ping": 1}
		}, "must be > 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := good
			d.MaxPendingMs = map[string]int64{"demo.echo": 1, "demo.ping": 1}
			d.Types = []string{"demo.echo", "demo.ping"}
			tc.mut(&d)
			err := d.Validate()
			if err == nil {
				t.Fatalf("Validate(%s) = nil, want error containing %q", tc.name, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate(%s) = %q, want substring %q", tc.name, err, tc.want)
			}
		})
	}
}

// TestRegisterDuplicatePanics asserts the package registry rejects
// duplicate Register calls — adapters are statically known and
// duplicates indicate a build/wiring bug.
func TestRegisterDuplicatePanics(t *testing.T) {
	ResetRegistry()
	defer ResetRegistry()

	Register("demo", func() Module { return newDefaultMockModule() })
	if _, ok := RegisteredModules()["demo"]; !ok {
		t.Fatalf("expected 'demo' in RegisteredModules")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate Register")
		}
	}()
	Register("demo", func() Module { return newDefaultMockModule() })
}

// TestRegisterEmptyArgsPanic asserts Register panics on empty name /
// nil factory — both are programming errors that should fail fast.
func TestRegisterEmptyArgsPanic(t *testing.T) {
	ResetRegistry()
	defer ResetRegistry()

	checkPanic(t, "empty name", func() {
		Register("", func() Module { return newDefaultMockModule() })
	})
	checkPanic(t, "nil factory", func() { Register("foo", nil) })
}

func checkPanic(t *testing.T, label string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("%s: expected panic", label)
		}
	}()
	fn()
}

// TestStatusIsValid covers the closed Status enum.
func TestStatusIsValid(t *testing.T) {
	if !StatusCompleted.IsValid() {
		t.Fatalf("StatusCompleted should be valid")
	}
	if !StatusFailed.IsValid() {
		t.Fatalf("StatusFailed should be valid")
	}
	if Status("nope").IsValid() {
		t.Fatalf("non-enum value should be invalid")
	}
}
