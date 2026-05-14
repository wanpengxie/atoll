package main_test

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildCLI compiles the cmd/cli binary into the test tempdir and returns
// the absolute path. The build is cached per-test-run via t.TempDir().
func buildCLI(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "coagent-cli")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/cli")
	cmd.Dir = repoRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build cmd/cli: %v\n%s", err, stderr.String())
	}
	return bin
}

// repoRoot walks up from the current package directory to find the
// module root (carrying go.mod). The CLI package is two levels deep
// (cmd/cli) so go up twice.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(wd, "..", "..")
}

// TestCLI_HelpExitsZero exercises the `help` happy-path: the binary
// must print usage to stderr and exit 0.
func TestCLI_HelpExitsZero(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)

	ctx := t.Context()
	cmd := exec.CommandContext(ctx, bin, "help")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		t.Fatalf("`coagent help` exited non-zero: %v\nstderr=%s", err, stderr.String())
	}
	// Usage is written to stderr per cmd/cli/main.go.
	if !strings.Contains(stderr.String(), "coagent-cli") {
		t.Errorf("help output missing identifier; stderr=%q", stderr.String())
	}
}

// TestCLI_VersionExitsZero — the `--version` flag must echo a version
// string and exit 0.
func TestCLI_VersionExitsZero(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)

	ctx := t.Context()
	cmd := exec.CommandContext(ctx, bin, "--version")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		t.Fatalf("`coagent --version` exited non-zero: %v", err)
	}
	if !strings.Contains(stdout.String(), "coagent-cli") {
		t.Errorf("version output missing identifier; stdout=%q", stdout.String())
	}
}

// TestCLI_UnknownCommandExitsTwo — invalid subcommands must surface a
// non-zero exit (2 per cmd/cli/main.go convention) so shell pipelines
// can detect misuse.
func TestCLI_UnknownCommandExitsTwo(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bin, "nosuchcommand")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("`coagent nosuchcommand` exited zero; want non-zero")
	}
	exit, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("err type=%T want *exec.ExitError", err)
	}
	if exit.ExitCode() != 2 {
		t.Errorf("exit code=%d want 2", exit.ExitCode())
	}
}
