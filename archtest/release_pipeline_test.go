package archtest

import (
	"os"
	"strings"
	"testing"
)

// TestReleaseCannotBypassCI freezes the release topology, not GitHub syntax:
// a tag must traverse the same backend workflow as main, the exact paired web
// tag must pass its full suite, and packaging must consume that tested artifact.
// This guard exists because independent green Release/red CI runs previously
// published several versions without ever crossing a quality gate.
func TestReleaseCannotBypassCI(t *testing.T) {
	ci := readWorkflow(t, "../.github/workflows/ci.yml")
	release := readWorkflow(t, "../.github/workflows/release.yml")

	requireWorkflowText(t, ci, "workflow_call:", "CI must remain callable by Release")
	for _, want := range []string{
		"uses: ./.github/workflows/ci.yml",
		"run: npm run test:ci",
		"uses: actions/upload-artifact@v4",
		"needs: [core-ci, web-ci]",
		"uses: actions/download-artifact@v4",
		"if: github.event_name == 'push'",
		"secrets.OSS_ACCESS_KEY_ID",
		"secrets.OSS_ACCESS_KEY_SECRET",
		"OSS_BUCKET: atoll-package",
		"releases/$VERSION",
		"name: publish GitHub release",
		"put_public /tmp/atoll-latest releases/latest 'no-cache'",
	} {
		requireWorkflowText(t, release, want, "Release quality gate is incomplete")
	}
}

func TestOSSMirrorMovesExistingReleaseBytes(t *testing.T) {
	mirror := readWorkflow(t, "../.github/workflows/mirror-release.yml")
	for _, want := range []string{
		"gh release download",
		"sha256sum -c checksums.txt",
		"secrets.OSS_ACCESS_KEY_ID",
		"secrets.OSS_ACCESS_KEY_SECRET",
		"--forbid-overwrite",
		"name: publish pointers last",
		"name: public read acceptance",
	} {
		requireWorkflowText(t, mirror, want, "OSS mirror must preserve and verify released bytes")
	}
}

func readWorkflow(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func requireWorkflowText(t *testing.T, workflow, want, reason string) {
	t.Helper()
	if !strings.Contains(workflow, want) {
		t.Fatalf("%s: missing %q", reason, want)
	}
}
