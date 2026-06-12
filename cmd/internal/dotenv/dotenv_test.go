package dotenv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_SetsMissingSkipsExistingAndComments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	content := "" +
		"# a comment\n" +
		"\n" +
		"KIMI_API_KEY=from-file\n" +
		"   KIMI_MODEL = deepseek-x \n" + // surrounding spaces trimmed
		"ALREADY_SET=from-file\n" +
		"malformed-no-equals\n" +
		"=novalue\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	// ALREADY_SET is exported before Load — it must win.
	t.Setenv("ALREADY_SET", "from-env")
	t.Setenv("KIMI_API_KEY", "") // present-but-empty still counts as set → not overwritten
	t.Setenv("KIMI_MODEL", "")
	// Re-clear the two we want filled (t.Setenv marks them present; LookupEnv
	// sees them as set). Use Unsetenv so Load treats them as missing.
	os.Unsetenv("KIMI_API_KEY")
	os.Unsetenv("KIMI_MODEL")

	n, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 vars set, got %d", n)
	}
	if got := os.Getenv("KIMI_API_KEY"); got != "from-file" {
		t.Fatalf("KIMI_API_KEY = %q, want from-file", got)
	}
	if got := os.Getenv("KIMI_MODEL"); got != "deepseek-x" {
		t.Fatalf("KIMI_MODEL = %q (want trimmed deepseek-x)", got)
	}
	if got := os.Getenv("ALREADY_SET"); got != "from-env" {
		t.Fatalf("ALREADY_SET = %q, the env must win over the file", got)
	}
}

func TestLoad_MissingFileIsNotError(t *testing.T) {
	n, err := Load(filepath.Join(t.TempDir(), "nope.env"))
	if err != nil || n != 0 {
		t.Fatalf("missing file: got (%d, %v), want (0, nil)", n, err)
	}
}
