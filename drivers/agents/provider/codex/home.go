package codex

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// HomeEnv names the environment variable an atoll process sets to point
// codex agents at the node-owned CODEX_HOME (`atoll up` sets it after
// PrepareHome). It takes precedence over the path conventions below.
const HomeEnv = "ATOLL_CODEX_HOME"

// homeRelPath is where a node keeps its codex home: under the server half of
// the node directory, beside registry.db. Hard-coded for this version.
const homeRelPath = "server/.codex"

// minimalConfig is the config.toml PrepareHome writes into a fresh atoll codex
// home. It deliberately carries nothing from the user's ~/.codex/config.toml:
// no mcp_servers, no skills, no rules, no memories. project_doc_max_bytes = 0
// stops AGENTS.md discovery so the only instructions the model receives are
// the ones atoll hands it (developerInstructions).
const minimalConfig = "# written by atoll — node-owned codex home; edit via the atoll decl, not here\nproject_doc_max_bytes = 0\n"

// ResolveHome returns the CODEX_HOME a codex child should run under, or ""
// when no atoll-owned home exists (the child then uses codex's default
// ~/.codex — the user's own configuration — which is the documented
// fallback for this version).
//
// Order: $ATOLL_CODEX_HOME; $ATOLL_HOME/server/.codex; ~/.atoll/server/.codex.
// A candidate counts only if the directory already exists: PrepareHome (run by
// `atoll up`) is what creates it, so a missing directory means this process
// is not running inside a prepared node.
func ResolveHome() string {
	if v := os.Getenv(HomeEnv); v != "" {
		if isDir(v) {
			return v
		}
		return ""
	}
	if v := os.Getenv("ATOLL_HOME"); v != "" {
		if p := filepath.Join(v, homeRelPath); isDir(p) {
			return p
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		if p := filepath.Join(home, ".atoll", homeRelPath); isDir(p) {
			return p
		}
	}
	return ""
}

// NodeHome returns the codex home path for a node directory (the `--dir` of
// `atoll up`), without checking existence.
func NodeHome(nodeDir string) string { return filepath.Join(nodeDir, homeRelPath) }

// PrepareHome makes home usable as a CODEX_HOME that borrows only the user's
// authorization: it creates the directory, copies ~/.codex/auth.json into it
// when the source exists (always refreshed, so a re-login on the user side is
// picked up at the next node start), and writes minimalConfig when no
// config.toml is present yet. It never copies anything else from ~/.codex.
//
// Missing user auth is not an error here — the child will simply be
// unauthenticated and codex reports that itself.
func PrepareHome(home string) error {
	if home == "" {
		return errors.New("codex: empty home")
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	if userHome, err := os.UserHomeDir(); err == nil {
		src := filepath.Join(userHome, ".codex", "auth.json")
		if st, err := os.Stat(src); err == nil && st.Mode().IsRegular() {
			if err := copyFile(src, filepath.Join(home, "auth.json"), 0o600); err != nil {
				return err
			}
		}
	}
	cfg := filepath.Join(home, "config.toml")
	if _, err := os.Stat(cfg); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(cfg, []byte(minimalConfig), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func isDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}
