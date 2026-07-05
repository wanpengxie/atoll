// Package dotenv is a minimal .env loader for the server composition root
// (cmd/server only — the daemon carries zero config file, A-P10; its creds ride
// the ?key= link auth). Config enters the process exactly one way — the
// environment — and this is the dev-convenience bridge that seeds it from a
// file so `make dev` and a bare binary run don't need a manual `source .env`.
//
// Precedence: a variable already present in the environment is NEVER
// overwritten — an explicit `export` (production) always wins over the file.
// The loader is best-effort: a missing file is not an error (production ships
// no .env; the real environment carries everything).
//
// Format: plain `KEY=VALUE`, one per line; full-line `#` comments and blank
// lines are skipped. No quoting / escaping / `export ` prefix handling — the
// project .env is plain KEY=VALUE. If that ever stops being true, this grows
// with the real need, not before it.
package dotenv

import (
	"bufio"
	"os"
	"strings"
)

// Load reads path and sets each KEY=VALUE into the process environment unless
// KEY is already set. It returns the number of variables newly set and any
// read error. A non-existent file returns (0, nil) — absence is not failure.
func Load(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, strings.TrimSpace(val)); err != nil {
			return n, err
		}
		n++
	}
	return n, sc.Err()
}
