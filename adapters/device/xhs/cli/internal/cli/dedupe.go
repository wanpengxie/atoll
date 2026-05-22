package cli

// dedupe.go — M1.6-T5 phase-4 business invariant: reject a `publish`
// command whose declared `note_id` already has a TERMINAL completed
// publish response in the local channel sqlite.
//
// Why CLI-side and not adapter-side:
//   - The xhs adapter sees ONE publish at a time; it cannot economically
//     check the whole-channel history for a prior completed publish
//     with the same note_id without re-implementing a query layer.
//   - domain-xhs-spec §2.5 declares "duplicate publish refusal" as a
//     business invariant; CLI is the right place because (a) the agent
//     drives publish through the CLI, (b) the channel.sqlite is local
//     to the daemon spawning the CLI, (c) the rejection happens BEFORE
//     a request is even written, saving a daemon round-trip.
//
// Mechanism (env-driven; safe-by-default):
//   - When COAGENT_CHANNEL_DB is unset → SKIP (legacy callers / unit
//     tests that don't have a channel sqlite to read).
//   - When the `publish` command receives --note-id <id> AND the env
//     COAGENT_CHANNEL_DB points at a readable file, query:
//
//       SELECT 1 FROM messages
//         WHERE type='xhs.publish'
//           AND kind='response'
//           AND is_terminal=1
//           AND json_extract(payload, '$.status') = 'completed'
//           AND json_extract(payload, '$.note_id') = ?
//         LIMIT 1
//
//     Match → return CLIError{Code: "duplicate_publish", Msg: "note_id <id> already published"}.
//
// The check is best-effort; transient sqlite errors (locked / corrupt /
// schema-drift) are converted to a soft pass-through with a warning to
// stderr so a malformed channel sqlite never blocks the agent.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite" // register driver
)

// EnvChannelDB is the env that scopes the dedupe query. Daemon-spawned
// CLIs receive this; manual `coagent-xhs publish` invocations during
// dev get the legacy skip path.
const EnvChannelDB = "COAGENT_CHANNEL_DB"

// dedupePublish queries the local channel.sqlite for a prior TERMINAL
// completed publish response with the supplied note_id. Returns
// (matched=true, nil) when a duplicate is found, (false, nil) on no
// match or unset env, (false, err) only on hard query failures the
// caller might want to surface.
func dedupePublish(ctx context.Context, noteID string) (bool, error) {
	noteID = strings.TrimSpace(noteID)
	if noteID == "" {
		return false, nil
	}
	dbPath := strings.TrimSpace(os.Getenv(EnvChannelDB))
	if dbPath == "" {
		return false, nil
	}
	if _, err := os.Stat(dbPath); err != nil {
		// Channel sqlite missing → bail out softly. The agent's first
		// publish attempt is by definition not a duplicate, so a
		// missing db is a non-event.
		fmt.Fprintf(os.Stderr, "coagent-xhs: dedupe skipped — %s=%q not found (%v)\n", EnvChannelDB, dbPath, err)
		return false, nil
	}

	// modernc.org/sqlite supports the mode=ro DSN form to avoid taking
	// any write lock. Pair with a small busy_timeout in case the
	// daemon is concurrently writing.
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(2000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "coagent-xhs: dedupe skipped — open %s failed: %v\n", dbPath, err)
		return false, nil
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	const q = `
SELECT 1 FROM messages
 WHERE type='xhs.publish'
   AND kind='response'
   AND is_terminal=1
   AND json_extract(payload, '$.status') = 'completed'
   AND json_extract(payload, '$.note_id') = ?
 LIMIT 1`

	row := db.QueryRowContext(ctx, q, noteID)
	var one int
	if err := row.Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		// Schema drift / json1 missing / etc. — soft pass so the agent
		// can still attempt the publish (the daemon harness remains
		// the authoritative gate).
		fmt.Fprintf(os.Stderr, "coagent-xhs: dedupe skipped — scan: %v\n", err)
		return false, nil
	}
	return one == 1, nil
}
