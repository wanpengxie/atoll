package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wanpengxie/ActOS/runtime/store"
)

// runKernel dispatches the `coagent kernel <sub>` family. Today only
// `events` exists — extra read-only kernel inspectors land later.
func runKernel(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: coagent kernel <events> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "events":
		// runKernelEvents returns an exit code so its `defer db.Close()` /
		// `defer rows.Close()` actually run before we exit.
		os.Exit(runKernelEvents(args[1:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown kernel subcommand: %s\n", args[0])
		os.Exit(2)
	}
}

// runKernelEvents reads messages from a channel's local sqlite log
// (daemon-side) and prints them as NDJSON. ReadOnly mode so the cli
// can safely target a live daemon. Returns the process exit code so
// callers can let `defer` cleanups run before `os.Exit`.
func runKernelEvents(args []string) int {
	fs := flag.NewFlagSet("kernel events", flag.ExitOnError)
	channelID := fs.String("channel", "", "channel id (required)")
	dataDir := fs.String("data-dir", defaultDataDir(), "daemon data dir (channels live at <data-dir>/channels/<id>/channel.sqlite)")
	since := fs.Int64("since", 0, "only emit rows with ts_received >= this unix-ms (0 = no filter)")
	limit := fs.Int("limit", 200, "max rows to emit (0 = no limit)")
	_ = fs.Parse(args) // ExitOnError handles errors

	if *channelID == "" {
		fmt.Fprintln(os.Stderr, "--channel required")
		return 2
	}

	dbPath := filepath.Join(*dataDir, "channels", *channelID, "channel.sqlite")
	if _, err := os.Stat(dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "channel sqlite not found: %s (%v)\n", dbPath, err)
		return 1
	}

	ctx := context.Background()
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{ReadOnly: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open channel db: %v\n", err)
		return 1
	}
	defer func() { _ = db.Close() }()

	q := `SELECT seq, id, ts, ts_received, sender_kind, sender_id, sender_name,
	             kind, type, payload, parent_id, correlation_id, audience, visibility
	      FROM messages
	      WHERE ts_received >= ?
	      ORDER BY ts_received ASC, seq ASC`
	if *limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", *limit)
	}
	rows, err := db.QueryContext(ctx, q, *since)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query messages: %v\n", err)
		return 1
	}
	defer func() { _ = rows.Close() }()

	enc := json.NewEncoder(os.Stdout)
	count := 0
	for rows.Next() {
		var (
			seq                            int64
			id                             string
			ts, tsRecv                     int64
			sKind, sID                     string
			sName, parentID, corrID        sql.NullString
			kind, mtype, payload, audience string
			visib                          string
		)
		if err := rows.Scan(
			&seq, &id, &ts, &tsRecv,
			&sKind, &sID, &sName,
			&kind, &mtype, &payload,
			&parentID, &corrID, &audience, &visib,
		); err != nil {
			fmt.Fprintf(os.Stderr, "scan row: %v\n", err)
			return 1
		}
		row := map[string]any{
			"seq":            seq,
			"id":             id,
			"ts":             ts,
			"ts_received":    tsRecv,
			"sender_kind":    sKind,
			"sender_id":      sID,
			"sender_name":    nullStr(sName),
			"kind":           kind,
			"type":           mtype,
			"payload":        json.RawMessage(payload),
			"parent_id":      nullStr(parentID),
			"correlation_id": nullStr(corrID),
			"audience":       json.RawMessage(audience),
			"visibility":     visib,
		}
		if err := enc.Encode(row); err != nil {
			fmt.Fprintf(os.Stderr, "encode row: %v\n", err)
			return 1
		}
		count++
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "iterate rows: %v\n", err)
		return 1
	}
	if count == 0 {
		fmt.Fprintln(os.Stderr, "[kernel events] no rows")
	}
	return 0
}

// nullStr unwraps sql.NullString into a plain string (empty when NULL).
func nullStr(s sql.NullString) string {
	if !s.Valid {
		return ""
	}
	return s.String
}

// defaultDataDir mirrors cmd/daemon's resolution so cli + daemon
// agree on the channel sqlite path.
func defaultDataDir() string {
	if v := os.Getenv("COAGENT_DATA_DIR"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".coagent"
	}
	return filepath.Join(home, ".coagent")
}
