// Command migrate is the daemon-go schema bootstrap + Node→v4 migration
// tool. It owns three subcommands:
//
//	migrate init <channel.sqlite>
//	    Initialize an empty channel-local sqlite (6 v4 tables + indexes).
//
//	migrate init-daemon <daemon.sqlite>
//	    Initialize the daemon-level sqlite (bootstrap_registry + index).
//
//	migrate from-node --src <node.sqlite> --dst <channel.sqlite>
//	    Read the legacy Node daemon `messages` table, transform it per
//	    m1.3-v4-foundation-spec §4.1, and INSERT into the v4 channel.
//	    Dst is created if it does not exist.
//
// The tool prints a one-line summary on success (or a non-zero exit on
// failure) and is safe to re-run — the destination DDL is built with
// CREATE TABLE IF NOT EXISTS, and re-migration into a non-empty dst
// surfaces UNIQUE constraint errors on `id`, which is the desired
// behaviour (caller must clean dst first).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// Version follows the daemon-go binary versioning scheme — bumped per
// ticket.
const Version = "0.1.0-t1"

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit(2)
	}

	cmd, args := os.Args[1], os.Args[2:]
	ctx := context.Background()

	var err error
	switch cmd {
	case "init":
		err = runInit(ctx, args)
	case "init-daemon":
		err = runInitDaemon(ctx, args)
	case "from-node":
		err = runFromNode(ctx, args)
	case "-h", "--help", "help":
		printUsage()
		return
	case "version", "--version", "-v":
		fmt.Println("migrate", Version)
		return
	default:
		fmt.Fprintf(os.Stderr, "migrate: unknown subcommand %q\n\n", cmd)
		printUsageAndExit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

// runInit handles `migrate init <channel.sqlite>`.
func runInit(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: migrate init <channel.sqlite>")
	}
	path := args[0]
	db, err := store.OpenChannel(ctx, path, store.OpenOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	fmt.Printf("migrate init ok path=%s tables=%d indexes=%d\n",
		path, len(store.ChannelLocalTables), len(store.ChannelLocalIndexes))
	return nil
}

// runInitDaemon handles `migrate init-daemon <daemon.sqlite>`.
func runInitDaemon(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: migrate init-daemon <daemon.sqlite>")
	}
	path := args[0]
	db, err := store.OpenDaemon(ctx, path, store.OpenOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	fmt.Printf("migrate init-daemon ok path=%s tables=%d indexes=%d\n",
		path, len(store.DaemonLevelTables), len(store.DaemonLevelIndexes))
	return nil
}

// runFromNode handles `migrate from-node --src <node.sqlite> --dst <channel.sqlite>`.
func runFromNode(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("from-node", flag.ContinueOnError)
	src := fs.String("src", "", "path to legacy Node daemon messages.sqlite")
	dst := fs.String("dst", "", "path to v4 channel messages.sqlite (created if missing)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *src == "" || *dst == "" {
		return errors.New("usage: migrate from-node --src <node.sqlite> --dst <channel.sqlite>")
	}

	// Open the legacy file read-only — never mutate the source.
	srcDB, err := store.OpenChannel(ctx, *src, store.OpenOptions{
		ReadOnly: true,
		SkipDDL:  true,
	})
	if err != nil {
		return fmt.Errorf("open src %s: %w", *src, err)
	}
	defer func() { _ = srcDB.Close() }()

	// Open / create the destination with full v4 DDL.
	dstDB, err := store.OpenChannel(ctx, *dst, store.OpenOptions{})
	if err != nil {
		return fmt.Errorf("open dst %s: %w", *dst, err)
	}
	defer func() { _ = dstDB.Close() }()

	report, err := store.MigrateFromNode(ctx, srcDB, dstDB)
	if err != nil {
		return err
	}

	fmt.Printf("migrate from-node ok src_rows=%d inserted=%d dropped=%s\n",
		report.SourceRows, report.InsertedRows, formatDropped(report.DroppedTypes))
	return nil
}

// formatDropped renders the dropped-types map deterministically so the
// log line is reproducible across runs (helps grep-based CI checks).
func formatDropped(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%s=%d", k, m[k])
	}
	return out
}

func printUsage() {
	fmt.Println(`migrate — daemon-go schema bootstrap + Node→v4 migration tool.

Usage:
  migrate init <channel.sqlite>
      Initialize an empty channel-local sqlite (6 v4 tables + indexes).

  migrate init-daemon <daemon.sqlite>
      Initialize the daemon-level sqlite (bootstrap_registry + index).

  migrate from-node --src <node.sqlite> --dst <channel.sqlite>
      Read the legacy Node daemon "messages" table, rewrite per
      m1.3-v4-foundation-spec §4.1, and INSERT into the v4 channel.
      The destination is created if missing.

  migrate help | version`)
}

func printUsageAndExit(code int) {
	printUsage()
	os.Exit(code)
}
