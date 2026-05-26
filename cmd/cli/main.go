// Package main is the coagent management CLI binary.
//
// Authoritative spec: launch-ticket notes §T7 (cmd/cli 子命令).
//
// Sub-commands:
//
//	coagent kernel events --channel <id> [--since <ts>] [--data-dir <p>] [--limit N]
//	coagent channel ls    [--workspace <id>]
//	coagent channel show  <chID>
//	coagent channel create --workspace <ws> --name <name> [--type group]
//	coagent daemon status
//	coagent ask     --type <T> --audience <A> [--channel <id>] (--payload <json> | --payload-file <p>)
//	coagent emit    --type <T>                [--channel <id>] (--payload <json> | --payload-file <p>)
//	coagent answer  --type <T> --parent-id <P> [--channel <id>] (--payload <json> | --payload-file <p>)
//
// Auth: cookie-less. Pass `--token <session_token>` or set
// COAGENT_SESSION_TOKEN — sent as `Authorization: Bearer …`. The
// server identity middleware accepts both cookie and bearer forms.
package main

import (
	"fmt"
	"os"
)

// version is set via -ldflags at build time.
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "kernel":
		runKernel(args)
	case "channel":
		runChannel(args)
	case "daemon":
		runDaemon(args)
	case "ask":
		runAsk(args)
	case "emit":
		runEmit(args)
	case "answer":
		runAnswer(args)
	case "-h", "--help", "help":
		usage()
	case "-v", "--version", "version":
		fmt.Println("coagent-cli", version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `coagent-cli — management CLI

USAGE
  coagent <command> [subcommand] [flags]

COMMANDS
  kernel events    Stream messages from a channel's local sqlite log
  channel ls       List workspaces + channels visible to the caller
  channel show     Show a single channel
  channel create   Create a channel inside a workspace
  daemon status    Report server health + active daemon placements
  ask              Write a kind=request envelope (audience required)
  emit             Write a kind=event envelope (audience optional)
  answer           Write a kind=response envelope (--parent-id required)

GLOBAL FLAGS (forwarded by subcommands that call the HTTP API)
  --server-url URL  Server base URL  (env COAGENT_SERVER_URL; default http://localhost:8832)
  --token   TOKEN   Session token    (env COAGENT_SESSION_TOKEN; sent as Bearer)

Run 'coagent <command> -h' for command-specific flags.
`)
}
