package main

import (
	"flag"
	"fmt"
	"os"
)

// runChannel dispatches the `coagent channel <sub>` family.
func runChannel(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: coagent channel <ls|show|create> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "ls":
		runChannelLs(args[1:])
	case "show":
		runChannelShow(args[1:])
	case "create":
		runChannelCreate(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown channel subcommand: %s\n", args[0])
		os.Exit(2)
	}
}

// runChannelLs prints every channel visible to the caller. With
// --workspace it scopes to a single workspace; otherwise it walks all
// workspaces returned by GET /api/workspaces.
func runChannelLs(args []string) {
	fs := flag.NewFlagSet("channel ls", flag.ExitOnError)
	wsID := fs.String("workspace", "", "limit to a single workspace id")
	serverURL, token := bindGlobalFlags(fs)
	_ = fs.Parse(args) // ExitOnError handles errors

	c, err := newHTTPClient(*serverURL, *token)
	if err != nil {
		fatal(err)
	}

	type wsRow struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	type listChannelsResp struct {
		Channels []map[string]any `json:"channels"`
	}

	listForWorkspace := func(id string) {
		var out listChannelsResp
		if err := c.do("GET", "/api/workspaces/"+id+"/channels", nil, &out); err != nil {
			fatal(err)
		}
		emitJSON(map[string]any{
			"workspace_id": id,
			"channels":     out.Channels,
		})
	}

	if *wsID != "" {
		listForWorkspace(*wsID)
		return
	}

	var rootResp []wsRow
	if err := c.do("GET", "/api/workspaces", nil, &rootResp); err != nil {
		fatal(err)
	}
	for _, w := range rootResp {
		listForWorkspace(w.ID)
	}
}

// runChannelShow prints a single channel record.
func runChannelShow(args []string) {
	fs := flag.NewFlagSet("channel show", flag.ExitOnError)
	serverURL, token := bindGlobalFlags(fs)
	_ = fs.Parse(args) // ExitOnError handles errors

	rest := fs.Args()
	if len(rest) < 1 {
		fmt.Fprintln(os.Stderr, "usage: coagent channel show <chID>")
		os.Exit(2)
	}
	chID := rest[0]

	c, err := newHTTPClient(*serverURL, *token)
	if err != nil {
		fatal(err)
	}
	var out map[string]any
	if err := c.do("GET", "/api/channels/"+chID, nil, &out); err != nil {
		fatal(err)
	}
	emitJSON(out)
}

// runChannelCreate creates a channel inside a workspace.
func runChannelCreate(args []string) {
	fs := flag.NewFlagSet("channel create", flag.ExitOnError)
	wsID := fs.String("workspace", "", "workspace id (required)")
	name := fs.String("name", "", "channel name (required)")
	ctype := fs.String("type", "group", "channel type (default 'group')")
	serverURL, token := bindGlobalFlags(fs)
	_ = fs.Parse(args) // ExitOnError handles errors

	if *wsID == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "--workspace and --name required")
		os.Exit(2)
	}

	c, err := newHTTPClient(*serverURL, *token)
	if err != nil {
		fatal(err)
	}
	req := map[string]any{"name": *name, "type": *ctype}
	var out map[string]any
	if err := c.do("POST", "/api/workspaces/"+*wsID+"/channels", req, &out); err != nil {
		fatal(err)
	}
	emitJSON(out)
}

// bindGlobalFlags registers the cross-subcommand --server-url +
// --token flags so each FlagSet can parse them inline. Returns
// pointers so callers can read after fs.Parse.
func bindGlobalFlags(fs *flag.FlagSet) (serverURL, token *string) {
	serverURL = fs.String("server-url", "", "server base URL (env COAGENT_SERVER_URL)")
	token = fs.String("token", "", "session token to send as Bearer (env COAGENT_SESSION_TOKEN)")
	return
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
