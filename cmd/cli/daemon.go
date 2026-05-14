package main

import (
	"flag"
	"fmt"
	"os"
)

// runDaemon dispatches the `coagent daemon <sub>` family.
func runDaemon(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: coagent daemon <status> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "status":
		runDaemonStatus(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown daemon subcommand: %s\n", args[0])
		os.Exit(2)
	}
}

// runDaemonStatus aggregates server health + active placements per
// daemon as a stand-in for a dedicated /api/daemons listing (which
// the server doesn't expose publicly at M1.5).
func runDaemonStatus(args []string) {
	fs := flag.NewFlagSet("daemon status", flag.ExitOnError)
	serverURL, token := bindGlobalFlags(fs)
	fs.Parse(args)

	c, err := newHTTPClient(*serverURL, *token)
	if err != nil {
		fatal(err)
	}

	type healthResp struct {
		Status string `json:"status"`
	}
	var hr healthResp
	healthErr := c.do("GET", "/healthz", nil, &hr)

	type placementRow struct {
		ChannelID   string `json:"channel_id"`
		DaemonID    string `json:"daemon_id"`
		State       string `json:"state"`
		ActivatedAt int64  `json:"activated_at"`
	}
	type placementsResp struct {
		Placements []placementRow `json:"placements"`
	}
	var pr placementsResp
	placementsErr := c.do("GET", "/api/placements?state=active", nil, &pr)

	// Group placements by daemon_id so the operator sees which
	// daemons are currently servicing channels.
	byDaemon := map[string][]placementRow{}
	for _, p := range pr.Placements {
		byDaemon[p.DaemonID] = append(byDaemon[p.DaemonID], p)
	}

	out := map[string]any{
		"server_url": c.baseURL,
		"health": map[string]any{
			"ok":     healthErr == nil && hr.Status == "ok",
			"status": hr.Status,
			"error":  errStr(healthErr),
		},
		"placements_by_daemon": byDaemon,
		"placements_error":     errStr(placementsErr),
	}
	emitJSON(out)

	if healthErr != nil {
		os.Exit(1)
	}
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// ensure flag is imported even if subcommand variants drop usage.
var _ = flag.ExitOnError
