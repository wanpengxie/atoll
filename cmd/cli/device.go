package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/google/uuid"
)

// runDevice dispatches the `coagent device <sub>` family.
func runDevice(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: coagent device <register> [flags]")
		os.Exit(2)
	}
	switch args[0] {
	case "register":
		runDeviceRegister(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown device subcommand: %s\n", args[0])
		os.Exit(2)
	}
}

// runDeviceRegister issues a device session for a channel via
// POST /api/channels/:chID/devices and prints the returned token.
func runDeviceRegister(args []string) {
	fs := flag.NewFlagSet("device register", flag.ExitOnError)
	channelID := fs.String("channel", "", "channel id (required)")
	deviceType := fs.String("type", "xhs", "device type (default 'xhs')")
	daemonID := fs.String("daemon", "", "daemon id that will own this device (required)")
	deviceID := fs.String("device-id", "", "stable device id (default: random uuid)")
	serverURL, token := bindGlobalFlags(fs)
	fs.Parse(args)

	if *channelID == "" || *daemonID == "" {
		fmt.Fprintln(os.Stderr, "--channel and --daemon required")
		os.Exit(2)
	}
	if *deviceID == "" {
		*deviceID = uuid.NewString()
	}

	c, err := newHTTPClient(*serverURL, *token)
	if err != nil {
		fatal(err)
	}
	req := map[string]any{
		"device_id":   *deviceID,
		"device_type": *deviceType,
		"daemon_id":   *daemonID,
	}
	var out map[string]any
	if err := c.do("POST", "/api/channels/"+*channelID+"/devices", req, &out); err != nil {
		fatal(err)
	}
	emitJSON(out)
}
