// Package buildinfo carries what a released binary can say about itself.
//
// A node ships as one file, so the file has to answer which node it is: the
// atoll tag it was cut from, the atoll-web tag whose UI is inside it, and the
// commit that produced both. None of it can be derived at runtime — it is
// stamped in at link time by the release build (see Makefile: LDFLAGS_VERSION).
// A binary built from a working tree says "dev", which is the honest answer.
package buildinfo

import "fmt"

var (
	// Version is the atoll release tag, e.g. "v0.1.0".
	Version = "dev"
	// WebVersion is the atoll-web tag whose built UI is embedded, e.g. "v0.1.0".
	WebVersion = "none"
	// Commit is the atoll commit the binary was built from.
	Commit = ""
	// Date is the build date, UTC, as YYYY-MM-DD.
	Date = ""
)

// Line is the one-line identity of a binary, named by the command printing it.
func Line(binary string) string {
	line := fmt.Sprintf("%s %s (web %s", binary, Version, WebVersion)
	if Commit != "" {
		line += ", commit " + Commit
	}
	if Date != "" {
		line += ", built " + Date
	}
	return line + ")"
}
