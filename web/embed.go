// Package web carries the browser UI a node serves. The files come from the
// atoll-web repository, built and copied into dist/ at release time; the tag
// they were built from is recorded in WEB_VERSION at the repository root.
//
// The UI reaches the node over relative paths (/api, /ws, /obs, /files), so it
// has to be same-origin with them — which is to say the node serves it itself.
// That is why it is here rather than beside the binary: `atoll up` is a whole
// personal node, and a node whose own interface has to be started separately is
// not whole.
//
// A source checkout has only the placeholder page in dist/. Building the real
// UI is a release step, not a prerequisite for compiling the node.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var embedded embed.FS

// Assets is the UI rooted at dist/ — what an HTTP handler should serve from.
func Assets() fs.FS {
	sub, err := fs.Sub(embedded, "dist")
	if err != nil {
		// dist/ is embedded above; its absence would not compile.
		panic(err)
	}
	return sub
}
