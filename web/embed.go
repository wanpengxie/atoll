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
// A source checkout has no built UI at all: dist/ holds only .gitkeep, an
// anchor so the embed directive still compiles, and the node serves the page in
// placeholder/ instead. Building the real UI is a release step, not a
// prerequisite for compiling the node.
//
// The two directories are kept apart on purpose. When the page-of-record lived
// in dist/ it was both the tracked source and the thing every build overwrote,
// so `make web` always showed up as a modified file — and "restoring" that file
// silently deleted the UI that had just been built.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var embedded embed.FS

//go:embed placeholder
var placeholder embed.FS

// Assets is the UI an HTTP handler should serve from: a real build if one was
// stamped into dist/, otherwise the page that explains how to get one.
func Assets() fs.FS {
	if dist, err := fs.Sub(embedded, "dist"); err == nil {
		if _, err := fs.Stat(dist, "index.html"); err == nil {
			return dist
		}
	}
	sub, err := fs.Sub(placeholder, "placeholder")
	if err != nil {
		// placeholder/ is embedded above; its absence would not compile.
		panic(err)
	}
	return sub
}
