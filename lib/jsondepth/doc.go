// Package jsondepth bounds the container nesting a client-supplied JSON blob
// may carry before it is decoded into an UNSTRUCTURED value. encoding/json's
// Unmarshal recurses one call-stack frame per nested container, so a deeply
// nested blob overflows the goroutine stack — a fatal, unrecoverable crash that
// no recover can catch. Callers decoding untrusted bytes into an open shape
// (map[string]any and friends) run this guard first. A plain stdlib util, no
// layer of its own — the same shape as lib/pathsafe.
package jsondepth
