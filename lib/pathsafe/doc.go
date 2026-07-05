// Package pathsafe maps opaque ids (actor/channel ids that may carry ':' like
// "agent:boost", or '/'/'\' path separators) to a single filesystem-safe path
// segment. A plain stdlib string util, no layer of its own.
package pathsafe
