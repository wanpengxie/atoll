package platform

// HumanSession is one live client connection belonging to a human principal.
// ID is the opaque address used by ui.state/ui.navigate/ui.open. Label is only
// descriptive; it is supplied by the client and is never used for authority.
type HumanSession struct {
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
}

// HumanSessionLister is the read-only assembly seam from the node gateway,
// which owns live connections, into each channel's built-in human cell.
// Implementations return a detached snapshot safe for the caller to retain.
type HumanSessionLister func(principal string) []HumanSession
