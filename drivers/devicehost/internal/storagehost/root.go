package storagehost

import (
	"fmt"
	"os"
)

// channelRoot is the one physical channel directory. Agent work and file
// access use this exact root; there is no sibling data tree.
type channelRoot struct{ root *os.Root }

func openChannelRoot(path string) (*channelRoot, error) {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("storagehost: create channel root: %w", err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open channel root: %w", err)
	}
	return &channelRoot{root: root}, nil
}

func (c *channelRoot) Close() error { return c.root.Close() }
