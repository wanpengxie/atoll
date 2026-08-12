package accessdoor

import "github.com/wanpengxie/atoll/runtime/resourcespec"

// FileAddress and ParseFileAddress are the access membrane's read-only view
// of the canonical resourcespec vocabulary. Platform callers cannot import
// the kernel-only resourcespec leaf directly.
type FileAddress = resourcespec.FileAddress
type FilePrefix = resourcespec.FilePrefix

func ParseFileAddress(raw string) (FileAddress, error) {
	return resourcespec.ParseFileAddress(raw)
}

func ParseFilePrefix(raw string) (FilePrefix, error) {
	return resourcespec.ParseFilePrefix(raw)
}
