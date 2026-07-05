package pathsafe

import "strings"

var replacer = strings.NewReplacer(":", "-", "/", "_", "\\", "_")

// Segment returns s with path-hostile characters replaced so it is safe as one
// directory/file name component.
func Segment(s string) string {
	return replacer.Replace(s)
}
