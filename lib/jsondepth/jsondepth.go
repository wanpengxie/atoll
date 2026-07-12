package jsondepth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// MaxDepth is the container-nesting ceiling. 64 levels is far past any
// legitimate payload or config.
const MaxDepth = 64

// Bounded scans raw's structural tokens WITHOUT materialising any value
// (json.Decoder.Token is iterative — a slice-backed scanner stack, never
// call-stack recursion) and errors if container nesting exceeds MaxDepth. A
// malformed blob is left to the caller's own decode to report (returns nil
// here); only over-deep nesting is refused up front.
func Bounded(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	depth := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil // malformed: the caller's own decode surfaces the parse error
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
			if depth > MaxDepth {
				return fmt.Errorf("jsondepth: json nesting exceeds %d levels", MaxDepth)
			}
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
	}
}
