package message

// Failure is the common core projected from a failed terminal response.
// ErrorCode belongs to the receiver's manifest vocabulary; individual words
// may add fields beside this core.
type Failure struct {
	ErrorCode string `json:"error_code"`
	Detail    string `json:"detail,omitempty"`
}
