package message

// Failure is the receiver-authored detail carried by a failed terminal
// response. ErrorCode belongs to the receiver's manifest vocabulary.
type Failure struct {
	ErrorCode string `json:"error_code"`
	Detail    string `json:"detail,omitempty"`
}
