package spacetool

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type DeclSummary struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Owner      string `json:"owner"`
	Visibility string `json:"visibility"`
	Class      string `json:"class"`
}

type DeclDetail struct {
	DeclSummary
	Config json.RawMessage `json:"config,omitempty"`
}

type DeclSpec struct {
	Name       string          `json:"name"`
	Class      string          `json:"class"`
	Visibility string          `json:"visibility"`
	Config     json.RawMessage `json:"config,omitempty"`
}

type IntroduceOpts struct{}

type Requester struct {
	ActorID   actor.ActorID `json:"actor_id"`
	ChannelID channel.ID    `json:"channel_id"`
	RequestID string        `json:"request_id"`
}

// The space error vocabulary (SpaceErrorCode / SpaceError) lives in
// platform/channelspec: the contract crosses the membrane, so its codes are
// boundary vocabulary shared by the membrane's read faces, the app gates and
// this codec — not any single party's property.

type ErrResultUnknown struct{ Ref string }

func (e *ErrResultUnknown) Error() string { return "result_unknown: " + e.Ref }

func DerivedSpaceToolRef(channelID channel.ID, requestID string) string {
	payload := appendLengthPrefixed(nil, string(channelID))
	payload = appendLengthPrefixed(payload, requestID)
	sum := sha256.Sum256(payload)
	return "adm:st:v1:" + hex.EncodeToString(sum[:])
}

func appendLengthPrefixed(dst []byte, value string) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len([]byte(value))))
	dst = append(dst, size[:]...)
	return append(dst, value...)
}
