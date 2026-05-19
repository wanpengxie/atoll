package ipc

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestCodecReadRejectsOversizedFrameBeforeAlloc(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(MaxFrameBytes+1))

	codec := NewCodec(bytes.NewReader(hdr[:]), io.Discard)
	if _, err := codec.Read(); err == nil {
		t.Fatal("Read returned nil error for oversized frame")
	} else if !strings.Contains(err.Error(), "frame too large") {
		t.Fatalf("Read err=%q want frame too large", err)
	}
	if cap(codec.rbuf) != 0 {
		t.Fatalf("Read allocated rbuf cap=%d for oversized frame", cap(codec.rbuf))
	}
}
