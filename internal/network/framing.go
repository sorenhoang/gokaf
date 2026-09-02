package network

import (
	"io"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func WriteFrame(w io.Writer, payload []byte) error {
	return protocol.WriteFrame(w, payload)
}

func ReadFrame(r io.Reader) ([]byte, error) {
	return protocol.ReadFrame(r)
}
