package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleFetchMetadataLog(header protocol.RequestHeader, body []byte) ([]byte, error) {
	if b.MetadataLog == nil {
		e := protocol.NewEncoder()
		e.WriteArrayLen(0)
		return e.Bytes(), nil
	}
	dec := protocol.NewDecoder(bytes.NewReader(body))
	fromOffset, err := dec.ReadInt64()
	if err != nil {
		return nil, err
	}
	payloads, err := b.MetadataLog.ReadRawFrom(fromOffset)
	if err != nil {
		return nil, err
	}
	e := protocol.NewEncoder()
	e.WriteArrayLen(len(payloads))
	for _, payload := range payloads {
		e.WriteBytes(payload)
	}
	return e.Bytes(), nil
}
