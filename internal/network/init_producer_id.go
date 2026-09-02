package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleInitProducerID(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	if _, err := dec.ReadNullableString(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}

	pid, epoch := b.Producers.InitProducerID()
	e := protocol.NewEncoder()
	e.WriteInt32(0)
	e.WriteInt16(protocol.ErrNone)
	e.WriteInt64(pid)
	e.WriteInt16(epoch)
	return e.Bytes(), nil
}
