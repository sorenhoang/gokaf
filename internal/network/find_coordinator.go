package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleFindCoordinator(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))

	// ponytail: single broker is coordinator for every group; real selection is
	// hash(group_id) % __consumer_offsets partitions -> that partition's leader.
	if _, err := dec.ReadString(); err != nil {
		return nil, err
	}

	e := protocol.NewEncoder()
	e.WriteInt16(protocol.ErrNone)
	e.WriteInt32(b.NodeID)
	e.WriteString(b.Host)
	e.WriteInt32(b.Port)
	return e.Bytes(), nil
}
