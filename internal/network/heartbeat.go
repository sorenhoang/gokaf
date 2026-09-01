package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleHeartbeat(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	generationID, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}
	memberID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	e := protocol.NewEncoder()
	e.WriteInt16(b.Groups.Heartbeat(group.HeartbeatRequest{
		GroupID:      groupID,
		GenerationID: generationID,
		MemberID:     memberID,
	}))
	return e.Bytes(), nil
}
