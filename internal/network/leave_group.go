package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleLeaveGroup(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	memberID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	e := protocol.NewEncoder()
	e.WriteInt16(b.Groups.Leave(group.LeaveRequest{GroupID: groupID, MemberID: memberID}))
	return e.Bytes(), nil
}
