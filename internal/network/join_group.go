package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleJoinGroup(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	sessionTimeoutMS, err := dec.ReadInt32()
	if err != nil {
		return nil, err
	}
	memberID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	protocolType, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	protocols, err := readJoinGroupProtocols(dec)
	if err != nil {
		return nil, err
	}

	clientID := ""
	if header.ClientID != nil {
		clientID = *header.ClientID
	}
	result := b.Groups.Join(group.JoinRequest{
		GroupID:          groupID,
		ClientID:         clientID,
		SessionTimeoutMS: sessionTimeoutMS,
		MemberID:         memberID,
		ProtocolType:     protocolType,
		Protocols:        protocols,
	})

	e := protocol.NewEncoder()
	e.WriteInt16(result.ErrorCode)
	e.WriteInt32(result.GenerationID)
	e.WriteString(result.Protocol)
	e.WriteString(result.LeaderID)
	e.WriteString(result.MemberID)
	e.WriteArrayLen(len(result.Members))
	for _, member := range result.Members {
		e.WriteString(member.ID)
		e.WriteBytes(member.Metadata)
	}
	return e.Bytes(), nil
}

func readJoinGroupProtocols(dec *protocol.Decoder) ([]group.Protocol, error) {
	protocolCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	protocols := make([]group.Protocol, 0, protocolCount)
	for i := 0; i < protocolCount; i++ {
		name, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		metadata, err := dec.ReadBytes()
		if err != nil {
			return nil, err
		}
		protocols = append(protocols, group.Protocol{Name: name, Metadata: metadata})
	}
	return protocols, nil
}
