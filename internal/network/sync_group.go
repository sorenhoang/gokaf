package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/group"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleSyncGroup(header protocol.RequestHeader, body []byte) ([]byte, error) {
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
	assignments, err := readSyncGroupAssignments(dec)
	if err != nil {
		return nil, err
	}

	result := b.Groups.Sync(group.SyncRequest{
		GroupID:      groupID,
		GenerationID: generationID,
		MemberID:     memberID,
		Assignments:  assignments,
	})

	e := protocol.NewEncoder()
	e.WriteInt16(result.ErrorCode)
	e.WriteBytes(result.Assignment)
	return e.Bytes(), nil
}

func readSyncGroupAssignments(dec *protocol.Decoder) (map[string][]byte, error) {
	assignmentCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	assignments := make(map[string][]byte, assignmentCount)
	for i := 0; i < assignmentCount; i++ {
		memberID, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		assignment, err := dec.ReadBytes()
		if err != nil {
			return nil, err
		}
		assignments[memberID] = assignment
	}
	return assignments, nil
}
