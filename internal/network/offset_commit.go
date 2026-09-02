package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

type offsetCommitTopicResponse struct {
	name       string
	partitions []offsetCommitPartitionResponse
}

type offsetCommitPartitionResponse struct {
	index     int32
	errorCode int16
}

func (b *Broker) handleOffsetCommit(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	responses := make([]offsetCommitTopicResponse, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		topicResponse := offsetCommitTopicResponse{name: topicName, partitions: make([]offsetCommitPartitionResponse, 0, partitionCount)}
		for j := 0; j < partitionCount; j++ {
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}
			committedOffset, err := dec.ReadInt64()
			if err != nil {
				return nil, err
			}
			if _, err := dec.ReadNullableString(); err != nil {
				return nil, err
			}

			errorCode := protocol.ErrNone
			if err := b.Offsets.Commit(groupID, topicName, partitionIndex, committedOffset); err != nil {
				errorCode = protocol.ErrUnknown
			}
			topicResponse.partitions = append(topicResponse.partitions, offsetCommitPartitionResponse{index: partitionIndex, errorCode: errorCode})
		}
		responses = append(responses, topicResponse)
	}

	e := protocol.NewEncoder()
	writeOffsetCommitResponse(e, responses)
	return e.Bytes(), nil
}

func writeOffsetCommitResponse(e *protocol.Encoder, responses []offsetCommitTopicResponse) {
	e.WriteArrayLen(len(responses))
	for _, topicResponse := range responses {
		e.WriteString(topicResponse.name)
		e.WriteArrayLen(len(topicResponse.partitions))
		for _, partitionResponse := range topicResponse.partitions {
			e.WriteInt32(partitionResponse.index)
			e.WriteInt16(partitionResponse.errorCode)
		}
	}
}
