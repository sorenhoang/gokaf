package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

type offsetFetchTopicResponse struct {
	name       string
	partitions []offsetFetchPartitionResponse
}

type offsetFetchPartitionResponse struct {
	index     int32
	offset    int64
	metadata  *string
	errorCode int16
}

func (b *Broker) handleOffsetFetch(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	groupID, err := dec.ReadString()
	if err != nil {
		return nil, err
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	responses := make([]offsetFetchTopicResponse, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		topicResponse := offsetFetchTopicResponse{name: topicName, partitions: make([]offsetFetchPartitionResponse, 0, partitionCount)}
		for j := 0; j < partitionCount; j++ {
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}
			topicResponse.partitions = append(topicResponse.partitions, offsetFetchPartitionResponse{
				index:     partitionIndex,
				offset:    b.Offsets.Fetch(groupID, topicName, partitionIndex),
				errorCode: protocol.ErrNone,
			})
		}
		responses = append(responses, topicResponse)
	}

	e := protocol.NewEncoder()
	writeOffsetFetchResponse(e, responses)
	return e.Bytes(), nil
}

func writeOffsetFetchResponse(e *protocol.Encoder, responses []offsetFetchTopicResponse) {
	e.WriteArrayLen(len(responses))
	for _, topicResponse := range responses {
		e.WriteString(topicResponse.name)
		e.WriteArrayLen(len(topicResponse.partitions))
		for _, partitionResponse := range topicResponse.partitions {
			e.WriteInt32(partitionResponse.index)
			e.WriteInt64(partitionResponse.offset)
			e.WriteNullableString(partitionResponse.metadata)
			e.WriteInt16(partitionResponse.errorCode)
		}
	}
}
