package network

import (
	"bytes"
	"log"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

const (
	listOffsetLatest   int64 = -1
	listOffsetEarliest int64 = -2
)

type listOffsetsTopicResponse struct {
	name       string
	partitions []listOffsetsPartitionResponse
}

type listOffsetsPartitionResponse struct {
	index     int32
	errorCode int16
	timestamp int64
	offset    int64
}

func (b *Broker) handleListOffsets(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))

	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	responses := make([]listOffsetsTopicResponse, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			return nil, err
		}

		topicResponse := listOffsetsTopicResponse{
			name:       topicName,
			partitions: make([]listOffsetsPartitionResponse, 0, partitionCount),
		}
		for j := 0; j < partitionCount; j++ {
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}
			timestamp, err := dec.ReadInt64()
			if err != nil {
				return nil, err
			}

			topicResponse.partitions = append(topicResponse.partitions, b.listOffset(topicName, partitionIndex, timestamp))
		}
		responses = append(responses, topicResponse)
	}

	e := protocol.NewEncoder()
	writeListOffsetsResponse(e, responses)
	return e.Bytes(), nil
}

func (b *Broker) listOffset(topicName string, partitionIndex int32, timestamp int64) listOffsetsPartitionResponse {
	response := listOffsetsPartitionResponse{
		index:     partitionIndex,
		errorCode: protocol.ErrUnknownTopicOrPartition,
		timestamp: -1,
		offset:    -1,
	}

	t, ok := b.Topics.Get(topicName)
	if !ok || partitionIndex < 0 || int(partitionIndex) >= len(t.Partitions) {
		return response
	}

	partitionLog, err := b.Logs.Log(topicName, partitionIndex)
	if err != nil {
		log.Printf("list offsets %s-%d: open log: %v", topicName, partitionIndex, err)
		response.errorCode = protocol.ErrUnknown
		return response
	}

	response.errorCode = protocol.ErrNone
	switch timestamp {
	case listOffsetLatest:
		response.offset = partitionLog.EndOffset()
	case listOffsetEarliest:
		response.offset = partitionLog.StartOffset()
	default:
		// ponytail: no time index yet; real timestamp lookup lands with .timeindex.
		response.offset = partitionLog.StartOffset()
	}
	return response
}

func writeListOffsetsResponse(e *protocol.Encoder, responses []listOffsetsTopicResponse) {
	e.WriteArrayLen(len(responses))
	for _, topicResponse := range responses {
		e.WriteString(topicResponse.name)
		e.WriteArrayLen(len(topicResponse.partitions))
		for _, partitionResponse := range topicResponse.partitions {
			e.WriteInt32(partitionResponse.index)
			e.WriteInt16(partitionResponse.errorCode)
			e.WriteInt64(partitionResponse.timestamp)
			e.WriteInt64(partitionResponse.offset)
		}
	}
}
