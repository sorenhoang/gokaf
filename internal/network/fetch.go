package network

import (
	"bytes"
	"log"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
)

type fetchTopicResponse struct {
	name       string
	partitions []fetchPartitionResponse
}

type fetchPartitionResponse struct {
	index         int32
	errorCode     int16
	highWatermark int64
	records       []byte
}

func (b *Broker) handleFetch(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))

	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	responses := make([]fetchTopicResponse, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			return nil, err
		}

		topicResponse := fetchTopicResponse{
			name:       topicName,
			partitions: make([]fetchPartitionResponse, 0, partitionCount),
		}
		for j := 0; j < partitionCount; j++ {
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}
			fetchOffset, err := dec.ReadInt64()
			if err != nil {
				return nil, err
			}
			partitionMaxBytes, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}

			topicResponse.partitions = append(topicResponse.partitions, b.fetchPartition(topicName, partitionIndex, fetchOffset, partitionMaxBytes))
		}
		responses = append(responses, topicResponse)
	}

	e := protocol.NewEncoder()
	writeFetchResponse(e, responses)
	return e.Bytes(), nil
}

func (b *Broker) fetchPartition(topicName string, partitionIndex int32, fetchOffset int64, partitionMaxBytes int32) fetchPartitionResponse {
	response := fetchPartitionResponse{index: partitionIndex}

	t, ok := b.Topics.Get(topicName)
	if !ok || partitionIndex < 0 || int(partitionIndex) >= len(t.Partitions) {
		response.errorCode = protocol.ErrUnknownTopicOrPartition
		return response
	}

	partitionLog, err := b.Logs.Log(topicName, partitionIndex)
	if err != nil {
		log.Printf("fetch %s-%d: open log: %v", topicName, partitionIndex, err)
		response.errorCode = protocol.ErrUnknown
		return response
	}

	highWatermark := partitionLog.EndOffset()
	response.highWatermark = highWatermark
	if fetchOffset < 0 || fetchOffset > highWatermark {
		response.errorCode = protocol.ErrOffsetOutOfRange
		return response
	}
	if fetchOffset == highWatermark {
		response.errorCode = protocol.ErrNone
		return response
	}

	records, err := partitionLog.Read(fetchOffset, int(partitionMaxBytes))
	if err != nil {
		log.Printf("fetch %s-%d: read: %v", topicName, partitionIndex, err)
		response.errorCode = protocol.ErrUnknown
		return response
	}

	response.errorCode = protocol.ErrNone
	response.records = joinRecordPayloads(records)
	return response
}

func joinRecordPayloads(records []storage.Record) []byte {
	var total int
	for _, record := range records {
		total += len(record.Payload)
	}

	out := make([]byte, 0, total)
	for _, record := range records {
		out = append(out, record.Payload...)
	}
	return out
}

func writeFetchResponse(e *protocol.Encoder, responses []fetchTopicResponse) {
	e.WriteArrayLen(len(responses))
	for _, topicResponse := range responses {
		e.WriteString(topicResponse.name)
		e.WriteArrayLen(len(topicResponse.partitions))
		for _, partitionResponse := range topicResponse.partitions {
			e.WriteInt32(partitionResponse.index)
			e.WriteInt16(partitionResponse.errorCode)
			e.WriteInt64(partitionResponse.highWatermark)
			e.WriteBytes(partitionResponse.records)
		}
	}
}
