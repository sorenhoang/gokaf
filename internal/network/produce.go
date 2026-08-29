package network

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"log"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

const recordBatchHeaderSize = 61

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type produceTopicResponse struct {
	name       string
	partitions []producePartitionResponse
}

type producePartitionResponse struct {
	index      int32
	errorCode  int16
	baseOffset int64
}

func (b *Broker) handleProduce(header protocol.RequestHeader, body []byte) ([]byte, error) {
	reader := bytes.NewReader(body)
	dec := protocol.NewDecoder(reader)

	if _, err := dec.ReadInt16(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	responses := make([]produceTopicResponse, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		topicName, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			return nil, err
		}

		topicResponse := produceTopicResponse{
			name:       topicName,
			partitions: make([]producePartitionResponse, 0, partitionCount),
		}
		for j := 0; j < partitionCount; j++ {
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				return nil, err
			}
			batch, err := dec.ReadBytes()
			if err != nil {
				return nil, err
			}

			topicResponse.partitions = append(topicResponse.partitions, b.producePartition(topicName, partitionIndex, batch))
		}
		responses = append(responses, topicResponse)
	}

	e := protocol.NewEncoder()
	writeProduceResponse(e, responses)
	return e.Bytes(), nil
}

func (b *Broker) producePartition(topicName string, partitionIndex int32, batch []byte) producePartitionResponse {
	response := producePartitionResponse{index: partitionIndex, errorCode: protocol.ErrUnknown, baseOffset: -1}

	t, ok := b.Topics.Get(topicName)
	if !ok || partitionIndex < 0 || int(partitionIndex) >= len(t.Partitions) {
		response.errorCode = protocol.ErrUnknownTopicOrPartition
		return response
	}
	if !validRecordBatch(batch) {
		response.errorCode = protocol.ErrCorruptMessage
		return response
	}

	partitionLog, err := b.Logs.Log(topicName, partitionIndex)
	if err != nil {
		log.Printf("produce %s-%d: open log: %v", topicName, partitionIndex, err)
		return response
	}

	baseOffset, err := partitionLog.AppendWithOffset(batch, func(offset int64) {
		binary.BigEndian.PutUint64(batch[0:8], uint64(offset))
	})
	if err != nil {
		log.Printf("produce %s-%d: append: %v", topicName, partitionIndex, err)
		return response
	}

	response.errorCode = protocol.ErrNone
	response.baseOffset = baseOffset
	return response
}

func validRecordBatch(batch []byte) bool {
	if len(batch) < recordBatchHeaderSize {
		return false
	}
	if batch[16] != 2 {
		return false
	}

	wantCRC := binary.BigEndian.Uint32(batch[17:21])
	gotCRC := crc32.Checksum(batch[21:], crc32cTable)
	return gotCRC == wantCRC
}

func writeProduceResponse(e *protocol.Encoder, responses []produceTopicResponse) {
	e.WriteArrayLen(len(responses))
	for _, topicResponse := range responses {
		e.WriteString(topicResponse.name)
		e.WriteArrayLen(len(topicResponse.partitions))
		for _, partitionResponse := range topicResponse.partitions {
			e.WriteInt32(partitionResponse.index)
			e.WriteInt16(partitionResponse.errorCode)
			e.WriteInt64(partitionResponse.baseOffset)
		}
	}
}
