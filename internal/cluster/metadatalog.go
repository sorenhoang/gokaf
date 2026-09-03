package cluster

import (
	"bytes"
	"fmt"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

type RecordType int8

const (
	TopicUpsert RecordType = iota
	TopicDelete
)

type Record struct {
	Type       RecordType
	Topic      string
	Partitions []topic.Partition
}

type MetadataLog struct {
	log *storage.Log
}

func Open(dir string) (*MetadataLog, error) {
	return OpenMetadataLog(dir)
}

func OpenMetadataLog(dir string) (*MetadataLog, error) {
	log, err := storage.Open(dir)
	if err != nil {
		return nil, err
	}
	return &MetadataLog{log: log}, nil
}

func (ml *MetadataLog) Append(record Record) (int64, error) {
	return ml.log.Append(encodeMetadataRecord(record))
}

func (ml *MetadataLog) AppendRaw(payload []byte) (int64, error) {
	return ml.log.Append(payload)
}

func (ml *MetadataLog) ReadFrom(offset int64) ([]Record, error) {
	raw, err := ml.ReadRawFrom(offset)
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, len(raw))
	for _, payload := range raw {
		record, err := decodeMetadataRecord(payload)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (ml *MetadataLog) ReplayAll() ([]Record, error) {
	return ml.ReadFrom(0)
}

func (ml *MetadataLog) ReadRawFrom(offset int64) ([][]byte, error) {
	var payloads [][]byte
	next := offset
	for {
		records, err := ml.log.Read(next, -1)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return payloads, nil
		}
		for _, record := range records {
			payloads = append(payloads, append([]byte(nil), record.Payload...))
			next = record.Offset + 1
		}
	}
}

func (ml *MetadataLog) EndOffset() int64 {
	return ml.log.EndOffset()
}

func (ml *MetadataLog) Close() error {
	return ml.log.Close()
}

func encodeMetadataRecord(record Record) []byte {
	e := protocol.NewEncoder()
	e.WriteInt8(int8(record.Type))
	e.WriteString(record.Topic)
	if record.Type == TopicUpsert {
		e.WriteArrayLen(len(record.Partitions))
		for _, partition := range record.Partitions {
			e.WriteInt32(partition.ID)
			e.WriteInt32(partition.Leader)
			writeMetadataIDs(e, partition.Replicas)
			writeMetadataIDs(e, partition.ISR)
		}
	}
	return e.Bytes()
}

func decodeMetadataRecord(payload []byte) (Record, error) {
	dec := protocol.NewDecoder(bytes.NewReader(payload))
	typeValue, err := dec.ReadInt8()
	if err != nil {
		return Record{}, err
	}
	if typeValue != int8(TopicUpsert) && typeValue != int8(TopicDelete) {
		return Record{}, fmt.Errorf("unknown metadata record type %d", typeValue)
	}
	topicName, err := dec.ReadString()
	if err != nil {
		return Record{}, err
	}
	record := Record{Type: RecordType(typeValue), Topic: topicName}
	if record.Type != TopicUpsert {
		return record, nil
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		return Record{}, err
	}
	record.Partitions = make([]topic.Partition, 0, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitionID, err := dec.ReadInt32()
		if err != nil {
			return Record{}, err
		}
		leader, err := dec.ReadInt32()
		if err != nil {
			return Record{}, err
		}
		replicas, err := readMetadataIDs(dec)
		if err != nil {
			return Record{}, err
		}
		isr, err := readMetadataIDs(dec)
		if err != nil {
			return Record{}, err
		}
		record.Partitions = append(record.Partitions, topic.Partition{ID: partitionID, Leader: leader, Replicas: replicas, ISR: isr})
	}
	return record, nil
}

func writeMetadataIDs(e *protocol.Encoder, ids []int32) {
	e.WriteArrayLen(len(ids))
	for _, id := range ids {
		e.WriteInt32(id)
	}
}

func readMetadataIDs(dec *protocol.Decoder) ([]int32, error) {
	count, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	ids := make([]int32, 0, count)
	for i := 0; i < count; i++ {
		id, err := dec.ReadInt32()
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
