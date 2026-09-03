package network

import (
	"encoding/binary"
	"errors"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
	"github.com/sorenhoang/gokaf/internal/topic"
)

// These errors are the operation-layer contract. Wire handlers translate them
// to Kafka error codes; HTTP handlers map them to status codes.
var (
	ErrNotController            = errors.New("not controller")
	ErrUnknownTopicOrPartition  = errors.New("unknown topic or partition")
	ErrNotLeader                = errors.New("not leader for partition")
	ErrCorruptBatch             = errors.New("corrupt record batch")
	ErrOffsetOutOfRange         = errors.New("offset out of range")
	ErrInvalidPartitions        = errors.New("invalid partition count")
	ErrInvalidReplicationFactor = errors.New("invalid replication factor")
)

// CreateTopic is the operation-layer entry point for topic creation, shared by
// the wire handler and the HTTP API.
func (b *Broker) CreateTopic(name string, partitions int32, replicationFactor int16) error {
	if b.MetadataLog != nil && b.controllerID() != b.NodeID {
		return ErrNotController
	}
	return codeToError(b.createTopic(name, partitions, replicationFactor, true))
}

// DeleteTopic is the operation-layer entry point for topic deletion.
func (b *Broker) DeleteTopic(name string) error {
	if b.MetadataLog != nil && b.controllerID() != b.NodeID {
		return ErrNotController
	}
	return codeToError(b.deleteTopic(name))
}

// Produce appends batch to (topic, partition) with acks=1 semantics and no
// idempotency — the path the HTTP produce endpoint uses. The batch's base
// offset is stamped in place, so the on-disk bytes match a wire Produce of the
// same batch.
func (b *Broker) Produce(topicName string, partition int32, batch []byte) (int64, error) {
	t, ok := b.Topics.Get(topicName)
	if !ok || partition < 0 || int(partition) >= len(t.Partitions) {
		return -1, ErrUnknownTopicOrPartition
	}
	if t.Partitions[partition].Leader != b.NodeID {
		return -1, ErrNotLeader
	}
	if !validRecordBatch(batch) {
		return -1, ErrCorruptBatch
	}
	partitionLog, err := b.Logs.Log(topicName, partition)
	if err != nil {
		return -1, err
	}
	return partitionLog.AppendWithOffset(batch, func(offset int64) {
		binary.BigEndian.PutUint64(batch[0:8], uint64(offset))
	})
}

// Fetch returns the stored record batches at or after offset (up to maxBytes)
// plus the partition high watermark.
func (b *Broker) Fetch(topicName string, partition int32, offset int64, maxBytes int32) ([]storage.Record, int64, error) {
	t, ok := b.Topics.Get(topicName)
	if !ok || partition < 0 || int(partition) >= len(t.Partitions) {
		return nil, 0, ErrUnknownTopicOrPartition
	}
	partitionLog, err := b.Logs.Log(topicName, partition)
	if err != nil {
		return nil, 0, err
	}
	endOffset := partitionLog.EndOffset()
	highWatermark := endOffset
	if b.Replication != nil {
		highWatermark = b.Replication.HighWatermark(topicName, partition, endOffset)
	}
	if offset < 0 || offset > endOffset {
		return nil, highWatermark, ErrOffsetOutOfRange
	}
	if offset == endOffset {
		return nil, highWatermark, nil
	}
	records, err := partitionLog.Read(offset, int(maxBytes))
	if err != nil {
		return nil, highWatermark, err
	}
	return records, highWatermark, nil
}

func codeToError(code int16) error {
	switch code {
	case protocol.ErrNone:
		return nil
	case protocol.ErrTopicAlreadyExists:
		return topic.ErrTopicExists
	case protocol.ErrUnknownTopicOrPartition:
		return topic.ErrTopicNotFound
	case protocol.ErrInvalidPartitions:
		return ErrInvalidPartitions
	case protocol.ErrInvalidReplicationFactor:
		return ErrInvalidReplicationFactor
	case protocol.ErrNotController:
		return ErrNotController
	default:
		return errors.New("broker operation failed")
	}
}

type BrokerInfo struct {
	NodeID       int32  `json:"node_id"`
	Host         string `json:"host"`
	Port         int32  `json:"port"`
	ControllerID int32  `json:"controller_id"`
}

type PartitionInfo struct {
	ID            int32   `json:"id"`
	Leader        int32   `json:"leader"`
	Replicas      []int32 `json:"replicas"`
	ISR           []int32 `json:"isr"`
	StartOffset   int64   `json:"start_offset"`
	EndOffset     int64   `json:"end_offset"`
	HighWatermark int64   `json:"high_watermark"`
}

type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

func (b *Broker) BrokerInfo() BrokerInfo {
	controllerID := b.NodeID
	if b.ControllerID != nil {
		controllerID = b.ControllerID()
	}
	return BrokerInfo{NodeID: b.NodeID, Host: b.Host, Port: b.Port, ControllerID: controllerID}
}

func (b *Broker) TopicInfos() []TopicInfo {
	topics := b.Topics.All()
	infos := make([]TopicInfo, 0, len(topics))
	for _, t := range topics {
		info := TopicInfo{Name: t.Name, Partitions: make([]PartitionInfo, 0, len(t.Partitions))}
		for _, p := range t.Partitions {
			endOffset := int64(0)
			startOffset := int64(0)
			if containsInt32(p.Replicas, b.NodeID) && b.Logs != nil {
				if partitionLog, err := b.Logs.Log(t.Name, p.ID); err == nil {
					startOffset = partitionLog.StartOffset()
					endOffset = partitionLog.EndOffset()
				}
			}
			highWatermark := endOffset
			if b.Replication != nil {
				highWatermark = b.Replication.HighWatermark(t.Name, p.ID, endOffset)
			}
			info.Partitions = append(info.Partitions, PartitionInfo{
				ID: p.ID, Leader: p.Leader, Replicas: append([]int32(nil), p.Replicas...), ISR: append([]int32(nil), p.ISR...),
				StartOffset: startOffset, EndOffset: endOffset, HighWatermark: highWatermark,
			})
		}
		infos = append(infos, info)
	}
	return infos
}

func wireError(err error) int16 {
	switch {
	case err == nil:
		return protocol.ErrNone
	case errors.Is(err, ErrNotController):
		return protocol.ErrNotController
	case errors.Is(err, topic.ErrTopicExists):
		return protocol.ErrTopicAlreadyExists
	case errors.Is(err, topic.ErrTopicNotFound), errors.Is(err, ErrUnknownTopicOrPartition):
		return protocol.ErrUnknownTopicOrPartition
	case errors.Is(err, ErrNotLeader):
		return protocol.ErrNotLeaderForPartition
	case errors.Is(err, ErrCorruptBatch):
		return protocol.ErrCorruptMessage
	case errors.Is(err, ErrOffsetOutOfRange):
		return protocol.ErrOffsetOutOfRange
	case errors.Is(err, ErrInvalidPartitions):
		return protocol.ErrInvalidPartitions
	case errors.Is(err, ErrInvalidReplicationFactor):
		return protocol.ErrInvalidReplicationFactor
	default:
		return protocol.ErrUnknown
	}
}

func containsInt32(values []int32, want int32) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
