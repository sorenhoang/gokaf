package network

import (
	"errors"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

// These errors are the operation-layer contract. Wire handlers translate them
// to Kafka error codes; HTTP handlers can later map them to status codes.
var (
	ErrNotController = errors.New("not controller")
)

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
	case errors.Is(err, topic.ErrTopicNotFound):
		return protocol.ErrUnknownTopicOrPartition
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
