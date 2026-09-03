package network

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/sorenhoang/gokaf/internal/assignor"
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

// --- consumer group + producer admin views ---

type GroupPartitionInfo struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	HighWatermark   int64  `json:"high_watermark"`
	Lag             int64  `json:"lag"`
}

type GroupMemberInfo struct {
	ID         string               `json:"id"`
	Assignment []GroupPartitionInfo `json:"assignment"`
}

type GroupInfo struct {
	ID           string            `json:"id"`
	State        string            `json:"state"`
	GenerationID int32             `json:"generation_id"`
	LeaderID     string            `json:"leader_id"`
	Protocol     string            `json:"protocol"`
	Members      []GroupMemberInfo `json:"members"`
}

func (b *Broker) GroupInfos() []GroupInfo {
	if b.Groups == nil {
		return nil
	}
	snaps := b.Groups.Snapshot()
	out := make([]GroupInfo, 0, len(snaps))
	for _, g := range snaps {
		info := GroupInfo{
			ID: g.ID, State: g.State, GenerationID: g.GenerationID,
			LeaderID: g.LeaderID, Protocol: g.Protocol, Members: []GroupMemberInfo{},
		}
		for _, m := range g.Members {
			member := GroupMemberInfo{ID: m.ID, Assignment: []GroupPartitionInfo{}}
			decoded, err := assignor.DecodeAssignment(m.Assignment)
			if err == nil {
				for _, tp := range decoded {
					for _, p := range tp.Partitions {
						member.Assignment = append(member.Assignment, b.groupPartitionInfo(g.ID, tp.Topic, p))
					}
				}
			}
			info.Members = append(info.Members, member)
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (b *Broker) groupPartitionInfo(groupID, topicName string, partition int32) GroupPartitionInfo {
	committed := int64(-1)
	if b.Offsets != nil {
		committed = b.Offsets.Fetch(groupID, topicName, partition)
	}
	hwm := b.highWatermarkOf(topicName, partition)
	from := committed
	if from < 0 {
		from = 0
	}
	lag := hwm - from
	if lag < 0 {
		lag = 0
	}
	return GroupPartitionInfo{Topic: topicName, Partition: partition, CommittedOffset: committed, HighWatermark: hwm, Lag: lag}
}

func (b *Broker) highWatermarkOf(topicName string, partition int32) int64 {
	if b.Logs == nil {
		return 0
	}
	partitionLog, err := b.Logs.Log(topicName, partition)
	if err != nil {
		return 0
	}
	end := partitionLog.EndOffset()
	if b.Replication != nil {
		return b.Replication.HighWatermark(topicName, partition, end)
	}
	return end
}

// ResetGroupOffset overwrites a group's committed offset for one partition.
func (b *Broker) ResetGroupOffset(groupID, topicName string, partition int32, offset int64) error {
	if b.Offsets == nil {
		return errors.New("offset store unavailable")
	}
	return b.Offsets.Commit(groupID, topicName, partition, offset)
}

type ProducerPartitionInfo struct {
	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	LastSequence int32  `json:"last_sequence"`
	LastOffset   int64  `json:"last_offset"`
}

type ProducerInfo struct {
	ProducerID int64                   `json:"producer_id"`
	Epoch      int16                   `json:"epoch"`
	Partitions []ProducerPartitionInfo `json:"partitions"`
}

func (b *Broker) ProducerInfos() []ProducerInfo {
	if b.Producers == nil {
		return nil
	}
	out := make([]ProducerInfo, 0)
	for _, s := range b.Producers.Snapshot() {
		info := ProducerInfo{ProducerID: s.ProducerID, Epoch: s.Epoch, Partitions: []ProducerPartitionInfo{}}
		for _, p := range s.Partitions {
			info.Partitions = append(info.Partitions, ProducerPartitionInfo{
				Topic: p.Topic, Partition: p.Partition, LastSequence: p.LastSequence, LastOffset: p.LastOffset,
			})
		}
		out = append(out, info)
	}
	return out
}

// --- cluster view ---

type PeerReachability struct {
	ID    int32 `json:"id"`
	Alive bool  `json:"alive"`
}

type ClusterInfo struct {
	Brokers      []BrokerInfo       `json:"brokers"`
	ControllerID int32              `json:"controller_id"`
	Self         int32              `json:"self"`
	Peers        []PeerReachability `json:"peers"`
}

// ClusterInfo is this broker's own view of the cluster: the membership, who it
// thinks the controller is, and which peers it can currently reach.
func (b *Broker) ClusterInfo() ClusterInfo {
	info := ClusterInfo{
		Brokers:      []BrokerInfo{},
		ControllerID: b.BrokerInfo().ControllerID,
		Self:         b.NodeID,
		Peers:        []PeerReachability{},
	}
	brokers := b.metadataBrokers()
	for _, br := range brokers {
		info.Brokers = append(info.Brokers, BrokerInfo{NodeID: br.ID, Host: br.Host, Port: br.Port, ControllerID: info.ControllerID})
		alive := br.ID == b.NodeID
		if !alive && b.IsPeerAlive != nil {
			alive = b.IsPeerAlive(br.ID)
		} else if !alive {
			alive = true
		}
		info.Peers = append(info.Peers, PeerReachability{ID: br.ID, Alive: alive})
	}
	return info
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
			isr := append([]int32(nil), p.ISR...)
			if b.Replication != nil {
				highWatermark = b.Replication.HighWatermark(t.Name, p.ID, endOffset)
				// When this broker leads the partition, prefer the live
				// time-based ISR from replication state — that's what shrinks
				// when a follower lags, before any failover touches the registry.
				if live := b.Replication.ISR(t.Name, p.ID); len(live) > 0 {
					isr = live
				}
			}
			info.Partitions = append(info.Partitions, PartitionInfo{
				ID: p.ID, Leader: p.Leader, Replicas: append([]int32(nil), p.Replicas...), ISR: isr,
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
