package network

import (
	"bytes"
	"errors"
	"log"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func (b *Broker) handleCreateTopics(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	results := make([]topicResult, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		name, err := dec.ReadString()
		if err != nil {
			return nil, err
		}
		numPartitions, err := dec.ReadInt32()
		if err != nil {
			return nil, err
		}
		replicationFactor, err := dec.ReadInt16()
		if err != nil {
			return nil, err
		}
		if err := drainCreateTopicAssignments(dec); err != nil {
			return nil, err
		}
		if err := drainCreateTopicConfigs(dec); err != nil {
			return nil, err
		}

		results = append(results, topicResult{
			name: name,
			code: b.createTopic(name, numPartitions, replicationFactor, true),
		})
	}

	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}

	e := protocol.NewEncoder()
	writeTopicResults(e, results)
	return e.Bytes(), nil
}

func (b *Broker) createTopic(name string, numPartitions int32, replicationFactor int16, fanOut bool) int16 {
	if numPartitions <= 0 {
		return protocol.ErrInvalidPartitions
	}
	brokerIDs := b.brokerIDs()
	if replicationFactor <= 0 || int(replicationFactor) > len(brokerIDs) {
		return protocol.ErrInvalidReplicationFactor
	}

	replicas := topic.AssignReplicas(numPartitions, brokerIDs, int(replicationFactor))
	partitions := make([]topic.Partition, numPartitions)
	for i := range partitions {
		partitionReplicas := replicas[i]
		partitions[i] = topic.Partition{
			ID:       int32(i),
			Leader:   partitionReplicas[0],
			Replicas: partitionReplicas,
			ISR:      partitionReplicas,
		}
	}

	err := b.Topics.Create(topic.Topic{Name: name, Partitions: partitions})
	switch {
	case errors.Is(err, topic.ErrTopicExists):
		return protocol.ErrTopicAlreadyExists
	case err != nil:
		return protocol.ErrUnknown
	default:
		t := topic.Topic{Name: name, Partitions: partitions}
		if b.Replication != nil {
			b.Replication.StartFollowing(t)
		}
		if fanOut {
			b.fanOutTopic(name, partitions)
		}
		return protocol.ErrNone
	}
}

func (b *Broker) brokerIDs() []int32 {
	if b.Cluster == nil {
		return []int32{b.NodeID}
	}
	brokers := b.Cluster.All()
	ids := make([]int32, 0, len(brokers))
	for _, broker := range brokers {
		ids = append(ids, broker.ID)
	}
	return ids
}

// fanOutTopic pushes the just-created topic to every peer so their registries
// (and Metadata responses) agree.
//
// ponytail: synchronous, best-effort, no retry. A peer that is down or slow
// stalls the client's CreateTopics response and then silently misses the topic
// until it is recreated. The Phase 23 metadata log replaces this with a
// replayable change log; DeleteTopics has the same gap and also does not fan
// out yet.
func (b *Broker) fanOutTopic(name string, partitions []topic.Partition) {
	if b.Cluster == nil {
		return
	}
	body := encodeApplyTopic(name, partitions)
	for _, peer := range b.Cluster.All() {
		if peer.ID == b.NodeID || !b.peerAlive(peer.ID) {
			continue
		}
		if _, err := cluster.NewBrokerClient(peer).Send(protocol.RequestHeader{APIKey: internalApplyTopicKey, APIVersion: 0, CorrelationID: 1}, body); err != nil {
			log.Printf("create topic %s: fan-out to broker %d failed: %v", name, peer.ID, err)
		}
	}
}

func drainCreateTopicAssignments(dec *protocol.Decoder) error {
	assignmentCount, err := dec.ReadArrayLen()
	if err != nil {
		return err
	}
	for i := 0; i < assignmentCount; i++ {
		if _, err := dec.ReadInt32(); err != nil {
			return err
		}
		brokerIDCount, err := dec.ReadArrayLen()
		if err != nil {
			return err
		}
		for j := 0; j < brokerIDCount; j++ {
			if _, err := dec.ReadInt32(); err != nil {
				return err
			}
		}
	}
	return nil
}

func drainCreateTopicConfigs(dec *protocol.Decoder) error {
	configCount, err := dec.ReadArrayLen()
	if err != nil {
		return err
	}
	for i := 0; i < configCount; i++ {
		if _, err := dec.ReadString(); err != nil {
			return err
		}
		if _, err := dec.ReadNullableString(); err != nil {
			return err
		}
	}
	return nil
}
