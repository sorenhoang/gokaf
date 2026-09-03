package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

// internalApplyTopicKey is a broker-to-broker only API key, deliberately
// outside the real Kafka range and never advertised in ApiVersions.
//
// ponytail: it rides the same listener as client traffic, so any client could
// inject a topic definition. Real Kafka isolates inter-broker calls on a
// separate listener; acceptable here since the broker has no auth anywhere.
const internalApplyTopicKey int16 = 1000

func (b *Broker) handleApplyTopic(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	name, partitions, err := decodeApplyTopic(dec)
	if err != nil {
		return nil, err
	}

	b.applyTopic(name, partitions)

	e := protocol.NewEncoder()
	writeTopicResults(e, []topicResult{{name: name, code: protocol.ErrNone}})
	return e.Bytes(), nil
}

func (b *Broker) applyTopic(name string, partitions []topic.Partition) {
	b.Topics.Upsert(topic.Topic{Name: name, Partitions: partitions})
	if b.Replication == nil {
		return
	}
	for _, partition := range partitions {
		b.Replication.StopFollowing(name, partition.ID)
	}
	b.Replication.StartFollowing(topic.Topic{Name: name, Partitions: partitions})
}

func encodeApplyTopic(name string, partitions []topic.Partition) []byte {
	e := protocol.NewEncoder()
	e.WriteString(name)
	e.WriteArrayLen(len(partitions))
	for _, partition := range partitions {
		e.WriteInt32(partition.ID)
		e.WriteInt32(partition.Leader)
		writeInt32Array(e, partition.Replicas)
		writeInt32Array(e, partition.ISR)
	}
	return e.Bytes()
}

func decodeApplyTopic(dec *protocol.Decoder) (string, []topic.Partition, error) {
	name, err := dec.ReadString()
	if err != nil {
		return "", nil, err
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		return "", nil, err
	}
	partitions := make([]topic.Partition, 0, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitionID, err := dec.ReadInt32()
		if err != nil {
			return "", nil, err
		}
		leader, err := dec.ReadInt32()
		if err != nil {
			return "", nil, err
		}
		replicas, err := readInt32Array(dec)
		if err != nil {
			return "", nil, err
		}
		isr, err := readInt32Array(dec)
		if err != nil {
			return "", nil, err
		}
		partitions = append(partitions, topic.Partition{
			ID:       partitionID,
			Leader:   leader,
			Replicas: replicas,
			ISR:      isr,
		})
	}
	return name, partitions, nil
}

func readInt32Array(dec *protocol.Decoder) ([]int32, error) {
	count, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	values := make([]int32, 0, count)
	for i := 0; i < count; i++ {
		value, err := dec.ReadInt32()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}
