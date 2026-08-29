package network

import (
	"bytes"
	"errors"

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
			code: b.createTopic(name, numPartitions, replicationFactor),
		})
	}

	if _, err := dec.ReadInt32(); err != nil {
		return nil, err
	}

	e := protocol.NewEncoder()
	writeTopicResults(e, results)
	return e.Bytes(), nil
}

func (b *Broker) createTopic(name string, numPartitions int32, replicationFactor int16) int16 {
	if numPartitions <= 0 {
		return protocol.ErrInvalidPartitions
	}
	if replicationFactor <= 0 || replicationFactor > 1 {
		return protocol.ErrInvalidReplicationFactor
	}

	partitions := make([]topic.Partition, numPartitions)
	for i := range partitions {
		partitions[i] = topic.Partition{
			ID:       int32(i),
			Leader:   b.NodeID,
			Replicas: []int32{b.NodeID},
			ISR:      []int32{b.NodeID},
		}
	}

	err := b.Topics.Create(topic.Topic{Name: name, Partitions: partitions})
	switch {
	case errors.Is(err, topic.ErrTopicExists):
		return protocol.ErrTopicAlreadyExists
	case err != nil:
		return protocol.ErrUnknown
	default:
		return protocol.ErrNone
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
