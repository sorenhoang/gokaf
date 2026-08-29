package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

const (
	errorNone                    int16 = 0
	errorUnknownTopicOrPartition int16 = 3
)

func (b *Broker) handleMetadata(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	var topics []topic.Topic
	var unknownTopics []string
	if topicCount <= 0 {
		// v0: empty array means "all topics". A null array (-1) can't come
		// from a v0 client; treat it the same to be safe.
		topics = b.Topics.All()
	} else {
		for i := 0; i < topicCount; i++ {
			name, err := dec.ReadString()
			if err != nil {
				return nil, err
			}

			if t, ok := b.Topics.Get(name); ok {
				topics = append(topics, t)
			} else {
				unknownTopics = append(unknownTopics, name)
			}
		}
	}

	e := protocol.NewEncoder()
	e.WriteArrayLen(1)
	e.WriteInt32(b.NodeID)
	e.WriteString(b.Host)
	e.WriteInt32(b.Port)

	e.WriteArrayLen(len(topics) + len(unknownTopics))
	for _, t := range topics {
		writeMetadataTopic(e, t)
	}
	for _, name := range unknownTopics {
		e.WriteInt16(errorUnknownTopicOrPartition)
		e.WriteString(name)
		e.WriteArrayLen(0)
	}

	return e.Bytes(), nil
}

func writeMetadataTopic(e *protocol.Encoder, t topic.Topic) {
	e.WriteInt16(errorNone)
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))
	for _, p := range t.Partitions {
		e.WriteInt16(errorNone)
		e.WriteInt32(p.ID)
		e.WriteInt32(p.Leader)
		writeInt32Array(e, p.Replicas)
		writeInt32Array(e, p.ISR)
	}
}

func writeInt32Array(e *protocol.Encoder, values []int32) {
	e.WriteArrayLen(len(values))
	for _, value := range values {
		e.WriteInt32(value)
	}
}
