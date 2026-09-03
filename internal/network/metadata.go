package network

import (
	"bytes"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func (b *Broker) handleMetadata(header protocol.RequestHeader, body []byte) ([]byte, error) {
	dec := protocol.NewDecoder(bytes.NewReader(body))
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	allTopics := b.TopicInfos()
	topicByName := make(map[string]TopicInfo, len(allTopics))
	for _, info := range allTopics {
		topicByName[info.Name] = info
	}
	var topics []TopicInfo
	var unknownTopics []string
	if topicCount <= 0 {
		// v0: empty array means "all topics". A null array (-1) can't come
		// from a v0 client; treat it the same to be safe.
		topics = allTopics
	} else {
		for i := 0; i < topicCount; i++ {
			name, err := dec.ReadString()
			if err != nil {
				return nil, err
			}

			if t, ok := topicByName[name]; ok {
				topics = append(topics, t)
			} else {
				unknownTopics = append(unknownTopics, name)
			}
		}
	}

	e := protocol.NewEncoder()
	brokers := b.metadataBrokers()
	e.WriteArrayLen(len(brokers))
	for _, broker := range brokers {
		e.WriteInt32(broker.ID)
		e.WriteString(broker.Host)
		e.WriteInt32(broker.Port)
	}
	e.WriteInt32(b.BrokerInfo().ControllerID)

	e.WriteArrayLen(len(topics) + len(unknownTopics))
	for _, t := range topics {
		writeMetadataTopic(e, t)
	}
	for _, name := range unknownTopics {
		e.WriteInt16(protocol.ErrUnknownTopicOrPartition)
		e.WriteString(name)
		e.WriteArrayLen(0)
	}

	return e.Bytes(), nil
}

func (b *Broker) metadataBrokers() []cluster.Broker {
	if b.Cluster == nil {
		return []cluster.Broker{{ID: b.NodeID, Host: b.Host, Port: b.Port}}
	}
	return b.Cluster.All()
}

func writeMetadataTopic(e *protocol.Encoder, t TopicInfo) {
	e.WriteInt16(protocol.ErrNone)
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))
	for _, p := range t.Partitions {
		e.WriteInt16(protocol.ErrNone)
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
