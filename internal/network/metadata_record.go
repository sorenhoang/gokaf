package network

import "github.com/sorenhoang/gokaf/internal/cluster"

func (b *Broker) ApplyMetadataRecord(record cluster.Record) {
	switch record.Type {
	case cluster.TopicUpsert:
		b.applyTopic(record.Topic, record.Partitions)
	case cluster.TopicDelete:
		_ = b.Topics.Delete(record.Topic)
	}
}
