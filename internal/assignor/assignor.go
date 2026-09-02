package assignor

import "sort"

// TopicPartitions is one topic and the partitions of it assigned to a member.
type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

// Subscription is what one member wants, decoded from its JoinGroup metadata blob.
type Subscription struct {
	MemberID string
	Topics   []string
}

// Range assigns each topic independently to its subscribed members.
func Range(subs []Subscription, partitionCounts map[string]int32) map[string][]TopicPartitions {
	assignments := map[string][]TopicPartitions{}
	topics := sortedTopics(partitionCounts)
	for _, topic := range topics {
		members := subscribedMembers(subs, topic)
		if len(members) == 0 {
			continue
		}

		partitionCount := partitionCounts[topic]
		perMember := partitionCount / int32(len(members))
		extra := partitionCount % int32(len(members))
		cursor := int32(0)
		for i, memberID := range members {
			count := perMember
			if int32(i) < extra {
				count++
			}
			if count == 0 {
				continue
			}
			assignments[memberID] = append(assignments[memberID], TopicPartitions{
				Topic:      topic,
				Partitions: contiguousPartitions(cursor, cursor+count),
			})
			cursor += count
		}
	}
	return assignments
}

// RoundRobin lays topic partitions out in sorted order and deals them to sorted members.
func RoundRobin(subs []Subscription, partitionCounts map[string]int32) map[string][]TopicPartitions {
	members := sortedMembers(subs)
	if len(members) == 0 {
		return map[string][]TopicPartitions{}
	}

	assignments := map[string][]TopicPartitions{}
	i := 0
	for _, topic := range sortedTopics(partitionCounts) {
		for partition := int32(0); partition < partitionCounts[topic]; partition++ {
			// ponytail: this phase assumes uniform subscriptions, so every sorted
			// member is eligible for every topic in the flat partition list.
			memberID := members[i%len(members)]
			assignments[memberID] = appendPartition(assignments[memberID], topic, partition)
			i++
		}
	}
	return assignments
}

func sortedTopics(partitionCounts map[string]int32) []string {
	topics := make([]string, 0, len(partitionCounts))
	for topic := range partitionCounts {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func sortedMembers(subs []Subscription) []string {
	members := make([]string, 0, len(subs))
	for _, sub := range subs {
		members = append(members, sub.MemberID)
	}
	sort.Strings(members)
	return members
}

func subscribedMembers(subs []Subscription, topic string) []string {
	members := make([]string, 0, len(subs))
	for _, sub := range subs {
		if subscribesTo(sub, topic) {
			members = append(members, sub.MemberID)
		}
	}
	sort.Strings(members)
	return members
}

func subscribesTo(sub Subscription, topic string) bool {
	for _, subscribedTopic := range sub.Topics {
		if subscribedTopic == topic {
			return true
		}
	}
	return false
}

func contiguousPartitions(start int32, end int32) []int32 {
	partitions := make([]int32, 0, end-start)
	for partition := start; partition < end; partition++ {
		partitions = append(partitions, partition)
	}
	return partitions
}

func appendPartition(assignments []TopicPartitions, topic string, partition int32) []TopicPartitions {
	if len(assignments) > 0 && assignments[len(assignments)-1].Topic == topic {
		last := &assignments[len(assignments)-1]
		last.Partitions = append(last.Partitions, partition)
		return assignments
	}
	return append(assignments, TopicPartitions{Topic: topic, Partitions: []int32{partition}})
}
