package assignor

import (
	"reflect"
	"sort"
	"testing"
)

func TestRangeMatchesKafkaJavadocExample(t *testing.T) {
	subs := []Subscription{
		{MemberID: "C1", Topics: []string{"t0", "t1"}},
		{MemberID: "C0", Topics: []string{"t0", "t1"}},
	}
	counts := map[string]int32{"t0": 3, "t1": 3}

	got := flattenAssignment(Range(subs, counts))
	want := map[string][]string{
		"C0": {"t0:0", "t0:1", "t1:0", "t1:1"},
		"C1": {"t0:2", "t1:2"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Range assignment mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestRoundRobinMatchesKafkaJavadocExample(t *testing.T) {
	subs := []Subscription{
		{MemberID: "C1", Topics: []string{"t0", "t1"}},
		{MemberID: "C0", Topics: []string{"t0", "t1"}},
	}
	counts := map[string]int32{"t0": 3, "t1": 3}

	got := flattenAssignment(RoundRobin(subs, counts))
	want := map[string][]string{
		"C0": {"t0:0", "t0:2", "t1:1"},
		"C1": {"t0:1", "t1:0", "t1:2"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RoundRobin assignment mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestSingleTopicExamples(t *testing.T) {
	tests := []struct {
		name      string
		assign    func([]Subscription, map[string]int32) map[string][]TopicPartitions
		parts     int32
		members   []string
		wantParts map[string][]int32
	}{
		{
			name:      "range 3 partitions 2 members",
			assign:    Range,
			parts:     3,
			members:   []string{"C0", "C1"},
			wantParts: map[string][]int32{"C0": {0, 1}, "C1": {2}},
		},
		{
			name:      "roundrobin 3 partitions 2 members",
			assign:    RoundRobin,
			parts:     3,
			members:   []string{"C0", "C1"},
			wantParts: map[string][]int32{"C0": {0, 2}, "C1": {1}},
		},
		{
			name:      "range 3 partitions 3 members",
			assign:    Range,
			parts:     3,
			members:   []string{"C0", "C1", "C2"},
			wantParts: map[string][]int32{"C0": {0}, "C1": {1}, "C2": {2}},
		},
		{
			name:      "roundrobin 3 partitions 3 members",
			assign:    RoundRobin,
			parts:     3,
			members:   []string{"C0", "C1", "C2"},
			wantParts: map[string][]int32{"C0": {0}, "C1": {1}, "C2": {2}},
		},
		{
			name:      "range 5 partitions 2 members",
			assign:    Range,
			parts:     5,
			members:   []string{"C0", "C1"},
			wantParts: map[string][]int32{"C0": {0, 1, 2}, "C1": {3, 4}},
		},
		{
			name:      "roundrobin 5 partitions 2 members",
			assign:    RoundRobin,
			parts:     5,
			members:   []string{"C0", "C1"},
			wantParts: map[string][]int32{"C0": {0, 2, 4}, "C1": {1, 3}},
		},
		{
			name:      "range 3 partitions 1 member",
			assign:    Range,
			parts:     3,
			members:   []string{"C0"},
			wantParts: map[string][]int32{"C0": {0, 1, 2}},
		},
		{
			name:      "roundrobin 3 partitions 1 member",
			assign:    RoundRobin,
			parts:     3,
			members:   []string{"C0"},
			wantParts: map[string][]int32{"C0": {0, 1, 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subs := subscriptions(tt.members, "events")
			got := partitionsForTopic(tt.assign(subs, map[string]int32{"events": tt.parts}), "events")

			if !reflect.DeepEqual(got, tt.wantParts) {
				t.Fatalf("assignment mismatch\ngot:  %#v\nwant: %#v", got, tt.wantParts)
			}
		})
	}
}

func TestAssignorsHandleEmptyInputs(t *testing.T) {
	if got := Range(nil, map[string]int32{"events": 3}); len(got) != 0 {
		t.Fatalf("Range with no members returned %#v", got)
	}
	if got := RoundRobin(nil, map[string]int32{"events": 3}); len(got) != 0 {
		t.Fatalf("RoundRobin with no members returned %#v", got)
	}
	if got := Range(subscriptions([]string{"C0"}, "events"), map[string]int32{"events": 0}); len(got["C0"]) != 0 {
		t.Fatalf("Range with no partitions returned %#v", got)
	}
	if got := RoundRobin(subscriptions([]string{"C0"}, "events"), map[string]int32{"events": 0}); len(got["C0"]) != 0 {
		t.Fatalf("RoundRobin with no partitions returned %#v", got)
	}
}

func subscriptions(memberIDs []string, topics ...string) []Subscription {
	subs := make([]Subscription, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		subs = append(subs, Subscription{MemberID: memberID, Topics: topics})
	}
	return subs
}

func flattenAssignment(assignments map[string][]TopicPartitions) map[string][]string {
	flat := map[string][]string{}
	for memberID, topicPartitions := range assignments {
		for _, assignment := range topicPartitions {
			for _, partition := range assignment.Partitions {
				flat[memberID] = append(flat[memberID], assignment.Topic+":"+itoa(partition))
			}
		}
		sort.Strings(flat[memberID])
	}
	return flat
}

func partitionsForTopic(assignments map[string][]TopicPartitions, topic string) map[string][]int32 {
	got := map[string][]int32{}
	for memberID, topicPartitions := range assignments {
		for _, assignment := range topicPartitions {
			if assignment.Topic == topic {
				got[memberID] = append(got[memberID], assignment.Partitions...)
			}
		}
	}
	return got
}

func itoa(n int32) string {
	if n == 0 {
		return "0"
	}
	var buf [11]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
