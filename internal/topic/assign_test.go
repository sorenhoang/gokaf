package topic

import (
	"reflect"
	"testing"
)

func TestAssignLeadersRoundRobinsOverBrokerIDs(t *testing.T) {
	tests := []struct {
		name      string
		count     int32
		brokerIDs []int32
		want      []int32
	}{
		{name: "six partitions three brokers", count: 6, brokerIDs: []int32{1, 2, 3}, want: []int32{1, 2, 3, 1, 2, 3}},
		{name: "four partitions three brokers", count: 4, brokerIDs: []int32{1, 2, 3}, want: []int32{1, 2, 3, 1}},
		{name: "three partitions three brokers", count: 3, brokerIDs: []int32{1, 2, 3}, want: []int32{1, 2, 3}},
		{name: "one partition three brokers", count: 1, brokerIDs: []int32{1, 2, 3}, want: []int32{1}},
		{name: "non contiguous broker ids", count: 6, brokerIDs: []int32{2, 5, 9}, want: []int32{2, 5, 9, 2, 5, 9}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AssignLeaders(tt.count, tt.brokerIDs)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("AssignLeaders(%d, %v) = %v, want %v", tt.count, tt.brokerIDs, got, tt.want)
			}
		})
	}
}

func TestAssignLeadersHandlesEmptyInputs(t *testing.T) {
	if got := AssignLeaders(0, []int32{1, 2, 3}); len(got) != 0 {
		t.Fatalf("AssignLeaders with zero partitions = %v, want empty", got)
	}
	if got := AssignLeaders(3, nil); len(got) != 0 {
		t.Fatalf("AssignLeaders with no brokers = %v, want empty", got)
	}
}
