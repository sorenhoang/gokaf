package topic

import (
	"reflect"
	"testing"
)

func TestAssignReplicasRotatesReplicaLists(t *testing.T) {
	tests := []struct {
		name      string
		count     int32
		brokerIDs []int32
		rf        int
		want      [][]int32
	}{
		{
			name:      "replication factor three",
			count:     6,
			brokerIDs: []int32{1, 2, 3},
			rf:        3,
			want: [][]int32{
				{1, 2, 3},
				{2, 3, 1},
				{3, 1, 2},
				{1, 2, 3},
				{2, 3, 1},
				{3, 1, 2},
			},
		},
		{
			name:      "replication factor two",
			count:     4,
			brokerIDs: []int32{1, 2, 3},
			rf:        2,
			want: [][]int32{
				{1, 2},
				{2, 3},
				{3, 1},
				{1, 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AssignReplicas(tt.count, tt.brokerIDs, tt.rf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("AssignReplicas(%d, %v, %d) = %v, want %v", tt.count, tt.brokerIDs, tt.rf, got, tt.want)
			}
		})
	}
}

func TestAssignReplicasHandlesEmptyInputs(t *testing.T) {
	if got := AssignReplicas(0, []int32{1, 2, 3}, 2); len(got) != 0 {
		t.Fatalf("AssignReplicas with zero partitions = %v, want empty", got)
	}
	if got := AssignReplicas(3, nil, 2); len(got) != 0 {
		t.Fatalf("AssignReplicas with no brokers = %v, want empty", got)
	}
	if got := AssignReplicas(3, []int32{1, 2, 3}, 0); len(got) != 0 {
		t.Fatalf("AssignReplicas with zero rf = %v, want empty", got)
	}
}
