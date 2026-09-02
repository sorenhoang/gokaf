package topic

// AssignReplicas returns the replica broker-id list for each partition 0..count-1.
// Partition i's replicas are rf consecutive brokers starting at index i%N,
// wrapping; replicas[0] is the leader.
func AssignReplicas(count int32, brokerIDs []int32, rf int) [][]int32 {
	if count <= 0 || len(brokerIDs) == 0 || rf <= 0 {
		return [][]int32{}
	}
	replicas := make([][]int32, count)
	for partition := int32(0); partition < count; partition++ {
		replicas[partition] = make([]int32, 0, rf)
		start := int(partition) % len(brokerIDs)
		for replica := 0; replica < rf; replica++ {
			replicas[partition] = append(replicas[partition], brokerIDs[(start+replica)%len(brokerIDs)])
		}
	}
	return replicas
}
