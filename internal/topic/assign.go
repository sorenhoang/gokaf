package topic

func AssignLeaders(count int32, brokerIDs []int32) []int32 {
	if count <= 0 || len(brokerIDs) == 0 {
		return []int32{}
	}
	leaders := make([]int32, count)
	for i := int32(0); i < count; i++ {
		leaders[i] = brokerIDs[int(i)%len(brokerIDs)]
	}
	return leaders
}
