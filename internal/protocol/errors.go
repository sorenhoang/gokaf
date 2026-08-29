package protocol

const (
	ErrUnknown                  int16 = -1
	ErrNone                     int16 = 0
	ErrUnknownTopicOrPartition  int16 = 3
	ErrTopicAlreadyExists       int16 = 36
	ErrInvalidPartitions        int16 = 37
	ErrInvalidReplicationFactor int16 = 38
)
