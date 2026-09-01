package protocol

const (
	ErrUnknown                  int16 = -1
	ErrNone                     int16 = 0
	ErrOffsetOutOfRange         int16 = 1
	ErrCorruptMessage           int16 = 2
	ErrUnknownTopicOrPartition  int16 = 3
	ErrTopicAlreadyExists       int16 = 36
	ErrInvalidPartitions        int16 = 37
	ErrInvalidReplicationFactor int16 = 38
	ErrIllegalGeneration        int16 = 22
	ErrUnknownMemberID          int16 = 25
	ErrRebalanceInProgress      int16 = 27
)
