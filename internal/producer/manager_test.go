package producer

import "testing"

func TestManagerAssignsMonotonicProducerIDs(t *testing.T) {
	m := NewManager()

	firstPID, firstEpoch := m.InitProducerID()
	secondPID, secondEpoch := m.InitProducerID()

	if firstPID != 1 || secondPID != 2 || firstEpoch != 0 || secondEpoch != 0 {
		t.Fatalf("unexpected producer ids: first=(%d,%d) second=(%d,%d)", firstPID, firstEpoch, secondPID, secondEpoch)
	}
}

func TestManagerChecksSequenceState(t *testing.T) {
	m := NewManager()
	pid := int64(7)
	topic := "orders"
	partition := int32(0)

	check := m.CheckSequence(pid, topic, partition, 0, 1)
	if check.Decision != Append {
		t.Fatalf("first sequence 0 decision=%v, want Append", check.Decision)
	}

	m.Committed(pid, topic, partition, 0, 1, 12)
	check = m.CheckSequence(pid, topic, partition, 0, 1)
	if check.Decision != Duplicate || check.CachedOffset != 12 {
		t.Fatalf("duplicate decision=%v offset=%d, want Duplicate offset 12", check.Decision, check.CachedOffset)
	}

	check = m.CheckSequence(pid, topic, partition, 1, 1)
	if check.Decision != Append {
		t.Fatalf("next sequence decision=%v, want Append", check.Decision)
	}

	m.Committed(pid, topic, partition, 1, 1, 13)
	check = m.CheckSequence(pid, topic, partition, 5, 1)
	if check.Decision != OutOfOrder {
		t.Fatalf("gap sequence decision=%v, want OutOfOrder", check.Decision)
	}
}

func TestManagerRequiresFreshKeyToStartAtSequenceZero(t *testing.T) {
	m := NewManager()

	check := m.CheckSequence(7, "orders", 1, 3, 1)
	if check.Decision != OutOfOrder {
		t.Fatalf("fresh key sequence 3 decision=%v, want OutOfOrder", check.Decision)
	}

	check = m.CheckSequence(7, "orders", 1, 0, 2)
	if check.Decision != Append {
		t.Fatalf("fresh key sequence 0 decision=%v, want Append", check.Decision)
	}
	m.Committed(7, "orders", 1, 0, 2, 20)

	check = m.CheckSequence(7, "orders", 1, 0, 2)
	if check.Decision != Duplicate || check.CachedOffset != 20 {
		t.Fatalf("multi-record duplicate decision=%v offset=%d, want Duplicate offset 20", check.Decision, check.CachedOffset)
	}
	check = m.CheckSequence(7, "orders", 1, 2, 1)
	if check.Decision != Append {
		t.Fatalf("after multi-record batch decision=%v, want Append", check.Decision)
	}
}
