package replication

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPartitionStateHighWatermarkTracksISRFollowerOffsets(t *testing.T) {
	state := NewPartitionState([]int32{1, 2, 3}, 1, time.Second)

	if got := state.HighWatermark(1); got != 0 {
		t.Fatalf("initial high watermark = %d, want 0", got)
	}
	state.RecordFollowerFetch(2, 1, 1)
	if got := state.HighWatermark(1); got != 0 {
		t.Fatalf("high watermark after one follower = %d, want 0", got)
	}
	state.RecordFollowerFetch(3, 1, 1)
	if got := state.HighWatermark(1); got != 1 {
		t.Fatalf("high watermark after all followers = %d, want 1", got)
	}
}

func TestPartitionStateWaitForHighWatermark(t *testing.T) {
	state := NewPartitionState([]int32{1, 2}, 1, time.Second)
	done := make(chan error, 1)
	go func() {
		done <- state.WaitForHighWatermark(1, time.Second)
	}()

	select {
	case err := <-done:
		t.Fatalf("wait returned before follower caught up: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	state.RecordFollowerFetch(2, 1, 1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after high watermark advanced")
	}
}

func TestPartitionStateWaitForHighWatermarkTimesOut(t *testing.T) {
	state := NewPartitionState([]int32{1, 2}, 1, time.Second)

	err := state.WaitForHighWatermark(1, 20*time.Millisecond)
	if !errors.Is(err, errHighWatermarkTimedOut) {
		t.Fatalf("error = %v, want high watermark timeout", err)
	}
	if len(state.waiters) != 0 {
		t.Fatalf("waiters = %d, want 0", len(state.waiters))
	}
}

func TestPartitionStateLaggedFollowerLeavesISR(t *testing.T) {
	state := NewPartitionState([]int32{1, 2}, 1, 20*time.Millisecond)

	if got := state.ISR(); !reflect.DeepEqual(got, []int32{1, 2}) {
		t.Fatalf("initial ISR = %v, want [1 2]", got)
	}
	time.Sleep(40 * time.Millisecond)
	if got := state.HighWatermark(1); got != 1 {
		t.Fatalf("high watermark after follower lag timeout = %d, want 1", got)
	}
	if got := state.ISR(); !reflect.DeepEqual(got, []int32{1}) {
		t.Fatalf("ISR after lag timeout = %v, want [1]", got)
	}
}
