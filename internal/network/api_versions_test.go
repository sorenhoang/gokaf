package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func TestHandleApiVersionsWritesV0ResponseBody(t *testing.T) {
	broker := &Broker{Topics: topic.NewRegistry()}
	body, err := broker.handleApiVersions(protocol.RequestHeader{APIKey: 18, APIVersion: 0}, nil)
	if err != nil {
		t.Fatalf("handleApiVersions: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	if errorCode != 0 {
		t.Fatalf("error code: got %d, want 0", errorCode)
	}

	apiCount, err := dec.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen api keys: unexpected error: %v", err)
	}
	if apiCount != 15 {
		t.Fatalf("api key count: got %d, want 15", apiCount)
	}

	foundProduce := false
	foundFetch := false
	foundListOffsets := false
	foundMetadata := false
	foundOffsetCommit := false
	foundOffsetFetch := false
	foundFindCoordinator := false
	foundJoinGroup := false
	foundHeartbeat := false
	foundLeaveGroup := false
	foundSyncGroup := false
	foundApiVersions := false
	foundCreateTopics := false
	foundDeleteTopics := false
	foundInitProducerID := false
	for i := 0; i < apiCount; i++ {
		apiKey, minVersion, maxVersion := readAPIVersionEntry(t, dec)
		if apiKey == 0 && minVersion == 0 && maxVersion == 0 {
			foundProduce = true
		}
		if apiKey == 1 && minVersion == 0 && maxVersion == 0 {
			foundFetch = true
		}
		if apiKey == 2 && minVersion == 1 && maxVersion == 1 {
			foundListOffsets = true
		}
		if apiKey == 3 && minVersion == 0 && maxVersion == 0 {
			foundMetadata = true
		}
		if apiKey == 8 && minVersion == 0 && maxVersion == 0 {
			foundOffsetCommit = true
		}
		if apiKey == 9 && minVersion == 0 && maxVersion == 0 {
			foundOffsetFetch = true
		}
		if apiKey == 10 && minVersion == 0 && maxVersion == 0 {
			foundFindCoordinator = true
		}
		if apiKey == 11 && minVersion == 0 && maxVersion == 0 {
			foundJoinGroup = true
		}
		if apiKey == 12 && minVersion == 0 && maxVersion == 0 {
			foundHeartbeat = true
		}
		if apiKey == 13 && minVersion == 0 && maxVersion == 0 {
			foundLeaveGroup = true
		}
		if apiKey == 14 && minVersion == 0 && maxVersion == 0 {
			foundSyncGroup = true
		}
		if apiKey == 18 && minVersion == 0 && maxVersion == 0 {
			foundApiVersions = true
		}
		if apiKey == 19 && minVersion == 0 && maxVersion == 0 {
			foundCreateTopics = true
		}
		if apiKey == 20 && minVersion == 0 && maxVersion == 0 {
			foundDeleteTopics = true
		}
		if apiKey == 22 && minVersion == 0 && maxVersion == 0 {
			foundInitProducerID = true
		}
	}
	if !foundProduce || !foundFetch || !foundListOffsets || !foundMetadata || !foundOffsetCommit || !foundOffsetFetch || !foundFindCoordinator || !foundJoinGroup || !foundHeartbeat || !foundLeaveGroup || !foundSyncGroup || !foundApiVersions || !foundCreateTopics || !foundDeleteTopics || !foundInitProducerID {
		t.Fatalf("api versions: found_produce=%t found_fetch=%t found_list_offsets=%t found_metadata=%t found_offset_commit=%t found_offset_fetch=%t found_find_coordinator=%t found_join_group=%t found_heartbeat=%t found_leave_group=%t found_sync_group=%t found_api_versions=%t found_create_topics=%t found_delete_topics=%t found_init_producer_id=%t, want all true",
			foundProduce, foundFetch, foundListOffsets, foundMetadata, foundOffsetCommit, foundOffsetFetch, foundFindCoordinator, foundJoinGroup, foundHeartbeat, foundLeaveGroup, foundSyncGroup, foundApiVersions, foundCreateTopics, foundDeleteTopics, foundInitProducerID)
	}
}

func TestApiVersionsHandlerIsRegistered(t *testing.T) {
	if dispatchTable[18] == nil {
		t.Fatal("ApiVersions handler for api_key 18 is not registered")
	}
}

func readAPIVersionEntry(t *testing.T, dec *protocol.Decoder) (int16, int16, int16) {
	t.Helper()

	apiKey, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 api key: unexpected error: %v", err)
	}
	minVersion, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 min version: unexpected error: %v", err)
	}
	maxVersion, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 max version: unexpected error: %v", err)
	}

	return apiKey, minVersion, maxVersion
}
