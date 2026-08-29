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
	if apiCount != 2 {
		t.Fatalf("api key count: got %d, want 2", apiCount)
	}

	foundMetadata := false
	foundApiVersions := false
	for i := 0; i < apiCount; i++ {
		apiKey, minVersion, maxVersion := readAPIVersionEntry(t, dec)
		if apiKey == 3 && minVersion == 0 && maxVersion == 0 {
			foundMetadata = true
		}
		if apiKey == 18 && minVersion == 0 && maxVersion == 0 {
			foundApiVersions = true
		}
	}
	if !foundMetadata || !foundApiVersions {
		t.Fatalf("api versions: found_metadata=%t found_api_versions=%t, want both true", foundMetadata, foundApiVersions)
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
