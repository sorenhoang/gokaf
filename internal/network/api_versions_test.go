package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleApiVersionsWritesV0ResponseBody(t *testing.T) {
	body, err := handleApiVersions(protocol.RequestHeader{APIKey: 18, APIVersion: 0}, nil)
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
	if apiCount != 1 {
		t.Fatalf("api key count: got %d, want 1", apiCount)
	}

	apiKey, minVersion, maxVersion := readAPIVersionEntry(t, dec)
	if apiKey != 18 || minVersion != 0 || maxVersion != 0 {
		t.Fatalf("api version entry: got {%d, %d, %d}, want {18, 0, 0}", apiKey, minVersion, maxVersion)
	}
}

func TestApiVersionsHandlerIsRegistered(t *testing.T) {
	if handlers[18] == nil {
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
