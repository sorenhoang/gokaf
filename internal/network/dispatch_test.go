package network

import (
	"bytes"
	"net"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestDispatchUnknownAPIWritesCorrelationIDAndErrorCode(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	req := protocol.NewEncoder()
	protocol.WriteRequestHeader(req, protocol.RequestHeader{
		APIKey:        9999,
		APIVersion:    0,
		CorrelationID: 42,
		ClientID:      nil,
	})

	errCh := make(chan error, 1)
	go func() {
		defer server.Close()
		errCh <- Dispatch(server, req.Bytes())
	}()

	respPayload, err := ReadFrame(client)
	if err != nil {
		t.Fatalf("ReadFrame response: unexpected error: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Dispatch: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		t.Fatalf("ReadResponseHeader: unexpected error: %v", err)
	}
	if respHeader.CorrelationID != 42 {
		t.Fatalf("response correlation id: got %d, want 42", respHeader.CorrelationID)
	}

	errCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16 error code: unexpected error: %v", err)
	}
	if errCode != -1 {
		t.Fatalf("error code: got %d, want -1", errCode)
	}
}

func TestDispatchApiVersionsWritesCorrelationIDAndSupportedAPIs(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	req := protocol.NewEncoder()
	protocol.WriteRequestHeader(req, protocol.RequestHeader{
		APIKey:        18,
		APIVersion:    0,
		CorrelationID: 43,
		ClientID:      nil,
	})

	errCh := make(chan error, 1)
	go func() {
		defer server.Close()
		errCh <- Dispatch(server, req.Bytes())
	}()

	respPayload, err := ReadFrame(client)
	if err != nil {
		t.Fatalf("ReadFrame response: unexpected error: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Dispatch: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		t.Fatalf("ReadResponseHeader: unexpected error: %v", err)
	}
	if respHeader.CorrelationID != 43 {
		t.Fatalf("response correlation id: got %d, want 43", respHeader.CorrelationID)
	}

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
