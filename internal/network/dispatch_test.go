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
