package cluster

import (
	"bytes"
	"net"
	"testing"

	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestBrokerClientSendFramesRequestAndReturnsResponseBody(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: unexpected error: %v", err)
	}
	defer listener.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		payload, err := protocol.ReadFrame(conn)
		if err != nil {
			errCh <- err
			return
		}
		dec := protocol.NewDecoder(bytes.NewReader(payload))
		header, err := protocol.ReadRequestHeader(dec)
		if err != nil {
			errCh <- err
			return
		}
		if header.APIKey != 1000 || header.CorrelationID != 99 {
			t.Errorf("request header = %+v, want api_key=1000 correlation_id=99", header)
		}
		body, err := dec.ReadString()
		if err != nil {
			errCh <- err
			return
		}
		if body != "request-body" {
			t.Errorf("request body = %q, want request-body", body)
		}

		resp := protocol.NewEncoder()
		protocol.WriteResponseHeader(resp, protocol.ResponseHeader{CorrelationID: header.CorrelationID})
		resp.WriteString("response-body")
		errCh <- protocol.WriteFrame(conn, resp.Bytes())
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: unexpected error: %v", err)
	}
	target, err := parseBroker("2@127.0.0.1:" + port)
	if err != nil {
		t.Fatalf("parseBroker: unexpected error: %v", err)
	}
	client := NewBrokerClient(target)
	req := protocol.NewEncoder()
	req.WriteString("request-body")
	respBody, err := client.Send(protocol.RequestHeader{APIKey: 1000, APIVersion: 0, CorrelationID: 99}, req.Bytes())
	if err != nil {
		t.Fatalf("Send: unexpected error: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server: unexpected error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respBody))
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString response body: unexpected error: %v", err)
	}
	if got != "response-body" {
		t.Fatalf("response body = %q, want response-body", got)
	}
}
