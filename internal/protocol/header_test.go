package protocol

import (
	"bytes"
	"testing"
)

func TestRequestHeaderExactBytesAndRoundTrip(t *testing.T) {
	clientID := "gokaf-client"
	header := RequestHeader{
		APIKey:        18,
		APIVersion:    1,
		CorrelationID: 42,
		ClientID:      &clientID,
	}
	want := []byte{
		0x00, 0x12,
		0x00, 0x01,
		0x00, 0x00, 0x00, 0x2a,
		0x00, 0x0c,
		'g', 'o', 'k', 'a', 'f', '-', 'c', 'l', 'i', 'e', 'n', 't',
	}

	e := NewEncoder()
	WriteRequestHeader(e, header)
	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("WriteRequestHeader: got % x, want % x", e.Bytes(), want)
	}

	got, err := ReadRequestHeader(NewDecoder(bytes.NewReader(want)))
	if err != nil {
		t.Fatalf("ReadRequestHeader: unexpected error: %v", err)
	}
	if got.APIKey != header.APIKey || got.APIVersion != header.APIVersion || got.CorrelationID != header.CorrelationID {
		t.Fatalf("ReadRequestHeader: got %+v, want %+v", got, header)
	}
	if got.ClientID == nil || *got.ClientID != clientID {
		t.Fatalf("ReadRequestHeader ClientID: got %v, want %q", got.ClientID, clientID)
	}
}

func TestRequestHeaderAllowsNilClientID(t *testing.T) {
	header := RequestHeader{APIKey: 9999, APIVersion: 0, CorrelationID: 42, ClientID: nil}
	want := []byte{0x27, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0xff, 0xff}

	e := NewEncoder()
	WriteRequestHeader(e, header)
	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("WriteRequestHeader nil ClientID: got % x, want % x", e.Bytes(), want)
	}

	got, err := ReadRequestHeader(NewDecoder(bytes.NewReader(want)))
	if err != nil {
		t.Fatalf("ReadRequestHeader nil ClientID: unexpected error: %v", err)
	}
	if got.ClientID != nil {
		t.Fatalf("ReadRequestHeader ClientID: got %q, want nil", *got.ClientID)
	}
}

func TestReadRequestHeaderReturnsFieldReadError(t *testing.T) {
	// Truncated after APIKey. A decoder that chains reads without checking each
	// error can accidentally hide where the malformed header failed.
	truncated := []byte{0x00, 0x12}

	if _, err := ReadRequestHeader(NewDecoder(bytes.NewReader(truncated))); err == nil {
		t.Fatal("ReadRequestHeader: expected error for truncated header, got nil")
	}
}

func TestResponseHeaderExactBytesAndRoundTrip(t *testing.T) {
	header := ResponseHeader{CorrelationID: 42}
	want := []byte{0x00, 0x00, 0x00, 0x2a}

	e := NewEncoder()
	WriteResponseHeader(e, header)
	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("WriteResponseHeader: got % x, want % x", e.Bytes(), want)
	}

	got, err := ReadResponseHeader(NewDecoder(bytes.NewReader(want)))
	if err != nil {
		t.Fatalf("ReadResponseHeader: unexpected error: %v", err)
	}
	if got != header {
		t.Fatalf("ReadResponseHeader: got %+v, want %+v", got, header)
	}
}
