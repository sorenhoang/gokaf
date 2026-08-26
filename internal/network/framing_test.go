package network

import (
	"bytes"
	"io"
	"testing"
)

type slowWriter struct {
	buf bytes.Buffer
}

func (w *slowWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return w.buf.Write(p[:1])
}

func TestWriteFramePrefixesPayloadLength(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	want := []byte{0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef}

	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame: unexpected error: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("WriteFrame bytes: got % x, want % x", buf.Bytes(), want)
	}
}

func TestWriteFrameHandlesShortWrites(t *testing.T) {
	var writer slowWriter
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	want := []byte{0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef}

	if err := WriteFrame(&writer, payload); err != nil {
		t.Fatalf("WriteFrame: unexpected error: %v", err)
	}
	if !bytes.Equal(writer.buf.Bytes(), want) {
		t.Fatalf("WriteFrame bytes: got % x, want % x", writer.buf.Bytes(), want)
	}
}

func TestReadFrameReadsExactPayload(t *testing.T) {
	frame := []byte{0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef}

	got, err := ReadFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("ReadFrame: unexpected error: %v", err)
	}
	if !bytes.Equal(got, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("ReadFrame payload: got % x", got)
	}
}

func TestReadFrameReturnsUnexpectedEOFForShortPayload(t *testing.T) {
	frame := []byte{0x00, 0x00, 0x00, 0x04, 0xde, 0xad}

	if _, err := ReadFrame(bytes.NewReader(frame)); err != io.ErrUnexpectedEOF {
		t.Fatalf("ReadFrame short payload error: got %v, want %v", err, io.ErrUnexpectedEOF)
	}
}
