package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/sorenhoang/gokaf/internal/cluster"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/storage"
)

func TestSplitBatchesReturnsEachRecordBatch(t *testing.T) {
	first := testRecordBatch("a", 0)
	second := testRecordBatch("b", 1)

	got := splitBatches(append(append([]byte{}, first...), second...))

	if len(got) != 2 || !bytes.Equal(got[0], first) || !bytes.Equal(got[1], second) {
		t.Fatalf("splitBatches returned %#v, want the two original batches", got)
	}
}

func TestFetcherFetchOnceAppendsLeaderBatchesByteIdentically(t *testing.T) {
	leaderBatch := testRecordBatch("hello", 0)
	leaderDir := t.TempDir()
	followerDir := t.TempDir()
	leaderLog, err := storage.Open(filepath.Join(leaderDir, "orders-0"))
	if err != nil {
		t.Fatalf("open leader log: %v", err)
	}
	defer leaderLog.Close()
	if _, err := leaderLog.Append(leaderBatch); err != nil {
		t.Fatalf("append leader batch: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()
	go serveFetchResponse(t, listener, leaderBatch)

	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := parsePort(portText)
	if err != nil {
		t.Fatalf("parsePort: %v", err)
	}
	logs := storage.NewManager(followerDir)
	defer logs.Close()
	localLog, err := logs.Log("orders", 0)
	if err != nil {
		t.Fatalf("open follower log: %v", err)
	}
	f := fetcher{
		topic:     "orders",
		partition: 0,
		leader:    cluster.Broker{ID: 1, Host: host, Port: port},
		localLog:  localLog,
		maxBytes:  1 << 20,
	}

	f.fetchOnce()

	leaderBytes, err := os.ReadFile(filepath.Join(leaderDir, "orders-0", "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("read leader segment: %v", err)
	}
	followerBytes, err := os.ReadFile(filepath.Join(followerDir, "orders-0", "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("read follower segment: %v", err)
	}
	if !bytes.Equal(followerBytes, leaderBytes) {
		t.Fatalf("follower log bytes differ from leader\nleader:   % x\nfollower: % x", leaderBytes, followerBytes)
	}
}

func serveFetchResponse(t *testing.T, listener net.Listener, batch []byte) {
	t.Helper()

	conn, err := listener.Accept()
	if err != nil {
		t.Errorf("Accept: %v", err)
		return
	}
	defer conn.Close()
	payload, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Errorf("ReadFrame: %v", err)
		return
	}
	dec := protocol.NewDecoder(bytes.NewReader(payload))
	header, err := protocol.ReadRequestHeader(dec)
	if err != nil {
		t.Errorf("ReadRequestHeader: %v", err)
		return
	}

	resp := protocol.NewEncoder()
	protocol.WriteResponseHeader(resp, protocol.ResponseHeader{CorrelationID: header.CorrelationID})
	resp.WriteArrayLen(1)
	resp.WriteString("orders")
	resp.WriteArrayLen(1)
	resp.WriteInt32(0)
	resp.WriteInt16(protocol.ErrNone)
	resp.WriteInt64(1)
	resp.WriteBytes(batch)
	if err := protocol.WriteFrame(conn, resp.Bytes()); err != nil {
		t.Errorf("WriteFrame: %v", err)
	}
}

func testRecordBatch(value string, baseOffset int64) []byte {
	record := testRecord(value)
	batch := make([]byte, 61, 61+len(record))
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	putInt32(batch[8:12], -1)
	putInt32(batch[12:16], -1)
	batch[16] = 2
	putInt16(batch[21:23], 0)
	putInt32(batch[23:27], 0)
	putInt64(batch[27:35], 1700000000000)
	putInt64(batch[35:43], 1700000000000)
	putInt64(batch[43:51], -1)
	putInt16(batch[51:53], -1)
	putInt32(batch[53:57], -1)
	putInt32(batch[57:61], 1)
	batch = append(batch, record...)
	putInt32(batch[8:12], int32(len(batch)-12))
	return batch
}

func testRecord(value string) []byte {
	body := protocol.NewEncoder()
	body.WriteInt8(0)
	body.WriteVarint(0)
	body.WriteVarint(0)
	body.WriteVarint(-1)
	body.WriteVarint(int32(len(value)))
	recordBody := append(body.Bytes(), []byte(value)...)
	trailer := protocol.NewEncoder()
	trailer.WriteVarint(0)
	recordBody = append(recordBody, trailer.Bytes()...)

	record := protocol.NewEncoder()
	record.WriteVarint(int32(len(recordBody)))
	return append(record.Bytes(), recordBody...)
}

func parsePort(s string) (int32, error) {
	var port int32
	_, err := fmt.Sscanf(s, "%d", &port)
	return port, err
}

func putInt16(dst []byte, value int16) {
	binary.BigEndian.PutUint16(dst, uint16(value))
}

func putInt32(dst []byte, value int32) {
	binary.BigEndian.PutUint32(dst, uint32(value))
}

func putInt64(dst []byte, value int64) {
	binary.BigEndian.PutUint64(dst, uint64(value))
}
