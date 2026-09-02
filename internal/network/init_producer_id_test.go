package network

import (
	"bytes"
	"testing"

	"github.com/sorenhoang/gokaf/internal/producer"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func TestHandleInitProducerIDReturnsPIDAndEpoch(t *testing.T) {
	broker := &Broker{Producers: producer.NewManager()}
	req := protocol.NewEncoder()
	req.WriteNullableString(nil)
	req.WriteInt32(-1)

	resp, err := broker.handleInitProducerID(protocol.RequestHeader{APIKey: 22}, req.Bytes())
	if err != nil {
		t.Fatalf("handleInitProducerID returned error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(resp))
	throttleTimeMS, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("read throttle_time_ms: %v", err)
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("read error_code: %v", err)
	}
	pid, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("read producer_id: %v", err)
	}
	epoch, err := dec.ReadInt16()
	if err != nil {
		t.Fatalf("read producer_epoch: %v", err)
	}

	if throttleTimeMS != 0 || errorCode != protocol.ErrNone || pid != 1 || epoch != 0 {
		t.Fatalf("unexpected InitProducerId response: throttle=%d error=%d pid=%d epoch=%d", throttleTimeMS, errorCode, pid, epoch)
	}
}

func TestHandleInitProducerIDIncrementsPID(t *testing.T) {
	broker := &Broker{Producers: producer.NewManager()}
	req := protocol.NewEncoder()
	req.WriteNullableString(nil)
	req.WriteInt32(-1)

	if _, err := broker.handleInitProducerID(protocol.RequestHeader{APIKey: 22}, req.Bytes()); err != nil {
		t.Fatalf("first handleInitProducerID returned error: %v", err)
	}
	resp, err := broker.handleInitProducerID(protocol.RequestHeader{APIKey: 22}, req.Bytes())
	if err != nil {
		t.Fatalf("second handleInitProducerID returned error: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(resp))
	if _, err := dec.ReadInt32(); err != nil {
		t.Fatalf("read throttle_time_ms: %v", err)
	}
	if _, err := dec.ReadInt16(); err != nil {
		t.Fatalf("read error_code: %v", err)
	}
	pid, err := dec.ReadInt64()
	if err != nil {
		t.Fatalf("read producer_id: %v", err)
	}
	if pid != 2 {
		t.Fatalf("second producer_id=%d, want 2", pid)
	}
}

func TestInitProducerIDHandlerIsRegistered(t *testing.T) {
	if dispatchTable[22] == nil {
		t.Fatal("InitProducerId handler is not registered for API key 22")
	}
}
