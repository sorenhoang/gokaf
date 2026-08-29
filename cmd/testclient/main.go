package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"strconv"

	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	checkUnknownAPI(conn)
	checkApiVersions(conn)
	checkCreateTopics(conn)
	checkMetadataTopic(conn, "orders", true, 3)
	checkProduce(conn, "first", 48, 0)
	checkProduce(conn, "second", 49, 1)
	checkCreateTopicsDuplicate(conn)
	checkDeleteTopics(conn, "orders", 0)
	checkMetadataTopic(conn, "orders", false, 0)
	checkDeleteTopics(conn, "ghost", 3)
}

func checkUnknownAPI(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        9999,
		APIVersion:    0,
		CorrelationID: 42,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	if err := network.WriteFrame(conn, e.Bytes()); err != nil {
		log.Fatal(err)
	}

	respPayload, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		log.Fatal(err)
	}
	errCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("correlation_id=%d error_code=%d", respHeader.CorrelationID, errCode)
	if respHeader.CorrelationID != 42 || errCode != -1 {
		log.Fatal("unexpected response: correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)) + " error_code=" + strconv.Itoa(int(errCode)))
	}
}

func checkApiVersions(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        18,
		APIVersion:    0,
		CorrelationID: 43,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	if err := network.WriteFrame(conn, e.Bytes()); err != nil {
		log.Fatal(err)
	}

	respPayload, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		log.Fatal(err)
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(err)
	}
	apiCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(err)
	}

	foundApiVersions := false
	for i := 0; i < apiCount; i++ {
		apiKey, minVersion, maxVersion := readAPIVersionEntry(dec)
		log.Printf("api_versions entry: api_key=%d min_version=%d max_version=%d", apiKey, minVersion, maxVersion)
		if apiKey == 18 && minVersion == 0 && maxVersion == 0 {
			foundApiVersions = true
		}
	}

	log.Printf("api_versions response: correlation_id=%d error_code=%d api_count=%d", respHeader.CorrelationID, errorCode, apiCount)
	if respHeader.CorrelationID != 43 || errorCode != 0 || !foundApiVersions {
		log.Fatal("unexpected ApiVersions response: correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)) + " error_code=" + strconv.Itoa(int(errorCode)) + " found_api_versions=" + strconv.FormatBool(foundApiVersions))
	}
}

func readAPIVersionEntry(dec *protocol.Decoder) (int16, int16, int16) {
	apiKey, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read api_key: %w", err))
	}
	minVersion, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read min_version: %w", err))
	}
	maxVersion, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read max_version: %w", err))
	}
	return apiKey, minVersion, maxVersion
}

func checkCreateTopics(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        19,
		APIVersion:    0,
		CorrelationID: 44,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(1)
	e.WriteString("orders")
	e.WriteInt32(3)
	e.WriteInt16(1)
	e.WriteArrayLen(0)
	e.WriteArrayLen(0)
	e.WriteInt32(5000)
	writeAndAssertTopicResult(conn, e.Bytes(), 44, "create_topics", "orders", 0)
}

func checkCreateTopicsDuplicate(conn net.Conn) {
	header := protocol.RequestHeader{
		APIKey:        19,
		APIVersion:    0,
		CorrelationID: 46,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(1)
	e.WriteString("orders")
	e.WriteInt32(3)
	e.WriteInt16(1)
	e.WriteArrayLen(0)
	e.WriteArrayLen(0)
	e.WriteInt32(5000)
	writeAndAssertTopicResult(conn, e.Bytes(), 46, "create_topics duplicate", "orders", 36)
}

func checkDeleteTopics(conn net.Conn, name string, wantCode int16) {
	header := protocol.RequestHeader{
		APIKey:        20,
		APIVersion:    0,
		CorrelationID: 47,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(1)
	e.WriteString(name)
	e.WriteInt32(5000)
	writeAndAssertTopicResult(conn, e.Bytes(), 47, "delete_topics", name, wantCode)
}

func checkProduce(conn net.Conn, value string, correlationID int32, wantBaseOffset int64) {
	batch := buildRecordBatch(value)

	header := protocol.RequestHeader{
		APIKey:        0,
		APIVersion:    0,
		CorrelationID: correlationID,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteInt16(1)
	e.WriteInt32(5000)
	e.WriteArrayLen(1)
	e.WriteString("orders")
	e.WriteArrayLen(1)
	e.WriteInt32(0)
	e.WriteInt32(int32(len(batch)))
	request := append(e.Bytes(), batch...)

	if err := network.WriteFrame(conn, request); err != nil {
		log.Fatal(err)
	}
	respPayload, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		log.Fatal(err)
	}
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected Produce correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce topic count: %w", err))
	}
	if topicCount != 1 {
		log.Fatal("unexpected Produce topic count=" + strconv.Itoa(topicCount))
	}
	topicName, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce topic name: %w", err))
	}
	partitionCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce partition count: %w", err))
	}
	if topicName != "orders" || partitionCount != 1 {
		log.Fatal("unexpected Produce topic response")
	}
	partition, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce partition: %w", err))
	}
	errorCode, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce error_code: %w", err))
	}
	baseOffset, err := dec.ReadInt64()
	if err != nil {
		log.Fatal(fmt.Errorf("read produce base_offset: %w", err))
	}

	log.Printf("produce response: topic=%s partition=%d error_code=%d base_offset=%d", topicName, partition, errorCode, baseOffset)
	if partition != 0 || errorCode != 0 || baseOffset != wantBaseOffset {
		log.Fatal("unexpected Produce response")
	}
}

func writeAndAssertTopicResult(conn net.Conn, request []byte, correlationID int32, label string, wantName string, wantCode int16) {
	if err := network.WriteFrame(conn, request); err != nil {
		log.Fatal(err)
	}

	respPayload, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		log.Fatal(err)
	}
	if respHeader.CorrelationID != correlationID {
		log.Fatal("unexpected " + label + " correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}
	resultCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s result count: %w", label, err))
	}
	if resultCount != 1 {
		log.Fatal("unexpected " + label + " result count=" + strconv.Itoa(resultCount))
	}
	name, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s topic name: %w", label, err))
	}
	code, err := dec.ReadInt16()
	if err != nil {
		log.Fatal(fmt.Errorf("read %s error_code: %w", label, err))
	}
	log.Printf("%s response: name=%s error_code=%d", label, name, code)
	if name != wantName || code != wantCode {
		log.Fatal("unexpected " + label + " response")
	}
}

func buildRecordBatch(value string) []byte {
	record := buildRecord(value)
	batch := make([]byte, 61, 61+len(record))

	binary.BigEndian.PutUint64(batch[0:8], 0)
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
	crc := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[17:21], crc)
	return batch
}

func buildRecord(value string) []byte {
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

func putInt16(dst []byte, value int16) {
	binary.BigEndian.PutUint16(dst, uint16(value))
}

func putInt32(dst []byte, value int32) {
	binary.BigEndian.PutUint32(dst, uint32(value))
}

func putInt64(dst []byte, value int64) {
	binary.BigEndian.PutUint64(dst, uint64(value))
}

func checkMetadataTopic(conn net.Conn, wantName string, wantPresent bool, wantPartitions int) {
	header := protocol.RequestHeader{
		APIKey:        3,
		APIVersion:    0,
		CorrelationID: 45,
		ClientID:      nil,
	}

	e := protocol.NewEncoder()
	protocol.WriteRequestHeader(e, header)
	e.WriteArrayLen(0)
	if err := network.WriteFrame(conn, e.Bytes()); err != nil {
		log.Fatal(err)
	}

	respPayload, err := network.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(respPayload))
	respHeader, err := protocol.ReadResponseHeader(dec)
	if err != nil {
		log.Fatal(err)
	}
	if respHeader.CorrelationID != 45 {
		log.Fatal("unexpected Metadata correlation_id=" + strconv.Itoa(int(respHeader.CorrelationID)))
	}

	brokerCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read broker count: %w", err))
	}
	if brokerCount != 1 {
		log.Fatal("unexpected Metadata broker count=" + strconv.Itoa(brokerCount))
	}
	nodeID, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read broker node_id: %w", err))
	}
	host, err := dec.ReadString()
	if err != nil {
		log.Fatal(fmt.Errorf("read broker host: %w", err))
	}
	port, err := dec.ReadInt32()
	if err != nil {
		log.Fatal(fmt.Errorf("read broker port: %w", err))
	}
	log.Printf("metadata broker: node_id=%d host=%s port=%d", nodeID, host, port)
	if nodeID != 1 || host != "localhost" || port != 9092 {
		log.Fatal("unexpected Metadata broker")
	}

	topicCount, err := dec.ReadArrayLen()
	if err != nil {
		log.Fatal(fmt.Errorf("read topic count: %w", err))
	}

	foundTopic := false
	for i := 0; i < topicCount; i++ {
		errorCode, err := dec.ReadInt16()
		if err != nil {
			log.Fatal(fmt.Errorf("read topic error_code: %w", err))
		}
		name, err := dec.ReadString()
		if err != nil {
			log.Fatal(fmt.Errorf("read topic name: %w", err))
		}
		partitionCount, err := dec.ReadArrayLen()
		if err != nil {
			log.Fatal(fmt.Errorf("read partition count: %w", err))
		}
		log.Printf("metadata topic: name=%s error_code=%d partition_count=%d", name, errorCode, partitionCount)

		topicHasExpectedLeaders := partitionCount == wantPartitions
		for j := 0; j < partitionCount; j++ {
			partitionErrorCode, err := dec.ReadInt16()
			if err != nil {
				log.Fatal(fmt.Errorf("read partition error_code: %w", err))
			}
			partitionIndex, err := dec.ReadInt32()
			if err != nil {
				log.Fatal(fmt.Errorf("read partition index: %w", err))
			}
			leaderID, err := dec.ReadInt32()
			if err != nil {
				log.Fatal(fmt.Errorf("read partition leader_id: %w", err))
			}
			replicaCount, err := dec.ReadArrayLen()
			if err != nil {
				log.Fatal(fmt.Errorf("read replica count: %w", err))
			}
			for k := 0; k < replicaCount; k++ {
				if _, err := dec.ReadInt32(); err != nil {
					log.Fatal(fmt.Errorf("read replica node: %w", err))
				}
			}
			isrCount, err := dec.ReadArrayLen()
			if err != nil {
				log.Fatal(fmt.Errorf("read isr count: %w", err))
			}
			for k := 0; k < isrCount; k++ {
				if _, err := dec.ReadInt32(); err != nil {
					log.Fatal(fmt.Errorf("read isr node: %w", err))
				}
			}
			log.Printf("metadata partition: topic=%s partition=%d error_code=%d leader_id=%d replicas=%d isr=%d", name, partitionIndex, partitionErrorCode, leaderID, replicaCount, isrCount)
			if partitionErrorCode != 0 || leaderID != 1 {
				topicHasExpectedLeaders = false
			}
		}

		if errorCode == 0 && name == wantName && partitionCount == wantPartitions && topicHasExpectedLeaders {
			foundTopic = true
		}
	}

	if wantPresent && !foundTopic {
		log.Fatal("Metadata response did not include expected topic " + wantName)
	}
	if !wantPresent && foundTopic {
		log.Fatal("Metadata response still includes deleted topic " + wantName)
	}
}
