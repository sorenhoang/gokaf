package main

import (
	"bytes"
	"fmt"
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
