package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/sorenhoang/gokaf/internal/network"
	"github.com/sorenhoang/gokaf/internal/protocol"
	"github.com/sorenhoang/gokaf/internal/topic"
)

func New(b *network.Broker) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/broker", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.BrokerInfo())
	})

	mux.HandleFunc("GET /api/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.TopicInfos())
	})

	mux.HandleFunc("POST /api/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name              string `json:"name"`
			Partitions        int32  `json:"partitions"`
			ReplicationFactor int16  `json:"replication_factor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if req.ReplicationFactor == 0 {
			req.ReplicationFactor = 1
		}
		if err := b.CreateTopic(req.Name, req.Partitions, req.ReplicationFactor); err != nil {
			writeError(w, statusFor(err), err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"name": req.Name})
	})

	mux.HandleFunc("DELETE /api/v1/topics/{name}", func(w http.ResponseWriter, r *http.Request) {
		if err := b.DeleteTopic(r.PathValue("name")); err != nil {
			writeError(w, statusFor(err), err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /api/v1/produce", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Key       string `json:"key"`
			Value     string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		var key []byte
		if req.Key != "" {
			key = []byte(req.Key)
		}
		batch := protocol.BuildRecordBatch([]protocol.BatchRecord{{Key: key, Value: []byte(req.Value)}})
		offset, err := b.Produce(req.Topic, req.Partition, batch)
		if err != nil {
			writeError(w, statusFor(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"base_offset": offset})
	})

	mux.HandleFunc("POST /api/v1/fetch", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
			MaxBytes  int32  `json:"max_bytes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if req.MaxBytes <= 0 {
			req.MaxBytes = 1 << 20
		}
		records, highWatermark, err := b.Fetch(req.Topic, req.Partition, req.Offset, req.MaxBytes)
		if err != nil {
			writeError(w, statusFor(err), err)
			return
		}
		var raw []byte
		for _, record := range records {
			raw = append(raw, record.Payload...)
		}
		decoded, err := protocol.DecodeRecordBatches(raw)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		out := make([]map[string]any, 0, len(decoded))
		for _, d := range decoded {
			entry := map[string]any{"offset": d.Offset, "value": string(d.Value), "timestamp": d.Timestamp}
			if d.Key != nil {
				entry["key"] = string(d.Key)
			}
			out = append(out, entry)
		}
		writeJSON(w, http.StatusOK, map[string]any{"high_watermark": highWatermark, "records": out})
	})

	mux.HandleFunc("GET /api/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.GroupInfos())
	})

	mux.HandleFunc("GET /api/v1/producers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.ProducerInfos())
	})

	mux.HandleFunc("POST /api/v1/groups/{id}/reset-offset", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := b.ResetGroupOffset(r.PathValue("id"), req.Topic, req.Partition, req.Offset); err != nil {
			writeError(w, statusFor(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"group": r.PathValue("id"), "topic": req.Topic, "partition": req.Partition, "offset": req.Offset,
		})
	})

	mux.Handle("/", staticHandler())

	return mux
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, network.ErrNotController), errors.Is(err, network.ErrNotLeader):
		return http.StatusMisdirectedRequest // 421 — retry against a different broker
	case errors.Is(err, topic.ErrTopicExists):
		return http.StatusConflict
	case errors.Is(err, topic.ErrTopicNotFound), errors.Is(err, network.ErrUnknownTopicOrPartition):
		return http.StatusNotFound
	case errors.Is(err, network.ErrCorruptBatch), errors.Is(err, network.ErrOffsetOutOfRange),
		errors.Is(err, network.ErrInvalidPartitions), errors.Is(err, network.ErrInvalidReplicationFactor):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
