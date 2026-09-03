package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/sorenhoang/gokaf/internal/network"
)

func New(b *network.Broker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/broker", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, b.BrokerInfo())
	})
	mux.HandleFunc("GET /api/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, b.TopicInfos())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
