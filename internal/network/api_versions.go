package network

import "github.com/sorenhoang/gokaf/internal/protocol"

type supportedAPI struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

var supportedAPIs = []supportedAPI{
	{APIKey: 18, MinVersion: 0, MaxVersion: 0},
}

func handleApiVersions(header protocol.RequestHeader, body []byte) ([]byte, error) {
	e := protocol.NewEncoder()
	e.WriteInt16(0)
	e.WriteArrayLen(len(supportedAPIs))
	for _, api := range supportedAPIs {
		e.WriteInt16(api.APIKey)
		e.WriteInt16(api.MinVersion)
		e.WriteInt16(api.MaxVersion)
	}
	return e.Bytes(), nil
}
