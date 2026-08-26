package protocol

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

type ResponseHeader struct {
	CorrelationID int32
}

func WriteRequestHeader(e *Encoder, h RequestHeader) {
	e.WriteInt16(h.APIKey)
	e.WriteInt16(h.APIVersion)
	e.WriteInt32(h.CorrelationID)
	e.WriteNullableString(h.ClientID)
}

func ReadRequestHeader(d *Decoder) (RequestHeader, error) {
	apiKey, err := d.ReadInt16()
	if err != nil {
		return RequestHeader{}, err
	}

	apiVersion, err := d.ReadInt16()
	if err != nil {
		return RequestHeader{}, err
	}

	correlationID, err := d.ReadInt32()
	if err != nil {
		return RequestHeader{}, err
	}

	clientID, err := d.ReadNullableString()
	if err != nil {
		return RequestHeader{}, err
	}

	return RequestHeader{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, nil
}

func WriteResponseHeader(e *Encoder, h ResponseHeader) {
	e.WriteInt32(h.CorrelationID)
}

func ReadResponseHeader(d *Decoder) (ResponseHeader, error) {
	correlationID, err := d.ReadInt32()
	if err != nil {
		return ResponseHeader{}, err
	}

	return ResponseHeader{CorrelationID: correlationID}, nil
}
