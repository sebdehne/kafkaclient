package kafka_client

import "bytes"

type RequestMessage struct {
	CorrelationId int32
	ClientId      string
}

func (r RequestMessage) headerToBytes(apiKey int16, apiVersion int16) *bytes.Buffer {
	b := bytes.NewBuffer([]byte{})
	b.Write(int16ToBytes(apiKey))
	b.Write(int16ToBytes(apiVersion))
	b.Write(int32ToBytes(r.CorrelationId))
	b.Write(stringToBytes(r.ClientId))
	return b;
}


type ResponseMessage struct {
	CorrelationId int32
}

