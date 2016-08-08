package kafka_client

import (
	"net"
	"bytes"
	"errors"
)

func NewClient(host string, port string) SimpleClient {
	return SimpleClient{host:host, port:port}
}

type SimpleClient struct {
	host string
	port string
	conn net.Conn
}

func (client *SimpleClient) Connect() error {
	conn, err := net.Dial("tcp", client.host + ":" + client.port)
	if err != nil {
		return err
	}
	client.conn = conn

	return nil
}

func (client *SimpleClient) Close() {
	client.conn.Close()
}

func (client *SimpleClient) send(data []byte) error {
	if client.conn == nil {
		return errors.New("Client not connected")
	}

	data = append(int32ToBytes(int32(len(data))), data...)

	client.conn.Write(data)

	return nil
}

func (client *SimpleClient) receive() ([]byte, error) {
	readBytes := make([]byte, 1024)
	readBuf := bytes.NewBuffer([]byte{})
	for true {
		n, err := client.conn.Read(readBytes)
		if err != nil {
			return nil, err
		}

		readBuf.Write(readBytes[:n])

		// are we done?
		if (readBuf.Len() >= 4) {
			respLen, _ := readInt32(readBuf.Bytes(), 0)
			if (int(respLen) == readBuf.Len() - 4) {
				break
			}
		}
	}

	// TODO handle when we read beyond the response
	return readBuf.Bytes()[4:], nil
}

func (client *SimpleClient) SendMetaData(req TopicMetadataRequest) (MetadataResponse, error) {
	client.send(req.Bytes())
	msg, err := client.receive()
	if err != nil {
		return MetadataResponse{}, err
	}

	correlationId, _ := readInt32(msg, 0)

	return ParseMetadataResponse(msg[4:], ResponseMessage{CorrelationId:correlationId}), nil
}

func (client *SimpleClient) SendProduce(req ProduceRequest) (ProduceResponse, error) {
	client.send(req.Bytes())
	msg, err := client.receive()
	if err != nil {
		return ProduceResponse{}, err
	}

	correlationId, _ := readInt32(msg, 0)

	return ParseProduceResponse(msg[4:], ResponseMessage{CorrelationId:correlationId}), nil
}

func (client *SimpleClient) SendFetch(req FetchRequest) (FetchResponse, error) {
	client.send(req.Bytes())
	msg, err := client.receive()
	if err != nil {
		return FetchResponse{}, err
	}

	correlationId, _ := readInt32(msg, 0)

	return ParseFetchResponse(msg[4:], ResponseMessage{CorrelationId:correlationId}), nil
}

func (client *SimpleClient) SendOffsetRequest(req OffsetRequest) ([]OffsetResponseItem, ResponseMessage, error) {
	client.send(req.Bytes())
	msg, err := client.receive()
	if err != nil {
		return []OffsetResponseItem{}, ResponseMessage{}, err
	}

	correlationId, _ := readInt32(msg, 0)

	return ParseOffsetResponse(msg[4:]), ResponseMessage{CorrelationId:correlationId}, nil
}

func (client *SimpleClient) SendGroupCoordinatorRequest(req GroupCoordinatorRequest) (GroupCoordinatorResponse, error) {
	client.send(req.Bytes())
	msg, err := client.receive()
	if err != nil {
		return GroupCoordinatorResponse{}, err
	}

	correlationId, _ := readInt32(msg, 0)

	return ParseGroupCoordinatorResponse(msg[4:], ResponseMessage{CorrelationId:correlationId}), nil
}

