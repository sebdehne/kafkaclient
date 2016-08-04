package kafka_client

import (
	"net"
	"log"
	"bytes"
	"fmt"
)

func NewClient(host string, port string) SimpleClient {
	return SimpleClient{host:host, port:port}
}

type SimpleClient struct {
	host string
	port string
	conn net.Conn
}

func (client *SimpleClient) Connect() {
	conn, err := net.Dial("tcp", client.host + ":" + client.port)
	if err != nil {
		log.Fatal(err)
	}
	client.conn = conn
}

func (client *SimpleClient) Close() {
	client.conn.Close()
}

func (client *SimpleClient) send(data []byte) {
	if client.conn == nil {
		panic("Client not connected")
	}

	data = append(int32ToBytes(int32(len(data))), data...)

	client.conn.Write(data)
}

func (client *SimpleClient) receive() []byte {
	readBytes := make([]byte, 1024)
	readBuf := bytes.NewBuffer([]byte{})
	for true {
		n, err := client.conn.Read(readBytes)
		if err != nil {
			log.Fatal(err)
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
	return readBuf.Bytes()[4:]
}

func (client *SimpleClient) SendMetaData(req TopicMetadataRequest) MetadataResponse {
	client.send(req.toBytes())
	fmt.Println("Sent", req)
	msg := client.receive()

	correlationId, _ := readInt32(msg, 0)

	return ParseMetadataResponse(msg[4:], ResponseMessage{CorrelationId:correlationId})
}

func (client *SimpleClient) SendProduce(req ProduceRequest) ProduceResponse {
	client.send(req.toBytes())
	fmt.Println("Sent", req)
	msg := client.receive()

	correlationId, _ := readInt32(msg, 0)

	return ParseProduceResponse(msg[4:], ResponseMessage{CorrelationId:correlationId})
}

