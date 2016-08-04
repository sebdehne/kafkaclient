package kafka_client

import (
	"bytes"
	"hash/crc32"
)

func ParseProduceResponse(in []byte, header ResponseMessage) ProduceResponse {
	var nextElementPos int

	// []ProduceTopicResponse
	arrayLength, nextElementPos := readInt32(in, nextElementPos)
	topics := make([]ProduceTopicResponse, 0)
	for i := 0; i < int(arrayLength); i++ {
		var topicName string
		var innerArrayLen int32

		topicName, nextElementPos = readString(in, nextElementPos)

		// []PartitionTopicResponse
		innerArrayLen, nextElementPos = readInt32(in, nextElementPos)
		partitions := make([]PartitionTopicResponse, 0)
		for i := 0; i < int(innerArrayLen); i++ {
			var partitionId int32
			var errorCode int16
			var offset, timestamp int64

			partitionId, nextElementPos = readInt32(in, nextElementPos)
			errorCode, nextElementPos = readInt16(in, nextElementPos)
			offset, nextElementPos = readInt64(in, nextElementPos)
			timestamp, nextElementPos = readInt64(in, nextElementPos)

			partitions = append(partitions, PartitionTopicResponse{
				Partition:partitionId,
				ErrorCode:errorCode,
				Offset:offset,
				Timestamp:timestamp})
		}

		topics = append(topics, ProduceTopicResponse{
			TopicName:topicName,
			PartitionTopicResponse:partitions})
	}

	// ThrottleTime
	var throttleTime int32
	throttleTime, nextElementPos = readInt32(in, nextElementPos)

	return ProduceResponse{header:header, ProduceTopicResponse:topics, ThrottleTime:throttleTime}
}

type ProduceRequest struct {
	header              RequestMessage
	RequiredAcks        int16
	Timeout             int32
	ProduceTopicRequest []ProduceTopicRequest
}

func (r ProduceRequest) Bytes() []byte {
	buf := r.header.headerToBytes(0, 2)

	buf.Write(int16ToBytes(r.RequiredAcks))
	buf.Write(int32ToBytes(r.Timeout))

	buf.Write(int32ToBytes(int32(len(r.ProduceTopicRequest))))
	for _, p := range r.ProduceTopicRequest {
		p.write(buf)
	}

	return buf.Bytes()
}

type ProduceTopicRequest struct {
	TopicName           string
	PartitionMessageSet []PartitionMessageSet
}

func (p ProduceTopicRequest) write(buf *bytes.Buffer) {
	buf.Write(stringToBytes(p.TopicName))

	buf.Write(int32ToBytes(int32(len(p.PartitionMessageSet))))
	for _, s := range p.PartitionMessageSet {
		s.write(buf)
	}
}

type PartitionMessageSet struct {
	Partition  int32
	MessageSet []MessageSetItem
}

func (s PartitionMessageSet) write(buf *bytes.Buffer) {
	buf.Write(int32ToBytes(s.Partition))

	msgSetBuf := bytes.NewBuffer([]byte{})
	for _, msg := range s.MessageSet {
		msgSetBuf.Write(msg.Bytes())
	}

	// messageSetSize
	buf.Write(int32ToBytes(int32(msgSetBuf.Len())))

	// messageSet
	buf.Write(msgSetBuf.Bytes())
}

type MessageSetItem struct {
	Offset  int64
	Message Message
}

func (s MessageSetItem) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	buf.Write(int64ToBytes(s.Offset))
	buf.Write(bytesToBytes(s.Message.Bytes()))
	return buf.Bytes()
}

type Message struct {
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

func (m Message) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})

	buf.Write(int8ToBytes(m.MagicByte))
	buf.Write(int8ToBytes(m.Attributes))
	buf.Write(int64ToBytes(m.Timestamp))
	buf.Write(bytesToBytes(m.Key))
	buf.Write(bytesToBytes(m.Value))

	msgWithoutCrc := buf.Bytes()
	crc32 := crc32.ChecksumIEEE(msgWithoutCrc)
	return append(int32ToBytes(int32(crc32)), msgWithoutCrc...)
}

type ProduceResponse struct {
	header               ResponseMessage
	ProduceTopicResponse []ProduceTopicResponse
	ThrottleTime         int32
}

type ProduceTopicResponse struct {
	TopicName              string
	PartitionTopicResponse []PartitionTopicResponse
}

type PartitionTopicResponse struct {
	Partition int32
	ErrorCode int16
	Offset    int64
	Timestamp int64
}

