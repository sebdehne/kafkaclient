package kafka_client

import "bytes"

func ParseFetchResponse(in []byte, header ResponseMessage) FetchResponse {

	var nextElementPos int

	// ThrottleTime
	var throttleTime int32
	throttleTime, nextElementPos = readInt32(in, nextElementPos)

	// []TopicFetchResponse
	topicsLen, nextElementPos := readInt32(in, nextElementPos)
	topics := make([]TopicFetchResponse, 0)
	for i := 0; i < int(topicsLen); i++ {
		var topicName string
		var partitionsLen int32

		topicName, nextElementPos = readString(in, nextElementPos)

		// []PartitionFetchResponse
		partitionsLen, nextElementPos = readInt32(in, nextElementPos)
		partitions := make([]PartitionFetchResponse, 0)
		for i := 0; i < int(partitionsLen); i++ {
			var partition, messageSetSize int32
			var errorCode int16
			var highwaterMarkOffset int64

			partition, nextElementPos = readInt32(in, nextElementPos)
			errorCode, nextElementPos = readInt16(in, nextElementPos)
			highwaterMarkOffset, nextElementPos = readInt64(in, nextElementPos)
			messageSetSize, nextElementPos = readInt32(in, nextElementPos)
			messageSet := in[nextElementPos: nextElementPos + int(messageSetSize)]
			nextElementPos += int(messageSetSize)

			var pos int
			messages := make([]MessageSetItem, 0)
			for pos < len(messageSet) {
				var offset int64
				var msgSize int32
				var msg Message

				offset, pos = readInt64(messageSet, pos)
				msgSize, pos = readInt32(messageSet, pos)
				msgData := messageSet[pos:pos + int(msgSize)]
				pos += int(msgSize)

				msg, _ = readMessage(msgData, 0)
				messages = append(messages, MessageSetItem{Offset:offset, Message:msg})
			}

			partitions = append(partitions, PartitionFetchResponse{Partition:partition, ErrorCode:errorCode,
				HighwaterMarkOffset:highwaterMarkOffset,
				MessageSet:messages})
		}

		topics = append(topics, TopicFetchResponse{TopicName:topicName, PartitionFetchResponse:partitions})
	}

	return FetchResponse{header:header, ThrottleTime:throttleTime, TopicFetchResponse:topics}
}

func readMessage(in []byte, nextElementPos int) (Message, int) {
	var MagicByte, Attributes int8
	var Timestamp int64
	var Key, Value        []byte

	_, nextElementPos = readInt32(in, nextElementPos) // crc32
	MagicByte, nextElementPos = readInt8(in, nextElementPos)
	Attributes, nextElementPos = readInt8(in, nextElementPos)
	Timestamp, nextElementPos = readInt64(in, nextElementPos)
	Key, nextElementPos = readBytes(in, nextElementPos)
	Value, nextElementPos = readBytes(in, nextElementPos)

	return Message{MagicByte:MagicByte, Attributes:Attributes, Timestamp:Timestamp, Key:Key, Value:Value}, nextElementPos
}

type FetchRequest struct {
	header            RequestMessage
	ReplicaId         int32
	MaxWaitTime       int32
	MinBytes          int32
	TopicFetchRequest []TopicFetchRequest
}

func (r FetchRequest) Bytes() []byte {
	buf := r.header.headerToBytes(1, 2)

	buf.Write(int32ToBytes(r.ReplicaId))
	buf.Write(int32ToBytes(r.MaxWaitTime))
	buf.Write(int32ToBytes(r.MinBytes))

	buf.Write(int32ToBytes(int32(len(r.TopicFetchRequest))))
	for _, t := range r.TopicFetchRequest {
		t.write(buf);
	}

	return buf.Bytes()
}

type TopicFetchRequest struct {
	TopicName                  string
	PartitionTopicFetchRequest []PartitionTopicFetchRequest
}

func (r TopicFetchRequest) write(buf *bytes.Buffer) {
	buf.Write(stringToBytes(r.TopicName))

	buf.Write(int32ToBytes(int32(len(r.PartitionTopicFetchRequest))))
	for _, p := range r.PartitionTopicFetchRequest {
		p.write(buf)
	}
}

type PartitionTopicFetchRequest struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (p PartitionTopicFetchRequest) write(buf *bytes.Buffer) {
	buf.Write(int32ToBytes(p.Partition))
	buf.Write(int64ToBytes(p.FetchOffset))
	buf.Write(int32ToBytes(p.MaxBytes))
}

type FetchResponse struct {
	header             ResponseMessage
	ThrottleTime       int32
	TopicFetchResponse []TopicFetchResponse
}

type TopicFetchResponse struct {
	TopicName              string
	PartitionFetchResponse []PartitionFetchResponse
}

type PartitionFetchResponse struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	MessageSet          []MessageSetItem
}