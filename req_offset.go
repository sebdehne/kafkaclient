package kafka_client

import "bytes"

func ParseOffsetResponse(in []byte) []OffsetResponseItem {

	var nextElementPos int
	var arrayLength int32

	result := make([]OffsetResponseItem, 0)
	arrayLength, nextElementPos = readInt32(in, 0)
	for i := 0; i < int(arrayLength); i++ {
		var topicName string
		var partitionLen int32

		topicName, nextElementPos = readString(in, nextElementPos)
		partitionLen, nextElementPos = readInt32(in, nextElementPos)

		partitions := make([]PartitionOffset, 0)
		for j := 0; j < int(partitionLen); j++ {
			var partition int32
			var errorCode int16
			var offsets []int64

			partition, nextElementPos = readInt32(in, nextElementPos)
			errorCode, nextElementPos = readInt16(in, nextElementPos)
			offsets, nextElementPos = readInt64Array(in, nextElementPos)

			partitions = append(partitions, PartitionOffset{Partition:partition, ErrorCode:errorCode, Offsets:offsets})
		}

		result = append(result, OffsetResponseItem{TopicName:topicName, PartitionOffset:partitions})
	}

	return result
}

type OffsetRequest struct {
	header             RequestMessage
	ReplicaId          int32
	TopicOffsetRequest []TopicOffsetRequest
}

func (r OffsetRequest) Bytes() []byte {
	buf := r.header.headerToBytes(2, 0)

	buf.Write(int32ToBytes(r.ReplicaId))
	buf.Write(int32ToBytes(int32(len(r.TopicOffsetRequest))))
	for _, t := range r.TopicOffsetRequest {
		t.write(buf)
	}

	return buf.Bytes()
}

type TopicOffsetRequest struct {
	TopicName              string
	PartitionOffsetRequest []PartitionOffsetRequest
}

func (r TopicOffsetRequest) write(buf *bytes.Buffer) {
	buf.Write(stringToBytes(r.TopicName))
	buf.Write(int32ToBytes(int32(len(r.PartitionOffsetRequest))))

	for _, t := range r.PartitionOffsetRequest {
		t.write(buf)
	}
}

type PartitionOffsetRequest struct {
	Partition          int32
	Time               int64
	MaxNumberOfOffsets int32
}

func (r PartitionOffsetRequest) write(buf *bytes.Buffer) {
	buf.Write(int32ToBytes(r.Partition))
	buf.Write(int64ToBytes(r.Time))
	buf.Write(int32ToBytes(r.MaxNumberOfOffsets))
}

type OffsetResponseItem struct {
	TopicName       string
	PartitionOffset []PartitionOffset
}

type PartitionOffset struct {
	Partition int32
	ErrorCode int16
	Offsets   []int64
}

