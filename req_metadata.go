package kafka_client

func ParseMetadataResponse(in []byte, header ResponseMessage) MetadataResponse {

	var nextElementPos int

	// list of brokers
	arrayLength, nextElementPos := readInt32(in, nextElementPos)
	brokers := make([]Broker, 0)
	for i := 0; i < int(arrayLength); i++ {
		var nodeId, port int32
		var host string

		nodeId, nextElementPos = readInt32(in, nextElementPos)
		host, nextElementPos = readString(in, nextElementPos)
		port, nextElementPos = readInt32(in, nextElementPos)

		brokers = append(brokers, Broker{
			NodeId:nodeId,
			Host:host,
			Port:port})
	}

	// list of TopicMetadata
	arrayLength, nextElementPos = readInt32(in, nextElementPos)
	topicMetadata := make([]TopicMetadata, 0)
	for i := 0; i < int(arrayLength); i++ {
		var topicErrorCode int16
		var topicName string
		var innerArrayLen int32
		topicErrorCode, nextElementPos = readInt16(in, nextElementPos)
		topicName, nextElementPos = readString(in, nextElementPos)

		partitionMetadata := make([]PartitionMetadata, 0)
		innerArrayLen, nextElementPos = readInt32(in, nextElementPos)
		for i := 0; i < int(innerArrayLen); i++ {
			var PartitionErrorCode int16
			var PartitionId, Leader int32
			var Replicas, Isr []int32

			PartitionErrorCode, nextElementPos = readInt16(in, nextElementPos)
			PartitionId, nextElementPos = readInt32(in, nextElementPos)
			Leader, nextElementPos = readInt32(in, nextElementPos)
			Replicas, nextElementPos = readInt32Array(in, nextElementPos)
			Isr, nextElementPos = readInt32Array(in, nextElementPos)

			partitionMetadata = append(partitionMetadata, PartitionMetadata{
				PartitionErrorCode:PartitionErrorCode,
				PartitionId:PartitionId,
				Leader:Leader,
				Replicas:Replicas,
				Isr:Isr})
		}

		topicMetadata = append(topicMetadata, TopicMetadata{
			TopicErrorCode:topicErrorCode,
			TopicName:topicName,
			meta:partitionMetadata})

	}

	return MetadataResponse{header:header, Brokers:brokers, TopicMetadata:topicMetadata}
}

type MetadataResponse struct {
	header        ResponseMessage
	Brokers       []Broker
	TopicMetadata []TopicMetadata
}

type TopicMetadataRequest struct {
	RequestMessage
	TopicNames []string
}

func (r TopicMetadataRequest) toBytes() []byte {
	b := r.RequestMessage.headerToBytes(3, 0)

	b.Write(int32ToBytes(int32(len(r.TopicNames))))
	for _, name := range r.TopicNames {
		b.Write(stringToBytes(name))
	}

	return b.Bytes()
}

type Broker struct {
	NodeId int32
	Host   string
	Port   int32
}

type TopicMetadata struct {
	TopicErrorCode int16
	TopicName      string
	meta           []PartitionMetadata
}
type PartitionMetadata struct {
	PartitionErrorCode int16
	PartitionId        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}
