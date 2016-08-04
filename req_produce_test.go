package kafka_client

import (
	"testing"
	"fmt"
	"time"
)

func TestParseProduceResponse(t *testing.T) {
	c := NewClient("localhost", "9092")
	c.Connect()
	defer c.Close()

	response := c.SendProduce(ProduceRequest{
		header:RequestMessage{
			CorrelationId:99,
			ClientId:"testclient"},
		RequiredAcks:-1,
		Timeout:5000,
		ProduceTopicRequest:[]ProduceTopicRequest{
			{TopicName:"test", PartitionMessageSet:[]PartitionMessageSet{
				{Partition:0, MessageSet:[]MessageSet{
					{Offset:0, Message:Message{
						MagicByte:1,
						Attributes:0,
						Timestamp:makeTimestamp(),
						Key:[]byte(""),
						Value:[]byte("TestMsg")}}}}}}}})

	fmt.Println(response)
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}