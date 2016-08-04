package kafka_client

import (
	"testing"
	"fmt"
)

func TestParseFetchResponse(t *testing.T) {
	c := NewClient("localhost", "9092")
	c.Connect()
	defer c.Close()

	response := c.SendFetch(FetchRequest{
		header:RequestMessage{
			CorrelationId:99,
			ClientId:"testclient"},
		ReplicaId:-1,
		MaxWaitTime:0,
		MinBytes:64000,
		TopicFetchRequest:[]TopicFetchRequest{
			{TopicName:"test", PartitionTopicFetchRequest:[]PartitionTopicFetchRequest{
				{Partition:0, FetchOffset:0, MaxBytes:1000000}}}}})

	for _, t := range response.TopicFetchResponse {
		fmt.Println("Topic: " + t.TopicName)

		for _, p := range t.PartitionFetchResponse {
			fmt.Println(" Partition: ", p.Partition)
			fmt.Println(" ErrorCode: ", p.ErrorCode)
			fmt.Println(" HighwaterMarkOffset: ", p.HighwaterMarkOffset)

			for _,mSet := range p.MessageSet {
				fmt.Println("  Offset: ", mSet.Offset)
				fmt.Println("  MagicByte: ", mSet.Message.MagicByte)
				fmt.Println("  Attributes: ",  mSet.Message.Attributes)
				fmt.Println("  Key: " + string(mSet.Message.Key))
				fmt.Println("  Value: " + string(mSet.Message.Value))
				fmt.Println("---")
			}

		}
	}

}

