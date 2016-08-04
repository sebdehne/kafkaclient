package kafka_client

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
)

func TestParseOffsetResponse(t *testing.T) {
	c := NewClient("localhost", "9092")
	c.Connect()
	defer c.Close()

	response, _, err := c.SendOffsetRequest(OffsetRequest{
		header:RequestMessage{CorrelationId:99,
			ClientId:"testclient"},
		ReplicaId:-1,
		TopicOffsetRequest:[]TopicOffsetRequest{
			{TopicName:"test", PartitionOffsetRequest:[]PartitionOffsetRequest{
				{Partition:0, Time:1000000, MaxNumberOfOffsets:1000}}}}})

	assert.NoError(t, err)
	fmt.Println(response)

}
