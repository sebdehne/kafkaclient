package kafka_client

import (
	"testing"
	"fmt"
)

func TestClient(t *testing.T) {

	c := NewClient("localhost", "9093")
	c.Connect()
	defer c.Close()

	response := c.SendMetaData(TopicMetadataRequest{
		RequestMessage{
			CorrelationId:99,
			ClientId:"testclient"},
		[]string{}})

	fmt.Println(response)
}
