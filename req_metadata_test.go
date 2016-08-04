package kafka_client

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {

	c := NewClient("localhost", "9093")
	c.Connect()
	defer c.Close()

	response, err := c.SendMetaData(TopicMetadataRequest{
		RequestMessage{
			CorrelationId:99,
			ClientId:"testclient"},
		[]string{}})

	assert.NoError(t, err)

	fmt.Println(response)
}
