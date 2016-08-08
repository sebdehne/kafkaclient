package kafka_client

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
)

func TestParseGroupCoordinatorResponse(t *testing.T) {

	c := NewClient("localhost", "9093")
	c.Connect()
	defer c.Close()

	response, err := c.SendGroupCoordinatorRequest(GroupCoordinatorRequest{
		Header:RequestMessage{
			CorrelationId:99,
			ClientId:"testclient"},
		GroupId:"testGroup"})

	assert.NoError(t, err)

	fmt.Println(response)
}

