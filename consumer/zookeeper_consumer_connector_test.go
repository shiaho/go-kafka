package kafkaconsumer_test

import (
	"fmt"
	c "github.com/shiaho/go-kafka/consumer"
	"github.com/shiaho/go-kafka/utils/zkutils"
	"testing"
)

func TestCreateZookeeperConsumerConnector(t *testing.T) {
	zcc, err := c.CreateZookeeperConsumerConnector(c.ConsumerConfig{
		GroupID: "test",
		ZKConfig: zkutils.ZKConfig{
			ZkConnect: "127.0.0.1:2181",
		},
		ConsumerTimeoutMs: -1,
		ClientId:          "test",
	})
	fmt.Println(zcc, err)

	topicCountMap := map[string]int{
		"test1": 3,
		"test2": 2,
	}

	ks, e := zcc.CreateMessageStreams(topicCountMap)

	fmt.Println(ks, e)

}
