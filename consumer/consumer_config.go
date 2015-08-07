package kafkaconsumer

import (
	"github.com/shiaho/go-kafka/utils/zkutils"
)

type ConsumerConfig struct {
	zkutils.ZKConfig

	GroupID           string `json:"group_id"`
	ConsumerId        string `json:"consumer_id"`
	ConsumerTimeoutMs int    `json:"consumer.timeout.ms"`
	ClientId          string `json:"client.id"`
}
