package kafkaconsumer

import "github.com/shiaho/go-kafka/utils/zkutils"

type ConsumerFetcherManager struct {
	consumerIdString string
	config           *ConsumerConfig
	zkClient         *zkutils.ZookeeperClient
}

// TODO: Implement ConsumerFetcherManager
