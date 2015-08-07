package kafkaconsumer

import "github.com/shiaho/go-kafka/utils/zkutils"

type ZKSessionExpireListener struct {
	dirs                 *zkutils.ZKGroupDirs
	consumerIdString     string
	topicCount           TopicCount
	loadBalancerListener *ZKRebalancerListener
}

//TODO: Implement ZKSessionExpireListener
