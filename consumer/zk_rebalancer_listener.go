package kafkaconsumer

type ZKRebalancerListener struct {
	group                          string
	consumerIdString               string
	kafkaMessageAndMetadataStreams map[string][]*KafkaStream
}

//TODO: Implement ZKRebalancerListener
func (zkRebalancer *ZKRebalancerListener) syncedRebalance() {

}
