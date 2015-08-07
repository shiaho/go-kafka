package kafkaconsumer

type KafkaStream struct {
	queue             chan FetchedDataChunk
	consumerTimeoutMs int
	clientId          string
}

// TODO: Implement KafkaStream
