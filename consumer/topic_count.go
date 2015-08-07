package kafkaconsumer

const (
	whiteListPattern = "white_list"
	blackListPattern = "black_list"
	staticPattern    = "static"
)

type TopicCount interface {
	getConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId
	getTopicCountMap() map[string]int
	pattern() string
}

type ConsumerThreadId struct {
	consumer string
	threadId int
}

func makeConsumerThreadIdsPerTopic(consumerIdString string, topicCountMap map[string]int) map[string][]ConsumerThreadId {
	consumerThreadIdsPerTopicMap := make(map[string][]ConsumerThreadId)
	for topic, nConsumers := range topicCountMap {
		consumerSet := make([]ConsumerThreadId, 0)

		for i := 0; i < nConsumers; i++ {
			consumerSet = append(consumerSet, ConsumerThreadId{consumer: consumerIdString, threadId: i})
		}
		consumerThreadIdsPerTopicMap[topic] = consumerSet
	}
	return consumerThreadIdsPerTopicMap
}

func constructTopicCount(consumerIdString string, topicCount map[string]int) *staticTopicCount {
	return &staticTopicCount{consumerIdString: consumerIdString, topicCountMap: topicCount}
}

type staticTopicCount struct {
	consumerIdString string
	topicCountMap    map[string]int
}

func (stc *staticTopicCount) getConsumerThreadIdsPerTopic() map[string][]ConsumerThreadId {
	return makeConsumerThreadIdsPerTopic(stc.consumerIdString, stc.topicCountMap)
}

func (stc *staticTopicCount) getTopicCountMap() map[string]int {
	return stc.topicCountMap
}

func (stc *staticTopicCount) pattern() string {
	return staticPattern
}
