package kafkaconsumer

import (
	"fmt"
	"os"
	"time"

	"encoding/json"
	"errors"
	"github.com/satori/go.uuid"
	"github.com/shiaho/go-kafka/utils/zkutils"
	"strconv"
	"strings"
)

type ConsumerConnector interface {
	CreateMessageStreams(topicCountMap map[string]int) (map[string][]*KafkaStream, error)
}

type zookeeperConsumerConnector struct {
	config                       *ConsumerConfig
	enableFetcher                bool
	consumerIdString             string
	zkClient                     *zkutils.ZookeeperClient
	fetcher                      *ConsumerFetcherManager
	isShuttingDown               bool
	messageStreamCreated         bool
	loadBalancerListener         *ZKRebalancerListener
	sessionExpirationListener    *ZKSessionExpireListener
	topicPartitionChangeListener *ZKTopicPartitionChangeListener

	topicThreadIdAndQueues map[TopicThreadIdPairs]chan FetchedDataChunk
}

type QueuesAndStreamPairs struct {
	queues chan FetchedDataChunk
	stream *KafkaStream
}

type TopicThreadIdPairs struct {
	topic            string
	consumerThreadId ConsumerThreadId
}

type ConsumerRegistrationInfo struct {
	Version      int            `json:"version"`
	Subscription map[string]int `json:"subscription"`
	Pattern      string         `json:"pattern"`
	Timestamp    string         `json:"timestamp"`
}

func CreateZookeeperConsumerConnector(config ConsumerConfig) (*zookeeperConsumerConnector, error) {
	return CreateZookeeperConsumerConnectorWithEnableFetcher(config, true)
}

func CreateZookeeperConsumerConnectorWithEnableFetcher(config ConsumerConfig, enableFetcher bool) (*zookeeperConsumerConnector, error) {
	if config.GroupID == "" {
		return nil, errors.New("GroupID con't empty")
	}
	consumerConnector := &zookeeperConsumerConnector{
		config:               &config,
		enableFetcher:        enableFetcher,
		messageStreamCreated: false,
	}
	consumerConnector.createConsumerIdString()
	consumerConnector.connectZK()
	consumerConnector.createFetcher()
	//TODO: autoCommitEnable
	return consumerConnector, nil
}

func (zc *zookeeperConsumerConnector) CreateMessageStreams(topicCountMap map[string]int) (map[string][]*KafkaStream, error) {
	if zc.messageStreamCreated {
		return nil, errors.New("ZookeeperConsumerConnector can create message streams at most once")
	}
	return zc.consume(topicCountMap)
}

func (zc *zookeeperConsumerConnector) CommitOffsets(retryOnFailure bool) bool {
	//TODO: CommitOffsets
	return false
}

func (zc *zookeeperConsumerConnector) Shutdown() {
	//TODO: Shutdown
}

func (zc *zookeeperConsumerConnector) createConsumerIdString() {
	var consumerUuid string
	if zc.config.ConsumerId == "" {
		name, _ := os.Hostname()
		consumerUuid = fmt.Sprintf("%s-%d-%s", name, time.Now().Unix(), uuid.NewV4().String()[:8])
	} else {
		consumerUuid = zc.config.ConsumerId
	}
	zc.consumerIdString = zc.config.GroupID + "_" + consumerUuid
}

func (zc *zookeeperConsumerConnector) connectZK() {
	if zc.config.ZkSessionTimeoutMs == 0 {
		zc.config.ZkSessionTimeoutMs = 6000
	}
	zc.zkClient = zkutils.NewZookeeperClient(&zkutils.ZookeeperConfig{
		Servers:     strings.Split(zc.config.ZkConnect, ","),
		RecvTimeout: time.Millisecond * time.Duration(zc.config.ZkSessionTimeoutMs),
	})
	zc.zkClient.Connect()
}

func (zc *zookeeperConsumerConnector) createFetcher() {
	if zc.enableFetcher {
		zc.fetcher = &ConsumerFetcherManager{
			consumerIdString: zc.consumerIdString,
			config:           zc.config,
			zkClient:         zc.zkClient,
		}
	}
}

func (zc *zookeeperConsumerConnector) consume(topicCountMap map[string]int) (map[string][]*KafkaStream, error) {
	if topicCountMap == nil {
		return nil, errors.New("topicCountMap con't be nil")
	}
	consumerThreadSum := 0
	for _, v := range topicCountMap {
		consumerThreadSum += v
	}
	queuesAndStreams := make([]QueuesAndStreamPairs, 0)
	for i := 0; i < consumerThreadSum; i++ {
		queues := make(chan FetchedDataChunk)
		stream := &KafkaStream{queue: queues, consumerTimeoutMs: zc.config.ConsumerTimeoutMs, clientId: zc.config.ClientId}

		queuesAndStreams = append(queuesAndStreams, QueuesAndStreamPairs{queues: queues, stream: stream})
	}

	dirs := zkutils.CreateZKGroupDirs(zc.config.GroupID)
	if err := zc.registerConsumerInZK(dirs, zc.consumerIdString, topicCountMap); err != nil {
		return nil, err
	}
	if err := zc.reinitializeConsumer(constructTopicCount(zc.consumerIdString, topicCountMap), queuesAndStreams); err != nil {
		return nil, err
	}

	return zc.loadBalancerListener.kafkaMessageAndMetadataStreams, nil //loadBalancerListener.kafkaMessageAndMetadataStreams
}

func (zc *zookeeperConsumerConnector) registerConsumerInZK(dirs *zkutils.ZKGroupDirs, consumerIdString string, topicCountMap map[string]int) error {

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	data := &ConsumerRegistrationInfo{
		Version:      1,
		Subscription: topicCountMap,
		Pattern:      "static",
		Timestamp:    timestamp,
	}
	consumerRegistrationInfo, _ := json.Marshal(data)

	// TODO: createEphemeralPathExpectConflictHandleZKBug
	return zkutils.CreateEphemeralPathExpectConflict(zc.zkClient, dirs.ConsumerRegistryDir+"/"+consumerIdString, consumerRegistrationInfo)
}

func (zc *zookeeperConsumerConnector) reinitializeConsumer(topicCount TopicCount, queuesAndStreams []QueuesAndStreamPairs) error {
	dirs := zkutils.CreateZKGroupDirs(zc.config.GroupID)

	// listener to consumer and partition changes
	if zc.loadBalancerListener == nil {
		topicStreamsMap := make(map[string][]*KafkaStream)
		zc.loadBalancerListener = &ZKRebalancerListener{
			group:                          zc.config.GroupID,
			consumerIdString:               zc.consumerIdString,
			kafkaMessageAndMetadataStreams: topicStreamsMap,
		}
	}

	// create listener for session expired event if not exist yet
	if zc.sessionExpirationListener == nil {
		zc.sessionExpirationListener = &ZKSessionExpireListener{
			dirs:                 dirs,
			consumerIdString:     zc.consumerIdString,
			topicCount:           topicCount,
			loadBalancerListener: zc.loadBalancerListener,
		}
	}

	// create listener for topic partition change event if not exist yet
	if zc.topicPartitionChangeListener == nil {
		zc.topicPartitionChangeListener = &ZKTopicPartitionChangeListener{loadBalancerListener: zc.loadBalancerListener}
	}

	topicStreamsMap := zc.loadBalancerListener.kafkaMessageAndMetadataStreams

	consumerThreadIdsPerTopic := topicCount.getConsumerThreadIdsPerTopic()

	var allQueuesAndStreams []QueuesAndStreamPairs
	switch topicCount.(type) {
	case *staticTopicCount:
		allQueuesAndStreams = queuesAndStreams
	}

	topicThreadIds := make([]TopicThreadIdPairs, 0)

	for topic, cThreadIds := range consumerThreadIdsPerTopic {
		for _, cid := range cThreadIds {
			topicThreadIds = append(topicThreadIds, TopicThreadIdPairs{topic: topic, consumerThreadId: cid})
		}
	}

	if len(allQueuesAndStreams) != len(topicThreadIds) {
		return errors.New(fmt.Sprintf("Mismatch between thread ID count (%d) and queue count (%d)", len(topicThreadIds), len(allQueuesAndStreams)))
	}

	zc.topicThreadIdAndQueues = make(map[TopicThreadIdPairs]chan FetchedDataChunk)
	length := len(topicThreadIds)
	for i := 0; i < length; i++ {
		zc.topicThreadIdAndQueues[topicThreadIds[i]] = allQueuesAndStreams[i].queues
		v, ok := topicStreamsMap[topicThreadIds[i].topic]
		if !ok {
			v = make([]*KafkaStream, 0)
		}
		v = append(v, allQueuesAndStreams[i].stream)
		topicStreamsMap[topicThreadIds[i].topic] = v
	}
	// TODO: subscribeStateChanges
	//	// listener to consumer and partition changes
	//	zkClient.subscribeStateChanges(sessionExpirationListener)

	zc.zkClient.SubscribeChildChanges(dirs.ConsumerRegistryDir, zc.loadBalancerListener)

	for topic, _ := range topicStreamsMap {
		topicPath := zkutils.BrokerTopicsPath + "/" + topic
		zc.zkClient.SubscribeDataChanges(topicPath, zc.topicPartitionChangeListener)
	}

	// explicitly trigger load balancing for this consumer
	zc.loadBalancerListener.syncedRebalance()
	return nil
}
