package kafka

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/stealthly/siesta"
	"os"
	"time"
)

type ConsumerConfig struct {
	GroupID          string          `json:"group_id"`
	Zookeeper_Config ZookeeperConfig `json:"zookeeper_config"`
}

type Consumer struct {
	subscription    map[string]int
	zkClient        *ZookeeperClient
	zc              *ZookeeperCoordinator
	consumerStreams []chan (*chan (*chan []siesta.Message))
	//	consumerStreams []chan string
	groupId    string
	consumerId string
}

func NewConsumer(groupId string, topics map[string]int, zookeeperUrl []string) *Consumer {
	zkClient := NewZookeeperClient(&ZookeeperConfig{Servers: zookeeperUrl, RecvTimeout: time.Second * 60})
	zc := NewZookeeperCoordinator(zkClient)
	return &Consumer{
		subscription:    topics,
		consumerStreams: make([]chan (*chan (*chan []siesta.Message)), 0),
		//		consumerStreams: make([]chan string, 0),
		zkClient: zkClient,
		zc:       zc,
		groupId:  groupId,
	}
}

func (c *Consumer) Init() {
	c.zkClient.Connect()
	for _, _ = range c.subscription {
		topicTchan := make(chan (*chan (*chan []siesta.Message)))
		//		topicTchan := make(chan string, 10)
		c.consumerStreams = append(c.consumerStreams, topicTchan)
	}
}

func (c *Consumer) JoinGroup() {
	consumerId := c.getConsumerId()
	c.consumerId = consumerId
	c.zc.JoinCumsuerGroup(c.groupId, c.consumerId, c.subscription)
}

func (c *Consumer) AssignPartition() {
	for topic, _ := range c.subscription {
		c.zc.assignPartition(topic)
	}

}

func (c *Consumer) getConsumerId() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%s-%d-%s", c.groupId, hostname, currentTimeMillis(), uuid.NewV4().String()[:8])
}
