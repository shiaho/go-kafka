package kafka

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/stealthly/siesta"
	"os"
	"time"
)

type Consumer struct {
	subscription    map[string]int `json:"subscription"`
	consumerStreams []chan (*chan (*chan []siesta.Message))
	zkClient        *ZookeeperClient
	zc              *ZookeeperCoordinator
	groupId         string
	consumerId      string
}

func NewConsumer(groupId string, topics map[string]int, zookeeperUrl []string) *Consumer {
	zkClient := NewZookeeperClient(&ZookeeperConfig{servers: zookeeperUrl, recvTimeout: time.Second * 60})
	zc := NewZookeeperCoordinator(zkClient)
	return &Consumer{
		subscription:    topics,
		consumerStreams: make([]chan (*chan (*chan []siesta.Message)), len(topics)),
		zkClient:        zkClient,
		zc:              zc,
		groupId:         groupId,
	}
}

func (c *Consumer) Init() {
	c.zkClient.Connect()
}

func (c *Consumer) JoinGroup() {
	consumerId := c.getConsumerId()
	c.consumerId = consumerId
	c.zc.JoinCumsuerGroup(c.groupId, c.consumerId, c.subscription)
}

func (c *Consumer) getConsumerId() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%s-%d-%s", c.groupId, hostname, currentTimeMillis(), uuid.NewV4().String()[:8])
}
