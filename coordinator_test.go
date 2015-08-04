package kafka

import (
	"testing"
	"time"
)

func TestNewZookeeper(t *testing.T) {
	zkClient := NewZookeeperClient(&ZookeeperConfig{servers: []string{"127.0.0.1:2181"}, recvTimeout: time.Second * 60})
	zc := NewZookeeperCoordinator(zkClient)
	zkClient.Connect()
	//	zc.watchBrokers()
	zc.JoinCumsuerGroup("devel-group", "test-0", map[string]int{"test_topic": 2})
	//	zc.watchConsumerGroup("devel-group")
	zkClient.treeAll()
}
