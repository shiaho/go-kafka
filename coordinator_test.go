package kafka

import (
	"fmt"
	"testing"
	"time"
)

func TestNewZookeeper(t *testing.T) {
	zkClient := NewZookeeperClient(&ZookeeperConfig{Servers: []string{"127.0.0.1:2181"}, RecvTimeout: time.Second * 60})
	zc := NewZookeeperCoordinator(zkClient)
	zkClient.Connect()
	//	zc.watchBrokers()
	zc.JoinCumsuerGroup("devel-group", "test-0", map[string]int{"test_topic": 2})
	//	zc.watchConsumerGroup("devel-group")
	zkClient.treeAll()
}

func TestGetConsumerNum(t *testing.T) {
	zkClient := NewZookeeperClient(&ZookeeperConfig{Servers: []string{"192.168.81.156:2181"}, RecvTimeout: time.Second * 60})
	zc := NewZookeeperCoordinator(zkClient)
	zkClient.Connect()
	//	zc.watchBrokers()
	zc.JoinCumsuerGroup("test_group", "test-0", map[string]int{})
	//	zc.watchConsumerGroup("devel-group")
	//	zkClient.treeAll()
	fmt.Println(zc.getConsumerNum("metric_thrift_2"))
	zkClient.treeAll()
}
