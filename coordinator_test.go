package kafka

import (
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
	zc.SetHandleOwnerChange(func(oldOwner map[string]string, newOwner map[string]string) {
		logger.Println(zc.getConsumerNum("metric_thrift_2"))
		zc.assignPartition("metric_thrift_2")
		zkClient.treeAll()
	})
	zc.JoinCumsuerGroup("test_group", "test-0", map[string]int{"metric_thrift_2": 2})
	//	go func() {
	//		time.Sleep(time.Second * 2)
	//		logger.Println("----------------------------------------")
	//		time.Sleep(time.Second * 2)
	//		zkClient.treeAll()
	//		logger.Println("----------------------------------------")
	//		time.Sleep(time.Second * 2)
	//		zkClient.treeAll()
	//		logger.Println("----------------------------------------")
	//		time.Sleep(time.Second * 2)
	//		zkClient.treeAll()
	//		logger.Println("----------------------------------------")
	//		time.Sleep(time.Second * 2)
	//		zkClient.treeAll()
	//		logger.Println("----------------------------------------")
	//	}()
	zc.assignPartition("metric_thrift_2")
	zc.watchPartitionOwner("metric_thrift_2")

}
