package kafka

import (
	"testing"
	"time"
)

func TestNewZookeeperClient(t *testing.T) {
	zkClient := NewZookeeperClient(&ZookeeperConfig{Servers: []string{"192.168.81.156:2181"}, RecvTimeout: time.Second * 60})
	zkClient.Connect()
	zkClient.treeAll()
}

func TestDeleteDir(t *testing.T) {
	zkClient := NewZookeeperClient(&ZookeeperConfig{Servers: []string{"127.0.0.1:2181"}, RecvTimeout: time.Second * 60})
	zkClient.Connect()
	zkClient.deleteDir("/consumers/devel-group")
	zkClient.treeAll()
}
