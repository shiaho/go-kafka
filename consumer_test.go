package kafka

import "testing"

func TestNewConsumer(t *testing.T) {
	conusmer := NewConsumer("devel-group", map[string]int{"test_topic": 3}, []string{"127.0.0.1:2181"})
	conusmer.Init()
	conusmer.JoinGroup()
	conusmer.zkClient.treeAll()
}
