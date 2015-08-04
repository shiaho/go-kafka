package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"math"
)

var (
	consumersPath    = "/consumers"
	brokerIdsPath    = "/brokers/ids"
	brokerTopicsPath = "/brokers/topics"
)

type ZookeeperCoordinator struct {
	zkClient          *ZookeeperClient
	consumerId        string
	groupId           string
	groupPath         string
	subscription      map[string]int
	handleOwnerChange func(map[string]string, map[string]string)
}

type ConsumerChangeHandler func(map[string]string, map[string]string)

type ConsumerInfo struct {
	Version      int            `json:"version"`
	Subscription map[string]int `json:"subscription"`
	Pattern      string         `json:"pattern"`
	Timestamp    int64          `json:"timestamp,string"`
}

func NewZookeeperCoordinator(zkClient *ZookeeperClient) *ZookeeperCoordinator {
	return &ZookeeperCoordinator{
		zkClient:          zkClient,
		handleOwnerChange: handleOwnerChange,
	}
}

func (z *ZookeeperCoordinator) JoinCumsuerGroup(groupId, consumerId string, subscription map[string]int) (err error) {
	z.groupId = groupId
	z.consumerId = consumerId
	z.groupPath = consumersPath + "/" + groupId
	z.subscription = subscription

	consumerPath := z.groupPath + "/ids/" + consumerId

	cInfo := &ConsumerInfo{
		Version:      1,
		Subscription: subscription,
		Pattern:      "static",
		Timestamp:    currentTimeMillis(),
	}
	consumerData, err := json.Marshal(cInfo)
	if err != nil {
		logger.Output(1, fmt.Sprintf("%v", err))
	}

	_, err = z.zkClient.zkConn.Create(consumerPath, consumerData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNoNode {
		err = z.createConsumersDir()
		if err != nil {
			logger.Output(1, fmt.Sprintf("createConsumersDir error: %v", err))
			return err
		}
		_, err = z.zkClient.zkConn.Create(consumerPath, consumerData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.Output(1, fmt.Sprintf("path:%v , %v", consumerPath, err))
			return err
		}

	} else if err == zk.ErrNodeExists {
		var stat *zk.Stat
		_, stat, err = z.zkClient.zkConn.Get(consumerPath)
		if err != nil {
			logger.Output(1, fmt.Sprintf("path:%v , %v", consumerPath, err))
			return err
		}
		_, err = z.zkClient.zkConn.Set(consumerPath, consumerData, stat.Version)
		if err != nil {
			logger.Output(1, fmt.Sprintf("path:%v , %v", consumerPath, err))
			return err
		}
	}
	return
}

func (z *ZookeeperCoordinator) createConsumersDir() (err error) {
	_, err = z.zkClient.zkConn.Create(z.groupPath, make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		logger.Output(1, fmt.Sprintf("path:%v , %v", z.groupPath, err))
		return err
	}
	_, err = z.zkClient.zkConn.Create(z.groupPath+"/ids", make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		logger.Output(1, fmt.Sprintf("path:%v , %v", z.groupPath+"/ids", err))
		return err
	}

	_, err = z.zkClient.zkConn.Create(z.groupPath+"/offsets", make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		logger.Output(1, fmt.Sprintf("path:%v , %v", z.groupPath+"/offsets", err))
		return err
	}

	_, err = z.zkClient.zkConn.Create(z.groupPath+"/owners", make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		logger.Output(1, fmt.Sprintf("path:%v , %v", z.groupPath+"/owners", err))
		return err
	}

	return
}

func (z *ZookeeperCoordinator) getConsumerNum(topic string) (num int) {
	cpaths := z.zkClient.getChildren(z.groupPath+"/ids", false)
	consumersInfo := z.zkClient.getValues(cpaths)

	for _, c := range consumersInfo {
		ci := &ConsumerInfo{}
		json.Unmarshal([]byte(c), ci)
		num += ci.Subscription[topic]
	}
	return
}

func (z *ZookeeperCoordinator) getTopicPartition(topic string) []string {
	cpaths := z.zkClient.getChildrenNode(brokerTopicsPath+"/"+topic+"/partitions", false)
	return cpaths
}

func (z *ZookeeperCoordinator) getTopicConsumingPartition(topic string) []string {
	path := z.groupPath + "/owners/" + topic
	logger.Printf("path: %v", path)
	cpaths := z.zkClient.getChildrenNode(path, false)
	return cpaths
}

func (z *ZookeeperCoordinator) assignPartition(topic string) {
	topicConsumerCount := z.getConsumerNum(topic)
	topicPartition := z.getTopicPartition(topic)
	eachThreadPartition := int(math.Floor(float64(len(topicPartition))/float64(topicConsumerCount))) * z.subscription[topic]
	fmt.Println("eachThreadPartition", eachThreadPartition)
	relsasePartitions, rnum := partitionReleaseCallback(topic, eachThreadPartition)
	fmt.Println(relsasePartitions, rnum)
	if len(relsasePartitions) > 0 {
		for _, p := range relsasePartitions {
			z.removePartitionOwner(topic, p)
		}
		return
	}

	// get consumable partition
	consumingPartition := z.getTopicConsumingPartition(topic)
	fmt.Println(consumingPartition)
	consumablePartition := stringArrayDiff(topicPartition, consumingPartition)
	fmt.Println(consumablePartition)
	// consumable partition to map for random iter
	consumablePartitionMap := make(map[string]string)
	for _, p := range consumablePartition {
		consumablePartitionMap[p] = p
	}
	fmt.Println(consumablePartitionMap)
	newOwnerPartition := make([]string, 0)
	for _, p := range consumablePartitionMap {
		err := z.getPartitionOwner(topic, p)
		if err == nil {
			rnum++
			newOwnerPartition = append(newOwnerPartition, p)
			if rnum >= 0 {
				break
			}
		}
		time.Sleep(time.Millisecond * 200)
	}
	fmt.Println(newOwnerPartition)
	newPartitionOwnerCallback(topic, newOwnerPartition)
}

func (z *ZookeeperCoordinator) getPartitionOwner(topic string, partition string) (err error) {
	_, err = z.zkClient.zkConn.Create(z.groupPath+"/owners/"+topic+"/"+partition, []byte(z.consumerId), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		logger.Output(1, fmt.Sprintf("get parition %v fail", partition))
	}
	if err != nil {
		logger.Printf("getPartitionOwner: %v", err)
	}
	return
}

func (z *ZookeeperCoordinator) removePartitionOwner(topic string, partition string) (err error) {
	err = z.zkClient.zkConn.Delete(z.groupId+"/owners/"+topic+"/"+partition, 0)
	return err
}

func (z *ZookeeperCoordinator) watchBrokers() {
	for {
		old_childrens, _, event, err := z.zkClient.zkConn.ChildrenW(brokerIdsPath)
		if err != nil {
			logger.Println(err)
		}
		old_brokers := z.zkClient.getChildrenValues(brokerIdsPath, old_childrens)
		e := <-event
		switch e.Type {
		case zk.EventNodeChildrenChanged:
			time.Sleep(time.Millisecond * 500)
			new_childrens, _, err := z.zkClient.zkConn.Children(brokerIdsPath)
			new_brokers := z.zkClient.getChildrenValues(brokerIdsPath, new_childrens)
			if err != nil {
				fmt.Println(err)
			}
			handleBrokerChange(old_brokers, new_brokers)
		}
	}
}

func (z *ZookeeperCoordinator) watchPartitionOwner(topic string) {
	ownerPath := z.groupPath + "/owners/" + topic
	logger.Println(ownerPath)
	for {
		old_childrens, _, event, err := z.zkClient.zkConn.ChildrenW(ownerPath)
		if err != nil {
			logger.Println(err)
		}
		old_owners := z.zkClient.getChildrenValues(ownerPath, old_childrens)
		e := <-event
		switch e.Type {
		case zk.EventNodeChildrenChanged:
			new_childrens, _, err := z.zkClient.zkConn.Children(ownerPath)
			new_owners := z.zkClient.getChildrenValues(ownerPath, new_childrens)
			if err != nil {
				fmt.Println(err)
			}
			z.handleOwnerChange(old_owners, new_owners)
		}
	}
}

func (z *ZookeeperCoordinator) watchConsumerGroup(groupId string) {
	groupPath := consumersPath + "/" + groupId
	consumersPath := groupPath + "/ids"
	for {
		oldChildren, _, event, err := z.zkClient.zkConn.ChildrenW(consumersPath)
		if err != nil {
			fmt.Println(err)
		}
		oldConsumers := z.zkClient.getChildrenValues(consumersPath, oldChildren)
		e := <-event
		newChildren, _, err := z.zkClient.zkConn.Children(consumersPath)
		newConsumers := z.zkClient.getChildrenValues(consumersPath, newChildren)
		if err != nil {
			fmt.Println(err)
		}
		handleConsumerChange(oldConsumers, newConsumers, e)
	}
}

func (z *ZookeeperCoordinator) listBrokers() {
	z.zkClient.tree(brokerIdsPath)
}

func (z *ZookeeperCoordinator) SetHandleOwnerChange(handleOwnerChange func(map[string]string, map[string]string)) {
	z.handleOwnerChange = handleOwnerChange
}

func handleOwnerChange(old_owners, new_owners map[string]string) {

}

func handleBrokerChange(oldBrokerIDs, newBrokerIDs map[string]string) {
	fmt.Println(time.Now(), oldBrokerIDs, newBrokerIDs)
}

func handleConsumerChange(oldConsumers, newConsumers map[string]string, event zk.Event) {
	fmt.Println(time.Now(), oldConsumers, newConsumers, event)
}

func partitionReleaseCallback(topic string, partitionNum int) (releasePartition []string, releaseNum int) {
	releasePartition = make([]string, 0)
	releaseNum = 0 - partitionNum
	return
}

func newPartitionOwnerCallback(topic string, partitions []string) (successPartition []string, err error) {
	successPartition = make([]string, 0)
	return
}
