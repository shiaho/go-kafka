package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	logger           *log.Logger
	consumersPath    = "/consumers"
	brokerIdsPath    = "/brokers/ids"
	brokerTopicsPath = "/brokers/topics"
)

type ZookeeperCoordinator struct {
	zkClient     *ZookeeperClient
	consumerId   string
	groupId      string
	subscription map[string]int
}

type ConsumerChangeHandler func(map[string]string, map[string]string)

type ConsumerInfo struct {
	Version      int            `json:"version"`
	Subscription map[string]int `json:"subscription"`
	Pattern      string         `json:"pattern"`
	Timestamp    int64          `json:"timestamp,string"`
}

func init() {
	logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func NewZookeeperCoordinator(zkClient *ZookeeperClient) *ZookeeperCoordinator {
	return &ZookeeperCoordinator{
		zkClient: zkClient,
	}
}

func (z *ZookeeperCoordinator) JoinCumsuerGroup(groupId, consumerId string, subscription map[string]int) (err error) {
	groupPath := consumersPath + "/" + groupId
	consumersPath := groupPath + "/ids"
	consumerPath := consumersPath + "/" + consumerId
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
		_, err = z.zkClient.zkConn.Create(groupPath, make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			logger.Output(1, fmt.Sprintf("path:%v , %v", groupPath, err))
			return err
		}
		_, err = z.zkClient.zkConn.Create(consumersPath, make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			logger.Output(1, fmt.Sprintf("path:%v , %v", consumersPath, err))
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

func (z *ZookeeperCoordinator) createConsumersDir() {

}

func (z *ZookeeperCoordinator) watchBrokers() {
	for {
		old_childrens, _, event, err := z.zkClient.zkConn.ChildrenW(brokerIdsPath)
		if err != nil {
			fmt.Println(err)
		}
		old_brokers := z.zkClient.getVlaues(brokerIdsPath, old_childrens)
		e := <-event
		switch e.Type {
		case zk.EventNodeChildrenChanged:
			new_childrens, _, err := z.zkClient.zkConn.Children(brokerIdsPath)
			new_brokers := z.zkClient.getVlaues(brokerIdsPath, new_childrens)
			if err != nil {
				fmt.Println(err)
			}
			handleBrokerChange(old_brokers, new_brokers)
		}
	}
}

func (z *ZookeeperCoordinator) watchConsumerGroup(groupId string) {
	groupPath := consumersPath + "/" + groupId
	consumersPath := groupPath + "/ids"
	for {
		old_childrens, _, event, err := z.zkClient.zkConn.ChildrenW(consumersPath)
		if err != nil {
			fmt.Println(err)
		}
		old_consumers := z.zkClient.getVlaues(consumersPath, old_childrens)
		e := <-event
		new_childrens, _, err := z.zkClient.zkConn.Children(consumersPath)
		new_consumers := z.zkClient.getVlaues(consumersPath, new_childrens)
		if err != nil {
			fmt.Println(err)
		}
		handleConsumerChange(old_consumers, new_consumers, e)
	}
}

func (z *ZookeeperCoordinator) listBrokers() {
	z.zkClient.tree(brokerIdsPath)
}

func handleBrokerChange(oldBrokerIDs, newBrokerIDs map[string]string) {
	fmt.Println(time.Now(), oldBrokerIDs, newBrokerIDs)
}

func handleConsumerChange(old_consumers, new_consumers map[string]string, event zk.Event) {
	fmt.Println(time.Now(), old_consumers, new_consumers, event)
}
