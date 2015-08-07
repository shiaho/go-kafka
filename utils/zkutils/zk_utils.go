package zkutils

import (
	"github.com/samuel/go-zookeeper/zk"
	"strings"
)

const (
	ConsumersPath                      = "/consumers"
	BrokerIdsPath                      = "/brokers/ids"
	BrokerTopicsPath                   = "/brokers/topics"
	TopicConfigPath                    = "/config/topics"
	TopicConfigChangesPath             = "/config/changes"
	ControllerPath                     = "/controller"
	ControllerEpochPath                = "/controller_epoch"
	ReassignPartitionsPath             = "/admin/reassign_partitions"
	DeleteTopicsPath                   = "/admin/delete_topics"
	PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election"
)

type ZKConfig struct {
	ZkConnect             string
	ZkSessionTimeoutMs    int64
	ZkConnectionTimeoutMs int64
	ZkSyncTimeMs          int64
}

type ZKGroupDirs struct {
	ConsumerDir, ConsumerGroupDir, ConsumerRegistryDir string
}

func CreateZKGroupDirs(group string) *ZKGroupDirs {
	return &ZKGroupDirs{
		ConsumerDir:         ConsumersPath,
		ConsumerGroupDir:    ConsumersPath + "/" + group,
		ConsumerRegistryDir: ConsumersPath + "/" + group + "/ids",
	}
}

func createParentPath(client *ZookeeperClient, path string) error {
	parentDir := path[0:strings.LastIndex(path, "/")]

	if len(parentDir) != 0 {
		return client.CreatePersistent(parentDir, true)
	}
	return nil
}

func CreateEphemeralPath(client *ZookeeperClient, path string, data []byte) error {
	err := client.CreateEphemeral(path, data)
	if err != nil && err == zk.ErrNoNode {
		if err := createParentPath(client, path); err != nil {
			return err
		}
		err := client.CreateEphemeral(path, data)
		return err
	}
	return err
}

func CreateEphemeralPathExpectConflict(client *ZookeeperClient, path string, data []byte) error {
	err := CreateEphemeralPath(client, path, data)
	if err != nil && err == zk.ErrNodeExists {

	}
	return err
}
