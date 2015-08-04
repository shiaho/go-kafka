package kafka

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZookeeperConfig struct {
	servers     []string
	recvTimeout time.Duration
}

type ZookeeperClient struct {
	config *ZookeeperConfig
	zkConn *zk.Conn
}

func NewZookeeperClient(config *ZookeeperConfig) *ZookeeperClient {
	return &ZookeeperClient{
		config: config,
	}
}

func (z *ZookeeperClient) Connect() {
	var err error
	z.zkConn, _, err = zk.Connect(z.config.servers, z.config.recvTimeout)
	if err != nil {
		fmt.Println(err)
	}
}

func (z *ZookeeperClient) getVlaues(pp string, cps []string) map[string]string {
	res := make(map[string]string)
	for _, cp := range cps {
		value, _, err := z.zkConn.Get(pp + "/" + cp)
		if err != nil {
			fmt.Println(err)
		}
		res[cp] = string(value)
	}
	return res
}



func (z *ZookeeperClient) tree(root string) {
	children, _, err := z.zkConn.Children(root)
	if err != nil {
		panic(err)
	}
	for _, cc := range children {
		cp := root + cc
		if root[len(root)-1:len(root)] != "/" {
			cp = root + "/" + cc
		}
		z.getChildren(cp)
	}
}


func (z *ZookeeperClient) getChildren(pp string) {
	children, _, err := z.zkConn.Children(pp)
	if err != nil {
		panic(err)
	}

	value, _, err := z.zkConn.Get(pp)
	if err != nil {
		panic(err)
	}
	if len(value) > 0 {
		fmt.Printf("path: %v, value: %v\n", pp, string(value))
	}

	ps := []string{}
	for _, cp := range children {
		ps = append(ps, pp+"/"+cp)
	}

	for _, p := range ps {
		z.getChildren(p)
	}
}


func (z *ZookeeperClient) treeAll() {
	z.tree("/")
}