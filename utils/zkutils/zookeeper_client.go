package zkutils

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/shiaho/go-kafka/utils/log"
	"strings"
)

type ZookeeperConfig struct {
	Servers     []string      `json:"servers"`
	RecvTimeout time.Duration `json:"recv_timeout"`
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
	z.zkConn, _, err = zk.Connect(z.config.Servers, z.config.RecvTimeout)
	if err != nil {
		fmt.Println(err)
	}
}

func (z *ZookeeperClient) GetValues(paths []string) map[string]string {
	res := make(map[string]string)
	for _, p := range paths {
		value, _, err := z.zkConn.Get(p)
		if err != nil {
			fmt.Println(err)
		}
		res[p] = string(value)
	}
	return res
}

func (z *ZookeeperClient) GetChildrenValues(pp string, cps []string) map[string]string {
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

func (z *ZookeeperClient) Tree(root string) {
	children, _, err := z.zkConn.Children(root)
	if err != nil {
		panic(err)
	}
	for _, cc := range children {
		cp := root + cc
		if root[len(root)-1:len(root)] != "/" {
			cp = root + "/" + cc
		}
		z.GetChildren(cp, true)
	}
}

func (z *ZookeeperClient) GetChildren(pp string, recursion bool) []string {
	children, _, err := z.zkConn.Children(pp)
	if err != nil {
		log.Logger.Panicf("error: %v", err)
	}

	value, _, err := z.zkConn.Get(pp)
	if err != nil {
		log.Logger.Panicf("error: %v", err)
	}
	if len(value) > 0 {
		fmt.Printf("path: %v, value: %v\n", pp, string(value))
	}

	ps := []string{}
	for _, cp := range children {
		ps = append(ps, pp+"/"+cp)
	}

	var psc = ps[:]
	if recursion {
		for _, p := range psc {
			ps = append(ps, z.GetChildren(p, true)...)
		}
	}
	//	fmt.Printf("pp: %v, psc: %v\n", pp, psc)
	return ps
}

func (z *ZookeeperClient) GetChildrenNode(pp string, recursion bool) []string {
	children, _, err := z.zkConn.Children(pp)
	if err != nil {
		log.Logger.Printf("Error: %v", err)
	}
	return children
}

func (z *ZookeeperClient) TreeAll() {
	z.Tree("/")
}

func (z *ZookeeperClient) DeleteDir(path string) (err error) {
	children := z.GetChildren(path, false)
	if len(children) > 0 {
		for _, c := range children {
			z.DeleteDir(c)
		}
	}
	err = z.DeleteNode(path)
	return err
}

func (z *ZookeeperClient) DeleteNode(path string) error {
	return z.zkConn.Delete(path, 0)
}

////////////////////////////////////////////////////////////////////////////////////////////

func (z *ZookeeperClient) CreatePersistent(path string, createParents bool) error {
	_, err := z.zkConn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		if !createParents {
			return err
		}
	} else if err == zk.ErrNoNode {
		if !createParents {
			return err
		}
		parentDir := path[0:strings.LastIndex(path, "/")]
		if err := z.CreatePersistent(parentDir, createParents); err != nil {
			return err
		}
		if err := z.CreatePersistent(path, createParents); err != nil {
			return err
		}
	}
	return nil
}

func (z *ZookeeperClient) CreatePersistentWithData(path string, data []byte) {
	z.zkConn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
}

func (z *ZookeeperClient) CreateEphemeral(path string, data []byte) error {
	_, err := z.zkConn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}

func (z *ZookeeperClient) SubscribeChildChanges(path string, listener interface{}) {
	// TODO: Implement SubscribeChildChanges
}

func (z *ZookeeperClient) SubscribeDataChanges(path string, listener interface{}) {
	// TODO: Implement SubscribeDataChanges
}
