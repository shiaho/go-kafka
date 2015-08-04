package kafka

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
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

func (z *ZookeeperClient) getVlaues(paths []string) map[string]string {
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

func (z *ZookeeperClient) getChildrenVlaues(pp string, cps []string) map[string]string {
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
		z.getChildren(cp, true)
	}
}

func (z *ZookeeperClient) getChildren(pp string, recursion bool) []string {
	children, _, err := z.zkConn.Children(pp)
	if err != nil {
		logger.Panicf("error: %v", err)
	}

	value, _, err := z.zkConn.Get(pp)
	if err != nil {
		logger.Panicf("error: %v", err)
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
			ps = append(ps, z.getChildren(p, true)...)
		}
	}
	//	fmt.Printf("pp: %v, psc: %v\n", pp, psc)
	return ps
}

func (z *ZookeeperClient) getChildrenNode(pp string, recursion bool) []string {
	children, _, err := z.zkConn.Children(pp)
	if err != nil {
		logger.Printf("Error: %v", err)
	}
	return children
}

func (z *ZookeeperClient) treeAll() {
	z.tree("/")
}

func (z *ZookeeperClient) deleteDir(path string) (err error) {
	children := z.getChildren(path, false)
	if len(children) > 0 {
		for _, c := range children {
			z.deleteDir(c)
		}
	}
	err = z.deleteNode(path)
	return err
}

func (z *ZookeeperClient) deleteNode(path string) error {
	return z.zkConn.Delete(path, 0)
}
