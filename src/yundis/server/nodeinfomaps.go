package server

import (
	"sync"

	"yundis/utils"

	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
)

type NodeInfoMaps struct {
	nodeInfoMap map[string]*NodeInfo
	locker      *sync.RWMutex
}

func (self *NodeInfoMaps) Initial() {
	self.nodeInfoMap = make(map[string]*NodeInfo)
	self.locker = new(sync.RWMutex)
}

func (self *NodeInfoMaps) GetNodeInfoMap() map[string]*NodeInfo {
	self.locker.RLock()
	defer self.locker.RUnlock()
	return self.nodeInfoMap
}
func (self *NodeInfoMaps) SetNodeInfoMap(infoMap map[string]*NodeInfo) {
	self.locker.Lock()
	defer self.locker.Unlock()
	self.nodeInfoMap = infoMap
}

/**
 * load nodeinfo map from zk
 */
func (self *NodeInfoMaps) LoadNodeInfoMap(zkConn *zk.Conn) {
	//read the node list from zk
	infoMap := self.getNodeInfoMapFromZk(zkConn)
	self.SetNodeInfoMap(infoMap)
	self.WatchNodeInfoMap(zkConn)
}

/**
 * reload the nodeinfo map from zk.
 */
func (self *NodeInfoMaps) getNodeInfoMapFromZk(zkConn *zk.Conn) map[string]*NodeInfo {
	var infoMap = make(map[string]*NodeInfo)
	//read the node list from zk
	children, _, err := zkConn.Children("/yundis/nodes")
	if err != nil {
		log.Error("Can not read the subpath of /yundis/nodes")
		log.Error(err)
	}
	log.Infof("The existed node list %s:", children)
	for _, v := range children {
		nodeInfo, err := self.getNodeInfoFromZk(zkConn, "/yundis/nodes/"+v)
		if err != nil {
			continue
		}
		infoMap[v] = nodeInfo
	}
	return infoMap
}

/**
 * get nodeinfo from specify path
 */
func (self *NodeInfoMaps) getNodeInfoFromZk(zkConn *zk.Conn, path string) (*NodeInfo, error) {
	var nodeInfo NodeInfo
	bytes, _, err := zkConn.Get(path)
	if err != nil {
		log.Errorf("Can not read the subpath of /yundis/nodes, err:%s", err)
		return nil, err
	}
	err = utils.JsonParse(string(bytes), &nodeInfo)
	if err != nil {
		log.Errorf("Can not parse data from node %s, err: %s", path, err)
		return nil, err
	}
	return &nodeInfo, nil
}

/**
 * watch the node list change.
 */
func (self *NodeInfoMaps) WatchNodeInfoMap(zkConn *zk.Conn) {
	_, _, ch, err := zkConn.ChildrenW("/yundis/nodes")
	if err != nil {
		log.Errorf("Can not watch path /yundis/nodes, err:", err)
	}
	go func() {
		for {
			event := <-ch
			log.Infof("node list change, %+v", event)
			children, _, ch1, err1 := zkConn.ChildrenW("/yundis/nodes")
			if err1 == nil {
				ch = ch1
				//handle the node list change event
				log.Infof("node list changed : %s", children)
				infoMap := self.getNodeInfoMapFromZk(zkConn)
				self.SetNodeInfoMap(infoMap) //refresh nodeinfo map by new zk data.
				log.Info("Refresh nodeinfo map by new zk data.")
			} else {
				break
			}
		}
	}()
}
