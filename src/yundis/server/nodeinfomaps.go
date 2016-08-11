package server

import (
	"strconv"
	"sync"
	"time"
	"yundis/utils"

	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
)

type NodeInfoMaps struct {
	nodeInfoMap  map[string]*NodeInfo
	locker       *sync.RWMutex
	zk           *utils.ZkHelper
	slotInfoMaps *SlotInfoMaps
	allocations  *SlotAllocation
}

func (self *NodeInfoMaps) Initial(zkHelper *utils.ZkHelper, slotInfos *SlotInfoMaps, alloc *SlotAllocation) {
	self.nodeInfoMap = make(map[string]*NodeInfo)
	self.locker = new(sync.RWMutex)
	self.zk = zkHelper
	self.slotInfoMaps = slotInfos
	self.allocations = alloc
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
func (self *NodeInfoMaps) LoadNodeInfoMap() {
	//read the node list from zk
	infoMap := self.getNodeInfoMapFromZk()
	self.SetNodeInfoMap(infoMap)
	self.WatchNodeInfoMap()
}

/**
 * reload the nodeinfo map from zk.
 */
func (self *NodeInfoMaps) getNodeInfoMapFromZk() map[string]*NodeInfo {
	var infoMap = make(map[string]*NodeInfo)
	//read the node list from zk
	children, _, err := self.zk.GetZkConn().Children("/yundis/nodes")
	if err != nil {
		log.Errorf("Can not read the subpath of /yundis/nodes, err:%s", err)
	}
	log.Infof("The existed node list %s:", children)
	for _, v := range children {
		nodeInfo, err := self.getNodeInfoFromZk("/yundis/nodes/" + v)
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
func (self *NodeInfoMaps) getNodeInfoFromZk(path string) (*NodeInfo, error) {
	var nodeInfo NodeInfo
	bytes, _, err := self.zk.GetZkConn().Get(path)
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
 * 1. find the increased node id
 */
func (self *NodeInfoMaps) ModifySlotState(newInfoMap map[string]*NodeInfo) {
	//nodeSlotsMap := self.allocations.getNodeMap()
	var disappearNodes []int
	for nodeId, _ := range self.nodeInfoMap {
		log.Debugf("nodeId : %s", nodeId)
		if _, ok := newInfoMap[nodeId]; !ok { // this node was gone
			log.Debugf("gone nodeId:%s", nodeId)
			intNodeId, err := strconv.Atoi(nodeId)
			if err != nil {
				log.Errorf("Conver nodeid to int from string fail. err:%s", err)
			} else {
				disappearNodes = append(disappearNodes, intNodeId)
			}
		}
	}
	if len(disappearNodes) > 0 { //need to change the slot's state
		nodeSlotsMap := self.allocations.getNodeMap()
		log.Debugf("nodeSlotsMap: %+v", nodeSlotsMap)
		//lock the zk node
		lock := self.zk.GetLocker("/yundis/idslocker")
		err := lock.Lock()
		if err != nil {
			log.Errorf("Fail to lock the path /yundis/idslocker. err:%s", err)
		} else {
			var affectedSlots []int
			for _, nodeId := range disappearNodes {
				log.Debugf("list disapper nodes:%d", nodeId)
				slots := nodeSlotsMap[nodeId]
				log.Debugf("The slots list of node:%d", slots)
				for _, slotId := range slots {
					//update the slot info to zk
					affectedSlots = append(affectedSlots, slotId)
					slotInfo := self.slotInfoMaps.GetSlotInfoMap()[strconv.Itoa(slotId)]
					slotInfo.State = SlotStateDead
					jsonStr, err := utils.ToJson(slotInfo)
					if err == nil {
						self.zk.CoverCreate("/yundis/ids/"+strconv.Itoa(slotId), []byte(jsonStr), 0, zk.WorldACL(zk.PermAll))
					} else {
						log.Errorf("Error when conver %s to jsonstr.", slotInfo)
					}
				}
			}
			if len(affectedSlots) > 0 {
				//update /yundis/ids
				json, err := utils.ToJson(affectedSlots)
				if err == nil {
					_, err = self.zk.Set("/yundis/ids", []byte(json), 1)
					if err != nil {
						log.Errorf("Can not set the value of /yundis/ids. err:%s", err)
					}
				} else {
					log.Errorf("Error when conver %s to jsonstr.", err)
				}
			}
			//Unlock
			lock.Unlock()
		}
	}
}

/**
 * watch the node list change.
 */
func (self *NodeInfoMaps) WatchNodeInfoMap() {
	_, _, ch, err := self.zk.GetZkConn().ChildrenW("/yundis/nodes")
	if err != nil {
		log.Errorf("Can not watch path /yundis/nodes, err:%s", err)
	}
	go func() {
		for {
			event := <-ch
			log.Infof("node list change, %+v", event)
			children, _, ch1, err1 := self.zk.GetZkConn().ChildrenW("/yundis/nodes")
			if err1 == nil {
				ch = ch1
				//handle the node list change event
				log.Infof("node list changed : %s", children)
				infoMap := self.getNodeInfoMapFromZk()
				//change the slotinfo state.
				log.Info("The node list changed, begin to change the affected slot's info.")
				self.ModifySlotState(infoMap)
				self.SetNodeInfoMap(infoMap) //refresh nodeinfo map by new zk data.
				log.Info("Refresh nodeinfo map by new zk data.")
			} else {
				log.Errorf("Can not watching the children of /yundis/nodes, err:%s", err1)
				break
			}
			time.Sleep(time.Second)
		}
	}()
}
