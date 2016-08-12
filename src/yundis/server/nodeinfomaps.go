package server

import (
	"sort"
	"strconv"
	"sync"
	"time"
	"yundis/utils"

	log "github.com/cihub/seelog"
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
 * 1. for each the slotinfo list
 */
func (self *NodeInfoMaps) ModifySlotState(newInfoMap map[string]*NodeInfo) {
	log.Info("Call modify slotinfo state.")
	// get the slotinfo map
	slotInfoMap := self.slotInfoMaps.CloneSlotInfoMap()
	var changedSlots []*SlotInfo
	for slotId, slotInfo := range slotInfoMap {
		log.Debugf("Orginal SlotInfo [SlotId:%s, State:%s, NodeId:%s, SrcNodeId:%s, TargetNodeId:%s]", slotInfo.SlotId, slotInfo.State, slotInfo.NodeId, slotInfo.SrcNodeId, slotInfo.TargetNodeId)
		isChanged := false
		if _, ok := newInfoMap[slotInfo.NodeId]; ok { //the node alive
			if slotInfo.State == SlotStateDead { //active this slot
				slotInfo.State = SlotStateNormal
				isChanged = true
				log.Infof("Active slot:%s", slotId)
			}
		} else { // the node dead
			if slotInfo.State == SlotStateNormal { //inactive this slot
				slotInfo.State = SlotStateDead
				isChanged = true
				log.Infof("Inactive slot:%s", slotId)
			}
		}
		if isChanged {
			changedSlots = append(changedSlots, slotInfo)
			log.Debugf("SlotInfo after changed [SlotId:%s, State:%s, NodeId:%s, SrcNodeId:%s, TargetNodeId:%s]", slotInfo.SlotId, slotInfo.State, slotInfo.NodeId, slotInfo.SrcNodeId, slotInfo.TargetNodeId)
		}
	}
	//change current slotInfoMap
	self.slotInfoMaps.SetSlotInfoMap(slotInfoMap)

	if len(changedSlots) > 0 { //wether need to sync the value to zk.
		log.Info("Update the zk by new slotinfo map.")
		//get the zk locker
		zkLocker := self.zk.GetLocker("/yundis/idslocker")
		err := zkLocker.Lock()
		if err != nil {
			log.Warnf("Can not lock the idslocker, err:%s", err)
		} else {
			log.Info("Get the latest slotinfo map from zk before update them.")
			latestSlotMap := self.slotInfoMaps.GetSlotInfoMapFromZk()
			//update the slot to zk
			var changedSlotIds []int
			for _, slotInfo := range changedSlots {
				if slotInfo.State == latestSlotMap[slotInfo.SlotId].State {
					continue
				}
				log.Infof("Begin to update the slotinfo[%s] to zk.", slotInfo.SlotId)
				//record the id
				slotId, err := strconv.Atoi(slotInfo.SlotId)
				if err == nil {
					changedSlotIds = append(changedSlotIds, slotId)
				} else {
					log.Errorf("Convert slotId to int from string. err:%s", err)
				}
				//update the slot value
				slotJsonStr, err := utils.ToJson(slotInfo)
				if err == nil {
					_, err = self.zk.Set("/yundis/ids/"+slotInfo.SlotId, []byte(slotJsonStr))
					if err != nil {
						log.Errorf("Can not set the value of /yundis/ids/"+slotInfo.SlotId+" to zk. err : %s", err)
					}
				} else {
					log.Errorf("Can not convert %+v to jsonstr. err : %s", slotInfo, err)
				}
			}
			//update the value of /yundis/ids
			if len(changedSlotIds) > 0 {
				sort.Ints(changedSlotIds) //sort the array.
				log.Infof("update the %s to /yundis/ids.", changedSlotIds)
				idsJsonStr, err := utils.ToJson(changedSlotIds)
				if err == nil {
					_, err = self.zk.Set("/yundis/ids", []byte(idsJsonStr))
					if err != nil {
						log.Errorf("Can not set the value of /yundis/ids. err : %s", err)
					}
				} else {
					log.Errorf("Can not convert %+v to jsonstr. err:%s", changedSlotIds, err)
				}
			}
			//Unlock
			err := zkLocker.Unlock()
			if err != nil {
				log.Errorf("Unlock the idslocker error. err:%s", err)
			}
		}
	} else {
		log.Info("Do not need to refresh slotinfo to zk.")
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
				self.SetNodeInfoMap(infoMap) //refresh nodeinfo map by new zk data.
				self.ModifySlotState(infoMap)
				log.Info("Refresh nodeinfo map by new zk data.")
			} else {
				log.Errorf("Can not watching the children of /yundis/nodes, err:%s", err1)
				break
			}
			time.Sleep(time.Second)
		}
	}()
}
