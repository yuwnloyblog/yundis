package server

import (
	"sort"
	"strconv"
	"yundis/utils"

	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
)

type SlotAllocation struct {
	Allocations map[string]int
	SlotCount   int
	NodeList    []int
}

type NodeSortItem struct {
	SlotCount int
	NodeId    int
}

type NodeSortItemSlice []NodeSortItem

func (self NodeSortItemSlice) Len() int {
	return len(self)
}

func (self NodeSortItemSlice) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self NodeSortItemSlice) Less(i, j int) bool {
	return self[j].SlotCount < self[i].SlotCount
}

func InitSlotAlloction(slotCount, nodeId int) *SlotAllocation {
	allocMap := make(map[string]int)
	for i := 0; i < slotCount; i++ {
		allocMap[strconv.Itoa(i)] = nodeId
	}
	return &SlotAllocation{
		Allocations: allocMap,
		SlotCount:   slotCount,
		NodeList:    []int{0},
	}
}

func InitSlotAlloctionWithData(data string) (*SlotAllocation, error) {
	var slots SlotAllocation
	err := utils.JsonParse(data, &slots)
	if err != nil {
		return nil, err
	}
	return &slots, nil
}

/**
 * When allocation changed, modify the state of slot and update slotmaps.
 */
func HandleAllocationChange(oldAllocations, newAllocations *SlotAllocation, slotinfoMaps *SlotInfoMaps, zkHelper *utils.ZkHelper) {
	isChanged := false
	newSlotInfoMap := make(map[string]*SlotInfo) //create a new slotinfo maps
	for i := 0; i < oldAllocations.SlotCount; i++ {
		oldNodeId := oldAllocations.Allocations[strconv.Itoa(i)]
		newNodeId := newAllocations.Allocations[strconv.Itoa(i)]
		newSlotInfoMap[strconv.Itoa(i)] = &SlotInfo{
			SlotId:       strconv.Itoa(i),
			State:        slotinfoMaps.GetSlotInfoMap()[strconv.Itoa(i)].State,
			NodeId:       slotinfoMaps.GetSlotInfoMap()[strconv.Itoa(i)].NodeId,
			SrcNodeId:    slotinfoMaps.GetSlotInfoMap()[strconv.Itoa(i)].SrcNodeId,
			TargetNodeId: slotinfoMaps.GetSlotInfoMap()[strconv.Itoa(i)].TargetNodeId,
		}
		if oldNodeId != newNodeId {
			isChanged = true
			log.Infof("The slot %d's node changed to %d from %d.", i, newNodeId, oldNodeId)
			newSlotInfoMap[strconv.Itoa(i)].State = SlotStateMigrating
			newSlotInfoMap[strconv.Itoa(i)].NodeId = strconv.Itoa(newNodeId)
			newSlotInfoMap[strconv.Itoa(i)].SrcNodeId = strconv.Itoa(oldNodeId)
			newSlotInfoMap[strconv.Itoa(i)].TargetNodeId = strconv.Itoa(newNodeId)
			//update the new slotinfo to zk
			jsonStr, err := utils.ToJson(newSlotInfoMap[strconv.Itoa(i)])
			if err != nil {
				log.Errorf("Can not convert to json string from obj [%s]", newSlotInfoMap[strconv.Itoa(i)])
			} else {
				_, err = zkHelper.CoverCreate("/yundis/ids/"+strconv.Itoa(i), []byte(jsonStr), 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					log.Errorf("Change the value of /yundis/ids/%d fail, err:%s.", i, err)
				}
				//zkHelper.Set("/yundis/ids/"+strconv.Itoa(i), []byte(jsonStr), 1)
			}
		}
	}
	if isChanged {
		log.Info("Update the slotinfoMaps.")
		//slotinfoMaps.SetSlotInfoMap(infoMap)
		slotinfoMaps.SetSlotInfoMap(newSlotInfoMap)
	}
}

/**
 * add new node and adjustment allocation map
 */
func (self *SlotAllocation) AddNode(nodeId int) error {
	oldNodeCount := len(self.NodeList)
	newEachNodeSlotCount := self.SlotCount / (oldNodeCount + 1)
	if newEachNodeSlotCount <= 0 {
		return &NodeError{"Have no more slot to allocate."}
	}
	//convert allocation map to node map
	nodeMap := self.getNodeMap()
	itemList := self.getNodeItemList(false)

	//reduce the old node allocation and create new node
	var newSlotArr []int
	isContinue := true
	for {
		for _, v := range itemList {
			eachSlotCount := len(nodeMap[v.NodeId])
			if eachSlotCount > 1 { //do not split if that node only have one slot
				newSlotArr = append(newSlotArr, nodeMap[v.NodeId][eachSlotCount-1])
				nodeMap[v.NodeId] = nodeMap[v.NodeId][0 : eachSlotCount-1]
				if len(newSlotArr) >= newEachNodeSlotCount {
					isContinue = false
					break
				}
			}
		}
		if !isContinue {
			break
		}
	}
	if len(newSlotArr) <= 0 {
		return &NodeError{"Have no more slot to allocate."}
	}
	nodeMap[nodeId] = newSlotArr
	//change the allocation map
	for k, v := range nodeMap {
		for _, slotId := range v {
			self.Allocations[strconv.Itoa(slotId)] = k
		}
	}
	//add the node list
	self.NodeList = append(self.NodeList, nodeId)

	return nil
}

/**
 * remove node and adjustment allocation map
 */
func (self *SlotAllocation) RemoveNode(nodeId int) error {
	if self.IsExistedNode(nodeId) {
		if len(self.NodeList) <= 1 {
			return &NodeError{"This is already the last node, cannot to be removed."}
		}
		nodeMap := self.getNodeMap()
		var itemList []NodeSortItem
		//remove this node from nodelist
		var nodeList []int
		for k, v := range nodeMap {
			if k != nodeId {
				itemList = append(itemList, NodeSortItem{
					SlotCount: len(v),
					NodeId:    k,
				})
				nodeList = append(nodeList, k)
			}
		}
		sort.Sort(sort.Reverse(NodeSortItemSlice(itemList)))
		tobeRemove := nodeMap[nodeId]
		if len(tobeRemove) <= 0 {
			return &NodeError{"This node have no slots."}
		}
		isContinue := true
		for {
			for _, v := range itemList {
				lenRemoveArr := len(tobeRemove)
				if lenRemoveArr > 0 {
					nodeMap[v.NodeId] = append(nodeMap[v.NodeId], tobeRemove[lenRemoveArr-1])
					tobeRemove = tobeRemove[0 : lenRemoveArr-1]
				} else {
					isContinue = false
					break
				}
			}
			if !isContinue {
				break
			}
		}
		delete(nodeMap, nodeId) //remove from node map
		//change the allocation map
		for k, v := range nodeMap {
			for _, slotId := range v {
				self.Allocations[strconv.Itoa(slotId)] = k
			}
		}
		sort.Ints(nodeList)
		self.NodeList = nodeList
	} else {
		return &NodeError{"Have not contained this node in node list."}
	}
	return nil
}

func (self *SlotAllocation) getNodeMap() map[int][]int {
	nodeMap := make(map[int][]int)
	for k, v := range self.Allocations {
		i, err := strconv.Atoi(k)
		if err == nil {
			nodeMap[v] = append(nodeMap[v], i)
		}
	}
	return nodeMap
}

func (self *SlotAllocation) getNodeItemList(reverse bool) []NodeSortItem {
	var itemList []NodeSortItem
	nodeMap := self.getNodeMap()
	for k, v := range nodeMap {
		itemList = append(itemList, NodeSortItem{
			SlotCount: len(v),
			NodeId:    k,
		})
	}
	//sort it by slotCount desc
	if reverse {
		sort.Sort(sort.Reverse(NodeSortItemSlice(itemList)))
	} else {
		sort.Sort(NodeSortItemSlice(itemList))
	}
	return itemList
}

/**
 * judge this node whether have been contained.
 */
func (self *SlotAllocation) IsExistedNode(nodeId int) bool {
	for _, v := range self.NodeList {
		if v == nodeId {
			return true
		}
	}
	return false
}

/**
 * generate the data of node by map
 */
func (self *SlotAllocation) ToNodeData() ([]byte, error) {
	str, err := utils.ToJson(self)
	if err != nil {
		return []byte{}, err
	}
	return []byte(str), nil
}
