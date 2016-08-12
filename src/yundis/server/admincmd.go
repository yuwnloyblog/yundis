package server

import (
	"bufio"
	"net"
	"sort"
	"strconv"
	"yundis/proxy"
	"yundis/utils"

	log "github.com/cihub/seelog"
)

const (
	REBLANCE = "reblance"
)

type AdminCmdHandler struct {
	yundis *YundisServer
}

func NewAdminCmdHandler(server *YundisServer) *AdminCmdHandler {
	handler := &AdminCmdHandler{
		yundis: server,
	}
	return handler
}

/**
 * filter out admin cmd.
 */
func (self *AdminCmdHandler) IsAdminCmd(cmd *proxy.RedisCmd) bool {
	if cmd.Cmd == "yundisctl" {
		return true
	}
	return false
}

/**
 * handle admin cmd
 */
func (self *AdminCmdHandler) ExecuteCmd(cmd *proxy.RedisCmd, conn net.Conn) {
	log.Info("Execute the admin cmd : ", cmd)

	success := self.DoReblance()

	clientBw := bufio.NewWriter(conn)
	if success {
		clientBw.WriteString("+OK\r\n")
	} else {
		clientBw.WriteString("-ERR fail.\r\n")
	}
	clientBw.Flush()
}

func (self *AdminCmdHandler) DoReblance() bool {
	log.Info("Do reblance.")
	//get the allocation from zk.
	allocationsFromZk := GetSlotAllocationsFromZk(self.yundis.zkHelper)
	if allocationsFromZk != nil {
		//get the node list
		nodeInfoMaps := self.yundis.nodeinfoMaps.getNodeInfoMapFromZk()
		self.yundis.nodeinfoMaps.SetNodeInfoMap(nodeInfoMaps) //update the nodeinfo map
		nodeSlotsMap := allocationsFromZk.getNodeMap()
		var addedNodeIds []int
		for _, nodeInfo := range nodeInfoMaps {
			if _, ok := nodeSlotsMap[nodeInfo.Id]; ok {

			} else {
				addedNodeIds = append(addedNodeIds, nodeInfo.Id)
			}
		}
		if len(addedNodeIds) > 0 {
			log.Info("New Added node list : ", addedNodeIds)
			for _, nodeId := range addedNodeIds {
				log.Infof("Add node %d to allocations.", nodeId)
				err := allocationsFromZk.AddNode(nodeId)
				if err != nil {
					log.Errorf("Error when add node %d to allocation. err : %s", nodeId, err)
				}
			}

			zkLocker := self.yundis.zkHelper.GetLocker("/yundis/idslocker")
			lockErr := zkLocker.Lock()
			if lockErr != nil {
				log.Errorf("Can not lock /yundis/idslocker. err : %s", lockErr)
				return false
			} else {
				//current slot map
				slotMap := self.yundis.slotinfoMaps.GetSlotInfoMapFromZk()
				var affectedSlots []*SlotInfo
				//for each slot:node map
				for slotId, nodeId := range allocationsFromZk.Allocations {
					slotInfo := slotMap[slotId]
					if slotInfo != nil && slotInfo.NodeId != strconv.Itoa(nodeId) {
						//modify the slot's migrate state
						slotInfo.MigrateState = MigStateMigrating
						slotInfo.SrcNodeId = slotInfo.NodeId
						slotInfo.NodeId = strconv.Itoa(nodeId)
						slotInfo.TargetNodeId = strconv.Itoa(nodeId)
						affectedSlots = append(affectedSlots, slotInfo)
					}
				}
				self.yundis.slotinfoMaps.SetSlotInfoMap(slotMap) //update local slot map.
				//update allocation to zk
				log.Info("Update the allocation after changed to zk.")
				err := SyncToZk(allocationsFromZk, self.yundis.zkHelper)
				if err != nil {
					log.Errorf("Can not sync allocations to zk.err : %s", err)
				}
				//update slot info to zk
				if len(affectedSlots) > 0 {
					log.Info("Update slotInfo to zk.")
					var affectedSlotIds []int
					for _, slotInfo := range affectedSlots {
						log.Infof("Begin to update the slotinfo[%s] to zk.", slotInfo.SlotId)
						//record the id
						slotId, err := strconv.Atoi(slotInfo.SlotId)
						if err == nil {
							affectedSlotIds = append(affectedSlotIds, slotId)
						} else {
							log.Errorf("Convert slotId to int from string. err:%s", err)
						}
						//update the slot value
						slotJsonStr, err := utils.ToJson(slotInfo)
						if err == nil {
							_, err = self.yundis.zkHelper.Set("/yundis/ids/"+slotInfo.SlotId, []byte(slotJsonStr))
							if err != nil {
								log.Errorf("Can not set the value of /yundis/ids/"+slotInfo.SlotId+" to zk. err : %s", err)
							}
						} else {
							log.Errorf("Can not convert %+v to jsonstr. err : %s", slotInfo, err)
						}
					}
					//update the value of /yundis/ids
					if len(affectedSlotIds) > 0 {
						sort.Ints(affectedSlotIds) //sort the array.
						log.Infof("update the %s to /yundis/ids.", affectedSlotIds)
						idsJsonStr, err := utils.ToJson(affectedSlotIds)
						if err == nil {
							_, err = self.yundis.zkHelper.Set("/yundis/ids", []byte(idsJsonStr))
							if err != nil {
								log.Errorf("Can not set the value of /yundis/ids. err : %s", err)
							}
						} else {
							log.Errorf("Can not convert %+v to jsonstr. err:%s", affectedSlotIds, err)
						}
					}
				}
				lockErr = zkLocker.Unlock()
				if lockErr != nil {
					log.Errorf("Can not release lock /yundis/dislocker. err : %s", lockErr)
				}
			}
		}
		return true
	} else {
		log.Error("When do reblance, can not reload allocations from zk.")
	}
	return false
}
