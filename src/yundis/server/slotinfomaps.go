package server

import (
	"strconv"
	"sync"
	"time"
	"yundis/utils"

	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
)

type SlotInfoMaps struct {
	slotInfoMap map[string]*SlotInfo // key is slotId, value is slotinfo.
	locker      *sync.RWMutex
	zk          *utils.ZkHelper
	slotCount   int
}

/**
 * initial the slotinfo maps
 */
func (self *SlotInfoMaps) Initial(zkHelper *utils.ZkHelper, slotCount int) {
	self.slotInfoMap = make(map[string]*SlotInfo)
	self.locker = new(sync.RWMutex)
	self.zk = zkHelper
	self.slotCount = slotCount
}

func (self *SlotInfoMaps) GetSlotInfoMap() map[string]*SlotInfo {
	self.locker.RLock()
	defer self.locker.RUnlock()
	return self.slotInfoMap
}

func (self *SlotInfoMaps) SetSlotInfoMap(infoMap map[string]*SlotInfo) {
	self.locker.Lock()
	defer self.locker.Unlock()
	self.slotInfoMap = infoMap
}

func (self *SlotInfoMaps) LoadSlotInfoMap() {
	infoMap := self.getSlotInfoMapFromZk()
	self.SetSlotInfoMap(infoMap)
	self.WatchSlotInfoMap()
}

/**
 * load the slot's info to map.
 */
func (self *SlotInfoMaps) getSlotInfoMapFromZk() map[string]*SlotInfo {
	log.Info("Read the slot's info from zk.")
	if !self.zk.PathExist("/yundis/ids") {
		_, err := self.zk.Create("/yundis/ids", []byte{}, 0, zk.WorldACL(zk.PermAll))
		log.Errorf("can not create path %s, err: %s", "/yundis/ids", err)
	}
	infoMap := make(map[string]*SlotInfo)
	for i := 0; i < self.slotCount; i++ {
		strI := strconv.Itoa(i)
		bytes, _, err := self.zk.Get("/yundis/ids/" + strI)
		if err != nil || len(bytes) == 0 {
			slotInfo := &SlotInfo{
				SlotId: strI,
				State:  SlotStateNormal,
			}
			infoMap[strI] = slotInfo
			dataStr, err := utils.ToJson(slotInfo)
			if err != nil {
				log.Errorf("Can not convert %s to json. err:%s", slotInfo, err)
				continue
			}
			self.zk.Create("/yundis/ids/"+strI, []byte(dataStr), 0, zk.WorldACL(zk.PermAll))
		} else {
			log.Infof("Read the data form path %s", "/yundis/ids/"+strI)
			var slotInfo SlotInfo
			err = utils.JsonParse(string(bytes), &slotInfo)
			if err != nil {
				log.Errorf("Can not parse data from node %s, err: %s", "/yundis/ids/"+strI, err)
				continue
			} else {
				infoMap[strI] = &slotInfo
			}
		}
	}
	return infoMap
}

/**
 * watch the slot list change.
 */
func (self *SlotInfoMaps) WatchSlotInfoMap() {
	_, _, ch, err := self.zk.GetZkConn().GetW("/yundis/ids")
	if err != nil {
		log.Errorf("Can not watch path /yundis/ids, err:%s", err)
	}

	go func() {
		for {
			event := <-ch
			log.Infof("node list event, %+v", event)
			data, _, ch1, err1 := self.zk.GetZkConn().GetW("/yundis/ids")
			if err1 == nil {
				ch = ch1
				//handle the node list change event
				log.Infof("node list changed : %s", data)
				infoMap := self.getSlotInfoMapFromZk()
				//change the slotinfo state.
				self.SetSlotInfoMap(infoMap) //refresh nodeinfo map by new zk data.
				log.Info("Refresh slotinfo map by new zk data.")
			} else {
				log.Errorf("Can not watching the children of /yundis/ids, err:%s", err1)
				break
			}
			time.Sleep(time.Second)
		}
	}()
}
