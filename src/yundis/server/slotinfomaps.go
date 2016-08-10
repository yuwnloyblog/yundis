package server

import (
	"strconv"
	"sync"
	"yundis/utils"

	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
)

type SlotInfoMaps struct {
	slotInfoMap map[string]*SlotInfo
	locker      *sync.RWMutex
	zk          *utils.ZkHelper
}

/**
 * initial the slotinfo maps
 */
func (self *SlotInfoMaps) Initial(zkHelper *utils.ZkHelper) {
	self.slotInfoMap = make(map[string]*SlotInfo)
	self.locker = new(sync.RWMutex)
	self.zk = zkHelper
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

/**
 * load the slot's info to map.
 */
func (self *SlotInfoMaps) LoadSlotInfoMap(slotCount int) {
	log.Info("Read the slot's info from zk.")
	if !self.zk.PathExist("/yundis/ids") {
		_, err := self.zk.Create("/yundis/ids", []byte{}, 0, zk.WorldACL(zk.PermAll))
		log.Errorf("can not create path %s, err: %s", "/yundis/ids", err)
	}
	infoMap := make(map[string]*SlotInfo)
	for i := 0; i < slotCount; i++ {
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
	self.SetSlotInfoMap(infoMap)
}
