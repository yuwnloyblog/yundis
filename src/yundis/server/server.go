package server

import (
	"net"
	"strconv"
	"sync"
	"yundis/proxy"
	"yundis/utils"

	log "github.com/cihub/seelog"
	"github.com/samuel/go-zookeeper/zk"
	commonutils "github.com/yuwnloyblog/go-commons-tool/utils"
)

type YundisServer struct {
	Id        int      //server id
	Host      string   //server's host
	Port      int      //server's listen port
	RedisHost string   //its redis host
	RedisPort int      //its redis port
	ZkAddress []string // zookeeper's connect string
	SlotCount int      // the count of slot
	zkHelper  *utils.ZkHelper
	//zkConn *zk.Conn
	allocations  *SlotAllocation
	allocLocker  *sync.RWMutex
	slotHashRing *commonutils.ConsistentHash
	nodeinfoMaps *NodeInfoMaps
	slotinfoMaps *SlotInfoMaps
}

var adminHandler *AdminCmdHandler

func (self *YundisServer) Start() {
	self.allocLocker = new(sync.RWMutex)
	self.zkHelper = utils.NewZkHelper(self.ZkAddress)

	log.Info("Begin to register itself to zookeeper.")

	self.registerToZk()

	// init the slot allocation.
	self.SetAllocations(InitSlotAllocation(self.zkHelper, self.SlotCount, self.Id))

	// initial the hash ring
	self.initialSlotHashRing()

	// update the slot info map
	self.slotinfoMaps = &SlotInfoMaps{}
	self.slotinfoMaps.Initial(self.zkHelper, self.SlotCount)
	self.slotinfoMaps.LoadSlotInfoMap()

	//load the nodeInfo map
	self.nodeinfoMaps = &NodeInfoMaps{}
	self.nodeinfoMaps.Initial(self.zkHelper, self.slotinfoMaps, self.allocations)
	self.nodeinfoMaps.LoadNodeInfoMap()

	//refresh the slotinfo map's state.
	self.nodeinfoMaps.ModifySlotState(self.nodeinfoMaps.GetNodeInfoMap())

	//init the admin cmd handler
	adminHandler = NewAdminCmdHandler(self)

	// start the agent to wait client connect.
	self.StartAgent()
	//log.Info("Start the yundis server ["+self.Name+"]")
}

/**
 * initial the hash ring for slots
 */
func (self *YundisServer) initialSlotHashRing() {
	cHashRing := commonutils.NewConsistentHash(false)
	for i := 0; i < self.SlotCount; i++ {
		cHashRing.Add("slot"+strconv.Itoa(i), i, 1)
	}
	cHashRing.Prepare()
	self.slotHashRing = cHashRing
}

func (self *YundisServer) StartAgent() {
	listen, err := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(self.Host), self.Port, ""})
	if err != nil {
		log.Errorf("Error when start the agent, err:%s", err)
		return
	}
	log.Info("Inited the connection, waitting for client...")
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Errorf("error when accept the connect from client:%s", err.Error())
			continue
		}
		log.Infof("client connected from:%s", conn.RemoteAddr().String())
		go func() {
			data := make([]byte, 8192)
			defer conn.Close()
			for {
				i, err := conn.Read(data)
				if err != nil {
					log.Warnf("error when read data from client:%s", err.Error())
					break
				}
				redisCmd, err := proxy.ParseRedisCmd(string(data[0:i]))
				if err != nil {
					log.Errorf("Error when parse the redis cmd from client:%s", string(data[0:i]))
					break
				}
				log.Infof("cmd: %s", redisCmd)
				if adminHandler.IsAdminCmd(redisCmd) {
					//execute the admin cmd.
					adminHandler.ExecuteCmd(redisCmd, conn)
				} else {
					//send to redis
					hashNode := self.slotHashRing.Get(redisCmd.Key)
					slotId := hashNode.Entry.(int)
					log.Debugf("Target slot is %d", slotId)
					if nodeId, ok := self.GetAllocations().Allocations[strconv.Itoa(slotId)]; ok {
						log.Debugf("Target node is %d", nodeId)
						if nodeInfo, ok := self.nodeinfoMaps.GetNodeInfoMap()[strconv.Itoa(nodeId)]; ok {
							nodeInfo.GetRedisProxy().SendToRedis(data[0:i], conn)
						} else {
							log.Errorf("Can not find the nodeinfo by nodeId %d", nodeId)
						}
					} else {
						log.Errorf("Can not get the nodeId by slotId %d", slotId)
						break
					}
				}
			}
		}()
	}
}

/**
 * register it self to zk
 * path :  /yundis/nodes/{id}
 */
func (self *YundisServer) registerToZk() {
	if !self.zkHelper.PathExist("/yundis/nodes") {
		self.zkHelper.RecCreatePathNx("/yundis/nodes")
	}
	//register itself to zk
	nodeinfo := NodeInfo{
		Id:        self.Id,
		Host:      self.Host,
		Port:      self.Port,
		RedisHost: self.RedisHost,
		RedisPort: self.RedisPort,
	}
	jsonStr, err := utils.ToJson(nodeinfo)
	if err != nil {
		log.Errorf("Convert to json string error:%s", err)
		panic(err)
	}
	_, err = self.zkHelper.CoverCreate("/yundis/nodes/"+strconv.Itoa(self.Id), []byte(jsonStr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Errorf("register node error:%s", err)
		panic(err)
	}
}

/**
 * set the allocations.
 */
func (self *YundisServer) SetAllocations(alloc *SlotAllocation) {
	self.allocLocker.Lock()
	defer self.allocLocker.Unlock()
	self.allocations = alloc
}

/**
 * get the allocations
 */
func (self *YundisServer) GetAllocations() *SlotAllocation {
	self.allocLocker.RLock()
	defer self.allocLocker.RUnlock()
	return self.allocations
}
