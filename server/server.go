package server
import(
	"time"
	"strings"
	"strconv"
	"net"
	"github.com/yuwnloyblog/yundis/proxy"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/yuwnloyblog/yundis/utils"
	commonutils "github.com/yuwnloyblog/go-commons-tool/utils"
	log "github.com/cihub/seelog"
)

type YundisServer struct{
	Id int         //server id
	//Name string    //server name
	Host string    //server's host
	Port int       //server's listen port
	RedisHost string  //its redis host
	RedisPort int     //its redis port
	ZkAddress []string // zookeeper's connect string
	zkConn *zk.Conn
	slots *SlotAllocation
	slotHashRing *commonutils.ConsistentHash
}
func (self *YundisServer)Start(){
	log.Info("Begin to register itself to zookeeper.")
	self.registerToZk()
	err:=self.handleSlotAllocations()
	if err != nil{
		log.Errorf("Error when handle allocations node. %s", err)
	}
	//load the nodeInfo map
	LoadNodeInfoMap(self.getZkConn())
	// initial the hash ring
	self.initialSlotHashRing()
	// start the agent to wait client connect.
	self.StartAgent()
	//log.Info("Start the yundis server ["+self.Name+"]")
}
/**
 * initial the hash ring for slots
 */
func (self *YundisServer)initialSlotHashRing(){
	cHashRing := commonutils.NewConsistentHash(false)
	for i := 0; i < slotCount; i++ {
		cHashRing.Add("slot"+strconv.Itoa(i), i, 1)
	}
	cHashRing.Prepare()
	self.slotHashRing = cHashRing
}

func (self *YundisServer)StartAgent(){
	listen,err := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(self.Host), self.Port, ""})  
    if err != nil {
    	log.Errorf("Error when start the agent, err:%s", err) 
        return
    }
    log.Info("Inited the connection, waitting for client...")
    for{
		conn, err := listen.Accept()
		if err != nil {
			log.Errorf("error when accept the connect from client:%s", err.Error())
			continue
		}
		log.Infof("client connected from:%s", conn.RemoteAddr().String())
		go func(){
			data := make([]byte, 8192)
			defer conn.Close()
			for {
				i,err := conn.Read(data)
				if err != nil{
					log.Warnf("error when read data from client:%s", err.Error())
					break
				}
				redisCmd,err := proxy.ParseRedisCmd(string(data[0:i]))
				if err!= nil{
					log.Errorf("Error when parse the redis cmd from client:%s",string(data[0:i]))
					break
				}
				log.Infof("cmd: %s",redisCmd)
				//send to redis
				hashNode := self.slotHashRing.Get(redisCmd.Key)
				slotId := hashNode.Entry.(int)
				log.Debugf("Target slot is %d", slotId)
				if nodeId,ok := self.slots.Allocations[strconv.Itoa(slotId)];ok{
					log.Debugf("Target node is %d", nodeId)
					if nodeInfo,ok:=nodeInfoMap[strconv.Itoa(nodeId)];ok{
						nodeInfo.GetRedisProxy().SendToRedis(data[0:i],conn)
					}else{
						log.Errorf("Can not find the nodeinfo by nodeId %d", nodeId)
					}
				}else{
					log.Errorf("Can not get the nodeId by slotId %d",slotId)
					break
				}
			}
		}()
	}
}

/**
 * read the control node 
 * /yundis/allocations
 */

func (self *YundisServer) handleSlotAllocations()error{
	slotsPath := "/yundis/allocations"
	if b:=self.pathExist(slotsPath);b{
		bytes,_,err:=self.getZkConn().Get(slotsPath)
		if err!=nil {
			log.Error("Read the data of '/yundis/allocations' error.")
			log.Error(err)
			return err
		}
		var slots SlotAllocation
		err = utils.JsonParse(string(bytes),&slots)
		if err != nil {
			log.Error("Parse the json data error.")
			log.Error(err)
			return err
		}
		self.slots = &slots
		return nil
	}else{
		slotAllocations := InitSlotAlloction(0)
		self.slots = slotAllocations
		bytes,err:=slotAllocations.ToNodeData()
		if err!=nil{
			return err
		}
		//create this path, and initial the data
		_,err=self.getZkConn().Create(slotsPath,bytes,0,zk.WorldACL(zk.PermAll))
		return err
	}
}

/**
 * register it self to zk
 * path :  /yundis/ids/{id}
 */
func (self *YundisServer)registerToZk(){
	self.recCreatePathNx("/yundis/ids")
	//register itself to zk
	nodeinfo := NodeInfo{
					Id : self.Id,
					//Name : self.Name,
					Host : self.RedisHost,
					Port : self.RedisPort,
			}
	jsonStr,err:=utils.ToJson(nodeinfo)
	if err != nil {
		log.Errorf("Convert to json string error:%s",err)
		panic(err)
	}
	_,err = self.coverCreate("/yundis/ids/"+strconv.Itoa(self.Id), []byte(jsonStr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Errorf("register node error:%s",err)
		panic(err)
	}
}
/**
 * delete that path if it have been existed befor create.
 */
func (self *YundisServer)coverCreate(path string, bytes []byte, flags int32, acls []zk.ACL)(string,error){
	if self.pathExist(path) {
		err:=self.getZkConn().Delete(path,0)
		if err != nil {
			return "",err
		}
	}
	return self.getZkConn().Create(path,bytes,flags,acls)
}
/**
 * judge the path whether exist.
 */
func (self *YundisServer)pathExist(path string)bool{
	b,_,err := self.getZkConn().Exists(path)
	if err != nil{
		log.Error(err)
		return false
	}
	return b
}
/**
 * create the path if it not exist.
 */
func (self *YundisServer)recCreatePathNx(path string){
	pathArr:=strings.Split(path,"/")
	for i:=0;i<len(pathArr);i++{
		childPath:=strings.Join(pathArr[0:i+1],"/")
		if childPath != "" {
			isExist:= self.pathExist(childPath)
			if !isExist {//that path have not been created.
				_,err:=self.getZkConn().Create(childPath,[]byte{},0,zk.WorldACL(zk.PermAll))
				if err != nil {
					log.Errorf("can not create path %s, err: %s", childPath, err)
					break
				}else{
					log.Infof("success to create the path %s", childPath)
				}
			}
		}
	}
}

func (self *YundisServer)getZkConn()*zk.Conn{
	if self.zkConn == nil {
		c, _, err := zk.Connect(self.ZkAddress, 10*time.Second)
		if err != nil {
			log.Errorf("connect zk error:",err)
		}else{
			log.Info("Success to connect zk.")
			self.zkConn = c
		}
	}
	return self.zkConn
}