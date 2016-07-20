package server
import(
	"fmt"
	"time"
	"strings"
	"strconv"
	"github.com/samuel/go-zookeeper/zk"
	_"github.com/yuwnloyblog/yundis/logs"
	"github.com/yuwnloyblog/yundis/utils"
	log "github.com/cihub/seelog"
)
const(
	NodeId = 0
	NodeName = "node0"
	RedisHost = "127.0.0.1"
	RedisPort = 6379
)
type YundisServer struct{
	Id int
	Name string
	zkConn *zk.Conn
	slots *SlotAllocation
}

func (self *YundisServer)Start(){
	log.Info("Begin to register itself to zookeeper.")
	self.registerToZk()
	err:=self.handleSlotAllocations()
	if err != nil{
		log.Error("Error when handle allocations node.")
		log.Error(err)
	}
	log.Info("Start the yundis server ["+self.Name+"]")
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
					Id : NodeId,
					Name : NodeName,
					Host : RedisHost,
					Port : RedisPort,
			}
	jsonStr,err:=utils.ToJson(nodeinfo)
	if err != nil {
		fmt.Println("Convert to json string error:",err)
		panic(err)
	}
	_,err = self.coverCreate("/yundis/ids/"+strconv.Itoa(self.Id), []byte(jsonStr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println("register node error:",err)
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
		fmt.Println(err)
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
					fmt.Println("create path '"+childPath+"' error!")
					break
				}else{
					fmt.Println("success to create the path '"+childPath+"'")
				}
			}
		}
	}
}

func (self *YundisServer)getZkConn()*zk.Conn{
	if self.zkConn == nil {
		c, _, err := zk.Connect([]string{"127.0.0.1:2181"}, 10*time.Second)
		if err != nil {
			fmt.Println("connect zk error:",err)
		}
		self.zkConn = c
	}
	return self.zkConn
}