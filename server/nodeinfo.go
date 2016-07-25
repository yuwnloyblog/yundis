package server

import(
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/cihub/seelog"
	"github.com/yuwnloyblog/yundis/utils"
	"github.com/yuwnloyblog/yundis/proxy"
	"sync"
)

type NodeInfo struct{
	Id int
	//Name string
	Host string
	Port int
	redisProxy *proxy.RedisProxy
}
/**
 * get the redis proxy
 */
func (self *NodeInfo)GetRedisProxy()(*proxy.RedisProxy){
	if self.redisProxy == nil {
		self.redisProxy = &proxy.RedisProxy{
							TargetHost : self.Host,
							TargetPort : self.Port,
						}
	}
	return self.redisProxy
}

var nodeInfoMap = make(map[string]*NodeInfo)
var locker = new(sync.RWMutex)

func GetNodeInfoMap()map[string]*NodeInfo{
	locker.RLock()
	defer locker.RUnlock()
	return nodeInfoMap
}
func SetNodeInfoMap(infoMap map[string]*NodeInfo){
	locker.Lock()
	defer locker.Unlock()
	nodeInfoMap = infoMap
}
/**
 * load nodeinfo map from zk
 */
func LoadNodeInfoMap(zkConn *zk.Conn){
	//read the node list from zk
	infoMap := getNodeInfoMapFromZk(zkConn)
	SetNodeInfoMap(infoMap)
	WatchNodeInfoMap(zkConn)
}
/**
 * reload the nodeinfo map from zk.
 */
func getNodeInfoMapFromZk(zkConn *zk.Conn)(map[string]*NodeInfo){
	var infoMap = make(map[string]*NodeInfo)
	//read the node list from zk
	children,_,err:=zkConn.Children("/yundis/ids")
	if err != nil{
		log.Error("Can not read the subpath of /yundis/ids")
		log.Error(err)
	}
	log.Infof("The existed node list %s:",children)
	for _,v:=range children{
		nodeInfo,err := getNodeInfoFromZk(zkConn,"/yundis/ids/"+v)
		if err != nil{
			continue
		}
		infoMap[v] = nodeInfo
	}
	return infoMap
}
/**
 * get nodeinfo from specify path
 */
func getNodeInfoFromZk(zkConn *zk.Conn, path string)(*NodeInfo,error){
	var nodeInfo NodeInfo
	bytes,_,err := zkConn.Get(path)
	if err != nil {
		log.Error("Can not read the subpath of /yundis/ids")
		log.Error(err)
		return nil,err
	}
	err = utils.JsonParse(string(bytes),&nodeInfo)
	if err != nil {
		log.Errorf("Can not parse data from node %s, err: %s", path, err)
		return nil,err
	}
	return &nodeInfo,nil
}
/**
 * watch the node list change.
 */
func WatchNodeInfoMap(zkConn *zk.Conn){
	_, _, ch, err := zkConn.ChildrenW("/yundis/ids")
	if err!=nil{
		log.Errorf("Can not watch path /yundis/ids, err:",err)
	}
	go func(){
		for{
			event := <-ch
			log.Infof("node list change, %+v",event)
			children,_,ch1,err1:= zkConn.ChildrenW("/yundis/ids")
			if err1==nil{
				ch = ch1
				//handle the node list change event
				log.Infof("node list changed : %s",children)
				infoMap := GetNodeInfoMap()
				SetNodeInfoMap(infoMap)//refresh nodeinfo map by new zk data.
				log.Info("Refresh nodeinfo map by new zk data.")
			}else{
				break
			}
		}
	}()
}
