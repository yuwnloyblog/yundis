package utils

import(
	"time"
	"strings"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/cihub/seelog"
)

type ZkHelper struct{
	address []string
	zkConn *zk.Conn
}

func NewZkHelper(zkAddress []string)*ZkHelper{
	return &ZkHelper{
		address : zkAddress,
	}
}

/**
 * delete that path if it have been existed befor create.
 */
func (self *ZkHelper)CoverCreate(path string, bytes []byte, flags int32, acls []zk.ACL)(string,error){
	if self.PathExist(path) {
		err:=self.GetZkConn().Delete(path,0)
		if err != nil {
			log.Errorf("Delete exist path from zk error. err:%s",err)
			return "",err
		}
	}
	return self.GetZkConn().Create(path,bytes,flags,acls)
}
func (self *ZkHelper)Create(path string, bytes []byte, flags int32, acls []zk.ACL)(string, error){
	return self.GetZkConn().Create(path,bytes,flags,acls)
}
/**
 * read the value of path.
 */
func (self *ZkHelper)Get(path string)([]byte,*zk.Stat,error){
	return self.GetZkConn().Get(path)
}

/**
 * judge the path whether exist.
 */
func (self *ZkHelper)PathExist(path string)bool{
	b,_,err := self.GetZkConn().Exists(path)
	if err != nil{
		log.Errorf("Access zk error, err:%s",err)
		return false
	}
	return b
}
/**
 * create the path if it not exist.
 */
func (self *ZkHelper)RecCreatePathNx(path string){
	pathArr:=strings.Split(path,"/")
	for i:=0;i<len(pathArr);i++{
		childPath:=strings.Join(pathArr[0:i+1],"/")
		if childPath != "" {
			isExist:= self.PathExist(childPath)
			if !isExist {//that path have not been created.
				_,err:=self.Create(childPath,[]byte{},0,zk.WorldACL(zk.PermAll))
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


func (self *ZkHelper)GetZkConn()*zk.Conn{
	if self.zkConn == nil {
		c, _, err := zk.Connect(self.address, 10*time.Second)
		if err != nil {
			log.Errorf("connect zk error:",err)
		}else{
			log.Info("Success to connect zk.")
			self.zkConn = c
		}
	}
	return self.zkConn
}