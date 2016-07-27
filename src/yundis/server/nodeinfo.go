package server

import (
	"yundis/proxy"

	log "github.com/cihub/seelog"
)

type NodeInfo struct {
	Id         int
	Host       string
	Port       int
	RedisHost  string
	RedisPort  int
	redisProxy *proxy.RedisProxy
}

/**
 * get the redis proxy
 */
func (self *NodeInfo) GetRedisProxy() *proxy.RedisProxy {
	if self.redisProxy == nil {
		log.Infof("Initial the redis proxy by %s:%d", self.RedisHost, self.Port)
		self.redisProxy = &proxy.RedisProxy{
			TargetHost: self.RedisHost,
			TargetPort: self.RedisPort,
		}
	}
	return self.redisProxy
}
