package main

import (
	"fmt"
	"os"
	"strings"
	mylogutil "yundis/logs"
	"yundis/server"

	log "github.com/cihub/seelog"
	"github.com/yuwnloyblog/go-commons-tool/utils"
)

func main() {
	args := os.Args
	if len(args) <= 1 {
		fmt.Println("Please specify the yundis.properties file.")
		return
	}
	configFile := args[1]
	fmt.Printf("Load the config file : %s.\n", configFile)
	props := utils.Properties{}
	err := props.LoadPropertyFile(configFile)
	mylogutil.InitLogger(props)
	if err != nil {
		log.Errorf("Load %s error: %s", configFile, err)
	} else {
		log.Info("Success to load the " + configFile)
		serverId, err := props.GetInt("server.id")
		if err != nil {
			log.Errorf("Read the server.id error:%s", err)
			return
		}
		serverHost, err := props.GetString("server.host")
		if err != nil {
			log.Errorf("Read the server.host error:%s", err)
			return
		}
		serverPort, err := props.GetInt("server.port")
		if err != nil {
			log.Errorf("Read the server.port error:%s", err)
			return
		}
		slotCount, err := props.GetInt("server.slots")
		if err != nil {
			log.Errorf("Read the server.slots error: %s", err)
			return
		}
		redisServer, err := props.GetString("redis.host")
		if err != nil {
			log.Errorf("Read the redis.server error:%s", err)
			return
		}
		redisPort, err := props.GetInt("redis.port")
		if err != nil {
			log.Errorf("Read the redis.port error:%s", err)
			return
		}
		zkconnect, err := props.GetString("zookeeper.connect")
		if err != nil {
			log.Errorf("Read the zookeeper.connect error:%s", err)
			return
		}

		zkAddress := strings.Split(zkconnect, ",")
		ser := &server.YundisServer{
			Id:        serverId,
			Host:      serverHost,
			Port:      serverPort,
			RedisHost: redisServer,
			RedisPort: redisPort,
			ZkAddress: zkAddress,
			SlotCount: slotCount,
		}
		log.Info("Begin to start the yundis server.")
		ser.Start()
	}

	//time.Sleep(time.Second*100)
}
