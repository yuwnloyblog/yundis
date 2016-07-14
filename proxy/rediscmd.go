package proxy

import (
	"strings"
	"strconv"
)
type RedisCmd struct{
	Cmd string
	Key string
	Args []string
	Orginal []string
}
func ParseRedisCmd(str string)(*RedisCmd,error){
	arr := strings.Split(str,"\r\n")
	leng := len(arr)
	if leng>=3 {
		strParmCount := arr[0][1:]
		parmCount,err := strconv.Atoi(strParmCount)
		if err!= nil {
			return nil,&CmdError{"Error when convert ["+strParmCount+"] to number."}
		}
		if parmCount*2 == leng - 2 {
			var args []string
			for i:=2;i<leng;i=i+2{
				args = append(args,arr[i])
			}
			if len(args)>=2 {
				redisCmd := RedisCmd{args[0], args[1], args[2:],arr}
				return &redisCmd,nil
			}
		}
	}
	return nil,&CmdError{"mistake cmd format.["+str+"]"}
}