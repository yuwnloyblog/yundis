package proxy

import(
	"net"
	"fmt"
	"bufio"
	"strings"
	"strconv"
)

type RedisProxy struct{
	Host string
	Port int
	TargetHost string
	TargetPort int
	redisConn net.Conn
	redisBw *bufio.Writer
	redisBr *bufio.Reader 
}

func (self *RedisProxy)Start()error{
	listen,err := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(self.Host), self.Port, ""})  
    if err != nil {
        fmt.Println("Error when listen the port:", err.Error())  
        return  err
    }  
    fmt.Println("Inited the connection, waitting for client...")
    self.process(listen)
	return nil;
}
/**
 * listening the tcp connection.
 */
func (self *RedisProxy)process(listen *net.TCPListener){
	for{
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("error when accept the connect from client:", err.Error())
			continue
		}
		fmt.Println("client connected from:", conn.RemoteAddr().String())
		go func(){
			data := make([]byte, 8192)
			defer conn.Close()
			for {
				i,err := conn.Read(data)
				if err != nil{
					fmt.Println("error when read data from client:", err.Error())
					break
				}
				fmt.Println("The data from client:\n", string(data[0:i]))
				redisCmd,err := ParseRedisCmd(string(data[0:i]))
				if err!= nil{
					fmt.Println("Error when parse the redis cmd from client:",string(data[0:i]))
					break
				}
				fmt.Println(redisCmd)
				//send to redis
				self.sendToRedis(data[0:i],conn)
			}
		}()
	}
}
/**
 * send data to redis
 */
func (self *RedisProxy)sendToRedis(data []byte, conn net.Conn){
	if self.redisConn == nil{
		rc,err := net.Dial("tcp",strings.Join([]string{self.TargetHost,strconv.Itoa(self.TargetPort)},":"))
		if err != nil{
			fmt.Println("connect redis error:", err.Error())
		}else{
			bw := bufio.NewWriter(rc)
			br := bufio.NewReader(rc)
			self.redisConn = rc
			self.redisBw = bw
			self.redisBr = br
		}
	}
	//write the data to redis
	clientBw := bufio.NewWriter(conn)
	self.redisBw.Write(data)
	if err:=self.redisBw.Flush();err==nil{
		//write the result from redis to client.
		for{
			buf := make([]byte,8192)
			count,err := self.redisBr.Read(buf)
			if err != nil{
				fmt.Println("Error when read data:",err.Error())
				break
			}
			clientBw.Write(buf[:count])
			if count<8192{
				break
			}
		}
	}else{
		clientBw.WriteString("-ERR target redis connection error.")
	}
	clientBw.Flush()
}
