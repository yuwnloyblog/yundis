package proxy_test
import(
	"testing"
	"github.com/yuwnloyblog/yundis/proxy"
)
func TestCmd(t *testing.T) {
	str := "*2\r\n$3\r\nget\r\n$4\r\nname\r\n";
	cmd,err:=proxy.ParseRedisCmd(str)
	if err==nil{
		t.Log(cmd)
	}else{
		t.Error(err)
	}
}