package proxy
import(
	"fmt"
	"time"
)
type ProxyError struct{
	What string
}
func (self *ProxyError) Error() string {
	return fmt.Sprintf("at %v, %s",
		time.Now(), self.What)
}