package proxy
import(
	"fmt"
	"time"
)
type CmdError struct{
	What string
}
func (self *CmdError) Error() string {
	return fmt.Sprintf("at %v, %s",
		time.Now(), self.What)
}