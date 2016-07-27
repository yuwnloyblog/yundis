package server
import(
	"fmt"
	"time"
)
type NotExistError struct{
	What string
}
func (self *NotExistError) Error() string {
	return fmt.Sprintf("at %v, %s",
		time.Now(), self.What)
}

type NodeError struct{
	What string
}

func (self *NodeError) Error()string{
	return fmt.Sprintf("at %v, %s",
		time.Now(), self.What)
}