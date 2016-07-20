package logs

import(
	"fmt"
	log "github.com/cihub/seelog"
)
//var Logger log.LoggerInterface
func init() {
	mylogger,err := log.LoggerFromConfigAsFile("log.xml")
	if err != nil {
		fmt.Println("load log.xml fail:",err)
	}
	log.ReplaceLogger(mylogger) 
}