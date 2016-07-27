package logs

import (
	"fmt"
	"strconv"

	log "github.com/cihub/seelog"
	"github.com/yuwnloyblog/go-commons-tool/utils"
)

//InitLogger
func InitLogger(props utils.Properties) {
	logType := props.GetStringWithDefault("log.type", "sync")
	typeStr := ""
	if logType == "sync" {
		typeStr = `type="sync"`
	} else if logType == "asynctimer" {
		asyncInterval := props.GetIntWithDefault("log.asyncinterval", 5000000)
		typeStr = `type="asynctimer" asyncinterval="` + strconv.Itoa(asyncInterval) + `"`
	}
	minLevel := props.GetStringWithDefault("log.minLevel", "info")
	maxlevel := props.GetStringWithDefault("log.maxlevel", "error")
	logDir := props.GetStringWithDefault("log.dirs", "./log")
	segmentBytes := props.GetIntWithDefault("log.segment.bytes", 53687092)
	segmentRolls := props.GetIntWithDefault("log.segment.rolls", 5)

	logConfig := `
		<seelog ` + typeStr + ` minlevel="` + minLevel + `" maxlevel="` + maxlevel + `">
				<outputs formatid="main">
				    <console/>
						<rollingfile type="size" filename="` + logDir + `/server.log" maxsize="` + strconv.Itoa(segmentBytes) + `" maxrolls="` + strconv.Itoa(segmentRolls) + `"/>
					  <filter levels="error">
								<file path="` + logDir + `/error.log"/>
						</filter>
				</outputs>
				<formats>
						<format id="main" format="%Date(2006 Jan 02 3:04:05.000000000 PM MST) [%Level] %Msg%n"/>
				</formats>
		</seelog>
	`
	mylogger, err := log.LoggerFromConfigAsString(logConfig)
	fmt.Println(logConfig)
	//mylogger, err := log.LoggerFromConfigAsFile("../config/log.xml")
	if err != nil {
		fmt.Println("load seelog config fail:", err)
	}
	log.ReplaceLogger(mylogger)
}
