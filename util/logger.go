package util

import (
	"log"
	"os"
)

var (
	Log *log.Logger
)

func InitLogger(LogFile string) {
	//创建输出日志文件
	logFile, err := os.Create(LogFile);
	if err != nil {
		panic(err);
	}
	Log = log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile);
}
