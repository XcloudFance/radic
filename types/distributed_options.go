package types

type DistOpts struct {
	Servers      []string      //所有索引服务器的hostname
	Groups       int           //分成几组
	Port         int           //工作端口
}
