package types

import (
	"runtime"
)

const (
	defaultIndexName             = "radic"
	defaultAddCacheSize          = 10000
	defaultDeleteCacheSize       = 1000
	defaultInvertIndexSegmentNum = 32
	defaultForwardIndexSize      = 100000
	defaultDbShardNum            = 16
	defaultDbPath                = "dbs"
	defaultDbType                = "badger"
	defaultMaxRetrieve           = 5000
	defaultMaxInvertListLen      = 50000
)

var (
	defaultAddDocThreads    = runtime.NumCPU()
	defaultDeleteDocThreads = runtime.NumCPU() / 2
)

type IndexerOpts struct {
	IndexName             string
	AddCacheSize          int
	DeleteCacheSize       int
	InvertIndexSegmentNum int
	ForwardIndexSize      int //正排索引在内存中放多少条记录。其余的交给localDB
	DbShardNum            int
	DbPath                string
	DbType                string //可以使用"badger"、"rocksdb"和"bolt"，rocksdb要求服务器上安装很多rocksDB相关的组件
	AddDocThreads         int
	DeleteDocThreads      int
	MaxRetrieve           int
	MaxInvertListLen      int //每条倒排链最多只读取前MaxInvertListLen个docid
}

func (options *IndexerOpts) Init() {
	if options.IndexName == "" {
		options.IndexName = defaultIndexName
	}
	if options.AddCacheSize == 0 {
		options.AddCacheSize = defaultAddCacheSize
	}
	if options.DeleteCacheSize == 0 {
		options.DeleteCacheSize = defaultDeleteCacheSize
	}
	if options.InvertIndexSegmentNum == 0 {
		options.InvertIndexSegmentNum = defaultInvertIndexSegmentNum
	}
	if options.ForwardIndexSize == 0 {
		options.ForwardIndexSize = defaultForwardIndexSize
	}
	if options.DbShardNum == 0 {
		options.DbShardNum = defaultDbShardNum
	}
	if options.DbPath == "" {
		options.DbPath = defaultDbPath
	}
	if options.DbType == "" {
		options.DbType = defaultDbType
	}
	if options.AddDocThreads == 0 {
		options.AddDocThreads = defaultAddDocThreads
	}
	if options.DeleteDocThreads == 0 {
		options.DeleteDocThreads = defaultDeleteDocThreads
	}
	if options.MaxRetrieve == 0 {
		options.MaxRetrieve = defaultMaxRetrieve
	}
	if options.MaxInvertListLen == 0 {
		options.MaxInvertListLen = defaultMaxInvertListLen
	}
}
