package radic

import (
	"os"
	"radic/inverted_index"
	"radic/distributed"
	"radic/types"
	"radic/util"
	"syscall"
	"time"
)

type IndexEngine struct {
	indexer         inverted_index.Indexer
	van             *distributed.Van
	distributedMode bool
	stopChanSig     chan os.Signal
}

//Init 初始化参数，并从local db中加载索引
func (engine *IndexEngine) Init(indexOpts types.IndexerOpts, distOpts types.DistOpts, logFile string) {
	util.InitLogger(logFile)
	engine.indexer.Init(indexOpts)
	engine.van = distributed.GetVanInstance(distOpts)
	if engine.van == nil {
		util.Log.Printf("init radic in single mode")
	} else {
		engine.distributedMode = true
		engine.stopChanSig = make(chan os.Signal, engine.van.Balancer.GetGroupCount())
		util.Log.Printf("init radic in distributed mode")
	}
}

//Destroy 释放各种资源
func (engine *IndexEngine) Destroy() {
	engine.indexer.Close()
	if engine.distributedMode {
		for i := 0; i < engine.van.Balancer.GetGroupCount(); i++ {
			engine.stopChanSig <- syscall.SIGTERM
		}
		time.Sleep(2 * time.Second)
		engine.van.Balancer.Destroy()
	}
}

//IndexDoc 往索引上添加一个doc。异步执行的，不会立即生效
func (engine *IndexEngine) IndexDoc(doc types.DocInfo) {
	engine.indexer.IndexDoc(doc)
}

//BatchIndexDoc 往索引上添加一批doc。同步执行
func (engine *IndexEngine) BatchIndexDoc(docs []*types.DocInfo) {
	engine.indexer.BatchIndexDoc(docs)
}

//RemoveDoc 从索引上删除doc。异步执行的，不会立即生效
func (engine *IndexEngine) RemoveDoc(docId uint32) {
	engine.indexer.RemoveDoc(docId)
}

//Search 根据条件去索引上搜索doc
func (engine *IndexEngine) Search(request types.SearchRequest) types.SearchResp {
	if !engine.distributedMode {
		//单机模式，在本机执行搜索
		return distributed.Search(&request, &engine.indexer)
	} else {
		//分布式，在集群上执行搜索
		return engine.van.Search(&request, &engine.indexer)
	}
}

//IterIndex 遍历索引里的数据
func (engine *IndexEngine) IterIndex(fn func(docId uint32, docInfo types.DocInfo) error) {
	engine.indexer.IterIndex(fn)
}

//IterKeyOnIndex 遍历索引里的key
func (engine *IndexEngine) IterKeyOnIndex(fn func(docId uint32) error) error {
	return engine.indexer.IterKeyOnIndex(fn)
}

//GetDoc 根据ID获取一个doc的详情。flag=1仅获取detail，flag=0都获取
func (engine *IndexEngine) GetDoc(docId uint32, flag int) *types.DocInfo {
	return engine.indexer.GetDoc(docId, flag)
}

//GetDocs 根据ID获取一批doc的详情
func (engine *IndexEngine) GetDocs(docIds []uint32) []*types.DocInfo {
	return engine.indexer.GetDocs(docIds)
}

//LoadDataFromDb2InvertIndex 从本地db加载数据，加到倒排索引中。仅支持在系统启动时一次性调用
func (engine *IndexEngine) LoadDataFromDb2InvertIndex() int {
	return engine.indexer.LoadDataFromDb2InvertIndex()
}

//QuickLoadDataFromDb2InvertIndex 从本地db加载数据，加到倒排索引中。仅支持在系统启动时一次性调用
func (engine *IndexEngine) QuickLoadDataFromDb2InvertIndex() int {
	return engine.indexer.QuickLoadDataFromDb2InvertIndex()
}

//DocNum 索引上的文档总数。若indexer.initOptions.PreRankCriteria为nil，则返回-1
func (engine *IndexEngine) DocNum() int {
	return engine.indexer.DocNum()
}

//DeleteFromInvertIndex 从倒排索引的一个key上把指定docID删掉。这是异步执行的，积累到一定量才会真正执行。彻底执行请调用FlushDeleteOnInvertedIndex
func (engine *IndexEngine) DeleteFromInvertIndex(keyword *types.Keyword, docID uint32) {
	engine.indexer.AddDeletePair(docID, keyword)
}

//AddToInvertIndex 向倒排上添加。立即执行
func (engine *IndexEngine) AddToInvertIndex(keywords []*types.Keyword, doc *types.DocInfo) {
	engine.indexer.AddToInvertIndex(keywords, doc)
}

//AddToLocalDB 向正排上添加。立即执行
func (engine *IndexEngine) AddToLocalDB(doc *types.DocInfo) {
	engine.indexer.AddToLocalDB(doc)
}

//FlushDeleteOnInvertedIndex 需要从倒排索引上删除的docID，彻底将其删除
func (engine *IndexEngine) FlushDeleteOnInvertedIndex() {
	engine.indexer.CleanToBeDeleteBuffer()
}

//GetLocalDbPath 获取local db的路径
func (engine *IndexEngine) GetLocalDbPath() string {
	return engine.indexer.GetDbPath()
}

//InvertIndexStatistics 统计倒排索引上每个关键词下面有几个docID
func (engine *IndexEngine) InvertIndexStatistics(outfile string) int {
	return engine.indexer.InvertIndexStatistics(outfile)
}

//ListenInnerRequest 开始接收集群内部其他server的检索请求
func (engine *IndexEngine) ListenInnerRequest() {
	if engine.distributedMode {
		engine.van.ReceiveInnerRequest(&engine.indexer, &engine.stopChanSig)
	}
}

//GetInvertListLen 获取倒排链上有几个docid
func (engine *IndexEngine) GetInvertListLen(keyword *types.Keyword, onFlag uint32, offFlag uint32, orFlags []uint32) int32 {
	if !engine.distributedMode {
		//单机模式，在本机上计数
		resp := distributed.GetInvertListLen(keyword, onFlag, offFlag, orFlags, &engine.indexer)
		return resp.Total
	} else {
		//分布式，各台worker上的倒排链求和
		resp := engine.van.GetInvertListLen(keyword, onFlag, offFlag, orFlags, &engine.indexer)
		return resp.Total
	}
}