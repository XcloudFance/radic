package distributed

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pebbe/zmq4"
	"os"
	"radic/inverted_index"
	"radic/types"
	"radic/util"
	"sync"
	"sync/atomic"
	"time"
)

//请求消息的类型
const (
	SEARCH_REQUEST  = iota //搜索请求
	GET_INVERT_LIST        //查询某个倒排链上有几个docid
)

var (
	SelfHost string
	vanOnce  sync.Once
	van      *Van
)

func init() {
	SelfHost = util.GetSelfHost()
}

type Van struct {
	Balancer   *Balancer
	listenOnce sync.Once
}

func ping(server string, searchRequest *types.SearchRequest, port int) bool {
	req, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		util.Log.Printf("could not create socket: %v", err)
		return false
	}
	defer req.Close()

	var requestB []byte
	if requestB, err = proto.Marshal(searchRequest); err != nil { //序列化request
		util.Log.Printf("marshal request by proto failed: %v", err)
		return false
	}
	err = req.Connect(fmt.Sprintf("tcp://%s:%d", server, port)) //建立TCP连接
	if err != nil {
		util.Log.Printf("could not dial port %s: %v", server, err)
		return false
	}
	_, err = req.SendBytes(requestB, 0) //发送请求
	if err != nil {
		util.Log.Printf("could not send search request: %v", err)
		return false
	}
	if _, err = req.RecvBytes(0); err != nil { //接收response
		util.Log.Printf("could not receive search response: %v", err)
		return false
	}

	return true
}

//GetVanInstance 单例
func GetVanInstance(opt types.DistOpts) *Van {
	vanOnce.Do(func() {
		if van == nil {
			balancer := NewBalancer(opt)
			if balancer != nil {
				van = &Van{
					Balancer: balancer,
				}
			}
		}
	})
	return van
}

//Search 在index集群上执行搜索，合并所有的结果（结果无序）
func (van *Van) Search(request *types.SearchRequest, indexer *inverted_index.Indexer) types.SearchResp {
	t0 := time.Now()
	resp := types.SearchResp{}
	var err error
	var requestB []byte
	const INNER_TIME_OUT_DELTA int32 = 50
	request.Timeout -= INNER_TIME_OUT_DELTA
	if requestB, err = proto.Marshal(request); err != nil { //序列化request
		util.Log.Printf("marshal request by proto failed: %v", err)
		return resp
	}
	//van.metric.UseTime("search", time.Since(t0), map[string]string{"step": "ser_request"})
	GroupCount := van.Balancer.GetGroupCount()
	const MAX_RESULT int32 = 100000 //最多返回10万个结果
	var resultCnt int32 = 0
	var total int32 = 0
	result := make(chan []byte, MAX_RESULT)
	wg := sync.WaitGroup{}
	wg.Add(GroupCount)

	//并行去请求各个group。各group的响应延迟差异很大，有时候会差一倍
	for i := 0; i < GroupCount; i++ {
		go func(i int) {
			defer wg.Done()
			var server string
			var respMsg types.SearchResp
			t2 := time.Now()
			//如果是自己所在的组，则自己去执行搜索
			if idx, exists := van.Balancer.serverGroupIndex[SelfHost]; exists && idx == i {
				if request.RequestType == GET_INVERT_LIST {
					if len(request.Must) > 0 {
						respMsg = GetInvertListLen(request.Must[0], request.OnFlag, request.OffFlag, request.OrFlags, indexer)
					} else {
						respMsg = GetInvertListLen(nil, request.OnFlag, request.OffFlag, request.OrFlags, indexer)
					}
				} else if request.RequestType == SEARCH_REQUEST {
					respMsg = Search(request, indexer)
					server = util.GetSelfHost()
				}
			} else {
				server = van.Balancer.ChooseServerFromGroup(i) //从group内选择一台server
				if server == "" {
					util.Log.Printf("index group %d all dead", i)
					return
				}
				socket := van.Balancer.GetSocketClient(server)
				if socket == nil {
					util.Log.Printf("could not get socket to %s", server)
					return
				}
				//send是异步执行的，即使没有server端，send函数也会立即返回，并且没有err
				_, err = socket.SendBytes(requestB, 0) //发送请求，默认情况下ZeroMQ对消息体的大小不做限制。如果连续2次send会报错：Operation cannot be accomplished in current state
				if err != nil {
					util.Log.Printf("could not send search request to server %s: %v", server, err)
					//如果send/recv失败，则不归还socket，让连接池新创建一个
					if err := socket.Close(); err != nil {
						util.Log.Printf("close socket to %s failed: %v", server, err)
					}
					van.Balancer.AddSocketClient(server)
					return
				}
				t5 := time.Now()
				util.Log.Printf("send search request to server %s at %d", server, t5.UnixNano())
				var respB []byte
				if respB, err = socket.RecvBytes(0); err != nil { //接收response，如果超时会报错：resource temporarily unavailable
					util.Log.Printf("could not receive search response from %s: %v", server, err)
					//如果send/recv失败，则不归还socket，让连接池新创建一个
					if err := socket.Close(); err != nil {
						util.Log.Printf("close socket to %s failed: %v", server, err)
					}
					van.Balancer.AddSocketClient(server)
					respMsg.Timeout = true
					return
				}
				util.Log.Printf("receive search response from server %s at %d", server, time.Now().UnixNano())
				//归还socket
				van.Balancer.ReturnSocketClient(server, socket)
				if len(respB) > 0 && proto.Unmarshal(respB, &respMsg) == nil { //反序列化response
					if respMsg.Timeout {
						util.Log.Printf("server %s timeout", server)
					}
				} else {
					util.Log.Printf("unmarshal search response from proto failed: %v, response bytes length %d", err, len(respB))
					return
				}
			}
			if respMsg.Docs != nil {
				util.Log.Printf("receive %d docs from server %s, use time %d ms", len(respMsg.Docs), server,
					time.Since(t2).Nanoseconds()/1e6)
			} else if request.RequestType == SEARCH_REQUEST {
				util.Log.Printf("receive 0 docs from server %s, use time %d ms", server, time.Since(t2).Nanoseconds()/1e6)
			}
			if respMsg.Timeout {
				resp.Timeout = true
			}
			atomic.AddInt32(&total, respMsg.Total)
			if respMsg.Docs != nil {
				for _, ele := range respMsg.Docs {
					if atomic.AddInt32(&resultCnt, 1) > MAX_RESULT {
						break
					}
					result <- ele
				}
			}
		}(i)
	}
	wg.Wait()
	close(result)

	request.Timeout += INNER_TIME_OUT_DELTA
	docs := make([][]byte, 0, len(result))
	for ele := range result {
		docs = append(docs, ele)
	}
	resp.Docs = docs
	resp.Total = atomic.LoadInt32(&total) //Total通常比len(Docs)要大
	totalUseTime := time.Since(t0)
	util.Log.Printf("search total use time %d miliseconds at %d", totalUseTime.Nanoseconds()/1e6, time.Now().UnixNano())
	return resp
}

//GetInvertListLen
func (van *Van) GetInvertListLen(keyword *types.Keyword, onFlag uint32, offFlag uint32, orFlags []uint32, indexer *inverted_index.Indexer) types.SearchResp {
	request := &types.SearchRequest{Must: []*types.Keyword{keyword}, RequestType: GET_INVERT_LIST, OnFlag: onFlag, OffFlag: offFlag, OrFlags: orFlags}
	return van.Search(request, indexer)
}

//ReceiveInnerRequest 监听端口，接收处理所有的搜索请求。调用时需要放到子协程里执行
func (van *Van) ReceiveInnerRequest(indexer *inverted_index.Indexer, stopSig *chan os.Signal) {
	van.listenOnce.Do(func() {
		go func() {
			context, _ := zmq4.NewContext()
			context.SetIoThreads(4) //从网络端口接收到的所有数据都会暂存在缓存中，recv
			// 不停地从缓存中取数据。所以一个线程也可以处理所有的请求，为提高响应速度，也可以设置多个线程
			socket, err := context.NewSocket(zmq4.REP) //创建的socket不是线程安全的，不能在多协程中共享
			if err != nil {
				util.Log.Printf("could not create socket: %v", err)
				return
			}
			defer context.Term()
			defer socket.Close()

			err = socket.Bind(fmt.Sprintf("tcp://*:%d", van.Balancer.port)) //监听端口
			if err != nil {
				util.Log.Printf("could not dial port %d: %v", van.Balancer.port, err)
				return
			}
			util.Log.Printf("bind on port %d to listen inner request", van.Balancer.port)

			for {
				select {
				case sig := <-*stopSig:
					util.Log.Printf("receive stop signal %v, close port %d", sig, van.Balancer.port)
					return
				default:
					if b, err := socket.RecvBytes(0); err == nil { //如果连续Recv会报错： Operation cannot be accomplished in current state
						if len(b) == 0 { //发送0字节是在心跳试探
							socket.SendBytes([]byte{}, 0)
						} else {
							t1 := time.Now()
							util.Log.Printf("receive request at %d", t1.UnixNano())
							var request types.SearchRequest
							if proto.Unmarshal(b, &request); err == nil { //反序列化请求
								var resp types.SearchResp
								if request.RequestType == SEARCH_REQUEST {
									util.Log.Printf("unarshal search request")
									resp = Search(&request, indexer) //执行请求，得到response
								} else if request.RequestType == GET_INVERT_LIST {
									if len(request.Must) > 0 {
										resp = GetInvertListLen(request.Must[0], request.OnFlag, request.OffFlag, request.OrFlags, indexer)
									} else {
										resp = GetInvertListLen(nil, request.OnFlag, request.OffFlag, request.OrFlags, indexer)
									}
								}
								if respB, err := proto.Marshal(&resp); err == nil { //序列化response
									if _, err = socket.SendBytes(respB, 0); err != nil { //返回response
										util.Log.Printf("send response failed: %v", err)
									} else {
										if resp.Docs != nil {
											util.Log.Printf("send search response at %d, use time %d ms, timeout %t, " +
												"%d docs, request type %d", time.Now().UnixNano(), time.Since(t1).Nanoseconds()/1e6, resp.Timeout, len(resp.Docs), request.RequestType)
										} else {
											if len(request.Must) > 0 {
												util.Log.Printf("send search response at %d, use time %d ms, timeout %t, " +
													"0 docs, request type %d, must %s", time.Now().UnixNano(), time.Since(t1).Nanoseconds()/1e6, resp.Timeout, request.RequestType, request.Must[0].ToString())
											} else {
												util.Log.Printf("send search response at %d, use time %d ms, timeout %t, " +
													"0 docs, request type %d, no must", time.Now().UnixNano(), time.Since(t1).Nanoseconds()/1e6, resp.Timeout, request.RequestType)
											}
										}
									}
								} else {
									util.Log.Printf("marshal search response failed: %v", err)
									socket.SendBytes([]byte{}, 0) //发生异常一定要Send，否则就会造成连续2次Recv
								}
							} else {
								util.Log.Printf("unmarshal search request from proto failed: %v", err)
								socket.SendBytes([]byte{}, 0)
							}
						}
					} else {
						util.Log.Printf("receive search request failed: %v", err)
						if err.Error() == "Operation cannot be accomplished in current state" {
							socket.SendBytes([]byte{}, 0)
						}
					}
				}
			}
		}()
	})
}

//Search 本地执行搜索
func Search(request *types.SearchRequest, indexer *inverted_index.Indexer) types.SearchResp {
	must := request.Must
	should := request.Should
	not := request.Not
	orderLess := request.Orderless
	offset := request.OutputOffset
	var timeout int32 = 2000 //默认超时2秒
	if request.Timeout > 0 {
		timeout = request.Timeout //以request.Timeout为准
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	defer cancel()
	result := make(chan []*types.DocInfo, 1)
	go func() {
		result <- indexer.Lookup(must, should, not, orderLess, request.FilterIds, request.OnFlag, request.OffFlag, request.OrFlags)
	}()

	resp := types.SearchResp{}
	select {
	case docInfos := <-result:
		resp.Total = int32(len(docInfos))
		if !request.CountDocsOnly && resp.Total > offset {
			count := resp.Total - offset
			docs := make([][]byte, count)
			for i := offset; i < offset+count; i++ {
				docs[i] = docInfos[i].Entry
			}
			resp.Docs = docs
		}
	case <-ctx.Done():
		resp.Timeout = true
	}

	return resp
}

func GetInvertListLen(keyword *types.Keyword, onFlag uint32, offFlag uint32, orFlags []uint32, 
	indexer *inverted_index.Indexer) types.SearchResp {
	var count int32 = 0
	if keyword != nil && len(keyword.Word) > 0 {
		count = indexer.GetInvertListLen(keyword, onFlag, offFlag, orFlags)
	}
	return types.SearchResp{Total: count}
}
