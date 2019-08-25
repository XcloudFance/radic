package distributed

import (
	"github.com/pebbe/zmq4"
	"github.com/robfig/cron"
	"math"
	"math/rand"
	"radic/types"
	"radic/util"
	"sync/atomic"
)

//Balancer 基于最小并发数做负载均衡
type Balancer struct {
	groups             [][]string     //每个组内有哪些机器
	serverIndex        map[string]int //每台机器的编号
	serverGroupIndex   map[string]int //每台机器属于第几组
	concurrency        []int32        //每台机器的并发调用数，机器编号与serverIndex对应
	dead               []bool         //记录每台机器是否已死
	port               int
	socketPoolToServer map[string]*util.SocketPool
}

func NewBalancer(opt types.DistOpts) *Balancer {
	if len(opt.Servers) < 2 || opt.Groups < 2 {
		return nil
	}

	balancer := &Balancer{
		groups:             make([][]string, opt.Groups),
		serverIndex:        make(map[string]int),
		serverGroupIndex:   make(map[string]int),
		concurrency:        make([]int32, len(opt.Servers)),
		dead:               make([]bool, len(opt.Servers)),
		port:               opt.Port,
		socketPoolToServer: make(map[string]*util.SocketPool),
	}
	for i := 0; i < opt.Groups; i++ {
		balancer.groups[i] = []string{}
	}

	groupSize := len(opt.Servers) / opt.Groups
	for i, server := range opt.Servers {
		balancer.serverIndex[server] = i
		groupIndex := i / groupSize
		balancer.serverGroupIndex[server] = groupIndex
		balancer.groups[groupIndex] = append(balancer.groups[groupIndex], server)
		if SelfHost != server {
			balancer.socketPoolToServer[server] = util.NewSocketPool(server, opt.Port, 40) //对每台server创建40个连接
		}
	}

	scheduler := cron.New()
	scheduler.AddFunc("0 */3 * * * *", balancer.recycle) //每隔3分钟执行一次recycle
	scheduler.Start()

	return balancer
}

func (balancer *Balancer) recycle() {
	for server, idx := range balancer.serverIndex {
		if balancer.dead[idx] {
			if balancer.socketPoolToServer[server].Recycle() > 0 {
				atomic.StoreInt32(&balancer.concurrency[idx], 0)
				balancer.dead[idx] = false
				util.Log.Printf("recycle server %s", server)
			}
		}
	}
}

func (balancer *Balancer) GetGroupCount() int {
	return len(balancer.groups)
}

//incConcurrency 发送一次请求，并发数加1
func (balancer *Balancer) incConcurrency(server string) {
	if idx, exists := balancer.serverIndex[server]; exists {
		atomic.AddInt32(&balancer.concurrency[idx], 1)
	}
}

//decConcurrency 正常接收到响应，并发数减1
func (balancer *Balancer) decConcurrency(server string) {
	if idx, exists := balancer.serverIndex[server]; exists {
		if atomic.LoadInt32(&balancer.concurrency[idx]) <= 1 {
			//确保不会减成负数。由于recycle()会强制清0，所以concurrency很有可能会减成负数。
			atomic.StoreInt32(&balancer.concurrency[idx], 0)
		} else {
			atomic.AddInt32(&balancer.concurrency[idx], -1)
		}
	}
}

//ChooseServerFromGroup 从一个组内选择并发数最小的那台server。同一组内如果所有server都宕机，则仍然会选中一台；如果至少存在一台未宕机，那宕机的server就不会再被选中。
func (balancer *Balancer) ChooseServerFromGroup(groupIndex int) string {
	rect := ""
	if balancer.groups == nil || len(balancer.groups) < groupIndex {
		return rect
	}
	servers := balancer.groups[groupIndex]
	minConcurrency := int32(math.MaxInt32)
	for _, server := range servers {
		if idx, exists := balancer.serverIndex[server]; exists {
			//如果组内server全是dead，则最终rect==""
			if !balancer.dead[idx] {
				if balancer.concurrency[idx] < minConcurrency {
					minConcurrency = balancer.concurrency[idx]
					rect = server
				} else if balancer.concurrency[idx] == minConcurrency {
					if rand.Int()%2 == 0 { //如果都是minConcurrency，则随机选择一台
						rect = server
					}
				}
			}
		} else {
			util.Log.Printf("could not get index of server %s\n", server)
		}
	}
	return rect
}

func (balancer *Balancer) GetSocketClient(server string) *zmq4.Socket {
	if socketPool, exists := balancer.socketPoolToServer[server]; exists {
		if socketPool != nil {
			socket := socketPool.TakeSocket()
			balancer.incConcurrency(server)
			if socket == nil {
				if idx, ok := balancer.serverIndex[server]; ok {
					balancer.dead[idx] = true //没有可用socket，置为dead
				}
			} else {
				util.Log.Printf("take socket to %s", server)
			}
			return socket
		}
	}
	return nil
}

func (balancer *Balancer) ReturnSocketClient(server string, socket *zmq4.Socket) {
	if socketPool, exists := balancer.socketPoolToServer[server]; exists {
		if socketPool != nil {
			socketPool.ReturnSocket(socket)
			//请求未超时会调用ReturnSocketClient，并发度减1
			balancer.decConcurrency(server)
		} else {
			util.Log.Printf("socket pool to %s is nil", server)
		}
	} else {
		util.Log.Printf("can not find server %s", server)
	}
}

func (balancer *Balancer) AddSocketClient(server string) {
	if socketPool, exists := balancer.socketPoolToServer[server]; exists {
		if socketPool != nil {
			socketPool.Add(1)
			//请求超时会调用AddSocketClient，并发度不减1
		} else {
			util.Log.Printf("socket pool to %s is nil", server)
		}
	} else {
		util.Log.Printf("can not find server %s", server)
	}
}

//ChooseGroup 根据docid找到它应该属于哪个组
func (balancer *Balancer) ChooseGroup(docid uint32) int {
	if docid <= 0 {
		return -1
	}
	return int(docid) % len(balancer.groups) //这种简单取模法把1亿用户分得还是非常均匀的，比AB测试使用的分流算法还均匀
}

//GetServersOfGroup 获取组中有哪些server
func (balancer *Balancer) GetServersOfGroup(groupIndex int) []string {
	if balancer.groups == nil || len(balancer.groups) < groupIndex {
		return []string{}
	}
	return balancer.groups[groupIndex]
}

//GetSelfGroupIndex 自己属于第几组。可能返回-1
func (balancer *Balancer) GetSelfGroupIndex() int {
	if i, exists := balancer.serverGroupIndex[SelfHost]; exists {
		return i
	}
	return -1
}

//BelongSelfGroup docid需要由本组来存储
func (balancer *Balancer) BelongSelfGroup(docid uint32) bool {
	gidx := balancer.ChooseGroup(docid)
	if gidx >= 0 {
		selfGidx := balancer.GetSelfGroupIndex()
		if selfGidx == gidx {
			return true
		}
	}
	return false
}

func (balancer *Balancer) Destroy() {
	for _, socketPool := range balancer.socketPoolToServer {
		if socketPool != nil {
			socketPool.Destroy()
		}
	}
}
