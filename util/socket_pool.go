package util

import (
	"fmt"
	"github.com/oleiade/lane"
	"github.com/pebbe/zmq4"
	"time"
)

const (
	RECEIVE_TIME = 2000 //recv超时为2000毫秒
)

//SocketPool TCP Client连接池
type SocketPool struct {
	host            string
	port            int
	collection      *lane.Deque
	availableCount  int
	lastRecycleTime time.Time
	capacity        int
}

func NewSocketPool(host string, port int, capacity int) *SocketPool {
	pool := &SocketPool{host: host, port: port, capacity: capacity}
	pool.collection = lane.NewCappedDeque(capacity)
	for i := 0; i < capacity; i++ {
		socket := pool.CreateSocket()
		if socket != nil {
			if pool.collection.Append(socket) == false {
				socket.Close()
			}
		}
	}
	fmt.Printf("add %d sockets to %s:%d to poll\n", pool.collection.Size(), host, port)
	return pool
}

func (self *SocketPool) CreateSocket() *zmq4.Socket {
	socket, err := zmq4.NewSocket(zmq4.REQ) //创建的socket不是线程安全的，不能在多协程中共享
	if err != nil {
		Log.Printf("could not create socket: %v", err)
		return nil
	}
	socket.SetRcvtimeo(time.Duration(RECEIVE_TIME) * time.Millisecond) //recv超时为2000毫秒
	if socket.Connect(fmt.Sprintf("tcp://%s:%d", self.host, self.port)); err != nil {
		Log.Printf("could not connect to %s:%d: %v", self.host, self.port, err)
		return nil
	}
	return socket
}

func (self *SocketPool) Add(n int) int {
	success := 0
	for i := 0; i < n; i++ {
		socket := self.CreateSocket()
		if socket != nil {
			if self.collection.Append(socket) == false {
				if err := socket.Close(); err != nil {
					Log.Printf("close socket to %s failed: %v", self.host, err)
				}
				break
			} else {
				success++
			}
		}
	}
	if success < n {
		Log.Printf("add %d sockets to %s:%d to poll, less than %d", success, self.host, self.port, n)
	} else {
		Log.Printf("add %d sockets to %s:%d to poll", success, self.host, self.port)
	}
	return success
}

//TakeSocket 如果server已死则会返回nil。用得好注意Return回来，用得不好就不要Return了
func (self *SocketPool) TakeSocket() *zmq4.Socket {
	socket := self.collection.Shift() //取出队首元素
	if socket == nil {
		time.Sleep(time.Millisecond * 50) //如果取不到socket，则休息50ms后重试一次
		socket := self.collection.Shift()
		if socket == nil {
			if self.Recycle() > 0 {
				socket = self.collection.Shift() //取出队首元素
				if socket == nil {
					Log.Printf("server %s:%d is dead", self.host, self.port)
				}
			}
		}
	}
	if socket == nil {
		return nil
	} else {
		return socket.(*zmq4.Socket)
	}
}

//ReturnSocket 如果使用的过程中有问题就不要return了
func (self *SocketPool) ReturnSocket(socket *zmq4.Socket) {
	if socket != nil {
		//放回队尾
		if self.collection.Append(socket) == false {
			socket.Close()
			Log.Printf("return socket to %s:%d failed, deque is full %d", self.host, self.port, self.collection.Size())
		} else {
			Log.Printf("return socket to %s:%d", self.host, self.port)
		}
	} else {
		Log.Printf("socket to is %s:%d nil", self.host, self.port)
	}
}

//Recycle 填补socket池。返回最终socket池的大小
func (self *SocketPool) Recycle() int {
	if time.Since(self.lastRecycleTime).Seconds() < 0.5 { //如果距上次Recycle()时间间隔比较短，则强行sleep一下
		time.Sleep(time.Millisecond * 100)
	}
	if self.collection.Size() < self.capacity/2 {
		self.lastRecycleTime = time.Now() //把lastRecycleTime置为现在
		self.Add(self.capacity - self.collection.Size())
	}
	return self.collection.Size()
}

//Destroy 断开所有可用连接
func (self *SocketPool) Destroy() {
	if self.collection != nil {
		for {
			if ele := self.collection.Shift(); ele != nil {
				socket := ele.(*zmq4.Socket)
				socket.Close()
			} else {
				break
			}
		}
		Log.Printf("close all socket to %s:%d", self.host, self.port)
	}
}
