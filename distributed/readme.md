# 分布式索引
## 数据切分方式
### 水平切分
![avatar](https://github.com/Orisun/radic/blob/master/img/hslice.png)
优势：遍历单条倒排链快。<br>
劣势：正排冗余存储在多台机器上。
### 垂直切分
![avatar](https://github.com/Orisun/radic/blob/master/img/vslice.png)
优势：某一台宕机后对搜索结果影响不大；每一台的搜索行为与单机索引时完全相同。<br>
劣势：查询速度由最慢（即链最长）的那台机器决定。
## 分布式集群
![avatar](https://github.com/Orisun/radic/blob/master/img/cluster.png)
- 由多个Group垂直切分整个倒排索引，每个Group内有多台Server做冗余备份。
- Server之间使用ØMQ通信，使用socket池以提高性能。
- Group内部使用最小并发度算法做负载均衡。
## 分布式通信--ØMQ
ØMQ的bind、conect、send、recv等操作都是异步的。send把数据写入本地缓存，recv从本地buffer读数据。所以即使server端还没有执行bind，client端的connect和send也可以正常返回。<br>
一个socket可以同时建立多个连接。<br>
通过后台的I/O线程进行消息传输，一个I/O线程已经足以处理多个套接字的数据传输要求。<br>
REQ-REP模式中send和recv必须交替进行，否则会报错： Operation cannot be accomplished in current state<br>
send和recv如果超时会报错： resource temporarily unavailable
