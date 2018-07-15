package main

import (
	"sync"
	"math/rand"
	"fmt"
	"time"
	"log"
	"net/rpc"
	"net/http"
)

//设置节点的个数为3
const  raftCount  =3

//声明leader存储对象AppendEntiresArgs
type AppendEntiresArgs struct {
	//声明第几任领导
	Term int
	//设置领导编号
	LeaderId int
}

//创建一个存储领导对象
var args = AppendEntiresArgs{0,-1}


//声明节点类型
type Raft struct {
	//线程所
	mu sync.Mutex
	//节点编号
	me int
	//当前领导期数
	currentTerm int
	//为哪个节点投票
	votedFor int
	//当前节点状态
	state int // 0 follower ,1 candidate ,2 leader
	//最后后一天发送消息的时间
	lastMessageTime int64
	//设置当前节点的领导
	currentLeader int

	//节点间发送消息的通道
	message chan  bool
	//选举通道
	eclectCh chan bool
	//心跳信号通道
	heartBeat chan  bool
	//返回心跳信号
	heartbeatRe chan  bool
	//超时时间
	timeout int

}

//产生随机值
func randRange(min ,max int64) int64 {
	return rand.Int63n(max-min) +min
}

//打印当前时间
func printTime() {
	fmt.Println(time.Now())
}

//获取当前时间的ms
func millisecond() int64 {
	return  time.Now().UnixNano()/int64(time.Millisecond)
}


func main() {
	//创建三个节点
	for i:=0;i<raftCount;i++ {
		//创建三个Raft
		Make(i)
	}

	//对raft结构体实现rpc注册
	rpc.Register(new(Raft))
	rpc.HandleHTTP()
	err:=http.ListenAndServe(":6000",nil )
	if err !=nil {
		log.Fatal(err)
	}

	for {;}
}


//通过Make函数创建Raft节点对象

func Make(me int ) *Raft {
	rf:=&Raft{}
	rf.me = me
	rf.votedFor = -1
	rf.state = 0
	rf.timeout = 0
	rf.currentLeader = -1
	rf.setTerm(0)
	//创建通道对象
	rf.eclectCh = make(chan  bool)
	rf.message = make(chan bool)
	rf.heartBeat = make(chan bool)
	rf.heartbeatRe = make(chan bool)
	rand.Seed(time.Now().UnixNano())


	go rf.election()
	go rf.sendLeaderHeartBeat()

	return rf

}

//创建Raft对象的方法
func (rf *Raft)setTerm(term int) {
	rf.currentTerm = term
}



//设置leader节点发送心跳信号的方法
func(rf *Raft)sendLeaderHeartBeat() {
	for {
		select {
		//其他节点接收到leader发射的心跳信号
		case <-rf.heartBeat:
			rf.sendAppendEntriesImpl()
		}
	}

}

//返回给leader确认信号
func (rf *Raft)sendAppendEntriesImpl() {
	if rf.currentLeader == rf.me {
		//记录返回确认信号的节点个数
		var success_cout int =0

		//设置返回确认信号的子节点
		for i:=0;i<raftCount;i++ {
			if i!=rf.me {
				go func() {
					//rf.heartbeatRe<-true
					//将返回信号发送给leader
					rp,err:=rpc.DialHTTP("tcp","127.0.0.1:6000")
					if err!=nil {
						log.Fatal(err)
					}
					//接收服务器返回的信息
					var ok bool= false

					er:=rp.Call("Raft.Communication",Param{"abc"},&ok)
					if er!=nil {
						log.Fatal(er)
					}

					if ok {
						rf.heartbeatRe <-true
					}

				}()
			}
		}

		//计算返回确信信号子节点的个数>raftCount/2，则校验成功
		for i:=0;i<raftCount-1;i++{
			select {
			case ok :=<-rf.heartbeatRe:
				if ok {
					success_cout++
					if success_cout>raftCount/2 {
						fmt.Println("leader是：",rf.me)
						fmt.Println("成功！leader收到多数节点的心跳返回")
						//结束程序进程
						log.Fatal("over")
					}
				}
			}
		}

	}

}

//设置节点投票的方法
func(rf *Raft)election() {
	//设置标签
	var result bool
	for {
		//设置超时时间
		timeout := randRange(150,300)
		//设置每个节点的时间
		rf.lastMessageTime = millisecond()
		select {
		//延时等待
		case <-time.After(time.Duration(timeout)*time.Millisecond):
			//打印当前节点的状态
			fmt.Println("节点状态为：",rf.state)
		}
		result = false
		for !result {
			//选择谁最为leader
			result = rf.election_one_round(&args)
		}
	}
}

//选择谁为leader
func(rf *Raft)election_one_round(args *AppendEntiresArgs) bool {
	var timeout int64
	var done int
	//是否开始心跳信号的产生
	var triggerHeartbeat bool
	last := millisecond()
	timeout = 100
	//标签
	success := false

	//修改当前Raft的状态为candidate状态
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	//开始选取leader
	fmt.Println("start electing leader")
	for {
		for i:=0;i<raftCount;i++ {
			if i != rf.me {
				//拉选票
				printTime()
				go func() {
					if args.LeaderId<0 {
						//设置此节点开始选举
						rf.eclectCh <-true
					}
				}()
			}
		}
		//设置投票数量
		done = 0
		triggerHeartbeat = false
		for i:=0;i<raftCount-1;i++ {
			//计算投票数量
			select {
			case ok:=<-rf.eclectCh:
				if ok {
					done++
					success = done> raftCount/2

					if success && !triggerHeartbeat {
						//选举成功了
						triggerHeartbeat = true
						rf.mu.Lock()
						rf.becomeLeader()
						rf.mu.Unlock()
						//由leader向其他节点发送心跳信号
						rf.heartBeat <-true
						fmt.Println("竞选节点发送心跳信号")

					}
				}
			}
		}

		if (timeout+last <millisecond()|| (done >= raftCount/2 || rf.currentLeader > -1)){
			fmt.Println("退出选举时，leader节点是：",rf.me)
			break
		} else {
			fmt.Println("延时操作")
			select {
			case <-time.After(time.Duration(10) * time.Millisecond):
			}
		}

	}
	return  success
}

func(rf *Raft)becomeLeader() {
	rf.state= 2
	rf.currentLeader = rf.me
}


func(rf *Raft)becomeCandidate() {
	rf.state = 1
	rf.setTerm(rf.currentTerm +1)
	rf.votedFor= rf.me
	rf.currentLeader = -1
}

//分布式通信
type Param struct {
	Msg string
}
//等待客户端消息
func(r *Raft)Communication(p Param, a *bool)error {
	fmt.Println(p.Msg)
	*a = true
	return  nil
}



