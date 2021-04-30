package main

import (
	"bufio"
	"dlog"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"math/rand"
	"net"
	"net/rpc"
	"runtime"
	"state"
	"time"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")

// 请求总量
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")

// noLeader 配置
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")

// 将全部请求分割为多轮
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", -1, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")

var N int

var successful []int

var rarray []int
var rsp []bool

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	randObj := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNb / *rounds + *eps))

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	// debug
	fmt.Printf("N=%d\n", N)

	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	rarray = make([]int, *reqsNb / *rounds + *eps)
	karray := make([]int64, *reqsNb / *rounds + *eps)
	put := make([]bool, *reqsNb / *rounds + *eps)
	perReplicaCount := make([]int, N)
	test := make([]int, *reqsNb / *rounds + *eps)
	for i := 0; i < len(rarray); i++ {
		r := rand.Intn(N)
		rarray[i] = r
		if i < *reqsNb / *rounds {
			perReplicaCount[r]++
		}

		if *conflicts >= 0 {
			r = rand.Intn(100)
			if r < *conflicts {
				karray[i] = 42
			} else {
				karray[i] = int64(43 + i)
			}
			r = rand.Intn(100)
			// debug
			fmt.Printf("random r=%d and writes=%d\n", r, writes)
			if r < *writes {
				put[i] = true
			} else {
				put[i] = false
			}
		} else {
			karray[i] = int64(zipf.Uint64())
			test[karray[i]]++
		}
	}
	if *conflicts >= 0 {
		fmt.Println("Uniform distribution")
	} else {
		fmt.Println("Zipfian distribution:")
		//fmt.Println(test[0:100])
	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
	}

	successful = make([]int, N)
	leader := 0

	if *noLeader == false {
		reply := new(masterproto.GetLeaderReply)
		if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
			log.Fatalf("Error making the GetLeader RPC\n")
		}
		leader = reply.LeaderId
		log.Printf("The leader is replica %d\n", leader)
	}

	var id int32 = 0
	done := make(chan bool, N)
	// 这里是初始化writer 传输的参数? id = 0 ,
	// 重申 Propose 的定义
	// type Propose struct {
	// 	*genericsmrproto.Propose
	// 	Reply *bufio.Writer
	// }
	args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, 0}, 0}

	before_total := time.Now()

	// 轮数循环
	for j := 0; j < *rounds; j++ {
		// 计算每一轮的请求数目
		// TODO 这个是不是可以放在外面？
		n := *reqsNb / *rounds

		// check 参数 用来检测 每个reply 是否恰有一次  这里初始化记录数字
		if *check {
			rsp = make([]bool, n)
			for j := 0; j < n; j++ {
				rsp[j] = false
			}
		}

		// TODO 需要搞明白 waitReplies 的功能
		// wairtReplies 等待接收 ProposeReplyTS 结构体
		if *noLeader {
			// 如果发现没有leader client需要向每一个 replica  waitReplies
			for i := 0; i < N; i++ {
				go waitReplies(readers, i, perReplicaCount[i], done)
			}
		} else {
			// 如果leader 确定  client 只需要向leader 发送 waitReplies
			go waitReplies(readers, leader, n, done)
		}

		before := time.Now()

		for i := 0; i < n+*eps; i++ {
			dlog.Printf("Sending proposal %d\n", id)
			// debug
			// fmt.Printf("Sending proposal %d\n", id)
			args.CommandId = id
			if put[i] {
				args.Command.Op = state.PUT
				// debug
				fmt.Println("Command is PUT")
			} else {
				args.Command.Op = state.GET
				// debug
				// fmt.Println("Command is GET")
			}
			args.Command.K = state.Key(karray[i])
			args.Command.V = state.Value(i)
			//args.Timestamp = time.Now().UnixNano()

			// 这里还是配置参数
			// Fast Paxos: send message directly to all replicas. Defaults to false.
			if !*fast {
				if *noLeader {
					leader = rarray[i]
				}
				// 都是先写一个常量？
				writers[leader].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(writers[leader])
			} else {
				// 为真的情况
				// debug
				fmt.Printf("Fast Paxos debug: %d", *fast)

				//send to everyone
				for rep := 0; rep < N; rep++ {
					writers[rep].WriteByte(genericsmrproto.PROPOSE)
					args.Marshal(writers[rep])
					writers[rep].Flush()
				}
			}
			//fmt.Println("Sent", id)
			id++
			if i%100 == 0 {
				// 内部 i 和 外部 i 指代不同
				// 每100 条 command ，io flush一次
				for i := 0; i < N; i++ {
					writers[i].Flush()
				}
			}
		}
		// Flush 剩余的消息
		for i := 0; i < N; i++ {
			writers[i].Flush()
		}

		// TODO 理解 noLeader 的情况
		err := false
		if *noLeader {
			for i := 0; i < N; i++ {
				e := <-done
				err = e || err
			}
		} else {
			err = <-done
		}

		after := time.Now()

		fmt.Printf("Round took %v\n", after.Sub(before))

		// 这里检测是否有没有接收的 Command
		if *check {
			for j := 0; j < n; j++ {
				if !rsp[j] {
					fmt.Println("Didn't receive", j)
				}
			}
		}

		if err {
			if *noLeader {
				N = N - 1
			} else {
				reply := new(masterproto.GetLeaderReply)
				master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
				leader = reply.LeaderId
				log.Printf("New leader is replica %d\n", leader)
			}
		}
	}

	after_total := time.Now()
	fmt.Printf("Test took %v\n", after_total.Sub(before_total))

	s := 0
	for _, succ := range successful {
		s += succ
	}

	fmt.Printf("Successful: %d\n", s)

	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	master.Close()
}

// waitReplies
func waitReplies(readers []*bufio.Reader, leader int, n int, done chan bool) {
	e := false

	reply := new(genericsmrproto.ProposeReplyTS)
	for i := 0; i < n; i++ {
		// Unmarshal，根据结构体成分进行解码赋值。
		if err := reply.Unmarshal(readers[leader]); err != nil {
			fmt.Println("Error when reading:", err)
			e = true
			continue
		}
		//fmt.Println(reply.Value)
		if *check {
			if rsp[reply.CommandId] {
				// 若 回复的 CommandId 重复， 即说明检测到多份回复
				fmt.Println("Duplicate reply", reply.CommandId)
			}
			rsp[reply.CommandId] = true
		}
		if reply.OK != 0 {
			successful[leader]++
		}
	}
	done <- e
}
