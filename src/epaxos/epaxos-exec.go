package epaxos

import (
	//    "state"
	"epaxosproto"
	"genericsmrproto"
	"sort"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

// Note
// 执行结构体，包装Replica 的
type Exec struct {
	r *Replica
}

// Note
// 强连通分量结构体，包装指向实例节点的指针数组，染色标记
type SCComponent struct {
	nodes []*Instance
	color int8
}

// TODO 执行命令
// 这里的Execute和State 里面 Command 的 Execute是什么关系
// 这里  InstanceSpace 所以应该和 EPaxos里面的instance二维表格是对应的
func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == epaxosproto.EXECUTED {
		return true
	}
	if inst.Status != epaxosproto.COMMITTED {
		return false
	}

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

// Note
// 寻找强连通分量
func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

// Tarjan算法
func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			for e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil || v.Cmds == nil {
				time.Sleep(1000 * 1000)
			}
			/*        if !state.Conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
			          continue
			          }
			*/
			if e.r.InstanceSpace[q][i].Status == epaxosproto.EXECUTED {
				continue
			}
			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
				time.Sleep(1000 * 1000)
			}
			w := e.r.InstanceSpace[q][i]

			if w.Index == 0 {
				//e.strongconnect(w, index)
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].Index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else { //if e.inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		// 作者在这里也注明了 执行命令
		sort.Sort(nodeArray(list))
		for _, w := range list {
			// w 是 单个的 Instance
			for w.Cmds == nil {
				// 阻塞等待
				time.Sleep(1000 * 1000)
			}
			for idx := 0; idx < len(w.Cmds); idx++ {
				// 便利 这个instance里面的所有命令
				// 这里的 Execute 对应的是 state.go 中 Commands 的 Execute
				val := w.Cmds[idx].Execute(e.r.State)
				if e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil {
					e.r.ReplyProposeTS(
						&genericsmrproto.ProposeReplyTS{
							TRUE,
							w.lb.clientProposals[idx].CommandId,
							val,
							w.lb.clientProposals[idx].Timestamp},
						w.lb.clientProposals[idx].Reply)
				}
			}
			w.Status = epaxosproto.EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
