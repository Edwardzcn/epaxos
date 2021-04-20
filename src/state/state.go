package state

import (
	"fmt"
	"sync"
	//"code.google.com/p/leveldb-go/leveldb"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	WLOCK
)

type Value int64

const NIL Value = 0

type Key int64

// 操作KV的命令接口
type Command struct {
	Op Operation
	K  Key
	V  Value
}

type State struct {
	mutex *sync.Mutex
	// Store map[Key]Value
	//DB *leveldb.DB
	DB *leveldb.DB
}

func InitState() *State {
	/*
	   d, err := leveldb.Open("/Users/iulian/git/epaxos-batching/dpaxos/bin/db", nil)

	   if err != nil {
	       fmt.Printf("Leveldb open failed: %v\n", err)
	   }

	   return &State{d}
	*/
	//    TODO 每个状态保存在本地，如果单机模拟的话需要各自找一个路径
	db, err := leveldb.OpenFile("/home/edwardzcn/epaxos/leveldb-data", nil)
	if err != nil {
		fmt.Printf("Leveldb open failed: %v \n",err)
	}

	// return &State{new(sync.Mutex), make(map[Key]Value), db}
	return &State{new(sync.Mutex), db}
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func IsRead(command *Command) bool {
	return command.Op == GET
}


// 底层操作的 Execute 是针对一个特定的状态来界定的
func (c *Command) Execute(st *State) Value {
	// debug
	switch c.Op {
	case PUT:
		fmt.Printf("Try Execute Command: PUT (%d, %d)\n", c.K, c.V)
	case GET:
		fmt.Printf("Try Execute Command: GET %d",c.K)
	}

	//var key, value [8]byte
	
	//    st.mutex.Lock()
	//    defer st.mutex.Unlock()
	
	switch c.Op {
	case PUT:
		/*
		binary.LittleEndian.PutUint64(key[:], uint64(c.K))
		binary.LittleEndian.PutUint64(value[:], uint64(c.V))
		st.DB.Set(key[:], value[:], nil)
		*/
		
		// st.Store[c.K] = c.V
		// Convert an int64 into a byte array in go
		// https://stackoverflow.com/questions/35371385/how-can-i-convert-an-int64-into-a-byte-array-in-go
		
		// 由于 Command 中 key value的值是 int64 格式 需要做 transfer
		// var key,val [8]byte
		// binary.LittleEndian.PutUint64(key[:], uint64(c.K))
		// binary.LittleEndian.PutUint64(val[:], uint64(c.V))
		// 使用make来获得切片
		key := make([]byte, 8)
		val := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(c.K))
		binary.LittleEndian.PutUint64(val, uint64(c.V))
		st.DB.Put(key,val, nil)
		fmt.Printf("PUT (%d,%d)\n",c.K,c.V)
		
		return c.V
		
	case GET:
		// if val, present := st.Store[c.K]; present {
		// 	return val
		// }
		key := make([]byte, 8)
		// val := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(c.K))
		if flg,_ := st.DB.Has(key,nil); flg{
			val, _ := st.DB.Get(key,nil)
			fmt.Printf("GET SUCCESS (%d,%d)\n",key,val)
			// Value 相当于 int64转化
			return Value(binary.LittleEndian.Uint64(val))
		} else {
			// TODO 没有该键值的
			fmt.Printf("GET NOT FOUND\n")
			return -1
		}
	}

	return NIL
}
