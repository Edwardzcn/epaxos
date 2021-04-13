package main

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	db, err := leveldb.OpenFile("/home/edwardzcn/epaxos/leveldb-data/", nil)

	if err != nil {
		fmt.Printf("Leveldb open FAIL!\nError:%v",err)
	} else {
		fmt.Printf("Leveldb open SUCCESS!\n")
	}
	iter := db.NewIterator(nil,nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		// debug
		fmt.Printf("Find k=%s\tv=%s\n",key,value)
	}
	iter.Release()
	
	// for true {
	// 	iter_over_db(db)
	// }
	
	defer db.Close()
}

func iter_over_db(db *leveldb.DB) {
	iter := db.NewIterator(nil,nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		// debug
		fmt.Printf("Find k=%s\tv=%s\n",key,value)
	}
	iter.Release()
}
