package main

import (
	"fmt"
	// "encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	db, err := leveldb.OpenFile("/home/edwardzcn/epaxos/leveldb-data/", nil)

	if err != nil {
		fmt.Printf("Leveldb open FAIL!\nError:%v", err)
	} else {
		fmt.Printf("Leveldb open SUCCESS!\n")
	}
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		// debug
		fmt.Printf("Find k=%s\tv=%s\n", key, value)
	}
	iter.Release()

	// for true {
	// 	iter_over_db(db)
	// }

	// i := int64(-123456789)

	// fmt.Println(i)
	// fmt.Printf("%x\n",i)
	// fmt.Printf("%v\n",i)

	// b := make([]byte, 8)
	// binary.LittleEndian.PutUint64(b, uint64(i))
	// fmt.Println(b)
	// // [235 50 164 248 255 255 255 255]
	// // (2**24)*7+(2**16)*91+(2**8)*205+20+1

	// i = int64(binary.LittleEndian.Uint64(b))
	// fmt.Println(i)

	defer db.Close()
}

func iter_over_db(db *leveldb.DB) {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		// debug
		fmt.Printf("Find k=%s\tv=%s\n", key, value)
	}
	iter.Release()
}
