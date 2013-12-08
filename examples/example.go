package main

import (
	"fmt"

	"github.com/senarukana/ratgo"
)

func main() {
	var err error
	opts := ratgo.NewOptions()
	opts.SetCache(ratgo.NewLRUCache(2 << 10))
	opts.SetCreateIfMissing(true)
	db, err := ratgo.Open("/home/ted/test.db", opts)
	if err != nil {
		fmt.Println("crete db failed", err)
	}

	//Write
	wo := ratgo.NewWriteOptions()
	defer wo.Close()
	err = db.Put(wo, []byte("user"), []byte("lizhe"))
	if err != nil {
		fmt.Println("put failed", err)
	}

	//Read
	ro := ratgo.NewReadOptions()
	defer ro.Close()
	data, err := db.Get(ro, []byte("user"))
	if err != nil {
		fmt.Println("read failed:", err)
	} else {
		fmt.Println("result is ", data)
	}

}
