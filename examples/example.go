package main

import (
	"fmt"
	"os"
	"path"

	"github.com/senarukana/ratgo"
)

func main() {
	var err error
	opts := ratgo.NewOptions()
	opts.SetCache(ratgo.NewLRUCache(2 << 10))
	opts.SetCreateIfMissing(true)
	dbPath, err := os.Getwd()
	if err != nil {
		panic(fmt.Sprintf("can't get current file path %v)", err))
	}
	dbName := path.Join(dbPath, "testdb")
	db, err := ratgo.Open(dbName, opts)
	if err != nil {
		fmt.Println("crete db failed", err)
	}

	//Write
	wo := ratgo.NewWriteOptions()
	err = db.Put(wo, []byte("user"), []byte("lizhe"))
	if err != nil {
		fmt.Println("put failed", err)
	}
	defer wo.Close()

	// Read
	ro := ratgo.NewReadOptions()
	data, err := db.Get(ro, []byte("user"))
	if err != nil {
		fmt.Println("read failed:", err)
	} else {
		fmt.Println("result is ", data)
	}
	ro.Close()

	// snapshot
	files, manifestFileSize, err := db.GetLiveFiles(false)
	if err != nil {
		fmt.Println("get live files encounter an error:", err.Error())
	} else {
		for _, file := range files {
			fmt.Printf("%v ", file)
		}
		fmt.Println()
		fmt.Println("manifestfile size is :", manifestFileSize)
	}

	//Close
	db.Close()

	// Destroy
	err = ratgo.DestroyDatabase(dbName, opts)
	if err != nil {
		fmt.Println(err)
	}
}
