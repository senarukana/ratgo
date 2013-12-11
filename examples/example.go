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
		return
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
	defer ro.Close()

	data, err := db.Get(ro, []byte("user"))
	if err != nil {
		fmt.Println("read failed:", err)
	}

	fmt.Printf("result is %s\n", string(data.Data))

	// Batch Write
	wb := ratgo.NewWriteBatch()
	wb.Put([]byte("user1"), []byte("li"))
	wb.Put([]byte("user2"), []byte("ted"))
	err = db.Write(wo, wb)
	if err != nil {
		fmt.Println("write batch error, ", err)
	}

	// Iterator
	iter := db.NewIterator(ro)
	fmt.Println("Begin iterate")
	iter.Seek([]byte("user"))
	for ; iter.Valid(); iter.Next() {
		fmt.Printf("%s = %s\n", string(iter.Key().Data), string(iter.Value().Data))
	}
	if err := iter.GetError(); err != nil {
		fmt.Println("iter error, ", err.Error())
	}
	iter.Close()
	fmt.Println("End iterate")

	// Multiget
	fmt.Println("Begin Multiget")
	keys := [][]byte{[]byte("user1"), []byte("user2")}
	values, errors := db.MultiGet(ro, keys)
	for i, value := range values {
		if errors[i] != nil {
			fmt.Printf("Get Key %s failed, %v\n", keys[i], errors[i])
		} else {
			fmt.Printf("%s = %s\n", keys[i], string(value.Data))
		}
	}
	fmt.Println("End Multiget")

	// Snapshot
	snap := db.NewSnapshot()
	ro.SetSnapshot(snap)
	db.Put(wo, []byte("sntest"), []byte("test"))
	v, err := db.Get(ro, []byte("sntest"))
	if v != nil {
		fmt.Println("something wrong with snapshot")
	} else {
		fmt.Println("snapshot ok")
	}

	// Backup
	// db.DisableFileDeletions()
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
	db.EnableFileDeletions()

	// property
	levelstats := db.PropertyValue("rocksdb.levelstatus")
	fmt.Println("level status is :", levelstats)
	stats := db.PropertyValue("rocksdb.stats")
	fmt.Println("stats is : ", stats)
	//Close
	db.Close()

	// Destroy
	err = ratgo.DestroyDatabase(dbName, opts)
	if err != nil {
		fmt.Println(err)
	}
	return
}
