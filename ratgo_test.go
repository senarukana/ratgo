package ratgo

import (
	"fmt"
	"os"
	"path"
	"runtime"

	"testing"
)

/*func TestMain(t *testing.T) {
	b := testRago(t)

	// test gc
	v := "value5"
	if string(b) != v {
		t.Errorf("expect return value is %s, but result is %s", v, string(b))
	}
	runtime.GC()
	if string(b) == v {
		t.Error("value5 shoud be freed after gc, but it's not")
	}

}*/

func TestRago(t *testing.T) {
	var err error
	// keys to be inserted
	k1 := []byte("user1")
	k2 := []byte("user2")
	k3 := []byte("user3")
	k4 := []byte("user4")
	k5 := []byte("user5")
	v1 := []byte("value1")
	v2 := []byte("value2")
	v3 := []byte("value3")
	v4 := []byte("value4")
	v5 := []byte("value5")

	options := NewOptions()
	dbPath, err := os.Getwd()
	if err != nil {
		panic(fmt.Sprintf("can't get current file path %v)", err))
	}
	dbName := path.Join(dbPath, "testdb")
	// make sure the db is not exist
	err = DestroyDatabase(dbName, options)
	if err != nil {
		t.Fatalf("Destroy db %s error, %v\n", dbName, err)
	}

	// Open the database
	if _, err := Open(dbName, options); err == nil {
		t.Fatal("don't set the CreateIfMissing tag but it still create a db")
	}

	options.SetCreateIfMissing(true)
	db, err := Open(dbName, options)
	if err != nil {
		t.Fatalf("can't create db:%s, err %v\n", dbName, err)
	}

	// Write
	wo := NewWriteOptions()
	err = db.Put(wo, k1, v1)
	if err != nil {
		t.Errorf("put key:%s failed, err %v\n", string(k1), err)
	}
	defer wo.Close()

	// Get
	ro := NewReadOptions()

	// key doesn't exist
	data, err := db.Get(ro, k2)
	if err != nil {
		t.Errorf("read key:%s failed, err %v\n", k2, err)
	}
	if data != nil {
		t.Errorf("not put key:%s, but get the result, the value is %s", string(k2), string(data))
	}

	err = db.Put(wo, k2, v2)
	if err != nil {
		t.Errorf("put key:%s failed, err %v\n", string(k2), err)
	}

	data, err = db.Get(ro, k2)
	if err != nil {
		t.Errorf("read key:%s failed, err %v\n", k2, err)
	}
	if data == nil {
		t.Errorf("already put key:%s, but can't find it", string(k2))
	} else {
		if string(data) != string(v2) {
			t.Errorf("key:%s=%s, expect %s\n", string(k2), string(data), string(v2))
		}
	}

	// Multiget
	keys := [][]byte{k1, k2, k3}
	values, errors := db.MultiGet(ro, keys)
	for i, value := range values {
		if errors[i] != nil {
			t.Errorf("Get key:%s failed, err %v\n", keys[i], errors[i])
		} else {
			switch i {
			case 0:
				if value == nil || string(value) != string(v1) {
					t.Errorf("Get key:%s failed, value is nil or value is not equal to the expected result", string(keys[i]))
				}
			case 1:
				if value == nil || string(value) != string(v2) {
					t.Errorf("Get key:%s failed, value is nil or value is not equal to the expected result", string(keys[i]))
				}
			case 2:
				if value != nil {
					t.Errorf("not put key:%s, but get the result, the value is %s", string(k3), string(value))
				}
			}
		}
	}

	// Batch Write
	wb := NewWriteBatch()
	wb.Put(k3, v3)
	wb.Put(k4, v4)
	wb.Delete(k4)
	err = db.Write(wo, wb)
	if err != nil {
		t.Errorf("write batch error, %v", err)
	}

	// Iterate k2~k4
	count := 0
	iter := db.NewIterator(ro)
	for iter.Seek(k2); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.GetError(); err != nil {
		t.Errorf("iter failed, error %v", err.Error())
	}
	if count != 2 {
		t.Errorf("should iter from k2~k3, expect 2 elements but the result is %d\n", count)
	}
	iter.Close()

	// Snapshot
	snap := db.NewSnapshot()
	ro.SetSnapshot(snap)
	db.Put(wo, k5, v5)
	v, err := db.Get(ro, k5)
	if v != nil {
		t.Errorf("should not get key:%s with snapshot\n", string(k5))
	}
	db.ReleaseSnapshot(snap)
	ro.Close()

	ro = NewReadOptions()
	getV5, err := db.Get(ro, k5)
	if getV5 == nil {
		t.Errorf("should get key:%s with no snapshot\n", string(k5))
	}

	// Backup operation
	db.DisableFileDeletions()
	beforeFiles, beforeManifestFileSize, err := db.GetLiveFiles(true)
	if err != nil {
		t.Errorf("get live files failed, error %v", err.Error())
	}
	db.EnableFileDeletions()

	db.Delete(wo, k1)

	afterFiles, afterManifestFileSize, err := db.GetLiveFiles(true)
	if err != nil {
		t.Errorf("get live files failed, error %v", err.Error())
	}

	if len(beforeFiles) == len(afterFiles) {
		t.Error("live files should not be the same")
	}

	if beforeManifestFileSize == afterManifestFileSize {
		t.Error("manifest file size should not be the same")
	}

	// Property
	stats := db.PropertyValue("rocksdb.stats")
	if len(stats) == 0 {
		t.Error("rocksdb.stats is nil")
	}

	//Close
	db.Close()

	// Destroy
	err = DestroyDatabase(dbName, options)
	if err != nil {
		t.Fatal("Destroy database error, %v\n", err)
	}
}
