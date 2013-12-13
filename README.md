# rago
ratgo is a Go wrapper for RocksDb
Package ratgo provides the ability to create and access RocksDB databases.
RocksDB is a database built by facebook and based on the leveldb.
For more information, see:https://github.com/facebook/rocksdb
This wrapper is based on the levigo which is a wrapper for leveldb. Thanks to the author Albert Strasheim.
For more information about levigo, see: https://github.com/jmhodges/levigo

# Warning

Because RocksDB is still under development, it's interface are not stable and may change during the process.

This wrapper only contains a PARTIAL range of functions.

If you are advanced user, I really recommend you to see their introduction to get full apis.

# Building

1. You'll need to clone a copy of RocksDB.

2. Clone a copy of ratgo, and do the following cmd to copy the c.h, and c.cc to the destination place.
  cp ratgoPATH/c.h RocksDBPATH/include/c.h
	cp ratgoPATH/c.cc RocksDBPATH/db/c.cc

3. Build the RocksDB and installed the library.

Here is a simple installation:
	cd RockDBPATH
	make
	sudo cp -r /include/rocksdb/ /usr/local/include/
	sudo cp librocksdb.a /usr/local/lib/

4. Now, if you build RocksDB and put the shared library and

	go get github.com/senarukana/ratgo

5. Test ratgo
  go test github.com/senarukana/ratgo
If it runs succeeded, congratulations.

But, suppose you put the shared LevelDB library somewhere weird like /path/to/lib and the headers were installed in /path/to/include.

To install ratgo remotely, you'll run:
	CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb/lib" go get github.com/senarukana/ratgo
	
# Usage
You can see the test in ratgo_test.go or examples in fold examples/ to see how to use it.

# Development

I currently use this to build a distributed database RationalDB, for more information, see:
https://github.com/senarukana/RelationalDB
