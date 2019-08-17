basepath="/home/gabe/go/src/github.com/immesys/wavemq"
build:
	CGO_CFLAGS="-I${basepath}/rocksdb/include" CGO_LDFLAGS="-L${basepath}/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go build
