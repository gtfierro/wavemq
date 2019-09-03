basepath="/home/gabe/go/src/github.com/immesys/wavemq"
build: pairing.a
	CGO_CFLAGS="-I${basepath}/rockstorage/include" CGO_LDFLAGS="-L${basepath}/rockstorage/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go build
pairing.a:
	cd vendor/github.com/ucbrise/jedi-pairing && make

prometheus:
	 docker run --name prometheus -d -p 127.0.0.1:9090:9090 -v `pwd`/testing/prometheus:/etc/prometheus --network=host prom/prometheus

stopprom:
	docker stop prometheus
	docker rm prometheus
