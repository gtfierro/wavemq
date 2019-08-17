package rockstorage

// #cgo CXXFLAGS: -I./rocksdb/include
// #cgo LDFLAGS: -L./rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd
import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/tecbot/gorocksdb"
)

var ro = gorocksdb.NewDefaultReadOptions()
var wo = gorocksdb.NewDefaultWriteOptions()
var to = gorocksdb.NewDefaultTransactionOptions()

type DB struct {
	rocks *gorocksdb.TransactionDB
}

func NewDB(path string) (*DB, error) {
	db := &DB{}
	var err error

	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetSkipLogErrorOnRecovery(true)
	topts := gorocksdb.NewDefaultTransactionDBOptions()
	db.rocks, err = gorocksdb.OpenTransactionDb(opts, topts, path)

	return db, errors.Wrap(err, "could not open database")
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	_value, _err := db.rocks.Get(ro, key)
	value = _value.Data()
	_value.Free()
	err = errors.Wrap(_err, "could not get")
	return
}

func (db *DB) Put(key, value []byte) error {
	if err := db.rocks.Put(wo, key, value); err != nil {
		return errors.Wrap(err, "rocks put")
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if err := db.rocks.Delete(wo, key); err != nil {
		return errors.Wrap(err, "delete")
	}
	return nil
}

func (db *DB) Close() error {
	db.rocks.Close()
	return nil
}

type Iterator struct {
	rocksIter *gorocksdb.Iterator
	txn       *gorocksdb.Transaction
	prefix    []byte
}

func (db *DB) NewIterator(prefix []byte) *Iterator {

	txn := db.rocks.TransactionBegin(wo, to, nil)
	it := &Iterator{
		txn:       txn,
		rocksIter: txn.NewIterator(ro),
		prefix:    prefix,
	}
	it.rocksIter.Seek(prefix)
	fmt.Println("open iter")

	return it
}

func (it *Iterator) HasNext() bool {
	return it.rocksIter.ValidForPrefix(it.prefix)
}

func (it *Iterator) Next() {
	it.rocksIter.Next()
}

func (it *Iterator) Key() (key []byte) {
	_key := it.rocksIter.Key()
	key = _key.Data()
	_key.Free()
	return
}

func (it *Iterator) Value() (value []byte) {
	_value := it.rocksIter.Value()
	value = _value.Data()
	_value.Free()
	return
}

func (it *Iterator) Put(key, value []byte) error {
	if err := it.txn.Put(key, value); err != nil {
		errors.Wrap(err, "it txn put")
	}
	return nil
}

func (it *Iterator) Delete(key []byte) error {
	if err := it.txn.Delete(key); err != nil {
		return errors.Wrap(err, "it txn put")
	}
	return nil
}

func (it *Iterator) Close() error {
	defer it.txn.Destroy()
	if err := it.rocksIter.Err(); err != nil {
		fmt.Println("rollback err?", it.txn.Rollback())
		return err
	}
	it.rocksIter.Close()
	fmt.Println("close iter")
	return it.txn.Commit()
}
