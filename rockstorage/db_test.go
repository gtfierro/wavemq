package rocksdb

import (
	"encoding/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSimple(t *testing.T) {
	require := require.New(t)
	Initialize("_testdb_", false)
	k1, v1 := []byte("key1"), []byte("value1")
	QueueSet(k1, v1)
	v1p, err := QueueGet(k1)
	require.NoError(err, "get key1")
	require.Equal(v1, v1p, "Retrieved value not equivalent")

	QueueDelete(k1)
	_, err = QueueGet(k1)
	require.Error(err, "get key1 (deleted)")
}

func TestIter(t *testing.T) {
	require := require.New(t)
	Initialize("_testdb_", false)

	k1, v1 := []byte("a/b/c"), []byte("h/value1")
	k2, v2 := []byte("a/b/d"), []byte("h/value2")
	k3, v3 := []byte("a/b/e"), []byte("h/value3")
	k4, v4 := []byte("b/a"), []byte("g/value4")
	QueueSet(k1, v1)
	QueueSet(k2, v2)
	QueueSet(k3, v3)
	QueueSet(k4, v4)
	v, e := QueueGet(k1)
	require.NoError(e, "get k1")
	require.NotNil(v, "value was nil")
	it := NewIterator([]byte("a/b"))
	require.True(it.HasNext(), "iterator validity")
	require.Equal(it.Key(), k1, "check key")
	require.Equal(it.Value(), v1, "check val")
	it.Next() // advance
	require.True(it.HasNext(), "iterator validity")
	require.Equal(it.Key(), k2, "check key")
	require.Equal(it.Value(), v2, "check val")
	it.Next() // advance
	require.True(it.HasNext(), "iterator validity")
	require.Equal(it.Key(), k3, "check key")
	require.Equal(it.Value(), v3, "check val")
	it.Next() // advance
	require.False(it.HasNext(), "iterator validity")

}

func BenchmarkInsertThenDelete(b *testing.B) {
	Initialize("_testdb_", false)
	for i := 0; i < b.N; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))
		QueueSet(key, []byte("random bytes"))
		QueueDelete(key)
	}
}

func BenchmarkSet(b *testing.B) {
	Initialize("_testdb_", false)
	for i := 0; i < b.N; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))
		QueueSet(key, []byte("random bytes"))
	}
}
