package go_mini_kv

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var dbIndex = 0

func testDb(t *testing.T, run func(db *DB, assert *assert.Assertions)) {
	assert := assert.New(t)
	dir, err := os.Getwd()
	assert.Nil(err)

	testDir := filepath.Join(dir, "_test")
	testId := strconv.Itoa(time.Now().Nanosecond()) + "_" + strconv.Itoa(dbIndex)
	dbDir := filepath.Join(testDir, testId)
	dbIndex += 1 // avoid collisions

	err = os.MkdirAll(dbDir, 0777)
	db, err := Open(dbDir)
	assert.Nil(err)

	assert.Nil(err)

	run(db, assert)
	err = os.RemoveAll(testDir)
	assert.Nil(err)
}

func TestOpen(t *testing.T) {
	testDb(t, func(db *DB, assert *assert.Assertions) {})
}

func TestDB_Set(t *testing.T) {
	testDb(t, func(db *DB, assert *assert.Assertions) {

		// Set a value
		err := db.Set([]byte("hello"), []byte("world"))
		assert.Nil(err)
	})
}

func TestDB_Get(t *testing.T) {
	values := map[string]string{
		"":      "",
		"foo":   "bar",
		"hello": "world",
		"baz":   "bam bar",
	}

	testDb(t, func(db *DB, assert *assert.Assertions) {

		// Set and get value
		for key, value := range values {
			bKey := []byte(key)
			bValue := []byte(value)

			err := db.Set(bKey, bValue)
			assert.Nil(err)

			result, err := db.Get(bKey)
			assert.Nil(err)
			assert.Equal(bValue, result)
		}

		// Read values
		for key, value := range values {
			result, err := db.Get([]byte(key))
			assert.Nil(err)
			assert.Equal(result, []byte(value))
		}

		// Read non-existing values
		result, err := db.Get([]byte("xyz"))
		assert.Nil(err)
		assert.Nil(result)
	})
}

func TestDB_Delete(t *testing.T) {
	testDb(t, func(db *DB, assert *assert.Assertions) {
		db.Set([]byte("foo"), []byte("bar"))
		db.Set([]byte("baz"), []byte("bam"))

		value, _ := db.Get([]byte("foo"))
		assert.Equal(value, []byte("bar"))

		value, _ = db.Get([]byte("baz"))
		assert.Equal(value, []byte("bam"))

		status, err := db.Delete([]byte("fo2"))
		assert.Nil(err)
		assert.False(status)

		status, err = db.Delete([]byte("foo"))
		assert.Nil(err)
		assert.True(status)

		value, err = db.Get([]byte("foo"))
		assert.Nil(err)
		assert.Nil(value)
	})
}

func TestDB_Size(t *testing.T) {
	testDb(t, func(db *DB, assert *assert.Assertions) {
		db.Set([]byte("foo"), []byte("bar"))
		db.Set([]byte("baz"), []byte("world"))
		size, err := db.Stat()

		assert.Nil(err)
		assert.Equal(size.dict, uint32(ValuePointerSize*2))
		assert.Equal(size.data, uint32(8))
		assert.Equal(size.entries, uint32(2))
	})
}

func TestDB_RunGC(t *testing.T) {
	testDb(t, func(db *DB, assert *assert.Assertions) {
		db.Set([]byte("foo"), []byte("bam"))
		db.Set([]byte("baz"), []byte("bare"))
		db.Set([]byte("boo"), []byte("var"))

		size, _ := db.Stat()
		assert.Equal(size.dict, uint32(ValuePointerSize*3))
		assert.Equal(size.data, uint32(10))
		assert.Equal(size.entries, uint32(3))

		db.Delete([]byte("foo"))
		db.Delete([]byte("boo"))

		size, _ = db.Stat()
		assert.Equal(size.dict, uint32(ValuePointerSize*3))
		assert.Equal(size.data, uint32(10))
		assert.Equal(size.entries, uint32(3))

		err := db.RunGC()
		assert.Nil(err)

		data, err := db.Get([]byte("baz"))
		assert.Nil(err)
		assert.Equal(data, []byte("bare"))

		size, _ = db.Stat()
		assert.Equal(size.dict, uint32(ValuePointerSize))
		assert.Equal(size.data, uint32(4))
		assert.Equal(size.entries, uint32(1))
	})
}
