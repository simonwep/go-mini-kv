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

func testDb(t *testing.T, run func(db *DB)) {
	dir, err := os.Getwd()
	assert.Nil(t, err)

	testDir := filepath.Join(dir, "_test")
	testId := strconv.Itoa(time.Now().Nanosecond()) + "_" + strconv.Itoa(dbIndex)
	dbDir := filepath.Join(testDir, testId)
	dbIndex += 1 // avoid collisions

	err = os.MkdirAll(dbDir, 0777)
	db, err := Open(dbDir)
	assert.Nil(t, err)

	assert.Nil(t, err)

	run(db)
	err = os.RemoveAll(testDir)
	assert.Nil(t, err)
}

func TestOpen(t *testing.T) {
	testDb(t, func(db *DB) {})
}

func TestDB_Set(t *testing.T) {
	testDb(t, func(db *DB) {

		// Set a value
		err := db.Set([]byte("hello"), []byte("world"))
		assert.Nil(t, err)
	})
}

func TestDB_Get(t *testing.T) {
	values := map[string]string{
		"":      "",
		"foo":   "bar",
		"hello": "world",
		"baz":   "bam bar",
	}

	testDb(t, func(db *DB) {

		// Set and get value
		for key, value := range values {
			bKey := []byte(key)
			bValue := []byte(value)

			err := db.Set(bKey, bValue)
			assert.Nil(t, err)

			result, err := db.Get(bKey)
			assert.Nil(t, err)
			assert.Equal(t, bValue, result)
		}

		// Read values
		for key, value := range values {
			result, err := db.Get([]byte(key))
			assert.Nil(t, err)
			assert.Equal(t, result, []byte(value))
		}

		// Read non-existing values
		result, err := db.Get([]byte("xyz"))
		assert.Nil(t, err)
		assert.Nil(t, result)

	})
}

func TestDB_Delete(t *testing.T) {
	testDb(t, func(db *DB) {
		db.Set([]byte("foo"), []byte("bar"))
		db.Set([]byte("baz"), []byte("bam"))

		value, _ := db.Get([]byte("foo"))
		assert.Equal(t, value, []byte("bar"))

		value, _ = db.Get([]byte("baz"))
		assert.Equal(t, value, []byte("bam"))

		status, err := db.Delete([]byte("fo2"))
		assert.Nil(t, err)
		assert.False(t, status)

		status, err = db.Delete([]byte("foo"))
		assert.Nil(t, err)
		assert.True(t, status)

		value, err = db.Get([]byte("foo"))
		assert.Nil(t, err)
		assert.Nil(t, value)
	})
}
