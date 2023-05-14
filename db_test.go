package go_mini_kv

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func createDb(t *testing.T) *DB {
	dir, err := os.Getwd()
	path := filepath.Join(dir, "_test", "db", strconv.FormatInt(time.Now().Unix(), 10))
	err = os.MkdirAll(path, 0777)
	db, err := Open(path)
	assert.Nil(t, err)
	return db
}

func TestOpen(t *testing.T) {
	createDb(t)
}

func TestDB_Set(t *testing.T) {
	db := createDb(t)

	// Set a value
	err := db.Set([]byte("hello"), []byte("world"))
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db := createDb(t)

	values := map[string]string{
		"foo":   "bar",
		"hello": "world",
		"baz":   "bam bar",
	}

	// Set and get value
	for key, value := range values {
		bKey := []byte(key)
		bValue := []byte(value)

		err := db.Set(bKey, bValue)
		result, err := db.Get(bKey)
		assert.Nil(t, err)
		assert.Equal(t, result, bValue)
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
}

func TestDB_Delete(t *testing.T) {
	db := createDb(t)
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
}
