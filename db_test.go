package go_mini_kv

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func createDb(t *testing.T) *DB {
	dir, err := os.Getwd()
	path := filepath.Join(dir, "_test", "db", strconv.FormatInt(time.Now().Unix(), 10))
	err = os.MkdirAll(path, 0777)
	db, err := Open(path)

	if err != nil {
		t.Errorf("failed to create database in tmpdir: %v", err)
	}
	return db
}

func TestOpen(t *testing.T) {
	createDb(t)
}

func TestDB_Set(t *testing.T) {
	db := createDb(t)

	// Set a value
	err := db.Set([]byte("hello"), []byte("world"))
	if err != nil {
		t.Errorf("failed to set value: %v", err)
	}
}

func TestDB_Get(t *testing.T) {
	db := createDb(t)
	err := db.Set([]byte("hello"), []byte("world"))

	value, err := db.Get([]byte("hello"))

	if err != nil {
		t.Errorf("failed to get value: %v", err)
	} else if !reflect.DeepEqual(value, []byte("world")) {
		t.Errorf("values are not equal: %v", value)
	}
}
