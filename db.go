package go_mini_kv

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

type Entry struct {
	offset int64
	size   int64
}

type DB struct {

	// Files
	index *os.File
	data  *os.File

	// In-memory index
	//entries map[[32]byte]Entry
}

// Open returns a new database instance
func Open(loc string) (*DB, error) {

	// Create files
	index, indexErr := os.OpenFile(filepath.Join(loc, "index.db"), os.O_RDWR|os.O_CREATE, 0666)
	data, dataErr := os.OpenFile(filepath.Join(loc, "data.db"), os.O_RDWR|os.O_CREATE, 0666)

	if indexErr != nil || dataErr != nil {
		return nil, fmt.Errorf("failed to open database files: %v / %v", indexErr, dataErr)
	}

	//entries, err := loadEntries(index)
	//if err != nil {
	//	return nil, err
	//}

	db := &DB{index, data}

	return db, nil
}

func (db *DB) Set(key []byte, value []byte) error {
	hashedKey := hashKey(key)
	entry, err := db.getOffset(hashedKey)

	if err != nil {
		return err
	}

	// Key doesn't exist yet
	if entry == nil {

		// Write data
		info, err := db.data.Stat()
		if err != nil {
			return fmt.Errorf("failed retrieving data info: %v", err)
		}

		_, err = db.data.Write(value)
		if err != nil {
			return fmt.Errorf("failed writing data to file: %v", err)
		}

		chunk := make([]byte, 32+8+8)
		copy(chunk[:32], hashedKey[:])
		copy(chunk[32:40], toBytes(info.Size()))
		copy(chunk[40:], toBytes(int64(len(value))))

		_, err = db.index.Write(chunk)
		if err != nil {
			return fmt.Errorf("failed storing index: %v", err)
		}
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	hashedKey := hashKey(key)
	entry, err := db.getOffset(hashedKey)

	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	buffer := make([]byte, entry.size)
	bytesRead, err := db.data.ReadAt(buffer, entry.offset)

	if int64(bytesRead) != entry.size {
		return nil, fmt.Errorf("failed to correctly read from database")
	} else if err != nil {
		return nil, fmt.Errorf("failed to read from databas: %v", err)
	}

	return buffer, nil
}

func (db *DB) getOffset(key []byte) (*Entry, error) {

	// Loop trough index file to find offset
	// Each chunk looks like the following:
	// {key (32 bytes), offset (8 bytes / uint64), size (8 bytes / uint64)}
	chunk := make([]byte, 32+8+8)
	offset := int64(0)

	for {
		bytesRead, err := db.index.ReadAt(chunk, offset)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read file at %v: %v", offset, err)
		}

		if reflect.DeepEqual(chunk[:32], key) {
			return &Entry{
				offset: fromBytes(chunk[32:40]),
				size:   fromBytes(chunk[40:48]),
			}, nil
		}

		offset += int64(bytesRead) + 8
	}

	return nil, nil
}

func hashKey(key []byte) []byte {
	hash := sha256.New()
	hash.Write(key)
	return hash.Sum(nil)
}

func toBytes(v int64) []byte {
	arr := make([]byte, 8)
	binary.PutVarint(arr, v)
	return arr
}

func fromBytes(b []byte) int64 {
	value, _ := binary.Varint(b)
	return value
}
