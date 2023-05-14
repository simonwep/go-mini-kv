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

// A chunk represents a database entry.
// Each chunk looks like the following:
// - key: 32 bytes, a sha256 hash. The first bit indicates if this chunk can be garbage collected.
// - offset: 8 bytes, an uint64 indicating the offset of the data in the data file.
// - size: 8 bytes, an uint64 indicating the size of the data stored.
const chunkSize = 32 + 8 + 8

type Entry struct {
	entryOffset int64
	dataOffset  int64
	size        int64
}

type DB struct {

	// Files
	index *os.File
	data  *os.File

	// In-memory index
	//entries map[[32]byte]Entry
}

// Open returns a new database instance.
func Open(loc string) (*DB, error) {

	// Create files
	index, indexErr := os.OpenFile(filepath.Join(loc, "index.bin"), os.O_RDWR|os.O_CREATE, 0666)
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

// Set can be used to store a new value based on the key.
func (db *DB) Set(key []byte, value []byte) error {
	hashedKey := hashKey(key)
	entry, err := db.getEntry(hashedKey)

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

		if _, err = db.data.Write(value); err != nil {
			return fmt.Errorf("failed writing data to file: %v", err)
		}

		chunk := make([]byte, chunkSize)
		copy(chunk[:32], hashedKey[:])
		copy(chunk[32:40], toBytes(info.Size()))
		copy(chunk[40:], toBytes(int64(len(value))))

		if _, err = db.index.Write(chunk); err != nil {
			return fmt.Errorf("failed storing index: %v", err)
		}
	}

	return nil
}

// Get returns the value for the given key, or nil of there is none.
func (db *DB) Get(key []byte) ([]byte, error) {
	entry, err := db.getEntry(key)

	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	buffer := make([]byte, entry.size)
	bytesRead, err := db.data.ReadAt(buffer, entry.dataOffset)

	if int64(bytesRead) != entry.size {
		return nil, fmt.Errorf("data expected to have %v bytes, but only received %v", entry.size, bytesRead)
	} else if err != nil {
		return nil, fmt.Errorf("failed to read from databas: %v", err)
	}

	return buffer, nil
}

// Delete removes a key and value associated.
func (db *DB) Delete(key []byte) (bool, error) {
	entry, err := db.getEntry(key)

	if err != nil {
		return false, fmt.Errorf("failed to retrieve entry: %v", err)
	} else if entry == nil {
		return false, nil
	} else if err := db.deleteEntry(entry); err != nil {
		return false, err
	}

	return true, nil
}

func (db *DB) deleteEntry(entry *Entry) error {
	zeroedChunk := make([]byte, chunkSize)
	zeroedChunk[0] = 128 // mark ready for garbage collection

	if _, err := db.index.WriteAt(zeroedChunk, entry.entryOffset); err != nil {
		return fmt.Errorf("failed to write to index: %v", err)
	}

	return nil
}

// getEntry returns the information relating the given key.
// In case there is no such entry it returns nil.
func (db *DB) getEntry(key []byte) (*Entry, error) {
	hashedKey := hashKey(key)
	chunk := make([]byte, chunkSize)
	offset := int64(0)

	// Loop trough index file to find offset
	for {
		_, err := db.index.ReadAt(chunk, offset)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read file at %v: %v", offset, err)
		} else if chunk[0]&128 != 128 { // check if chunk is not marked for garbage collection
			if reflect.DeepEqual(chunk[:32], hashedKey) {
				return &Entry{
					entryOffset: offset,
					dataOffset:  fromBytes(chunk[32:40]),
					size:        fromBytes(chunk[40:48]),
				}, nil
			}
		}

		offset += chunkSize
	}

	return nil, nil
}

// hashKey hashes the key for a database entry.
func hashKey(key []byte) []byte {
	hash := sha256.New()
	hash.Write(key)
	sum := hash.Sum(nil)
	sum[0] &= 0b01111111 // the first bit is reserved
	return sum
}

// toBytes takes an int64 and converts it into a byte array.
func toBytes(v int64) []byte {
	arr := make([]byte, 8)
	binary.PutVarint(arr, v)
	return arr
}

// fromBytes takes a byte array and converts it into an int64.
func fromBytes(b []byte) int64 {
	value, _ := binary.Varint(b)
	return value
}
