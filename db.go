package go_mini_kv

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

type DB struct {
	index *os.File // Database index
	data  *os.File // Actual data
}

// Open returns a new database instance.
func Open(loc string) (*DB, error) {

	// Create files
	index, indexErr := os.OpenFile(filepath.Join(loc, "index.bin"), os.O_RDWR|os.O_CREATE, 0666)
	data, dataErr := os.OpenFile(filepath.Join(loc, "data.db"), os.O_RDWR|os.O_CREATE, 0666)

	if indexErr != nil || dataErr != nil {
		return nil, fmt.Errorf("failed to open database files: %v / %v", indexErr, dataErr)
	}

	return &DB{index, data}, nil
}

// Set can be used to store a new value based on the key.
func (db *DB) Set(key []byte, value []byte) error {
	hashedKey := hashKey(key)

	if entry, _, err := db.findValuePointerForKey(hashedKey); err != nil {
		return err
	} else if entry == nil {
		info, err := db.data.Stat()
		if err != nil {
			return fmt.Errorf("failed retrieving data info: %v", err)
		}

		chunk := EncodeValuePointer(&ValuePointer{
			hash:   hashedKey,
			size:   uint32(len(value)),
			offset: uint32(info.Size()),
		})

		// Write data and value pointer.
		// If the latter fails, we won't have a broken database but data to garbage collect.
		if _, err = db.data.Write(value); err != nil {
			return fmt.Errorf("failed writing data to file: %v", err)
		} else if _, err = db.index.Write(chunk); err != nil {
			return fmt.Errorf("failed storing index: %v", err)
		}
	}

	return nil
}

// Get returns the value for the given key, or nil of there is none.
func (db *DB) Get(key []byte) ([]byte, error) {
	entry, _, err := db.findValuePointerForKey(key)

	if err != nil {
		return nil, err
	} else if entry == nil {
		return nil, nil
	}

	buffer := make([]byte, entry.size)
	bytesRead, err := db.data.ReadAt(buffer, int64(entry.offset))

	if err != nil {
		return nil, fmt.Errorf("failed to read from databas: %v", err)
	} else if uint32(bytesRead) != entry.size {
		return nil, fmt.Errorf("data expected to have %v bytes, but only received %v", entry.size, bytesRead)
	}

	return buffer, nil
}

// Delete removes a key and the value associated.
func (db *DB) Delete(key []byte) (bool, error) {
	pointer, offset, err := db.findValuePointerForKey(key)

	if err != nil {
		return false, fmt.Errorf("failed to retrieve pointer: %v", err)
	} else if pointer == nil {
		return false, nil
	}

	zeroedChunk := make([]byte, ValuePointerSize)
	if _, err := db.index.WriteAt(zeroedChunk, int64(offset)); err != nil {
		return false, fmt.Errorf("failed to write to file: %v", err)
	}

	return true, nil
}

// findValuePointerForKey returns the information relating the given key.
// In case there is no such entry it returns nil.
func (db *DB) findValuePointerForKey(key []byte) (*ValuePointer, uint32, error) {
	hashedKey := hashKey(key)
	chunk := make([]byte, ValuePointerSize)
	offset := uint32(0)

	// Loop trough index file to find offset
	for {
		_, err := db.index.ReadAt(chunk, int64(offset))

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, fmt.Errorf("failed to read file at %v: %v", offset, err)
		} else if reflect.DeepEqual(chunk[:32], hashedKey) {
			pointer, err := DecodeValuePointer(chunk)

			if pointer.IsZero() {
				return nil, offset, nil // Entry still exist, but marked for gc
			} else {
				return pointer, offset, err
			}
		}

		offset += ValuePointerSize
	}

	return nil, 0, nil // Entry not found
}

// hashKey hashes the key for a database entry.
func hashKey(key []byte) []byte {
	hash := sha256.New()
	hash.Write(key)
	return hash.Sum(nil)
}
