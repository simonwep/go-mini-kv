package go_mini_kv

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
)

type DB struct {
	dict *os.File // Database dict
	data *os.File // Actual data
}

const filePermissions = os.O_RDWR | os.O_CREATE | os.O_TRUNC

// Open returns a new database instance.
func Open(loc string) (*DB, error) {

	// Create files
	dict, dictErr := os.OpenFile(filepath.Join(loc, "dict.bin"), filePermissions, 0666)
	data, dataErr := os.OpenFile(filepath.Join(loc, "data.db"), filePermissions, 0666)

	if dictErr != nil || dataErr != nil {
		return nil, fmt.Errorf("failed to open database files: %v / %v", dictErr, dataErr)
	}

	// Create buffer for dict in case it's a new file
	if stat, err := dict.Stat(); err != nil {
		return nil, fmt.Errorf("failed read database file: %v", err)
	} else if stat.Size() == 0 {
		if _, err := data.Write([]byte{255}); err != nil {
			return nil, fmt.Errorf("failed to initialize database file: %v", err)
		}
	}

	return &DB{dict, data}, nil
}

// Set can be used to store a new value based on the key.
func (db *DB) Set(key []byte, value []byte) error {
	hashedKey, err := SHA256(key)
	if err != nil {
		return err
	}

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
		} else if _, err = db.dict.Write(chunk); err != nil {
			return fmt.Errorf("failed storing dict: %v", err)
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
		return nil, fmt.Errorf("failed to read from database: %v", err)
	} else if uint32(bytesRead) != entry.size {
		return nil, fmt.Errorf("data expected to have %v bytes, but only received %v", entry.size, bytesRead)
	}

	return buffer, nil
}

// Delete removes a key and the value associated.
// TODO: allow list of keys
func (db *DB) Delete(key []byte) (bool, error) {
	pointer, offset, err := db.findValuePointerForKey(key)

	if err != nil {
		return false, fmt.Errorf("failed to retrieve pointer: %v", err)
	} else if pointer == nil {
		return false, nil
	}

	zeroedChunk := make([]byte, ValuePointerSize)
	if _, err := db.dict.WriteAt(zeroedChunk, int64(offset)); err != nil {
		return false, fmt.Errorf("failed to write to file: %v", err)
	}

	return true, nil
}

// Stat returns statistical information about the database.
func (db *DB) Stat() (*DataBaseStats, error) {
	dictInfo, dictErr := db.dict.Stat()
	if dictErr != nil {
		return nil, dictErr
	}

	dataInfo, dataErr := db.data.Stat()
	if dataErr != nil {
		return nil, dictErr
	}

	return &DataBaseStats{
		entries: uint32(dictInfo.Size() / ValuePointerSize),
		data:    uint32(dataInfo.Size() - 1),
		dict:    uint32(dictInfo.Size()),
	}, nil
}

// RunGC runs the garbage collector to compress both the value pointer file
// and remove no longer needed data from the data file.
// TODO: keep track of offset for removal
func (db *DB) RunGC() error {
	dictChunk := make([]byte, ValuePointerSize)
	dictWriteOffset := int64(0)
	dictReadOffset := int64(0)

	dataWriteOffset := uint32(1)

	// shift data to the left
	for {
		if _, err := db.dict.ReadAt(dictChunk, dictReadOffset); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to read file at %v: %v", dictReadOffset, err)
		}

		valuePointer, err := DecodeValuePointer(dictChunk)
		if err != nil {
			return fmt.Errorf("database corrupt: %v", err)
		}

		// check if there is a "hole"
		if !valuePointer.IsEmpty() {

			// check if there is space to shift data
			if dictReadOffset > dictWriteOffset {

				// read data referring to in this chunk
				dataChunk := make([]byte, valuePointer.size)
				if _, err = db.data.ReadAt(dataChunk, int64(valuePointer.offset)); err != nil {
					return fmt.Errorf("failed to read file at %v: %v", valuePointer.offset, err)
				}

				// update offset in dict entry
				valuePointer.offset = dataWriteOffset

				// update and move chunk in dictionary
				if _, err := db.dict.WriteAt(EncodeValuePointer(valuePointer), dictWriteOffset); err != nil {
					return fmt.Errorf("failed to shift valuePointer: %v", err)
				}

				// move actual data
				// TODO: if this fails the database is corrupt
				if _, err := db.data.WriteAt(dataChunk, int64(dataWriteOffset)); err != nil {
					return fmt.Errorf("failed to shift valuePointer: %v", err)
				}
			}

			dataWriteOffset += valuePointer.size
			dictWriteOffset += ValuePointerSize
		}

		dictReadOffset += ValuePointerSize
	}

	// truncate files
	if err := TruncateAndSeek(db.dict, dictWriteOffset); err != nil {
		return fmt.Errorf("failed to truncate dictionary: %v", err)
	}

	if err := TruncateAndSeek(db.data, int64(dataWriteOffset)); err != nil {
		return fmt.Errorf("failed to truncate data: %v", err)
	}

	return nil
}

// findValuePointerForKey returns the information relating the given key.
// In case there is no such entry it returns nil.
func (db *DB) findValuePointerForKey(key []byte) (*ValuePointer, uint32, error) {
	hashedKey, err := SHA256(key)
	if err != nil {
		return nil, 0, err
	}

	chunk := make([]byte, ValuePointerSize)
	offset := uint32(0)

	// Loop trough dict file to find offset
	for {
		_, err := db.dict.ReadAt(chunk, int64(offset))

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, fmt.Errorf("failed to read file at %v: %v", offset, err)
		} else if reflect.DeepEqual(chunk[:32], hashedKey) {

			// Check if pointer is marked for garbage collection
			if IsValuePointerEmpty(chunk) {
				return nil, offset, nil
			} else {
				pointer, err := DecodeValuePointer(chunk)
				return pointer, offset, err
			}
		}

		offset += ValuePointerSize
	}

	return nil, 0, nil // Entry not found
}
