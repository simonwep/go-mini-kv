package go_mini_kv

import (
	"encoding/binary"
	"fmt"
)

const ValuePointerSize = 32 + 4 + 4

// A ValuePointer represents a single value in the database.
// Each pointer looks like the following:
// +------+--------+------+
// | Hash | Offset | Stat |
// +------+--------+------+
type ValuePointer struct {
	hash   []byte // 256bit hash
	offset uint32
	size   uint32
}

func DecodeValuePointer(data []byte) (*ValuePointer, error) {
	if len(data) != ValuePointerSize {
		return nil, fmt.Errorf("a ValuePointer must have a size of %v bytes", ValuePointerSize)
	}

	return &ValuePointer{
		hash:   GetValuePointerHash(data),
		offset: GetValuePointerOffset(data),
		size:   GetValuePointerSize(data),
	}, nil
}

func EncodeValuePointer(pointer *ValuePointer) []byte {
	data := make([]byte, ValuePointerSize)
	copy(data[:32], pointer.hash)
	copy(data[32:36], encodeUint32(pointer.offset))
	copy(data[36:40], encodeUint32(pointer.size))
	return data
}

func (v *ValuePointer) IsEmpty() bool {
	return v.offset == 0
}

func IsValuePointerEmpty(data []byte) bool {
	return decodeUint32(data[32:36]) == 0
}

func GetValuePointerHash(data []byte) []byte {
	return data[:32]
}

func GetValuePointerSize(data []byte) uint32 {
	return decodeUint32(data[36:40])
}

func GetValuePointerOffset(data []byte) uint32 {
	return decodeUint32(data[32:36])
}

func encodeUint32(v uint32) []byte {
	arr := make([]byte, 4)
	binary.BigEndian.PutUint32(arr, v)
	return arr
}

func decodeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

type DataBaseStats struct {
	entries uint32
	data    uint32
	dict    uint32
}
