package go_mini_kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeValuePointer(t *testing.T) {
	hash, err := SHA256([]byte("hello world"))
	assert.Nil(t, err)

	pointer := &ValuePointer{
		hash:   hash,
		offset: 2,
		size:   5,
	}

	encoded := EncodeValuePointer(pointer)
	decoded, err := DecodeValuePointer(encoded)

	assert.Nil(t, err)
	assert.Equal(t, *pointer, *decoded)
}

func TestValuePointer_IsZero(t *testing.T) {
}
