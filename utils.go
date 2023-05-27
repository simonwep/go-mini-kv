package go_mini_kv

import (
	"crypto/sha256"
	"fmt"
	"os"
)

func SHA256(data []byte) ([]byte, error) {
	hash := sha256.New()
	_, err := hash.Write(data)
	return hash.Sum(nil), err
}

func TruncateAndSeek(file *os.File, newSize int64) error {
	if err := file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to truncate file: %v", err)
	} else if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed seek file: %v", err)
	}

	return nil
}
