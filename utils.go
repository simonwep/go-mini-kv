package go_mini_kv

import "crypto/sha256"

func SHA256(data []byte) ([]byte, error) {
	hash := sha256.New()
	_, err := hash.Write(data)
	return hash.Sum(nil), err
}
