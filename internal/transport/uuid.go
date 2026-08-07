package transport

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"proxy/internal/constants"
)

var (
	uuidBytes     []byte
	uuidOnce      sync.Once
	uuidInitError error
)

func InitUUID(uuidStr string) error {
	uuidOnce.Do(func() {
		cleaned := strings.ReplaceAll(uuidStr, "-", "")
		if len(cleaned) != 32 {
			uuidInitError = fmt.Errorf("invalid UUID length: %d", len(cleaned))
			return
		}
		b, err := hex.DecodeString(cleaned)
		if err != nil {
			uuidInitError = err
			return
		}
		if len(b) != constants.UUIDLength {
			uuidInitError = fmt.Errorf("decoded UUID length != 16: %d", len(b))
			return
		}
		uuidBytes = b
	})
	return uuidInitError
}

// GetUUIDBytes 返回内部 UUID 字节的只读副本。
// 调用方不得修改返回的切片内容。
func GetUUIDBytes() []byte {
	if uuidBytes == nil {
		return nil
	}
	copyBytes := make([]byte, constants.UUIDLength)
	copy(copyBytes, uuidBytes)
	return copyBytes
}

func MatchUUID(data []byte) bool {
	if len(data) < constants.UUIDLength+1 {
		return false
	}
	if uuidBytes == nil || len(uuidBytes) != constants.UUIDLength {
		return false
	}
	for i := 0; i < constants.UUIDLength; i++ {
		if data[1+i] != uuidBytes[i] {
			return false
		}
	}
	return true
}
