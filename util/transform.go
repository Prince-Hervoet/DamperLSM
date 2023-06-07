package util

import (
	"encoding/binary"
	"os"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

// 字节数组转16位int
func BytesToInt16(data []byte) int16 {
	return int16(binary.BigEndian.Uint16(data))
}

func Int16ToBytes(num int16) []byte {
	ans := make([]byte, 4)
	binary.BigEndian.PutUint16(ans, uint16(num))
	return ans
}

// 字节数组转32位int
func BytesToInt32(data []byte) int32 {
	return int32(binary.BigEndian.Uint32(data))
}

func Int32ToBytes(num int32) []byte {
	ans := make([]byte, 4)
	binary.BigEndian.PutUint32(ans, uint32(num))
	return ans
}

func Int64ToBytes(number int64) []byte {
	ans := make([]byte, 8)
	binary.BigEndian.PutUint64(ans, uint64(number))
	return ans
}

func BytesToInt64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data))
}

func Int32Min(a, b int32) int32 {
	if a > b {
		return b
	}
	return a
}

func IntMin(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// 获取文件大小
func GetFileSize(filePath string) (int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// 获取一个新的文件名称
func GetNewFileName(preFileName string) string {
	id, _ := gonanoid.New()
	return preFileName + id
}
