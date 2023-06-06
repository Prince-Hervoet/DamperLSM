package core

import (
	"DamperLSM/util"
	"errors"
	"fmt"
	"os"
	"syscall"
)

type MmapMemory struct {
	filePath string
	filePtr  *os.File
	isOpened bool
	mapping  []byte
	size     int32
	cap      int32
}

func OpenShareMemory() *MmapMemory {
	return &MmapMemory{
		filePath: "",
		filePtr:  nil,
		isOpened: false,
	}
}

func (here *MmapMemory) Size() int32 {
	return here.size
}

func (here *MmapMemory) OpenFile(filePath string, cap int32) error {
	if here.isOpened {
		return errors.New("a mapping has been established")
	}
	filePtr, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("open file error")
		filePtr.Close()
		return err
	}

	mapping, err := mmap(filePtr, cap)
	if err != nil {
		fmt.Println("mmap file error")
		filePtr.Close()
		return err
	}
	err = grow(filePtr, cap)
	if err != nil {
		fmt.Println("grow file error")
		filePtr.Close()
		return err
	}
	here.readHeader()
	here.filePtr = filePtr
	here.cap = cap
	here.filePath = filePath
	here.isOpened = true
	here.mapping = mapping
	return nil
}

func (here *MmapMemory) Close() error {
	if !here.isOpened {
		return nil
	}
	err := munmap(here.mapping)
	if err != nil {
		return err
	}
	err = here.filePtr.Close()
	if err != nil {
		return err
	}
	here.filePtr = nil
	here.filePath = ""
	here.mapping = nil
	here.isOpened = false
	return nil
}

func (here *MmapMemory) Append(data []byte) int {
	if !here.isOpened {
		return -1
	} else if len(data)+int(here.size) > int(here.cap) {
		return 0
	}
	start := here.size
	run := 0
	for i := start; i < here.cap && run < len(data); i++ {
		here.mapping[i] = data[run]
		run += 1
	}
	here.size += int32(len(data))
	here.writeHeader(here.size)
	// here.readHeader()
	return 1
}

func (here *MmapMemory) Read(bs []byte) (int32, error) {
	if !here.isOpened {
		return 0, errors.New("please open a file")
	}
	if here.size == 0 {
		return 0, nil
	}
	ansLen := util.IntMin(len(bs), int(here.size))
	for i := 0; i < ansLen; i++ {
	}
	return int32(ansLen), nil
}

func (here *MmapMemory) readHeader() {
	temp := here.mapping[0:4]
	num := util.BytesToInt32(temp)
	here.size = num + 4
}

func (here *MmapMemory) writeHeader(size int32) {
	bs := util.Int32ToBytes(size)
	for i := 0; i < 4; i++ {
		here.mapping[i] = bs[i]
	}
}

func mmap(file *os.File, cap int32) ([]byte, error) {
	bs, err := syscall.Mmap(int(file.Fd()), 0, int(cap), syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func munmap(mapping []byte) error {
	err := syscall.Munmap(mapping)
	if err != nil {
		return err
	}
	return nil
}

func grow(file *os.File, cap int32) error {
	err := file.Truncate(int64(cap))
	if err != nil {
		return err
	}
	return nil
}

func PathExists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		return os.IsExist(err)
	}
	return true
}
