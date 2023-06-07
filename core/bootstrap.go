package core

import (
	"fmt"
	"os"
)

type Bootstrap struct {
	mcer      *MemoryController
	dir       string
	size      int64
	isStarted bool
}

func NewBootstrap(dir string) (*Bootstrap, error) {
	mcer := NewMemoryController(dir)
	err := mcer.RecoverFromFiles()
	if err != nil {
		return nil, err
	}
	return &Bootstrap{
		mcer:      mcer,
		dir:       dir,
		size:      0,
		isStarted: false,
	}, nil
}

func (here *Bootstrap) Set(key string, value []byte) error {
	err := here.mcer.Write(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (here *Bootstrap) Get(key string) ([]byte, bool) {
	v, has := here.mcer.Read(key)
	return v, has
}

func (here *Bootstrap) Remove(key string) {

}

func (here *Bootstrap) ContainsKey(key string) bool {
	return false
}

func (here *Bootstrap) Size() int64 {
	return 0
}

func (here *Bootstrap) Start() error {
	// 检查路径合法性
	_, err := os.Stat(here.dir)
	if err != nil {
		fmt.Println("not exist dir")
		return err
	}
	// 创建sstables

	here.isStarted = true
	return nil
}

func (here *Bootstrap) createSstable() {

}
