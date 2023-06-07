package core

import (
	pojo "DamperLSM/po"
	"DamperLSM/util"
	"errors"
	"io/ioutil"
	"os"
	"strings"
)

type memoryController struct {
	// 正在使用的内存结构
	running *memoryTable
	// 正在持久化的内存结构
	immuTable *memoryTable
	dir       string
}

type memoryTable struct {
	fileName     string
	walMapping   *mmapMemory
	memoryStruct *SkipTable
}

type tableDataNode struct {
	Value   []byte
	Deleted int8
}

func newMemoryController(dir string) *memoryController {
	return &memoryController{
		running:   nil,
		immuTable: nil,
		dir:       dir,
	}
}

func newMemoryTable(dir, fileName string) (*memoryTable, error) {
	mm := openShareMemory()
	err := mm.openFile(dir+fileName, util.WAL_FILE_MAX_SIZE)
	if err != nil {
		return nil, err
	}
	return &memoryTable{
		fileName:     fileName,
		walMapping:   mm,
		memoryStruct: NewSkipTable(),
	}, nil
}

func (here *memoryController) recoverFromFiles() error {

	hasWalFile := false
	var st *SkipTable = nil
	immuWalFileNames := make([]string, 0, 8)

	dirInfo, err := ioutil.ReadDir(here.dir)
	if err != nil {
		return err
	}

	for _, file := range dirInfo {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		if fileName == util.WAL_SAVE_FILE_NAME {
			hasWalFile = true
		} else if strings.HasPrefix(fileName, util.IMMU_WAL_SAVE_FILE_NAME) {
			immuWalFileNames = append(immuWalFileNames, fileName)
		}
	}

	// 打开共享内存，如果不存在wal文件则自动创建一个
	mm := openShareMemory()
	err = mm.openFile(here.dir+util.WAL_SAVE_FILE_NAME, util.WAL_FILE_MAX_SIZE)
	if err != nil {
		return err
	}

	if hasWalFile {
		// 如果目录中原来有wal文件，则进行恢复
		st, err = readWalFileToSkipTable(mm.mapping)
		if err != nil {
			return err
		}
	}

	if st == nil {
		st = NewSkipTable()
	}

	mtable := &memoryTable{
		fileName:     util.WAL_SAVE_FILE_NAME,
		walMapping:   mm,
		memoryStruct: st,
	}
	here.running = mtable
	return nil
}

func (here *memoryController) write(key string, value []byte) error {
	wf := pojo.NewWalForm(1, key, value)
	walBuffer := wf.ToBytes()
	res := here.running.walMapping.append(walBuffer)
	if res == 0 {
		// 空间已满
		nFileName := util.GetNewFileName(here.dir+util.IMMU_WAL_SAVE_FILE_NAME) + util.FILE_NAME_ADD
		os.Rename(here.dir+here.running.fileName, nFileName)
		here.immuTable = here.running
		here.immuTable.walMapping.close()
		here.immuTable.walMapping = nil
		here.immuTable.fileName = nFileName
		mt, err := newMemoryTable(here.dir, util.WAL_SAVE_FILE_NAME)
		if err != nil {
			return err
		}
		here.running = mt
		here.running.walMapping.append(walBuffer)
		dumpQueue <- here.immuTable
	} else if res == -1 {
		return errors.New("write error")
	}
	here.running.memoryStruct.Insert(key, &tableDataNode{
		Value:   value,
		Deleted: 0,
	})
	return nil
}

func (here *memoryController) read(key string) ([]byte, bool) {
	_, info, has := here.running.memoryStruct.Get(key)

	if !has && here.immuTable != nil {
		_, info, has = here.immuTable.memoryStruct.Get(key)
	}
	if !has {
		return nil, has
	}
	return info.Value, true
}

// 从共享内存中恢复跳表结构
func readWalFileToSkipTable(mm []byte) (*SkipTable, error) {
	if len(mm) < 5 {
		return nil, errors.New("empty wal file")
	}

	// 判断魔数
	if int8(mm[0]) != util.MAGIC_NUMBER {
		return nil, errors.New("magic number error")
	}

	st := NewSkipTable()
	// 获取size
	cap := util.BytesToInt32(mm[1:5])
	current := int32(5)

	for current < cap {
		op := int8(mm[current])
		current += 1

		keyLen := util.BytesToInt32(mm[current : current+4])
		current += 4

		valueLen := util.BytesToInt32(mm[current : current+4])
		current += 4

		key := string(mm[current : current+keyLen])
		current += keyLen

		value := mm[current : current+valueLen]
		current += valueLen

		st.Insert(key, &tableDataNode{
			Value:   value,
			Deleted: op,
		})
	}

	return st, nil
}
