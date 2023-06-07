package core

import (
	"DamperLSM/pojo"
	"DamperLSM/util"
	"errors"
	"io/ioutil"
	"os"
	"strings"
)

type MemoryController struct {
	running   *MemoryTable
	immuTable *MemoryTable
	scer      *SstableController
	waitQueue chan *MemoryTable
	dir       string
}

type MemoryTable struct {
	fileName     string
	walMapping   *MmapMemory
	memoryStruct *SkipTable
}

type TableDataNode struct {
	Value   []byte
	Deleted bool
}

func NewMemoryController(dir string) *MemoryController {
	return &MemoryController{
		running:   nil,
		immuTable: nil,
		dir:       dir,
		scer:      NewSstableController(dir),
		waitQueue: make(chan *MemoryTable, 64),
	}
}

func newMemoryTable(dir, fileName string) (*MemoryTable, error) {
	mm := OpenShareMemory()
	err := mm.OpenFile(dir+fileName, util.WAL_FILE_MAX_SIZE)
	if err != nil {
		return nil, err
	}
	return &MemoryTable{
		fileName:     fileName,
		walMapping:   mm,
		memoryStruct: NewSkipTable(),
	}, nil
}

func (here *MemoryController) Init() error {
	walFileName := ""
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
		if fileName == util.WAL_SAVE_FILE_NAME && walFileName == "" {
			walFileName = fileName
		} else if strings.HasPrefix(fileName, util.IMMU_WAL_SAVE_FILE_NAME) {
			immuWalFileNames = append(immuWalFileNames, fileName)
		}
	}

	var st *SkipTable = nil
	if walFileName == "" {
		walFileName = util.WAL_SAVE_FILE_NAME
		st = NewSkipTable()
	} else {
		st, err = ReadWalFileToSkipTable(here.dir + walFileName)
		if err != nil {
			return err
		}
	}

	mm := OpenShareMemory()
	err = mm.OpenFile(here.dir+util.WAL_SAVE_FILE_NAME, util.WAL_FILE_MAX_SIZE)
	if err != nil {
		return err
	}
	mtable := &MemoryTable{
		fileName:     walFileName,
		walMapping:   mm,
		memoryStruct: st,
	}
	here.running = mtable
	go here.flush()
	return nil
}

func (here *MemoryController) Write(key string, value []byte) error {
	wf := pojo.NewWalForm(1, key, value)
	walBuffer := wf.ToBytes()
	res := here.running.walMapping.Append(walBuffer)
	if res == 0 {
		// 空间已满
		nFileName := util.GetNewFileName(here.dir + util.IMMU_WAL_SAVE_FILE_NAME)
		os.Rename(here.dir+here.running.fileName, nFileName)
		here.immuTable = here.running
		here.immuTable.walMapping.Close()
		here.immuTable.walMapping = nil
		here.immuTable.fileName = nFileName
		mt, err := newMemoryTable(here.dir, util.WAL_SAVE_FILE_NAME)
		if err != nil {
			return err
		}
		here.running = mt
		here.running.walMapping.Append(walBuffer)
		// here.waitQueue <- here.immuTable
	} else if res == -1 {
		return errors.New("write error")
	}
	here.running.memoryStruct.Insert(key, &TableDataNode{
		Value:   value,
		Deleted: false,
	})
	return nil
}

func (here *MemoryController) Read(key string) ([]byte, error) {
	_, v, err := here.running.memoryStruct.Get(key)
	if err != nil {
		return nil, err
	}
	if v.Deleted {
		return nil, nil
	}
	return v.Value, nil
}

func (here *MemoryController) flush() {
	for v := range here.waitQueue {
		here.scer.DumpMemory(v)
	}
}

// 从文件中读取信息恢复跳表结构
func ReadWalFileToSkipTable(filePath string) (*SkipTable, error) {
	filePtr, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer filePtr.Close()

	opBuffer := make([]byte, 1)
	buffer := make([]byte, 4)
	current := int32(0)

	// 判断魔数
	_, err = filePtr.Read(opBuffer)
	if err != nil {
		return nil, err
	}
	magicNumber := int8(opBuffer[0])
	if magicNumber != util.MAGIC_NUMBER {
		return nil, errors.New("magic number error")
	}

	// 读取文件数据容量
	_, err = filePtr.Read(buffer)
	if err != nil {
		return nil, err
	}
	cap := util.BytesToInt32(buffer)
	st := NewSkipTable()
	for current < cap {
		// 读取操作类型
		_, err := filePtr.Read(opBuffer)
		if err != nil {
			return nil, err
		}
		op := int8(opBuffer[0])

		// 读取key的长度信息
		_, err = filePtr.Read(buffer)
		if err != nil {
			return nil, err
		}

		keyLen := util.BytesToInt32(buffer)

		// 读取value的长度信息
		_, err = filePtr.Read(buffer)
		if err != nil {
			return nil, err
		}
		valueLen := util.BytesToInt32(buffer)

		// 根据上述信息读取key和value的字节数组
		keyBuffer := make([]byte, keyLen)
		valueBuffer := make([]byte, valueLen)

		_, err = filePtr.Read(keyBuffer)
		if err != nil {
			return nil, err
		}

		_, err = filePtr.Read(valueBuffer)
		if err != nil {
			return nil, err
		}

		current += int32(1 + 4 + 4 + keyLen + valueLen)

		if op == util.OP_TYPE_DELETE {
			continue
		}

		key := string(keyBuffer)

		td := &TableDataNode{
			Value:   valueBuffer,
			Deleted: false,
		}
		st.Insert(key, td)
	}

	return st, nil
}
