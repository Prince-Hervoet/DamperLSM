package core

import (
	"DamperLSM/util"
	"os"
	"strconv"
	"sync"
)

// 磁盘映射结构
type SstableController struct {
	headList []*SstableHeadNode
	maxLevel int32
	nodeSize int32
	dir      string
	mu       sync.Mutex
}

// 映射结构的头节点
type SstableHeadNode struct {
	next *SstableNode
	size int32
}

// 映射结构的节点
type SstableNode struct {
	next *SstableNode
	keys map[string]*FileIndexInfo
}

type FileIndexInfo struct {
	valueLen int32
	offset   int32
}

func NewSstableController(dir string) *SstableController {
	return &SstableController{
		headList: make([]*SstableHeadNode, util.DEFAULT_LEVEL),
		maxLevel: 0,
		nodeSize: 0,
		dir:      dir,
	}
}

func (here *SstableController) AddNodeFromFile(filePath string) {
	// dbfile_1_2
	// 获取第一层的最新一个id
	// id := here.headList[0].size + 1
	// 拼接文件名
	// nFileName := util.DB_SAVE_FILE_NAME + "_" + "1" + "_" + strconv.FormatInt(int64(id), 10)
	filePtr, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return
	}

	count, err := util.GetFileSize(filePath)
	if err != nil {
		return
	}
	if count < 8 {
		return
	}

	// 读取元数据区
	indexLen, _, err := readMeta(filePtr)

	indexBuffer := make([]byte, indexLen)
	n, err := filePtr.Read(indexBuffer)
	keys := make(map[string]*FileIndexInfo)
	current := 0
	for current < n {
		fi := &FileIndexInfo{}
		keyLen := util.BytesToInt32(indexBuffer[0:4])
		valueLen := util.BytesToInt32(indexBuffer[4:8])
		offset := util.BytesToInt32(indexBuffer[8:12])
		key := string(indexBuffer[12 : 12+keyLen])
		fi.offset = offset
		fi.valueLen = valueLen
		keys[key] = fi
		current += int(12 + keyLen)
	}
	indexBuffer = nil
	node := &SstableNode{
		next: nil,
		keys: keys,
	}
	here.mu.Lock()
	defer here.mu.Unlock()
	node.next = here.headList[0].next
	here.headList[0].next = node
	here.headList[0].size += 1

}

func (here *SstableController) DumpMemory(immuTable *MemoryTable) {
	if immuTable == nil {
		return
	}
	id := strconv.FormatInt(int64(here.headList[0].size+1), 10)
	filePtr, err := os.OpenFile(here.dir+id, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return
	}

	target := immuTable.memoryStruct
	run := target.headNode.Levels[0]
	offsets := make([]int32, 0, target.size)
	offsetRun := int32(0)
	dataSizeRun := int32(0)
	indexSizeRun := int32(0)

	// 写入数据区
	for run != nil {
		key := []byte(run.Key)
		value := run.Value
		if value.Deleted {
			run = run.Levels[0]
			continue
		}
		filePtr.Write(key)
		filePtr.Write(value.Value)
		offsets = append(offsets, offsetRun)
		dataSizeRun += (int32(len(key)) + int32(len(value.Value)))
		run = run.Levels[0]
	}

	// 写入索引区
	for i := 0; i < len(offsets); i++ {
		bs := util.Int32ToBytes(offsets[i])
		filePtr.Write(bs)
		indexSizeRun += int32(len(bs))
	}

	// 写入元数据区
	indexLenBs := util.Int32ToBytes(indexSizeRun)
	dataLenBs := util.Int32ToBytes(dataSizeRun)
	filePtr.Write(indexLenBs)
	filePtr.Write(dataLenBs)
}

func readMeta(file *os.File) (int32, int32, error) {
	buffer := make([]byte, 8)
	_, err := file.Read(buffer)
	if err != nil {
		return -1, -1, nil
	}
	indexLen := util.BytesToInt32(buffer[0:4])
	dataLen := util.BytesToInt32(buffer[4:])
	return indexLen, dataLen, nil
}
