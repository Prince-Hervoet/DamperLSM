package core

import (
	"DamperLSM/util"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
)

// 文件存储结构
// 	    数据区			  索引区                 元数据区
// valueLen value	offset keyLen key    dataAreaLen indexAreaLen

var dumpQueue chan *memoryTable = make(chan *memoryTable, 64)

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
	head *SstableNode
	size int32
}

// 映射结构的节点
type SstableNode struct {
	next     *SstableNode
	fileName string
	count    int32
	keys     map[string]int32
}

func NewSstableController(dir string) *SstableController {
	headList := make([]*SstableHeadNode, util.SSTABLE_LEVEL_SIZE)
	for i := 0; i < len(headList); i++ {
		headList[i] = &SstableHeadNode{
			head: &SstableNode{
				next:     nil,
				fileName: "",
				count:    -1,
				keys:     nil,
			},
			size: 0,
		}
	}
	return &SstableController{
		headList: headList,
		maxLevel: 0,
		nodeSize: 0,
		dir:      dir,
	}
}

func (here *SstableController) searchData(key string) ([]byte, bool) {
	headList := here.headList
	for i := 0; i < len(headList); i++ {
		shn := headList[i]
		if shn.size == 0 {
			continue
		}
		node := shn.head.next
		for node != nil {
			if _, has := node.keys[key]; has {
				offset := node.keys[key]
				filePath := here.dir + node.fileName
				value, err := searchKvFromFile(filePath, offset)
				if err != nil {
					return nil, false
				}
				return value, true
			}
			node = node.next
		}
	}
	return nil, false
}

func (here *SstableController) RecoverFromFiles() error {
	// dbfile_1_2
	dirInfo, err := ioutil.ReadDir(here.dir)
	if err != nil {
		return err
	}

	for _, file := range dirInfo {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		if !strings.HasPrefix(fileName, util.DB_SAVE_FILE_NAME) {
			continue
		}

		ss := strings.Split(fileName, "_")
		if len(ss) < 3 {
			continue
		}

		levelNumber, err := strconv.ParseInt(ss[1], 10, 64)
		if err != nil {
			continue
		}

		count, err := strconv.ParseInt(ss[2], 10, 64)
		if err != nil {
			continue
		}

		keys, err := getIndexDataFromFile(here.dir + fileName)
		if err != nil {
			continue
		}

		node := &SstableNode{
			next:     nil,
			fileName: fileName,
			count:    int32(count),
			keys:     keys,
		}
		here.addNode(int32(levelNumber), node)
	}
	go here.DumpTaskFunc()
	return nil
}

func (here *SstableController) DumpTaskFunc() {
	for v := range dumpQueue {
		here.DumpMemory(v)
	}
}

func (here *SstableController) DumpMemory(immuTable *memoryTable) {
	if immuTable == nil {
		return
	}

	id := strconv.FormatInt(int64(here.headList[0].size+1), 10)
	nFileName := util.DB_SAVE_FILE_NAME + "_" + "1" + "_" + id
	filePtr, err := os.OpenFile(here.dir+nFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return
	}

	target := immuTable.memoryStruct
	run := target.headNode.Levels[0]
	offsets := make([]int32, 0, target.size)
	dataSizeRun := int32(0)
	indexSizeRun := int32(0)

	// 写入数据区
	for run != nil {
		value := run.Value
		if value.Deleted {
			run = run.Levels[0]
			continue
		}
		valueLenBs := util.Int32ToBytes(int32(len(value.Value)))
		filePtr.Write(valueLenBs)
		filePtr.Write(value.Value)
		offsets = append(offsets, dataSizeRun)
		dataSizeRun += (4 + int32(len(value.Value)))
		run = run.Levels[0]
	}

	// 写入索引区
	run = target.headNode.Levels[0]
	i := 0
	for run != nil {
		keyBs := []byte(run.Key)
		bs := util.Int32ToBytes(offsets[i])
		keyLenBs := util.Int32ToBytes(int32(len(keyBs)))
		filePtr.Write(bs)
		filePtr.Write(keyLenBs)
		filePtr.Write(keyBs)
		indexSizeRun += (4 + 4 + int32(len(keyBs)))
		run = run.Levels[0]
		i += 1
	}

	// 写入元数据区
	indexLenBs := util.Int32ToBytes(indexSizeRun)
	dataLenBs := util.Int32ToBytes(dataSizeRun)
	filePtr.Write(dataLenBs)
	filePtr.Write(indexLenBs)
}

func (here *SstableController) addNode(level int32, node *SstableNode) {
	headNode := here.headList[level-1]
	run := headNode.head

	if run.next == nil {
		run.next = node
		headNode.size += 1
		return
	}

	isOk := false
	for run.next != nil {
		if run.next.count < node.count {
			temp := run.next
			run.next = node
			node.next = temp
			isOk = true
			break
		} else {
			run = run.next
		}
	}

	if !isOk {
		run.next = node
	}
	headNode.size += 1
}

func searchKvFromFile(filePath string, offset int32) ([]byte, error) {
	filePtr, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer filePtr.Close()
	// 设置偏移量
	_, err = filePtr.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	// 数据区的格式  valueLen  value
	//              4         ?
	lenBuffer := make([]byte, 4)
	_, err = filePtr.Read(lenBuffer)
	if err != nil {
		return nil, err
	}
	valueLen := util.BytesToInt32(lenBuffer)
	valueBuffer := make([]byte, valueLen)
	_, err = filePtr.Read(valueBuffer)
	if err != nil {
		return nil, err
	}
	return valueBuffer, nil
}

func getIndexDataFromFile(filePath string) (map[string]int32, error) {
	filePtr, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer filePtr.Close()

	keys := make(map[string]int32)
	_, err = filePtr.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	_, err = filePtr.Seek(-8, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// 读取元数据区
	lenBuffer := make([]byte, 4)
	_, err = filePtr.Read(lenBuffer)
	if err != nil {
		return nil, err
	}
	dataAreaLen := util.BytesToInt32(lenBuffer)

	_, err = filePtr.Read(lenBuffer)
	if err != nil {
		return nil, err
	}
	indexAreaLen := util.BytesToInt32(lenBuffer)

	// 跳过数据区
	_, err = filePtr.Seek(int64(dataAreaLen), io.SeekStart)
	if err != nil {
		return nil, err
	}

	// 开始读取索引区
	buffer := make([]byte, indexAreaLen)
	_, err = filePtr.Read(buffer)
	if err != nil {
		return nil, err
	}
	current := int32(0)
	for current < indexAreaLen {
		offset := util.BytesToInt32(buffer[current : current+4])
		current += 4
		keyLen := util.BytesToInt32(buffer[current : current+4])
		current += 4
		keyBuffer := buffer[current : current+keyLen]
		current += keyLen
		key := string(keyBuffer)
		keys[key] = offset
	}
	return keys, nil
}
