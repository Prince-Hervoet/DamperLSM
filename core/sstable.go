package core

import (
	pojo "DamperLSM/po"
	"DamperLSM/util"
	"fmt"
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
	file     *os.File
	count    int32
	keys     map[string]int32
}

func newSstableController(dir string) *SstableController {
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
				value, has, err := searchKvFromFile(node.file, offset)
				if err != nil {
					return nil, false
				}
				return value, has
			}
			node = node.next
		}
	}
	return nil, false
}

func (here *SstableController) recoverFromFiles() error {
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

		file, err := os.Open(here.dir + fileName)
		if err != nil {
			return err
		}

		node := &SstableNode{
			next:     nil,
			fileName: fileName,
			file:     file,
			count:    int32(count),
			keys:     keys,
		}
		here.addNode(int32(levelNumber), node)
	}
	go here.dumpTaskFunc()
	return nil
}

func (here *SstableController) dumpTaskFunc() {
	for v := range dumpQueue {
		fileName, count := here.dumpMemory(v)
		filepath := here.dir + fileName
		file, err := os.Open(filepath)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		keys, err := getIndexDataFromFile(filepath)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		node := &SstableNode{
			next:     nil,
			fileName: fileName,
			file:     file,
			count:    count,
			keys:     keys,
		}
		here.addNode(1, node)
		err = os.Remove(v.fileName)
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

// func (here *SstableController) dumpMemoryMmap(immuTable *memoryTable) (string, int32) {
// 	if immuTable == nil {
// 		return "", -1
// 	}
// 	count := here.headList[0].size + 1
// 	id := strconv.FormatInt(int64(count), 10)
// 	nFileName := util.DB_SAVE_FILE_NAME + "_" + "1" + "_" + id
// 	mm := openShareMemory()
// 	err := mm.openFile(here.dir+nFileName, util.WAL_FILE_MAX_SIZE)
// 	if err != nil {
// 		return "", -1
// 	}

// }

// 将数据持久化到磁盘文件中
func (here *SstableController) dumpMemory(immuTable *memoryTable) (string, int32) {
	if immuTable == nil {
		return "", -1
	}
	count := here.headList[0].size + 1
	id := strconv.FormatInt(int64(count), 10)
	nFileName := util.DB_SAVE_FILE_NAME + "_" + "1" + "_" + id
	filePtr, err := os.OpenFile(here.dir+nFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return "", -1
	}

	target := immuTable.memoryStruct
	run := target.headNode.Levels[0]
	offsets := make([]int32, 0, target.size)
	dataSizeRun := int32(0)
	indexSizeRun := int32(0)

	// 写入数据区
	for run != nil {
		value := run.Value
		valueLen := int32(len(value.Value))
		daf := pojo.NewDataAreaForm(value.Deleted, valueLen, value.Value)
		filePtr.Write(daf.ToBytes())
		offsets = append(offsets, dataSizeRun)
		dataSizeRun += (1 + 4 + valueLen)
		run = run.Levels[0]
	}

	// 写入索引区
	run = target.headNode.Levels[0]
	i := 0
	for run != nil {
		keyBs := []byte(run.Key)
		iaf := pojo.NewIndexAreaForm(offsets[i], int32(len(keyBs)), run.Key)
		filePtr.Write(iaf.ToBytes())
		indexSizeRun += (4 + 4 + int32(len(keyBs)))
		run = run.Levels[0]
		i += 1
	}

	// 写入元数据区
	indexLenBs := util.Int32ToBytes(indexSizeRun)
	dataLenBs := util.Int32ToBytes(dataSizeRun)
	filePtr.Write(dataLenBs)
	filePtr.Write(indexLenBs)

	return nFileName, count
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

func searchKvFromFile(filePtr *os.File, offset int32) ([]byte, bool, error) {
	defer filePtr.Seek(0, 0)
	// 设置偏移量
	_, err := filePtr.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, false, err
	}
	// 数据区的格式  deleted  valueLen  value
	//              1        4         ?

	buffer := make([]byte, 5)
	_, err = filePtr.Read(buffer)
	if err != nil {
		return nil, false, err
	}
	deleted := int8(buffer[0])
	if deleted == util.OP_TYPE_DELETE {
		return nil, false, nil
	}
	valueLen := util.BytesToInt32(buffer[1:5])
	valueBuffer := make([]byte, valueLen)
	_, err = filePtr.Read(valueBuffer)
	if err != nil {

		return nil, false, err
	}

	return valueBuffer, true, nil
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
