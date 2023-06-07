package core

import (
	"math/rand"
)

const (
	MAX_LEVEL = 32
)

var LEVEL_FLAG = 0.45

type SkipTable struct {
	maxLevel int32
	size     int32
	headNode *SkipTableNode
}

type SkipTableNode struct {
	Levels []*SkipTableNode
	Prev   *SkipTableNode
	Key    string
	Value  *tableDataNode
}

func NewSkipTable() *SkipTable {
	return &SkipTable{
		maxLevel: 0,
		size:     0,
		headNode: newSkipTableNode("", nil, MAX_LEVEL),
	}
}

func newSkipTableNode(key string, value *tableDataNode, needLevel int32) *SkipTableNode {
	return &SkipTableNode{
		Levels: make([]*SkipTableNode, needLevel+1),
		Key:    key,
		Value:  value,
	}
}

func (here *SkipTable) Insert(key string, value *tableDataNode) {
	if key == "" {
		return
	}
	needLevel := int32(0)
	for (rand.Uint32()&0xFFFF) < uint32(0xFFFF*LEVEL_FLAG) && (needLevel < MAX_LEVEL-1) {
		needLevel += 1
	}
	if needLevel > here.maxLevel {
		here.maxLevel = needLevel
	}
	run := here.headNode
	stn := newSkipTableNode(key, value, needLevel)
	needUpdate := make([]*SkipTableNode, needLevel+1)

	for i := here.maxLevel; i >= 0; i-- {
		for run.Levels[i] != nil && ((run.Levels[i].Key < key) || (run.Levels[i].Key == key)) {
			if run.Levels[i].Key == key {
				run.Levels[i].Value = value
				return
			}
			run = run.Levels[i]
		}
		if i <= needLevel {
			needUpdate[i] = run
		}
	}

	for i := int32(0); i <= needLevel; i++ {
		stn.Levels[i] = needUpdate[i].Levels[i]
		needUpdate[i].Levels[i] = stn
	}
	here.size += 1
}

func (here *SkipTable) Get(key string) (string, *tableDataNode, bool) {
	if here.size == 0 {
		return "", nil, false
	}
	run := here.headNode
	for i := here.maxLevel; i >= 0; i-- {
		for run.Levels[i] != nil && run.Levels[i].Key < key {
			run = run.Levels[i]
		}
		if run.Levels[i] != nil && run.Levels[i].Key == key {
			return key, run.Levels[i].Value, true
		}
	}
	return "", nil, false
}

func (here *SkipTable) Remove(key string) {
	if here.size == 0 {
		return
	}
	run := here.headNode
	needMax := int32(-1)
	needUpdate := make([]*SkipTableNode, here.maxLevel)
	for i := here.maxLevel; i >= 0; i-- {
		for run.Levels[i] != nil && ((run.Levels[i].Key < key) || (run.Levels[i].Key == key)) {
			if run.Levels[i].Key == key {
				needUpdate[i] = run
				if needMax == -1 {
					needMax = i
				}
			} else {
				run = run.Levels[i]
			}
		}
	}
	if needMax != -1 {
		for i := int32(0); i < needMax; i++ {
			if needUpdate[i].Levels[i] != nil {
				needUpdate[i].Levels[i] = needUpdate[i].Levels[i].Levels[i]
			} else {
				needUpdate[i].Levels[i] = nil
			}
		}
		here.size -= 1
	}
}

func (here *SkipTable) Size() int32 {
	return here.size
}

func (here *SkipTable) Level() int32 {
	return here.maxLevel
}
