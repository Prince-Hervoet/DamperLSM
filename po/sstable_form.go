package pojo

import "DamperLSM/util"

type IndexAreaForm struct {
	Offset int32
	KeyLen int32
	Key    string
}

func NewIndexAreaForm(offset, keyLen int32, key string) *IndexAreaForm {
	return &IndexAreaForm{
		Offset: offset,
		KeyLen: keyLen,
		Key:    key,
	}
}

func (here *IndexAreaForm) ToBytes() []byte {
	ans := make([]byte, 0, 4)
	bs := util.Int32ToBytes(here.Offset)
	ans = append(ans, bs...)
	bs = util.Int32ToBytes(here.KeyLen)
	ans = append(ans, bs...)
	keyBs := []byte(here.Key)
	ans = append(ans, keyBs...)
	return ans
}

type DataAreaForm struct {
	Deleted  int8
	ValueLen int32
	Value    []byte
}

func NewDataAreaForm(deleted int8, valueLen int32, value []byte) *DataAreaForm {
	return &DataAreaForm{
		Deleted:  deleted,
		ValueLen: valueLen,
		Value:    value,
	}
}

func (here *DataAreaForm) ToBytes() []byte {
	ans := make([]byte, 0, 5)
	ans = append(ans, byte(here.Deleted))
	bs := util.Int32ToBytes(here.ValueLen)
	ans = append(ans, bs...)
	ans = append(ans, here.Value...)
	return ans
}
