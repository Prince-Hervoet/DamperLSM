package core

import "DamperLSM/util"

type walForm struct {
	OpType   int8
	KeyLen   int32
	ValueLen int32
	Key      string
	Value    []byte
}

func newWalForm(opType int8, key string, value []byte) *walForm {
	keyBs := []byte(key)
	return &walForm{
		OpType:   opType,
		KeyLen:   int32(len(keyBs)),
		ValueLen: int32(len(value)),
		Key:      key,
		Value:    value,
	}
}

func (here *walForm) ToBytes() []byte {
	ans := make([]byte, 0, 9)
	ans = append(ans, byte(here.OpType))
	bs := util.Int32ToBytes(here.KeyLen)
	ans = append(ans, bs...)
	bs = util.Int32ToBytes(here.ValueLen)
	ans = append(ans, bs...)
	keyBs := []byte(here.Key)
	ans = append(ans, keyBs...)
	ans = append(ans, here.Value...)
	return ans
}
