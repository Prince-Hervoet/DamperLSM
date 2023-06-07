package pojo

import "DamperLSM/util"

type WalForm struct {
	OpType   int8
	KeyLen   int32
	ValueLen int32
	Key      string
	Value    []byte
}

func NewWalForm(opType int8, key string, value []byte) *WalForm {
	keyBs := []byte(key)
	return &WalForm{
		OpType:   opType,
		KeyLen:   int32(len(keyBs)),
		ValueLen: int32(len(value)),
		Key:      key,
		Value:    value,
	}
}

func (here *WalForm) ToBytes() []byte {
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
