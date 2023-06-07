package core

type DamperDb struct {
	mcer      *memoryController
	scer      *SstableController
	dir       string
	size      int64
	isStarted bool
}

func NewDamperDb(dir string) (*DamperDb, error) {

	// 创建启动内存控制器
	mcer := newMemoryController(dir)
	err := mcer.recoverFromFiles()
	if err != nil {
		return nil, err
	}

	// 创建启动磁盘映射控制器
	scer := newSstableController(dir)
	err = scer.recoverFromFiles()
	if err != nil {
		return nil, err
	}

	return &DamperDb{
		mcer:      mcer,
		scer:      scer,
		dir:       dir,
		size:      0,
		isStarted: false,
	}, nil
}

func (here *DamperDb) Set(key string, value []byte) error {
	err := here.mcer.write(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (here *DamperDb) Get(key string) ([]byte, bool) {
	v, has := here.mcer.read(key)
	if has {
		return v, has
	}
	value, has := here.scer.searchData(key)
	return value, has
}

func (here *DamperDb) Remove(key string) {

}

func (here *DamperDb) ContainsKey(key string) bool {
	return false
}

func (here *DamperDb) Size() int64 {
	return 0
}

func (here *DamperDb) createSstable() {

}
