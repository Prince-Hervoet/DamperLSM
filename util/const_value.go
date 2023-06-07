package util

const (
	// 文件名称
	DB_SAVE_FILE_NAME       = "dbfile"
	WAL_SAVE_FILE_NAME      = "walfile"
	IMMU_WAL_SAVE_FILE_NAME = "immuwalfile_"

	// 操作类型
	OP_TYPE_WRITE  = 1
	OP_TYPE_DELETE = 2

	// wal文件默认大小
	WAL_FILE_MAX_SIZE = 25

	// 魔数
	MAGIC_NUMBER = int8(64)

	// 默认sstable树的层数
	SSTABLE_LEVEL_SIZE = int32(15)
)
