package util

const (
	DB_SAVE_FILE_NAME       = "dbfile"
	WAL_SAVE_FILE_NAME      = "walfile"
	IMMU_WAL_SAVE_FILE_NAME = "immuwalfile_"

	OP_TYPE_WRITE  = 1
	OP_TYPE_DELETE = 2

	// WAL_FILE_MAX_SIZE = 524288
	WAL_FILE_MAX_SIZE = 524288
	MAGIC_NUMBER      = int8(71)

	DEFAULT_LEVEL = int32(64)
)
