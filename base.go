package datatunnelcore

import (
	"context"

	common "github.com/Ants24/data-tunnel-common"
)

var (
	JobCancelList                = make(map[uint]context.CancelFunc, 0)
	TaskFullTableResultChannel   = make(chan TaskFullTableResult, 10000)
	TaskVerifyTableResultChannel = make(chan TaskVerifyTableResult, 10000)
	TaskObjectTableResultChannel = make(chan TaskObjectTableResult, 10000)
)

type MigrationTask[T TaskObjectTable | TaskFullTable | TaskVerifyTable] struct {
	JobCode      string
	JobId        uint
	Parallel     int
	Tables       []T
	SourceConfig common.DBConfig
	DestConfig   common.DBConfig
}

type TableBase struct {
	SchemeName string
	TableName  string
	Filter     string
}
type TableDetail struct {
	TableId     uint
	FromDBType  common.DBType
	Indexes     []common.TableIndex
	Columns     []common.TableColumn
	PrimaryKeys []string
	Comment     string
}
