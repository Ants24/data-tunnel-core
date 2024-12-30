package datatunnelcore

import (
	"context"
	"database/sql"

	common "github.com/Ants24/data-tunnel-common"
)

type DBClient interface {
	GetTableNames(logger common.Logger, schema string) ([]string, error)
	GetSchemaNames(logger common.Logger) ([]string, error)
	ConventTableAndColumnName(name string, fromDBType common.DBType) string
	GetTableColumnNames(ctx context.Context, schema string, tableName string, columnNames []string, fromDBType common.DBType) ([]string, []string, error)
	ReadData(ctx context.Context, logger common.Logger, taskConfig TaskFullTable, columnNames []string, columnTypes []string, channel chan []sql.NullString) (uint64, error)
	WriteData(ctx context.Context, logger common.Logger, taskConfig TaskFullTable, columnNames []string, columnTypes []string, channel chan []sql.NullString) error
	GenerateSubTasks(ctx context.Context, logger common.Logger, taskConfig TaskFullTable) ([]TaskFullTable, error)
	GetTablePrimaryKeys(ctx context.Context, logger common.Logger, schemaName string, tableName string) ([]string, error)
	GetTablePartitionNames(ctx context.Context, logger common.Logger, schemaName string, tableName string) ([]string, error)
	SelectTableCount(ctx context.Context, logger common.Logger, tableBase TableBase) (uint64, error)
	TruncateTable(ctx context.Context, logger common.Logger, schemaName string, tableName string) error
	GetTableDetail(ctx context.Context, logger common.Logger, taskConfig TaskObjectTable) (TableDetail, error)
	GenerateTableDetail(ctx context.Context, logger common.Logger, tableDetail TableDetail) (TableDetail, error)
	GenerateTableSql(ctx context.Context, logger common.Logger, taskConfig TaskObjectTable, tableDetail TableDetail) ([]string, error)
}
