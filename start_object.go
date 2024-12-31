package datatunnelcore

import (
	"context"
	"sync"

	common "github.com/Ants24/data-tunnel-common"
	semaphore "github.com/marusama/semaphore/v2"
)

type TaskObjectTable struct {
	TableId      uint
	JobId        uint
	SourceSchema string
	SourceTable  string
	DestSchema   string
	DestTable    string
	Config       common.TableObjectConfig
}

type TaskObjectTableResult struct {
	JobId            uint
	TableId          uint
	TableComment     string
	FromDBType       common.DBType
	TableIndexes     []common.TableIndex
	TableColumns     []common.TableColumn
	TablePrimaryKeys []string
	Error            error
	Status           common.JobStatus
}

type TaskObjectStarter struct {
	MigrationTask[TaskObjectTable]
}

func (f *TaskObjectStarter) Start(ctx context.Context) error {

	logger := common.NewLogWithoutConfig(f.JobCode)
	defer logger.Sync()
	sourceDBClient, err := GetDBClient(ctx, *logger, f.SourceConfig)
	if err != nil {
		return err
	}
	destDBClient, err := GetDBClient(ctx, *logger, f.DestConfig)
	if err != nil {
		return err
	}
	var lastError error
	var wg sync.WaitGroup                   //并发控制,确保所有任务都完成
	semaphores := semaphore.New(f.Parallel) //限制并发数
	for _, table := range f.Tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + table.SourceTable)
		defer loggerTable.Sync()
		result := TaskObjectTableResult{
			JobId:   f.JobId,
			TableId: table.TableId,
			Status:  common.JobStatusRunning,
		}
		TaskObjectTableResultChannel <- result
		semaphores.Acquire(ctx, 1) //获取信号量
		wg.Add(1)
		go func(table TaskObjectTable) {
			defer wg.Done()
			defer semaphores.Release(1) // 确保信号量在任务完成后释放
			tableDetail, err := sourceDBClient.GetTableDetail(ctx, *loggerTable, table)
			if err != nil {
				if lastError == nil {
					lastError = err
				}
				f.handleError(*loggerTable, result, &tableDetail, err)
				return
			}
			tableDetail.FromDBType = f.SourceConfig.DBType
			tableDetail, err = destDBClient.GenerateTableDetail(ctx, *loggerTable, tableDetail)
			if err != nil {
				if lastError == nil {
					lastError = err
				}
				f.handleError(*loggerTable, result, &tableDetail, err)
				return
			}
			sql, err := destDBClient.GenerateTableSql(ctx, *loggerTable, table, tableDetail)
			if err != nil {
				if lastError == nil {
					lastError = err
				}
				f.handleError(*loggerTable, result, &tableDetail, err)
				return
			}
			loggerTable.Logger.Sugar().Infof("sqls: %v\n", sql)
			result.TableComment = tableDetail.Comment
			result.TableColumns = tableDetail.Columns
			result.Status = common.JobStatusFinished
			result.FromDBType = tableDetail.FromDBType
			result.TableIndexes = tableDetail.Indexes
			result.TablePrimaryKeys = tableDetail.PrimaryKeys
			TaskObjectTableResultChannel <- result
		}(table)
	}
	wg.Wait()
	return lastError
}

func (f *TaskObjectStarter) handleError(loggerTable common.Logger, result TaskObjectTableResult, tableDetail *TableDetail, err error) {
	loggerTable.Logger.Sugar().Errorf(" %v\n", err)
	result.Error = err
	result.Status = common.JobStatusFailed
	if tableDetail != nil {
		result.TableComment = tableDetail.Comment
		result.TableColumns = tableDetail.Columns
		result.FromDBType = tableDetail.FromDBType
		result.TableIndexes = tableDetail.Indexes
		result.TablePrimaryKeys = tableDetail.PrimaryKeys
	}
	TaskObjectTableResultChannel <- result
}
func (f *TaskObjectStarter) Execute(ctx context.Context, tableInfo TaskObjectTable, tableDetail TableDetail) error {
	loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + tableInfo.SourceTable)
	defer loggerTable.Sync()

	destDBClient, err := GetDBClient(ctx, *loggerTable, f.DestConfig)
	if err != nil {
		return err
	}
	result := TaskObjectTableResult{
		JobId:   f.JobId,
		TableId: tableInfo.TableId,
		Status:  common.JobStatusRunning,
	}
	TaskObjectTableResultChannel <- result
	sqls, err := destDBClient.GenerateTableSql(ctx, *loggerTable, tableInfo, tableDetail)
	if err != nil {
		f.handleError(*loggerTable, result, &tableDetail, err)
		return err
	}
	loggerTable.Logger.Sugar().Infof("sqls: %v\n", sqls)
	result.TableComment = tableDetail.Comment
	result.TableColumns = tableDetail.Columns
	result.Status = common.JobStatusFinished
	result.FromDBType = tableDetail.FromDBType
	result.TableIndexes = tableDetail.Indexes
	result.TablePrimaryKeys = tableDetail.PrimaryKeys
	TaskObjectTableResultChannel <- result
	return nil
}
