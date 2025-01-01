package datatunnelcore

import (
	"context"
	"sync"

	common "github.com/Ants24/data-tunnel-common"
	semaphore "github.com/marusama/semaphore/v2"
)

type TaskObjectStarter struct {
	common.MigrationTask[common.TaskObjectTable]
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
		result := common.TaskObjectTableResult{
			JobId:   f.JobId,
			TableId: table.TableId,
			Status:  common.JobStatusRunning,
		}
		common.TaskObjectTableResultChannel <- result
		semaphores.Acquire(ctx, 1) //获取信号量
		wg.Add(1)
		go func(table common.TaskObjectTable) {
			defer wg.Done()
			defer semaphores.Release(1) // 确保信号量在任务完成后释放
			tableDetail, err := sourceDBClient.GetTableDetail(ctx, *loggerTable, table)
			if err != nil {
				lastError = err
				f.handleError(*loggerTable, result, &tableDetail, err)
				return
			}
			tableDetail.FromDBType = f.SourceConfig.DBType
			tableDetail, err = destDBClient.GenerateTableDetail(ctx, *loggerTable, tableDetail)
			if err != nil {
				lastError = err
				f.handleError(*loggerTable, result, &tableDetail, err)
				return
			}
			successSqls, failedSqls, err := destDBClient.GenerateTableSql(ctx, *loggerTable, table, tableDetail)
			result.SuccessSqls = successSqls
			result.FailedSqls = failedSqls
			if err != nil {
				lastError = err
				f.handleError(*loggerTable, result, &tableDetail, err)
				return
			}
			loggerTable.Logger.Sugar().Infof("successSqls: %v\n", successSqls)
			loggerTable.Logger.Sugar().Infof("failedSqls: %v\n", failedSqls)
			result.TableComment = tableDetail.Comment
			result.TableColumns = tableDetail.Columns
			result.Status = common.JobStatusFinished
			result.FromDBType = tableDetail.FromDBType
			result.TableIndexes = tableDetail.Indexes
			result.TablePrimaryKeys = tableDetail.PrimaryKeys
			common.TaskObjectTableResultChannel <- result
		}(table)
	}
	wg.Wait()
	return lastError
}

func (f *TaskObjectStarter) handleError(loggerTable common.Logger, result common.TaskObjectTableResult, tableDetail *common.TableDetail, err error) {
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
	common.TaskObjectTableResultChannel <- result
}
func (f *TaskObjectStarter) Execute(ctx context.Context, tableInfo common.TaskObjectTable, tableDetail common.TableDetail) error {
	loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + tableInfo.SourceTable)
	defer loggerTable.Sync()

	destDBClient, err := GetDBClient(ctx, *loggerTable, f.DestConfig)
	if err != nil {
		return err
	}
	result := common.TaskObjectTableResult{
		JobId:   f.JobId,
		TableId: tableInfo.TableId,
		Status:  common.JobStatusRunning,
	}
	common.TaskObjectTableResultChannel <- result
	successSqls, failedSqls, err := destDBClient.GenerateTableSql(ctx, *loggerTable, tableInfo, tableDetail)
	result.SuccessSqls = successSqls
	result.FailedSqls = failedSqls
	if err != nil {
		f.handleError(*loggerTable, result, &tableDetail, err)
		return err
	}
	loggerTable.Logger.Sugar().Infof("successSqls: %v\n", successSqls)
	loggerTable.Logger.Sugar().Infof("failedSqls: %v\n", failedSqls)
	result.TableComment = tableDetail.Comment
	result.TableColumns = tableDetail.Columns
	result.Status = common.JobStatusFinished
	result.FromDBType = tableDetail.FromDBType
	result.TableIndexes = tableDetail.Indexes
	result.TablePrimaryKeys = tableDetail.PrimaryKeys
	common.TaskObjectTableResultChannel <- result
	return nil
}
