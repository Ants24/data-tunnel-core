package datatunnelcore

import (
	"context"
	"sync"

	common "github.com/Ants24/data-tunnel-common"
	semaphore "github.com/marusama/semaphore/v2"
)

type TaskVerifyTable struct {
	TableId      uint
	JobId        uint
	SourceSchema string
	SourceTable  string
	DestSchema   string
	DestTable    string
	Filter       string
	Config       common.TableVerifyConfig
}

type TaskVerifyTableResult struct {
	JobId       uint
	TableId     uint
	Status      common.JobVerifyStatus
	SourceCount uint64
	DestCount   uint64
	Error       error
}

type TaskVerifyTableStarter struct {
	MigrationTask[TaskVerifyTable]
}

func (f *TaskVerifyTableStarter) Start(ctx context.Context) error {
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
	var wg sync.WaitGroup                   //并发控制,确保所有任务都完成
	semaphores := semaphore.New(f.Parallel) //限制并发数
	var mu sync.Mutex
	var lastError error
	for _, table := range f.Tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		semaphores.Acquire(ctx, 1) //获取信号量
		loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + table.SourceTable)
		defer loggerTable.Sync()
		result := TaskVerifyTableResult{
			JobId:   f.JobId,
			TableId: table.TableId,
			Status:  common.VerifyStatusRunning,
		}
		TaskVerifyTableResultChannel <- result
		wg.Add(1)
		go func(table TaskVerifyTable) {
			defer wg.Done()
			defer semaphores.Release(1)
			sourceCount := uint64(0)
			destCount := uint64(0)
			var subWg sync.WaitGroup
			subWg.Add(1)
			go func(table TaskVerifyTable) {
				defer subWg.Done()
				count, err := sourceDBClient.SelectTableCount(ctx, *loggerTable, TableBase{
					TableName:  table.SourceTable,
					SchemeName: table.SourceSchema,
					Filter:     table.Filter,
				})
				if err != nil {
					mu.Lock()
					lastError = err
					mu.Unlock()
				}
				sourceCount = count
			}(table)
			subWg.Add(1)
			go func(table TaskVerifyTable) {
				defer subWg.Done()
				count, err := destDBClient.SelectTableCount(ctx, *loggerTable, TableBase{
					TableName:  table.DestTable,
					SchemeName: table.DestSchema,
					Filter:     table.Filter,
				})
				if err != nil {
					mu.Lock()
					lastError = err
					mu.Unlock()
				}
				destCount = count
			}(table)
			subWg.Wait()
			loggerTable.Logger.Sugar().Infof("source count: %d, dest count: %d", sourceCount, destCount) // 合并日志记录语句
			result.SourceCount = sourceCount
			result.DestCount = destCount
			if lastError != nil {
				result.Status = common.VerifyStatusFailed
				result.Error = lastError
			} else if result.SourceCount == result.DestCount {
				result.Status = common.VerifyStatusConsistent
			} else {
				result.Status = common.VerifyStatusInconsistent
			}
			TaskVerifyTableResultChannel <- result

		}(table)
	}
	wg.Wait()
	return lastError
}
