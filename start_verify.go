package datatunnelcore

import (
	"context"
	"fmt"
	"sync"

	common "github.com/Ants24/data-tunnel-common"
	semaphore "github.com/marusama/semaphore/v2"
)

type TaskVerifyTableStarter struct {
	common.MigrationTask[common.TaskVerifyTable]
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
		loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + table.SourceTableInfo.TableName)
		defer loggerTable.Sync()
		result := common.TaskVerifyTableResult{
			JobId:   f.JobId,
			TableId: table.TableId,
			Status:  common.VerifyStatusRunning,
		}
		// 行数校验
		if table.Config.IsCount {
			semaphores.Acquire(ctx, 1) //获取信号量
			wg.Add(1)
			common.TaskVerifyTableResultChannel <- result
			go func(table common.TaskVerifyTable) {
				defer wg.Done()
				defer semaphores.Release(1)
				sourceCount := uint64(0)
				destCount := uint64(0)
				var subWg sync.WaitGroup
				subWg.Add(1)
				go func(table common.TaskVerifyTable) {
					defer subWg.Done()
					count, err := sourceDBClient.SelectTableCount(ctx, *loggerTable, table.SourceTableInfo)
					if err != nil {
						mu.Lock()
						lastError = err
						mu.Unlock()
					}
					sourceCount = count
				}(table)
				subWg.Add(1)
				go func(table common.TaskVerifyTable) {
					defer subWg.Done()
					count, err := destDBClient.SelectTableCount(ctx, *loggerTable, table.DestTableInfo)
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
				common.TaskVerifyTableResultChannel <- result
			}(table)
		}
	}
	wg.Wait()
	f.StartData(ctx, sourceDBClient, destDBClient)
	return lastError
}
func (f *TaskVerifyTableStarter) StartData(ctx context.Context, sourceDBClient common.DBClient, destDBClient common.DBClient) error {
	var wg sync.WaitGroup                   //并发控制,确保所有任务都完成
	semaphores := semaphore.New(f.Parallel) //限制并发数
	for _, table := range f.Tables {
		if !table.Config.IsData {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		semaphores.Acquire(ctx, 1) //获取信号量
		wg.Add(1)
		go func(table common.TaskVerifyTable) {
			defer semaphores.Release(1) //释放信号量
			defer wg.Done()
			//考虑到源端和目标端,分区表的情况,可能一方有一方没有,也可能分区类型不同,取消分区切片
			table.Config.Partition = false

			var err error
			loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + table.SourceTableInfo.TableName)
			var totalCount uint64
			{
				totalCount, err = sourceDBClient.SelectTableCount(ctx, *loggerTable, table.SourceTableInfo)
				if err != nil {
					return
				}
				//数量等于0 跳过
				if totalCount == 0 {
					loggerTable.Info("table count is 0, skip")
					//推送
					return
				}
			}
			sourceColumnNames := make([]string, 0)
			sourceColumnTypes := make([]string, 0)
			destColumnNames := make([]string, 0)
			destColumnTypes := make([]string, 0)
			isUseDestColumn := false
			//如果使用目标表的列,则需要获取目标表的列名
			if isUseDestColumn {
				destColumnNames, destColumnTypes, err = destDBClient.GetTableColumnNames(ctx, table.DestTableInfo, []string{}, f.DestConfig.DBType)
				if err != nil {
					return
				}
				destinationColumnNamesCopy := make([]string, len(destColumnNames))
				copy(destinationColumnNamesCopy, destColumnNames)
				sourceColumnNames, sourceColumnTypes, err = sourceDBClient.GetTableColumnNames(ctx, table.SourceTableInfo, destinationColumnNamesCopy, f.DestConfig.DBType)
				if err != nil {
					return
				}
			} else {
				//不使用目标表的列,则需要获取源表的列名
				sourceColumnNames, sourceColumnTypes, err = sourceDBClient.GetTableColumnNames(ctx, table.SourceTableInfo, []string{}, f.SourceConfig.DBType)
				if err != nil {
					return
				}
				sourceColumnNamesCopy := make([]string, len(sourceColumnNames))
				copy(sourceColumnNamesCopy, sourceColumnNames)
				destColumnNames, destColumnTypes, err = destDBClient.GetTableColumnNames(ctx, table.DestTableInfo, sourceColumnNamesCopy, f.SourceConfig.DBType)
				if err != nil {
					return
				}
			}
			var subFullTasks []common.TaskFullTable
			tableFull := common.TaskFullTable{
				TaskTableBase: table.TaskTableBase,
				Config: common.TableFullConfig{
					Parallel:       table.Config.Parallel,
					Split:          table.Config.Split,
					SplitBatchSize: table.Config.SplitBatchSize,
					Partition:      table.Config.Partition,
				},
			}
			//如果总数量小于切片数量,则不用进行切片
			if totalCount <= uint64(table.Config.SplitBatchSize) {
				tableFull.SplitId = 1
				subFullTasks = append(subFullTasks, tableFull)
			} else {
				//对表进行切片
				subFullTasks, err = sourceDBClient.GenerateSubTasks(ctx, *loggerTable, table.SourceTableInfo, tableFull)
				if err != nil {
					return
				}
				for index := range subFullTasks {
					subFullTasks[index].SplitId = index + 1
				}
			}
			subSemaphores := semaphore.New(table.Config.Parallel) //限制并发数
			var subWg sync.WaitGroup
			for _, subFullTask := range subFullTasks {
				select {
				case <-ctx.Done():
					return
				default:
				}
				subSemaphores.Acquire(ctx, 1)
				subWg.Add(1)
				go func(subFullTask common.TaskFullTable, wg *sync.WaitGroup, subSemaphores semaphore.Semaphore) {
					defer subWg.Done()
					defer subSemaphores.Release(1)
					var subWg2 sync.WaitGroup
					subWg2.Add(1)
					go func(subFullTask common.TaskFullTable, subWgs *sync.WaitGroup, subSemaphores semaphore.Semaphore) {
						defer func() {
							if r := recover(); r != nil {
								fmt.Println("product panicked:", r)
							}
						}()
						defer subWgs.Done()
						sourceColumnNamesCopy := make([]string, len(sourceColumnNames))
						copy(sourceColumnNamesCopy, sourceColumnNames)
						sourceMD5, err := sourceDBClient.MD5(ctx, *loggerTable, subFullTask.SourceTableInfo, subFullTask, sourceColumnNamesCopy, sourceColumnTypes, f.SourceConfig.DBType)
						if err != nil {
							return
						}
						loggerTable.Logger.Sugar().Infof("sourceMD5: %s", sourceMD5)
					}(subFullTask, &subWg2, subSemaphores)
					subWg2.Add(1)
					go func(subFullTask common.TaskFullTable, subWgs *sync.WaitGroup, subSemaphores semaphore.Semaphore) {
						defer func() {
							if r := recover(); r != nil {
								fmt.Println("product panicked:", r)
							}
						}()
						defer subWgs.Done()
						destColumnNamesCopy := make([]string, len(destColumnNames))
						copy(destColumnNamesCopy, destColumnNames)
						destMD5, err := destDBClient.MD5(ctx, *loggerTable, subFullTask.DestTableInfo, subFullTask, destColumnNamesCopy, destColumnTypes, f.DestConfig.DBType)
						if err != nil {
							return
						}
						loggerTable.Logger.Sugar().Infof("destMD5: %s", destMD5)
					}(subFullTask, &subWg2, subSemaphores)
					subWg2.Wait()
				}(subFullTask, &wg, subSemaphores)
			}
			subWg.Wait()
		}(table)
	}
	wg.Wait()
	return nil
}
