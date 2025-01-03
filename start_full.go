package datatunnelcore

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	common "github.com/Ants24/data-tunnel-common"

	semaphore "github.com/marusama/semaphore/v2"
)

type TaskFullTableStarter struct {
	common.MigrationTask[common.TaskFullTable]
}

func (f *TaskFullTableStarter) Start(ctx context.Context, isUseDestColumn bool) error {
	logger := common.NewLogWithoutConfig(f.JobCode)
	defer logger.Sync()

	// Initialize clients with enhanced error context
	sourceDBClient, err := GetDBClient(ctx, *logger, f.SourceConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize source DB client: %w", err)
	}

	destDBClient, err := GetDBClient(ctx, *logger, f.DestConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize destination DB client: %w", err)
	}

	errTables := make(map[string]string, 0)
	var wg sync.WaitGroup //并发控制,确保所有任务都完成
	// 修正变量名: laseError -> lastError
	semaphores := semaphore.New(f.Parallel) //限制并发数
	for _, table := range f.Tables {
		semaphores.Acquire(ctx, 1) //获取信号量

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		wg.Add(1)
		go func(table common.TaskFullTable) {
			var lastError error
			defer wg.Done()
			defer semaphores.Release(1)
			loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + table.SourceTable)
			defer loggerTable.Sync()
			//推送
			result := common.TaskFullTableResult{
				JobId:   f.JobId,
				TableId: table.TableId,
				Status:  common.JobStatusRunning,
			}
			common.TaskFullTableResultChannel <- result
			//获取表的总数据量
			var totalCount uint64
			{
				totalCount, err = sourceDBClient.SelectTableCount(ctx, *loggerTable, common.TableBase{
					SchemeName: table.SourceSchema,
					TableName:  table.SourceTable,
					Filter:     table.Filter,
				})
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
				//数量等于0 跳过
				if totalCount == 0 {
					loggerTable.Info("table count is 0, skip")
					//推送
					result.TotalNum = 0
					result.Status = common.JobStatusFinished
					common.TaskFullTableResultChannel <- result
					return
				} else {
					//总数量,发送通道
					//推送
					result.TotalNum = totalCount
					common.TaskFullTableResultChannel <- result
				}
				result.TotalNum = 0 // readNum只表示总数量,方便更新操作,此处重置,方便后续对result的使用
			}

			sourceColumnNames := make([]string, 0)
			sourceColumnTypes := make([]string, 0)
			destColumnNames := make([]string, 0)
			destColumnTypes := make([]string, 0)
			//如果使用目标表的列,则需要获取目标表的列名
			if isUseDestColumn {
				destColumnNames, destColumnTypes, err = destDBClient.GetTableColumnNames(ctx, table.DestSchema, table.DestTable, []string{}, f.DestConfig.DBType)
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					lastError = err
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
				destinationColumnNamesCopy := make([]string, len(destColumnNames))
				copy(destinationColumnNamesCopy, destColumnNames)
				sourceColumnNames, sourceColumnTypes, err = sourceDBClient.GetTableColumnNames(ctx, table.SourceSchema, table.SourceTable, destinationColumnNamesCopy, f.DestConfig.DBType)
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					lastError = err
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
			} else {
				//不使用目标表的列,则需要获取源表的列名
				sourceColumnNames, sourceColumnTypes, err = sourceDBClient.GetTableColumnNames(ctx, table.SourceSchema, table.SourceTable, []string{}, f.SourceConfig.DBType)
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					lastError = err
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
				sourceColumnNamesCopy := make([]string, len(sourceColumnNames))
				copy(sourceColumnNamesCopy, sourceColumnNames)
				destColumnNames, destColumnTypes, err = destDBClient.GetTableColumnNames(ctx, table.DestSchema, table.DestTable, sourceColumnNamesCopy, f.SourceConfig.DBType)
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
			}
			var subFullTasks []common.TaskFullTable
			//如果总数量小于切片数量,则不用进行切片
			if totalCount <= uint64(table.Config.SplitBatchSize) {
				table.SplitId = 1
				subFullTasks = append(subFullTasks, table)
			} else {
				//对表进行切片
				subFullTasks, err = sourceDBClient.GenerateSubTasks(ctx, *loggerTable, table)
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					lastError = err
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
				for index := range subFullTasks {
					subFullTasks[index].SplitId = index + 1
				}
			}
			//TODO 保存切片信息

			//清空表
			if table.Config.Truncate {
				err := destDBClient.TruncateTable(ctx, *loggerTable, table.DestSchema, table.DestTable)
				if err != nil {
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
			}
			// 数据通道
			dataChannel := make(chan []sql.NullString, table.Config.ChannelSize)
			dataChannelClose := false
			//启动写入任务
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Consumer panicked:", r)
					}
				}()
				err := destDBClient.WriteData(ctx, *loggerTable, table, destColumnNames, destColumnTypes, dataChannel)
				if err != nil {
					errTables[table.SourceTable] = err.Error()
					//推送
					f.handleError(*loggerTable, result, err)
				}
				if !dataChannelClose {
					dataChannelClose = true
					close(dataChannel)
				}
			}()
			subSemaphores := semaphore.New(table.Config.Parallel) //限制并发数
			var subWg sync.WaitGroup
			//启动读取任务
			for _, subFullTask := range subFullTasks {
				select {
				case <-ctx.Done():
					return
				default:
				}
				subSemaphores.Acquire(ctx, 1)
				subWg.Add(1)
				go func(subFullTask common.TaskFullTable, subWg *sync.WaitGroup, subSemaphores semaphore.Semaphore) {
					defer func() {
						if r := recover(); r != nil {
							fmt.Println("product panicked:", r)
						}
					}()
					defer subWg.Done()
					defer subSemaphores.Release(1) //释放信号量
					sourceColumnNamesCopy := make([]string, len(sourceColumnNames))
					copy(sourceColumnNamesCopy, sourceColumnNames)
					_, err := sourceDBClient.ReadData(ctx, *loggerTable, subFullTask, sourceColumnNamesCopy, sourceColumnTypes, dataChannel)
					if err != nil {
						errTables[table.SourceTable] = err.Error()
						lastError = err
						//推送
						f.handleError(*loggerTable, result, err)
					}
				}(subFullTask, &subWg, subSemaphores)
			}
			subWg.Wait() //等待子任务完成
			if !dataChannelClose {
				dataChannelClose = true
				close(dataChannel)
			}
			if lastError != nil {
				//推送
				result.Status = common.JobStatusFailed
			} else {
				//推送
				result.Status = common.JobStatusFinished
			}
			common.TaskFullTableResultChannel <- result
		}(table)
	}
	wg.Wait()
	if len(errTables) > 0 {
		return fmt.Errorf("Have %d table error: ", len(errTables))
	}
	return nil
}

func (f *TaskFullTableStarter) handleError(loggerTable common.Logger, result common.TaskFullTableResult, err error) {
	loggerTable.Error(err.Error())
	result.Status = common.JobStatusFailed
	result.Err = err
	common.TaskFullTableResultChannel <- result
}
