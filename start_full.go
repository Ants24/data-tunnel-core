package datatunnelcore

import (
	"context"
	"database/sql"
	"sync"
	"time"

	common "github.com/Ants24/data-tunnel-common"

	semaphore "github.com/marusama/semaphore/v2"
)

type TaskFullTable struct {
	TableId      uint
	JobId        uint
	SourceSchema string
	SourceTable  string
	DestSchema   string
	DestTable    string
	Filter       string
	Config       common.TableFullConfig
	// 一下做切片的配置
	SplitId       int
	SplitColumn   string
	PartitionName string
	StartValue    interface{}
	EndValue      interface{}
}

type TaskFullTableResult struct {
	JobId       uint
	TableId     uint
	SplitId     int
	HandlerTime time.Time //处理时间
	Status      common.JobStatus
	TotalNum    uint64
	SuccessNum  uint64
	FailedNum   uint64
	TakeTime    float64 //耗时
	Err         error
}

type TaskFullTableStarter struct {
	MigrationTask[TaskFullTable]
}

func (f *TaskFullTableStarter) Start(ctx context.Context, isUseDestColumn bool) error {
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
	var lastError error                     // 修正变量名: laseError -> lastError
	semaphores := semaphore.New(f.Parallel) //限制并发数
	for _, table := range f.Tables {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		semaphores.Acquire(ctx, 1) //获取信号量
		wg.Add(1)
		go func(table TaskFullTable) {
			defer wg.Done()
			defer semaphores.Release(1)
			loggerTable := common.NewLogWithoutConfig(f.JobCode + "-" + table.SourceTable)
			defer loggerTable.Sync()
			//推送
			result := TaskFullTableResult{
				JobId:   f.JobId,
				TableId: table.TableId,
				Status:  common.JobStatusRunning,
			}
			TaskFullTableResultChannel <- result

			//获取表的总数据量
			var totalCount uint64
			{
				totalCount, err = sourceDBClient.SelectTableCount(ctx, *loggerTable, TableBase{
					SchemeName: table.SourceSchema,
					TableName:  table.SourceTable,
					Filter:     table.Filter,
				})
				if err != nil {
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
					TaskFullTableResultChannel <- result
					return
				} else {
					//总数量,发送通道
					//推送
					result.TotalNum = totalCount
					TaskFullTableResultChannel <- result
				}
				result.TotalNum = 0 // readNum只表示总数量,方便更新操作,此处重置,方便后续对result的使用
			}
			//数据通道
			dataChannel := make(chan []sql.NullString, table.Config.ChannelSize)

			sourceColumnNames := make([]string, 0)
			sourceColumnTypes := make([]string, 0)
			destColumnNames := make([]string, 0)
			destColumnTypes := make([]string, 0)
			if isUseDestColumn {
				destColumnNames, destColumnTypes, err = destDBClient.GetTableColumnNames(ctx, table.DestSchema, table.DestTable, []string{}, f.DestConfig.DBType)
				if err != nil {
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
				destinationColumnNamesCopy := make([]string, len(destColumnNames))
				copy(destinationColumnNamesCopy, destColumnNames)
				sourceColumnNames, sourceColumnTypes, err = sourceDBClient.GetTableColumnNames(ctx, table.SourceSchema, table.SourceTable, destinationColumnNamesCopy, f.DestConfig.DBType)
				if err != nil {
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
			} else {
				sourceColumnNames, sourceColumnTypes, err = sourceDBClient.GetTableColumnNames(ctx, table.SourceSchema, table.SourceTable, []string{}, f.SourceConfig.DBType)
				if err != nil {
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
				sourceColumnNamesCopy := make([]string, len(sourceColumnNames))
				copy(sourceColumnNamesCopy, sourceColumnNames)
				destColumnNames, destColumnTypes, err = destDBClient.GetTableColumnNames(ctx, table.DestSchema, table.DestTable, sourceColumnNamesCopy, f.SourceConfig.DBType)
				if err != nil {
					//推送
					f.handleError(*loggerTable, result, err)
					return
				}
			}
			var subFullTasks []TaskFullTable
			//如果总数量小于切片数量,则不用进行切片
			if totalCount <= uint64(table.Config.SplitBatchSize) {
				table.SplitId = 1
				subFullTasks = append(subFullTasks, table)
			} else {
				//对表进行切片
				subFullTasks, err = sourceDBClient.GenerateSubTasks(ctx, *loggerTable, table)
				if err != nil {
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

			//启动写入任务
			wg.Add(1)
			go func() {
				defer wg.Done()

				err := destDBClient.WriteData(ctx, *loggerTable, table, destColumnNames, destColumnTypes, dataChannel)
				if err != nil {
					lastError = err // 修正变量名: laseError -> lastError
					//推送
					f.handleError(*loggerTable, result, err)
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
				go func(subFullTask TaskFullTable, subWg *sync.WaitGroup, subSemaphores semaphore.Semaphore) {
					defer subWg.Done()
					defer subSemaphores.Release(1) //释放信号量
					sourceColumnNamesCopy := make([]string, len(sourceColumnNames))
					copy(sourceColumnNamesCopy, sourceColumnNames)
					_, err := sourceDBClient.ReadData(ctx, *loggerTable, subFullTask, sourceColumnNamesCopy, sourceColumnTypes, dataChannel)
					if err != nil {
						lastError = err // 修正变量名: laseError -> lastError
						//推送
						f.handleError(*loggerTable, result, err)
					}
				}(subFullTask, &subWg, subSemaphores)
			}
			subWg.Wait() //等待子任务完成
			close(dataChannel)
			if lastError != nil {
				//推送
				result.Status = common.JobStatusFailed
			} else {
				//推送
				result.Status = common.JobStatusFinished
			}
			TaskFullTableResultChannel <- result
		}(table)
	}
	wg.Wait()
	return lastError
}

func (f *TaskFullTableStarter) handleError(loggerTable common.Logger, result TaskFullTableResult, err error) {
	loggerTable.Error(err.Error())
	result.Status = common.JobStatusFailed
	result.Err = err
	TaskFullTableResultChannel <- result
}
