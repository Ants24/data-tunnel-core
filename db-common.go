package datatunnelcore

import (
	"context"
	"database/sql"
	"time"

	common "github.com/Ants24/data-tunnel-common"
)

func ReadData(ctx context.Context, logger common.Logger, conn *sql.Conn,
	tableConfig TaskFullTable, columnNames []string, columnTypes []string,
	dataChannel chan []sql.NullString,
	genReadDataSql func(columnNames []string, taskConfig TaskFullTable) (string, []interface{})) (uint64, error) {
	startTime := time.Now()
	querySql, args := genReadDataSql(columnNames, tableConfig)
	logger.Logger.Sugar().Debugf("Query table data sql: %s,args: %v", querySql, args)
	rows, err := conn.QueryContext(ctx, querySql, args...)
	if err != nil {
		logger.Error(err.Error())
		return 0, err
	}
	defer rows.Close()
	readTotal := uint64(0)
	for rows.Next() {
		select {
		case <-ctx.Done():
			return readTotal, ctx.Err()
		default:
		}
		values := make([]sql.NullString, len(columnNames))
		scanArgs := make([]interface{}, len(columnNames))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			logger.Error(err.Error())
			return readTotal, err
		}
		readTotal++
		dataChannel <- values
	}
	duration := time.Since(startTime)
	logger.Logger.Sugar().Infof("Read data from postgres database finished,read %d rows,elapsed time: %fs", readTotal, float32(duration.Seconds()))
	return readTotal, nil
}
func GetTableCount(ctx context.Context, logger common.Logger, conn *sql.Conn, querySql string) (uint64, error) {
	args := []interface{}{}
	row := conn.QueryRowContext(ctx, querySql, args...)
	var count uint64
	row.Scan(&count)
	return count, nil
}

func WriteData(ctx context.Context, logger common.Logger, tableConfig TaskFullTable, columnNames []string, columnTypes []string,
	dataChannel chan []sql.NullString,
	convertValue func(columnType string, orginValue sql.NullString, logger common.Logger) (interface{}, error),
	batchInsert func(ctx context.Context, data [][]interface{}, columnNames []string, taskConfig TaskFullTable) (int64, error),
	insertSingleData func(ctx context.Context, logger common.Logger, rows [][]interface{}, columnNames []string, tableConfig TaskFullTable) (uint64, uint64, error)) error {
	logger.Info("Start read data form channel")
	rows := make([][]interface{}, 0)
	//定时提交
	tick := time.NewTicker(time.Duration(tableConfig.Config.CommitBatchTime) * time.Second)
	defer tick.Stop()

	startTime := time.Now()
	var lastError error
	writerData := func(rows [][]interface{}) error {
		count, err := batchInsert(ctx, rows, columnNames, tableConfig)
		if err != nil {
			logger.Logger.Sugar().Errorf("Batch insert error: %s", err)
			{
				//启动单条插入
				if insertSingleData != nil {
					successNum, failedNum, err := insertSingleData(ctx, logger, rows, columnNames, tableConfig)
					//推送
					result := TaskFullTableResult{
						JobId:       tableConfig.JobId,
						TableId:     tableConfig.TableId,
						HandlerTime: time.Now(),
						SplitId:     tableConfig.SplitId,
						TakeTime:    float64(time.Since(startTime).Seconds()),
						SuccessNum:  successNum,
						FailedNum:   failedNum,
						Err:         err,
					}
					TaskFullTableResultChannel <- result
					return err
				}
			}
		}
		logger.Logger.Sugar().Infof("insert %d rows, elapsed time: %fs", count, float32(time.Since(startTime).Seconds()))
		//推送
		result := TaskFullTableResult{
			JobId:       tableConfig.JobId,
			TableId:     tableConfig.TableId,
			SplitId:     tableConfig.SplitId,
			HandlerTime: time.Now(),
			TakeTime:    float64(time.Since(startTime).Seconds()),
			Err:         err,
		}
		if err != nil {
			result.FailedNum = uint64(len(rows))
		} else {
			result.SuccessNum = uint64(len(rows))
		}
		TaskFullTableResultChannel <- result
		startTime = time.Now()
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		originalValues, ok := <-dataChannel
		if !ok {
			break
		}
		row := make([]interface{}, 0, len(columnNames))
		for i, v := range originalValues {
			value, err := convertValue(columnTypes[i], v, logger)
			if err != nil {
				logger.Error(err.Error())
				return err
			}
			row = append(row, value)
		}
		rows = append(rows, row)
		select {
		case <-tick.C:
			//时间到提交
			lastError = writerData(rows)
			if lastError != nil {
				return lastError
			}
			rows = make([][]interface{}, 0)
		default:
			if len(rows) == tableConfig.Config.CommitBatchSize {
				lastError = writerData(rows)
				if lastError != nil {
					return lastError
				}
				rows = make([][]interface{}, 0)
			}
		}
	}
	duration := time.Since(startTime)
	logger.Logger.Sugar().Infof("Read all data form channel,elapsed time: %fs", float32(duration.Seconds()))
	if len(rows) > 0 {
		lastError = writerData(rows)
		if lastError != nil {
			return lastError
		}
	}
	return lastError
}

func TruncateTable(ctx context.Context, logger common.Logger, conn *sql.Conn, deleteSql string) error {
	_, err := conn.ExecContext(ctx, deleteSql)
	if err != nil {
		return err
	}
	return nil
}

func GenerateSubTask(ctx context.Context, logger common.Logger, taskConfig TaskFullTable, getTablePrimaryKeys func(ctx context.Context, logger common.Logger, schemaName string, tableName string) ([]string, error), getTablePartitionNames func(ctx context.Context, logger common.Logger, schemaName string, tableName string) ([]string, error), getSplitValue func(ctx context.Context, logger common.Logger, taskConfig TaskFullTable) ([]interface{}, error)) ([]TaskFullTable, error) {
	subTasks := make([]TaskFullTable, 0)
	if !taskConfig.Config.Split {
		subTasks = append(subTasks, taskConfig)
		return subTasks, nil
	}
	primaryKeys, err := getTablePrimaryKeys(ctx, logger, taskConfig.SourceSchema, taskConfig.SourceTable)
	if err != nil {
		return nil, err
	}
	var partitionKeys []string
	if taskConfig.Config.Partition {
		partitionKeys, err = getTablePartitionNames(ctx, logger, taskConfig.SourceSchema, taskConfig.SourceTable)
		if err != nil {
			return nil, err
		}
	}
	subTasks = generateSubTasksForKeys(ctx, logger, taskConfig, primaryKeys, partitionKeys, getSplitValue)
	return subTasks, nil
}

func generateSubTasksForKeys(ctx context.Context, logger common.Logger, taskConfig TaskFullTable, primaryKeys, partitionKeys []string, getSplitValue func(ctx context.Context, logger common.Logger, taskConfig TaskFullTable) ([]interface{}, error)) []TaskFullTable {
	subTasks := make([]TaskFullTable, 0)

	if len(partitionKeys) == 0 {
		subTasks = generateSubTasksWithoutPartitions(ctx, getSplitValue, logger, taskConfig, primaryKeys)
	} else {
		for _, partitionKey := range partitionKeys {
			taskConfig.PartitionName = partitionKey
			subTasks = append(subTasks, generateSubTasksWithoutPartitions(ctx, getSplitValue, logger, taskConfig, primaryKeys)...)
		}
	}
	return subTasks
}

func generateSubTasksWithoutPartitions(ctx context.Context, getSplitValue func(ctx context.Context, logger common.Logger, taskConfig TaskFullTable) ([]interface{}, error), logger common.Logger, taskConfig TaskFullTable, primaryKeys []string) []TaskFullTable {
	subTasks := make([]TaskFullTable, 0)
	if len(primaryKeys) == 0 || len(primaryKeys) > 1 {
		subTasks = append(subTasks, taskConfig)
		return subTasks
	}
	taskConfig.SplitColumn = primaryKeys[0]
	splitValues, err := getSplitValue(ctx, logger, taskConfig)
	if err != nil {
		return nil
	}
	if len(splitValues) == 0 {
		taskConfig.SplitColumn = ""
		subTasks = append(subTasks, taskConfig)
		return subTasks
	}
	splitValueRangs := SplitKeyValueCombination(splitValues)
	for _, splitValueRange := range splitValueRangs {
		taskConfig.StartValue = splitValueRange.StartValue
		taskConfig.EndValue = splitValueRange.EndValue
		subTasks = append(subTasks, taskConfig)
	}
	return subTasks
}

type SplitKeyValue struct {
	StartValue interface{}
	EndValue   interface{}
}

func SplitKeyValueCombination(values []interface{}) []SplitKeyValue {
	var result []SplitKeyValue
	result = append(result, SplitKeyValue{
		StartValue: "",
		EndValue:   values[0],
	})
	for i := 0; i < len(values)-1; i++ {
		result = append(result, SplitKeyValue{
			StartValue: values[i],
			EndValue:   values[i+1],
		})
	}
	result = append(result, SplitKeyValue{
		StartValue: values[len(values)-1],
		EndValue:   "",
	})
	return result
}
