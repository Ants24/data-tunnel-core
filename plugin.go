package datatunnelcore

import (
	"context"
	"fmt"
	"plugin"

	common "github.com/Ants24/data-tunnel-common"
)

var DBCLIENT_CACHE = make(map[string]DBClient, 0)

func GetDBClient(ctx context.Context, logger common.Logger, dbConfig common.DBConfig) (DBClient, error) {
	key := fmt.Sprintf("%s:%s:%d:%s", dbConfig.Username, dbConfig.Host, dbConfig.Port, dbConfig.Database)
	client, ok := DBCLIENT_CACHE[key]
	if ok {
		return client, nil
	}
	// 打开插件
	p, err := plugin.Open("./plugins/" + string(dbConfig.DBType) + ".so")
	if err != nil {
		logger.Logger.Sugar().Errorf("failed to load plugin %s: %v", dbConfig.DBType, err)
		return nil, fmt.Errorf("failed to load plugin %s: %v", dbConfig.DBType, err)
	}
	// 查找导出的 NewProcessor 函数
	symbol, err := p.Lookup("NewDBClient")
	if err != nil {
		err = fmt.Errorf("failed to find DBClientInstance in %s: %v", dbConfig.DBType, err)
		logger.Logger.Sugar().Error(err)
		return nil, err
	}
	// 断言 NewProcessor 为函数类型
	newDBClientFunc, ok := symbol.(func(context.Context, common.Logger, common.DBConfig) (DBClient, error))
	if !ok {
		panic(fmt.Sprintf("symbol DBClientInstance in %s is not of expected type", dbConfig.DBType))
	}
	dbClient, err := newDBClientFunc(ctx, logger, dbConfig)
	if err != nil {
		logger.Logger.Sugar().Errorf("failed to create DBClient: %v", err)
		return nil, fmt.Errorf("failed to create %s DBClient: %v", dbConfig.DBType, err)
	}
	DBCLIENT_CACHE[key] = dbClient
	return dbClient, nil
}
