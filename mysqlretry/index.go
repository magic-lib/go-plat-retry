// Package mysqlretry 重试器
package mysqlretry

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/magic-lib/go-plat-mysql/sqlstatement"
	"github.com/magic-lib/go-plat-retry/retry"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"github.com/samber/lo"
	"sync"
	"time"
)

const (
	retryStatusPending = "pending"
	retryStatusFailure = "failure"
	retryStatusSuccess = "success"

	defaultTableName = "retry_records"
	maxRetries       = 3
	defaultInterval  = 60
	maxScanListNum   = 100 //一次扫描的最大数量
)

var (
	registerCallback    = make(map[string]RetryExecutor)
	registerErrCallback = make(map[string]ErrCallbackFunc)
	lockMutex           = make(map[string]bool)
	onceLocker          sync.Mutex
)

// retryRecord 定义请求记录结构体
type retryRecord struct {
	Id             int64     `json:"id"`
	Namespace      string    `json:"namespace"`  // 命名空间，用于区分不同的业务
	RetryType      string    `json:"retry_type"` // 重试类型，函数里定义特殊的执行方法
	Param          string    `json:"param"`
	Extend         string    `json:"extend"` // 扩展字段，用于存储额外的信息，比如请求的URL、请求的参数、请求的头信息等
	Errors         string    `json:"errors"`
	Response       string    `json:"response"`
	Retries        int       `json:"retries"`
	MaxRetries     int       `json:"max_retries"`
	IntervalSecond int64     `json:"interval_second"` // 重试间隔时间，单位秒
	NextRetry      time.Time `json:"next_retry"`
	Status         string    `json:"status"`
	CreateTime     time.Time `json:"create_time"`
	UpdateTime     time.Time `json:"update_time"`
}

type RetryConfig struct {
	DSN       string
	SqlDB     *sql.DB
	TableName string `json:"table_name"`
	Namespace string `json:"namespace"`
}
type RetryService struct {
	sqlExec   func(query string, args ...any) error
	sqlQuery  func(query string, args ...any) (*sql.Rows, error)
	tableName string
	namespace string
}
type RetryRecord struct {
	RetryType     string        `json:"retry_type"`
	Param         []any         `json:"param"`
	Extend        any           `json:"extend"`
	MaxRetries    int           `json:"max_retries"`
	Interval      time.Duration `json:"interval"`
	RetryExecutor RetryExecutor
}

type RetryExecutor func(args []any) (any, error)
type ErrCallbackFunc func(err error, index int) error

func NewMysqlRetry(rc *RetryConfig) (*RetryService, error) {
	if rc.SqlDB == nil {
		if rc.DSN != "" {
			sqlDB, err := sql.Open("mysql", rc.DSN)
			if err != nil {
				return nil, fmt.Errorf("初始化数据库连接失败: %v", err)
			}
			rc.SqlDB = sqlDB
		}
	}

	if rc.Namespace == "" {
		return nil, fmt.Errorf("namespace is empty")
	}

	if rc.TableName == "" {
		rc.TableName = defaultTableName
	}
	if rc.SqlDB == nil {
		return nil, fmt.Errorf("sqldb is empty")
	}
	rs := new(RetryService)
	rs.tableName = rc.TableName
	rs.namespace = rc.Namespace
	rs.sqlQuery = func(query string, args ...any) (*sql.Rows, error) {
		rows, err := rc.SqlDB.Query(query, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	rs.sqlExec = func(query string, args ...any) error {
		_, err := rc.SqlDB.Exec(query, args...)
		if err != nil {
			return err
		}
		return nil
	}

	err := rs.createRetryTable()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func getCallbackKey(namespace, retryType string) string {
	if namespace == "" || retryType == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s", namespace, retryType)
}

func Register(namespace, retryType string, fun RetryExecutor, errFun ...ErrCallbackFunc) error {
	var err1, err2 error
	err1 = registerFun(namespace, retryType, fun)
	if len(errFun) > 0 {
		err2 = registerErrFun(namespace, retryType, errFun[0])
	}
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}
func registerFun(namespace, retryType string, fun RetryExecutor) error {
	callbackKey := getCallbackKey(namespace, retryType)
	if callbackKey == "" {
		return fmt.Errorf("namespace or retryType is empty")
	}
	if _, ok := registerCallback[callbackKey]; ok {
		return fmt.Errorf("retryType is already registered")
	}
	if fun != nil {
		registerCallback[callbackKey] = fun
	}
	return nil
}
func registerErrFun(namespace, retryType string, errFun ErrCallbackFunc) error {
	callbackKey := getCallbackKey(namespace, retryType)
	if callbackKey == "" {
		return fmt.Errorf("namespace or retryType is empty")
	}
	if _, ok := registerErrCallback[callbackKey]; ok {
		return nil
	}
	if errFun != nil {
		registerErrCallback[callbackKey] = errFun
	}
	return nil
}

func (rs *RetryService) getCallback(namespace, retryType string) (RetryExecutor, error) {
	callbackKey := getCallbackKey(namespace, retryType)
	if callbackKey == "" {
		return nil, fmt.Errorf("namespace or retryType is empty")
	}
	if f, ok := registerCallback[callbackKey]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("find no function")
}
func (rs *RetryService) getErrCallback(namespace, retryType string) (ErrCallbackFunc, error) {
	callbackKey := getCallbackKey(namespace, retryType)
	if callbackKey == "" {
		return nil, fmt.Errorf("namespace or retryType is empty")
	}
	if f, ok := registerErrCallback[callbackKey]; ok {
		return f, nil
	}
	return nil, fmt.Errorf("find no function")
}

// 创建请求记录表
func (rs *RetryService) createRetryTable() error {
	creatSql := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id bigint AUTO_INCREMENT PRIMARY KEY,
            namespace VARCHAR(100) NOT NULL,
            retry_type VARCHAR(100) NOT NULL,
            param TEXT NOT NULL,
            extend TEXT,
            errors TEXT,
            response TEXT,
            retries INT DEFAULT 0,
            max_retries INT DEFAULT %d,
            interval_second bigint DEFAULT %d,
            next_retry DATETIME NOT NULL,
            status VARCHAR(20) DEFAULT '%s',
            create_time DATETIME,
            update_time DATETIME
        )
    `, rs.tableName, maxRetries, defaultInterval, retryStatusPending)
	return rs.sqlExec(creatSql)
}

func (rs *RetryService) doRecord(r *RetryRecord) error {
	if r.Interval == 0 {
		r.Interval = defaultInterval * time.Second
	}
	if r.MaxRetries == 0 {
		r.MaxRetries = maxRetries
	}
	if r.RetryType == "" {
		return fmt.Errorf("retryType is empty")
	}

	f, err := rs.getCallback(rs.namespace, r.RetryType)
	if err != nil || f == nil {
		if r.RetryExecutor == nil {
			if err != nil {
				return err
			}
			err = fmt.Errorf("has no retry-executor")
			return err
		}
		err = Register(rs.namespace, r.RetryType, r.RetryExecutor)
		if err != nil {
			fmt.Println("retryService register:", rs.namespace, r.RetryType, err.Error())
		}
	}

	return nil
}

// Do 同步重试，失败以后异步重试
func (rs *RetryService) Do(r *RetryRecord, valuePtr ...any) error {
	err := rs.DoSync(r, valuePtr...)
	if err == nil {
		return nil
	}
	return rs.DoAsync(r)
}

// DoSync 同步重试
func (rs *RetryService) DoSync(r *RetryRecord, valuePtr ...any) error {
	err := rs.doRecord(r)
	if err != nil {
		return err
	}

	f, err := rs.getCallback(rs.namespace, r.RetryType)
	if err != nil || f == nil {
		return fmt.Errorf("has no retry-executor")
	}
	retryModel := retry.New().WithInterval(r.Interval).WithAttemptCount(r.MaxRetries)

	errCallFun, _ := rs.getErrCallback(rs.namespace, r.RetryType)
	if errCallFun != nil {
		retryModel = retryModel.WithErrCallback(func(err error, index int) error {
			return errCallFun(err, index)
		})
	}
	err = retryModel.Do(nil, func(ctx context.Context) (any, error) {
		ret, err := f(r.Param)
		if err != nil {
			return nil, err
		}
		return ret, nil
	}, valuePtr...)
	if err != nil {
		return err
	}
	return nil
}

// DoAsync 异步重试
func (rs *RetryService) DoAsync(r *RetryRecord) error {
	err := rs.doRecord(r)
	if err != nil {
		return err
	}

	nextRetry := time.Now().Add(r.Interval)
	query, data, err := sqlstatement.NewSqlStruct(
		sqlstatement.SetTableName(rs.tableName),
		sqlstatement.SetStructData(retryRecord{})).InsertSql(&retryRecord{
		Namespace:      rs.namespace,
		RetryType:      r.RetryType,
		Param:          conv.String(r.Param),
		Extend:         conv.String(r.Extend),
		MaxRetries:     r.MaxRetries,
		IntervalSecond: int64(r.Interval.Seconds()),
		NextRetry:      nextRetry,
		Status:         retryStatusPending,
		CreateTime:     time.Now(),
		UpdateTime:     time.Now(),
	})
	if err != nil {
		return err
	}

	return rs.sqlExec(query, data...)
}

// 扫描需要重试的请求记录
func (rs *RetryService) scanRecords() ([]map[string]string, error) {
	whereCond := sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "namespace",
				Operator: "=",
				Value:    rs.namespace,
			},
			sqlstatement.Condition{
				Field:    "status",
				Operator: "=",
				Value:    retryStatusPending,
			},
			sqlstatement.Condition{
				Field: "`retries`<`max_retries`",
			},
			sqlstatement.Condition{
				Field:    "next_retry",
				Operator: "<=",
				Value:    conv.String(time.Now()),
			},
		},
		Operator: "AND",
	}
	st := sqlstatement.Statement{}
	whereQuery, data := st.GenerateWhereClause(whereCond)
	sqlStr := fmt.Sprintf("SELECT * FROM %s WHERE %s order by next_retry asc limit %d",
		rs.tableName, whereQuery, maxScanListNum)

	rows, err := rs.sqlQuery(sqlStr, data...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]string
	// 为每一行创建一个 []any 数组
	values := make([]any, len(columns))
	valuePtrList := make([]any, len(columns))
	for i := range values {
		valuePtrList[i] = &values[i]
	}

	// 遍历结果集
	for rows.Next() {
		err = rows.Scan(valuePtrList...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		lo.ForEach(columns, func(item string, index int) {
			row[item] = conv.String(values[index])
		})
		result = append(result, row)
	}

	return result, nil
}

func (rs *RetryService) scanCurrentRecord(id int64) (map[string]string, error) {
	whereCond := sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "namespace",
				Operator: "=",
				Value:    rs.namespace,
			},
			sqlstatement.Condition{
				Field:    "id",
				Operator: "=",
				Value:    id,
			},
		},
		Operator: "AND",
	}
	st := sqlstatement.Statement{}
	whereQuery, data := st.GenerateWhereClause(whereCond)
	sqlStr := fmt.Sprintf("SELECT * FROM %s WHERE %s order by next_retry asc limit %d",
		rs.tableName, whereQuery, 1)

	rows, err := rs.sqlQuery(sqlStr, data...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 为每一行创建一个 []any 数组
	values := make([]any, len(columns))
	valuePtrList := make([]any, len(columns))
	for i := range values {
		valuePtrList[i] = &values[i]
	}

	// 遍历结果集
	for rows.Next() {
		err = rows.Scan(valuePtrList...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		lo.ForEach(columns, func(item string, index int) {
			row[item] = conv.String(values[index])
		})
		return row, nil
	}

	return nil, nil
}

// 执行失败
func (rs *RetryService) failureExec(item *retryRecord, err error) error {
	errList := make([]string, 0)
	if item.Errors != "" {
		_ = conv.Unmarshal(item.Errors, &errList)
	}

	errCallFun, _ := rs.getErrCallback(rs.namespace, item.RetryType)
	if errCallFun != nil {
		index := 1
		oneRecord, _ := rs.scanCurrentRecord(item.Id)
		if oneRecord != nil {
			if retries, ok := oneRecord["retries"]; ok {
				index, _ = conv.Convert[int](retries)
				index += 1
			}
		}
		endErr := errCallFun(err, index)
		if endErr != nil {
			//表示这个是致命错误，不用重试了
			failSql := fmt.Sprintf(`
				UPDATE %s
				SET errors = ?, update_time=?, status='%s', retries = retries + 1, next_retry = '%s' + INTERVAL %d SECOND
				WHERE id = ?
			`, rs.tableName, retryStatusFailure, conv.String(time.Now()), 0)
			errList = append(errList, err.Error(), endErr.Error())
			return rs.sqlExec(failSql, conv.String(errList), conv.String(time.Now()), item.Id)
		}
	}

	failSql := fmt.Sprintf(`
        UPDATE %s
        SET errors = ?, update_time=?, status=IF(retries+1 >= max_retries, '%s', '%s'), retries = retries + 1, next_retry = '%s' + INTERVAL %d SECOND
        WHERE id = ?
    `, rs.tableName, retryStatusFailure, retryStatusPending, conv.String(time.Now()), item.IntervalSecond)

	errList = append(errList, err.Error())

	return rs.sqlExec(failSql, conv.String(errList), conv.String(time.Now()), item.Id)
}

// 执行成功
func (rs *RetryService) successExec(item *retryRecord, resp string) error {
	successSql := fmt.Sprintf(`
        UPDATE %s
        SET response = ?, retries = retries + 1, update_time=?, status='%s'
        WHERE id = ?
    `, rs.tableName, retryStatusSuccess)
	return rs.sqlExec(successSql, resp, conv.String(time.Now()), item.Id)
}

// 执行请求
func (rs *RetryService) execute() error {
	list, err := rs.scanRecords()
	if err != nil {
		return err
	}
	dataList := make([]*retryRecord, 0)
	err = conv.Unmarshal(list, &dataList)
	if err != nil {
		return err
	}

	lo.ForEach(dataList, func(item *retryRecord, i int) {
		paramList := make([]any, 0)
		err := conv.Unmarshal(item.Param, &paramList)
		if err != nil {
			_ = rs.failureExec(item, err)
			return
		}
		f, err := rs.getCallback(item.Namespace, item.RetryType)
		if err != nil {
			_ = rs.failureExec(item, err)
			return
		}
		response, err := f(paramList)
		if err != nil {
			_ = rs.failureExec(item, err)
			return
		}
		_ = rs.successExec(item, conv.String(response))
	})
	return nil
}

func (rs *RetryService) Start() {
	onceLocker.Lock()
	defer onceLocker.Unlock()
	if _, ok := lockMutex[rs.namespace]; ok {
		return
	}
	lockMutex[rs.namespace] = true
	goroutines.GoAsync(func(param ...any) {
		// 定时扫描需要重试的请求记录
		ticker := time.NewTicker(defaultInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := rs.execute()
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}, nil)
}
