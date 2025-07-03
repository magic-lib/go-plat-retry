// Package retry 重试器
package retry

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"reflect"
	"time"
)

var (
	errNotPointer = fmt.Errorf("valuePtr parameter is not a pointer")
	errSetValue   = fmt.Errorf("call of reflect.Value.Set on zero Value")
)

type retry struct {
	attemptCount int             //最大尝试次数
	interval     time.Duration   //间隔时间
	errCallFun   ErrCallbackFunc //执行错误的方法
}

type Executable func(context.Context) (any, error)

/*
ErrCallbackFunc 回调函数

	nowAttemptCount 当前尝试次数
	err 错误
*/
type ErrCallbackFunc func(err error, index int) error

// New 创建一个重试器
func New() *retry {
	r := &retry{
		attemptCount: 1,
		interval:     5 * time.Second,
	}
	return r
}

// WithInterval 设置间隔时间
func (r *retry) WithInterval(interval time.Duration) *retry {
	r.interval = interval
	return r
}

// WithAttemptCount 设置最大尝试次数, 0为不限次数
func (r *retry) WithAttemptCount(attemptCount int) *retry {
	r.attemptCount = attemptCount
	return r
}

// WithErrCallback 设置错误回调函数, 每次执行时有任何错误都会报告给该函数
func (r *retry) WithErrCallback(errFun ErrCallbackFunc) *retry {
	r.errCallFun = errFun
	return r
}

// Do 执行一个函数
func (r *retry) Do(ctx context.Context, f Executable, valuePtr ...any) error {
	var valueFirst any = nil
	if len(valuePtr) > 0 {
		valueFirst = valuePtr[0]
	}

	if valueFirst != nil {
		rf := reflect.ValueOf(valueFirst)
		if rf.Type().Kind() != reflect.Ptr {
			return errNotPointer
		}
	}

	var retData any
	var err error
	if ctx == nil {
		ctx = context.Background()
	}
	retData, err = r.doRetryWithCtx(ctx, f)
	if len(valuePtr) == 0 || valueFirst == nil {
		return err
	}

	rf := reflect.ValueOf(valueFirst)
	if rf.Elem().CanSet() {
		fv := reflect.ValueOf(retData)
		if fv.IsValid() {
			if fv.Kind() == reflect.Ptr {
				fv = reflect.Indirect(fv)
			}
			rf.Elem().Set(fv)
		} else {
			if err == nil {
				err = errSetValue
			}
		}
	}
	return err
}

// doRetryWithCtx 执行一个函数
func (r *retry) doRetryWithCtx(parentCtx context.Context, fn Executable) (any, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	nowAttemptCount := 0
	for {
		fail := make(chan error, 1)
		success := make(chan any, 1)
		goroutines.GoAsync(func(params ...any) {
			val, err := fn(ctx)
			if err != nil {
				fail <- err
				return
			}
			success <- val
		})

		select {
		case <-parentCtx.Done():
			return nil, parentCtx.Err()
		case err := <-fail:
			if parentCtx.Err() != nil {
				return nil, parentCtx.Err()
			}

			if r.errCallFun != nil {
				callbackErr := r.errCallFun(err, nowAttemptCount)
				if callbackErr != nil {
					//表示这个是致命错误，不用重试了
					return nil, callbackErr
				}
			}

			nowAttemptCount++

			if nowAttemptCount >= r.attemptCount {
				return nil, fmt.Errorf("max retries exceeded (%v)", nowAttemptCount)
			}

			if r.interval > 0 {
				time.Sleep(r.interval)
			}

		case val := <-success:
			return val, nil
		}
	}
}
