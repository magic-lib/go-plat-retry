// Package backoffretry 基于 github.com/cenkalti/backoff/v4 的退避重试封装，方便直接调用
package backoffretry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// 类型别名，方便直接使用 backoff 原生能力，无需重复 import
type (
	BackOff            = backoff.BackOff
	BackOffContext     = backoff.BackOffContext
	ExponentialBackOff = backoff.ExponentialBackOff
	ConstantBackOff    = backoff.ConstantBackOff
	ZeroBackOff        = backoff.ZeroBackOff
	StopBackOff        = backoff.StopBackOff
	Operation          = backoff.Operation
	OperationWithData  = backoff.OperationWithData[any]
	Notify             = backoff.Notify
	Ticker             = backoff.Ticker
	PermanentError     = backoff.PermanentError
)

// Stop 表示不再重试
const Stop = backoff.Stop

// backoff 默认参数的别名，方便外部设置
const (
	DefaultInitialInterval     = backoff.DefaultInitialInterval
	DefaultRandomizationFactor = backoff.DefaultRandomizationFactor
	DefaultMultiplier          = backoff.DefaultMultiplier
	DefaultMaxInterval         = backoff.DefaultMaxInterval
	DefaultMaxElapsedTime      = backoff.DefaultMaxElapsedTime
)

var (
	errNotPointer   = fmt.Errorf("valuePtr parameter is not a pointer")
	errSetValue     = fmt.Errorf("call of reflect.Value.Set on zero Value")
	errNoExecutable = fmt.Errorf("executable function is nil")
)

// Executable 待执行的函数
type Executable func(ctx context.Context) (any, error)

// ExecutableWithData 带返回值的待执行函数
type ExecutableWithData[T any] func(ctx context.Context) (T, error)

// ErrCallbackFunc 错误回调，err 为本次错误，next 为下次重试等待时间
type ErrCallbackFunc func(err error, next time.Duration)

// RetryExecutor 退避重试执行器
type RetryExecutor struct {
	backOff    backoff.BackOff
	notify     backoff.Notify
	maxRetries uint64 // 最大重试次数，0 为不限制
}

// New 创建一个退避重试执行器，默认使用指数退避策略
func New() *RetryExecutor {
	return &RetryExecutor{
		backOff: backoff.NewExponentialBackOff(),
	}
}

// WithBackOff 自定义退避策略，会覆盖默认的指数退避
func (r *RetryExecutor) WithBackOff(b backoff.BackOff) *RetryExecutor {
	if b != nil {
		r.backOff = b
	}
	return r
}

// WithExponentialBackOff 使用指数退避策略，可选参数见 backoff 的 WithInitialInterval/WithMultiplier 等
func (r *RetryExecutor) WithExponentialBackOff(opts ...backoff.ExponentialBackOffOpts) *RetryExecutor {
	r.backOff = backoff.NewExponentialBackOff(opts...)
	return r
}

// WithConstantBackOff 使用固定间隔退避策略
func (r *RetryExecutor) WithConstantBackOff(interval time.Duration) *RetryExecutor {
	r.backOff = backoff.NewConstantBackOff(interval)
	return r
}

// WithMaxRetries 设置最大重试次数，0 为不限制（由退避策略自行停止）
func (r *RetryExecutor) WithMaxRetries(maxRetries uint64) *RetryExecutor {
	r.maxRetries = maxRetries
	return r
}

// WithMaxElapsedTime 设置最长执行时间，仅对指数退避策略生效
func (r *RetryExecutor) WithMaxElapsedTime(d time.Duration) *RetryExecutor {
	if exp, ok := r.backOff.(*backoff.ExponentialBackOff); ok && exp != nil {
		exp.MaxElapsedTime = d
	}
	return r
}

// WithErrCallback 设置错误回调，每次失败后触发
func (r *RetryExecutor) WithErrCallback(fn ErrCallbackFunc) *RetryExecutor {
	if fn == nil {
		r.notify = nil
		return r
	}
	r.notify = func(err error, next time.Duration) {
		fn(err, next)
	}
	return r
}

// Do 执行一个函数，失败后按退避策略重试，valuePtr 可选，用于接收返回值（必须传指针）
func (r *RetryExecutor) Do(ctx context.Context, fn Executable, valuePtr ...any) error {
	if fn == nil {
		return errNoExecutable
	}
	retData, err := DoWithData[any](r, ctx, func(ctx context.Context) (any, error) {
		return fn(ctx)
	})
	if err != nil {
		return err
	}
	if len(valuePtr) == 0 || valuePtr[0] == nil {
		return nil
	}
	if err = setValue(valuePtr[0], retData); err != nil {
		return err
	}
	return nil
}

// DoWithData 泛型版本，按 r 的退避策略执行 fn 并返回结果
func DoWithData[T any](r *RetryExecutor, ctx context.Context, fn ExecutableWithData[T]) (T, error) {
	var zero T
	if r == nil {
		r = New()
	}
	if fn == nil {
		return zero, errNoExecutable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return backoff.RetryNotifyWithData(func() (T, error) {
		return fn(ctx)
	}, r.buildBackOff(ctx), r.notify)
}

// buildBackOff 组装最终的退避策略
func (r *RetryExecutor) buildBackOff(ctx context.Context) backoff.BackOff {
	b := r.backOff
	if b == nil {
		b = backoff.NewExponentialBackOff()
	}
	if r.maxRetries > 0 {
		b = backoff.WithMaxRetries(b, r.maxRetries)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return backoff.WithContext(b, ctx)
}

// Retry 快捷方法，使用默认指数退避策略执行 fn
func Retry(ctx context.Context, fn Executable, valuePtr ...any) error {
	return New().Do(ctx, fn, valuePtr...)
}

// RetryWithData 快捷方法，泛型版本，使用默认指数退避策略
func RetryWithData[T any](ctx context.Context, fn ExecutableWithData[T]) (T, error) {
	return DoWithData(New(), ctx, fn)
}

// RetryNotify 直接调用 backoff 原生方法
func RetryNotify(operation backoff.Operation, b backoff.BackOff, notify backoff.Notify) error {
	return backoff.RetryNotify(operation, b, notify)
}

// NewExponentialBackOff 创建指数退避策略，可选参数见 backoff 的 WithInitialInterval/WithMultiplier 等
func NewExponentialBackOff(opts ...backoff.ExponentialBackOffOpts) *backoff.ExponentialBackOff {
	return backoff.NewExponentialBackOff(opts...)
}

// NewConstantBackOff 创建固定间隔退避策略
func NewConstantBackOff(interval time.Duration) *backoff.ConstantBackOff {
	return backoff.NewConstantBackOff(interval)
}

// NewZeroBackOff 创建立即重试策略
func NewZeroBackOff() *backoff.ZeroBackOff {
	return &backoff.ZeroBackOff{}
}

// NewStopBackOff 创建不重试策略
func NewStopBackOff() *backoff.StopBackOff {
	return &backoff.StopBackOff{}
}

// WithMaxRetries 给退避策略叠加最大重试次数
func WithMaxRetries(b backoff.BackOff, maxRetries uint64) backoff.BackOff {
	return backoff.WithMaxRetries(b, maxRetries)
}

// WithContext 把 context 绑定到退避策略上，ctx 取消后会停止重试
func WithContext(b backoff.BackOff, ctx context.Context) backoff.BackOffContext {
	return backoff.WithContext(b, ctx)
}

// NewTicker 创建一个类似 time.Ticker 的退避定时器
func NewTicker(b backoff.BackOff) *backoff.Ticker {
	return backoff.NewTicker(b)
}

// Permanent 包装成永久错误，遇到该错误不再重试
func Permanent(err error) error {
	return backoff.Permanent(err)
}

// IsPermanentError 判断错误是否为 Permanent 包装的永久错误
// 注意：Retry 返回时会把 PermanentError 拆包成原始错误，故此函数主要用于判断自行构造的错误
func IsPermanentError(err error) bool {
	var permanent *backoff.PermanentError
	return errors.As(err, &permanent)
}

// setValue 把执行结果写入指针
func setValue(valuePtr any, retData any) error {
	rf := reflect.ValueOf(valuePtr)
	if rf.Kind() != reflect.Ptr || rf.IsNil() {
		return errNotPointer
	}
	if !rf.Elem().CanSet() {
		return errSetValue
	}
	fv := reflect.ValueOf(retData)
	if !fv.IsValid() {
		return errSetValue
	}
	if fv.Kind() == reflect.Ptr {
		fv = reflect.Indirect(fv)
	}
	if !fv.Type().AssignableTo(rf.Elem().Type()) {
		return fmt.Errorf("cannot assign %s to %s", fv.Type(), rf.Elem().Type())
	}
	rf.Elem().Set(fv)
	return nil
}
