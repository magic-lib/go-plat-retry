// Package backoffretry 基于 github.com/cenkalti/backoff/v4 的退避重试封装，方便直接调用
package backoffretry

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"math"
	"time"
)

// SyncBackoffRetry 同步指数退避重试，一直重试直到函数执行成功 nil
// fn: 需要执行的业务函数，返回nil代表成功，error代表失败
// initialDelay: 初始等待时长
// maxDelay: 最大等待时长（退避不会超过这个值）
func SyncBackoffRetry(ctx context.Context, fn func() error, delays ...time.Duration) error {
	var initialDelay, maxDelay time.Duration
	if len(delays) == 1 {
		initialDelay = delays[0]
	} else if len(delays) >= 2 {
		initialDelay = delays[0]
		maxDelay = delays[1]
	}

	if initialDelay == 0 {
		initialDelay = time.Second
	} else if initialDelay < 0 {
		// 表示稍微长一点，默认1小时，避免太频繁调用
		initialDelay = time.Hour
	}
	if maxDelay == 0 {
		maxDelay = time.Hour //默认不设置的话，1小时一执行
	} else if maxDelay < 0 {
		// 表示不限制时间
		maxDelay = 24 * time.Hour //最长一天一执行
	}
	if initialDelay > maxDelay {
		initialDelay, maxDelay = maxDelay, initialDelay
	}

	attempt := 0
	for {
		err := fn()
		if err == nil {
			return nil
		}

		attempt++
		// 指数计算：delay = initialDelay * 2^(attempt-1)
		delay := float64(initialDelay) * math.Pow(2, float64(attempt-1))
		dur := time.Duration(delay)

		// 限制最大间隔
		if dur > maxDelay {
			dur = maxDelay
		}

		fmt.Printf("执行失败，第%d次重试，等待 %v，err=%v\n", attempt, dur, err)

		select {
		case <-ctx.Done():
			return ctx.Err() // 上下文取消，退出重试
		case <-time.After(dur):
			// 等待完成，继续下一轮
		}
	}
}

// AsyncBackoffRetry 异步指数退避重试
// 立即执行一次 fn：成功返回 nil；失败则丢到 goroutine 里继续按 SyncBackoffRetry 规则重试，
// 重试结果不返回（如果需要拿到最终结果，请使用同步版本 SyncBackoffRetry）。
// 参数语义与 SyncBackoffRetry 完全一致。
func AsyncBackoffRetry(ctx context.Context, fn func() error, delays ...time.Duration) error {
	if fn == nil {
		return errNoExecutable
	}

	err := fn()
	if err == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// 构造传给 GoAsync 的参数列表：ctx + fn + 每个 delay
	args := make([]any, 0, 2+len(delays))
	args = append(args, ctx, fn)
	for _, d := range delays {
		args = append(args, d)
	}

	goroutines.GoAsync(func(params ...any) {
		// 从 params 中还原参数（GoAsync 通过 any 透传，需要做类型断言）
		var (
			inCtx  context.Context
			inFun  func() error
			inDels = make([]time.Duration, 0, len(params)-2)
		)
		if len(params) >= 1 {
			if v, ok := params[0].(context.Context); ok {
				inCtx = v
			}
		}
		if len(params) >= 2 {
			if v, ok := params[1].(func() error); ok {
				inFun = v
			}
		}
		for _, p := range params[2:] {
			if d, ok := p.(time.Duration); ok {
				inDels = append(inDels, d)
			}
		}
		if inCtx == nil {
			inCtx = context.Background()
		}
		if inFun == nil {
			return
		}
		_ = SyncBackoffRetry(inCtx, inFun, inDels...)
	}, args...)
	return err
}
