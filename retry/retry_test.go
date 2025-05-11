package retry_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-retry/retry"
	"testing"
	"time"
)

type AA struct {
	Name string
}

func TestCacheMap(t *testing.T) {

	//str := "Hell!"
	//
	//// 获取前3个字符
	//firstThree := str[:6]
	//fmt.Println(firstThree)

	var a AA
	err := retry.New().WithInterval(1*time.Second).WithAttemptCount(7).Do(nil, func(ctx context.Context) (any, error) {
		a.Name = "aaa"
		return a, nil
	}, &a)

	fmt.Println(err, a)
}
