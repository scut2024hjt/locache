package consistenthash

import (
	"sync"
	"testing"
)

// TestConcurrentGetBug 用于直接复现并发写入 Map 导致的 Panic 崩溃
func TestConcurrentGetBug(t *testing.T) {
	// 1. 初始化一致性哈希实例
	hashMap := New()
	// 添加几个测试节点
	hashMap.Add("Node-A", "Node-B", "Node-C")

	// 2. 模拟高并发环境
	concurrency := 100         // 开启 100 个并发 goroutine
	requestsPerRoutine := 1000 // 每个 goroutine 执行 1000 次 Get 请求

	t.Logf("🚗 开始发车，模拟高并发 Get 请求...")
	t.Logf("并发数: %d, 每个协程请求数: %d, 总请求期望数: %d", concurrency, requestsPerRoutine, concurrency*requestsPerRoutine)

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < requestsPerRoutine; j++ {
				// 并发调用 Get。
				// Get 方法首行用了 m.mu.RLock() 读锁，使得所有 goroutine 可以同时进入函数体。
				// 然后程序会同时执行到： `m.nodeCounts[node] = count + 1`
				// Go 语言底层检测到对 map 的并发修改，会直接宕机（Panic）
				hashMap.Get("test-key")
			}
		}(i)
	}

	wg.Wait() // 等待所有请求完成

	// 根据 Go 的特性，程序绝对执行不到这里就会崩溃退出
	t.Log("✅ 并发测试完成，如果没有触发 Panic，说明你已经修复了 Bug！")
}
