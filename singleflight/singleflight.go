package singleflight

import (
	"sync"
)

// 代表正在进行或已结束的请求
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group manages all kinds of calls
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能
}

// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
// func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
// 	// Check if there is already an ongoing call for this key
// 	if existing, ok := g.m.Load(key); ok {
// 		c := existing.(*call)
// 		c.wg.Wait()         // Wait for the existing request to finish
// 		return c.val, c.err // Return the result from the ongoing call
// 	}

// 	// If no ongoing request, create a new one
// 	c := &call{}
// 	c.wg.Add(1)
// 	g.m.Store(key, c) // Store the call in the map

// 	// Execute the function and set the result
// 	c.val, c.err = fn()
// 	c.wg.Done() // Mark the request as done

// 	// After the request is done, clean up the map
// 	g.m.Delete(key)

// 	return c.val, c.err
// }

// Gemini 3.1 Pro(Preview)的优化
// 注意 由于LoadOrStore要求将要传的值存进去！所以要在开头就初始化，于是为了不用真正执行的Do调用
// 不会因为创建大量的call对象浪费空间并增大GC压力，所以采用了两次检查的方式：
// 第一次检查（Fast-path）是纯读，避免不必要的内存分配；第二次检查（Slow-path验证）用原子操作防并发竞争，只有真正需要执行fn的协程才会创建call对象。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// 1. 第一次检查（Fast-path）：纯读，避免不必要的内存分配
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)
		c.wg.Wait()         // 等待已存在的请求完成
		return c.val, c.err // 返回该请求的结果
	}

	// 2. 如果没命中，说明大概率需要发起请求，此时再分配内存
	c := &call{}
	c.wg.Add(1)

	// 3. 第二次检查（Slow-path验证）：用原子操作防并发竞争
	if actual, loaded := g.m.LoadOrStore(key, c); loaded {
		// 说明在我们创建 call 的间隙（极小的时间窗口），有其他协程抢先塞进了它的 call
		// 那我们直接用抢先者的，放弃自己刚创建的（它会在随后被 GC 回收），并等待
		waitCall := actual.(*call)
		waitCall.wg.Wait()
		return waitCall.val, waitCall.err
	}

	// 4. 执行到这里说明我们真正抢到了这唯一的执行权
	c.val, c.err = fn()
	c.wg.Done() // 通知所有阻塞在 Wait() 的协程

	// 请求结束后清理 map
	g.m.Delete(key)

	return c.val, c.err
}
