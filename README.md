# locache

一个 Go 实现的分布式缓存库，以 Library 形式与业务进程同进程运行：节点间通过 gRPC 通信共享缓存空间，本地未命中时向持有该分片的对端节点回源，从而把多台机器的内存聚合成一个逻辑缓存池。

## 特性

- **缓存分组**：以 namespace 隔离不同业务的缓存，各自独立配置回源函数与容量
- **两级淘汰**：内置 LRU（按字节容量）与 LRU-2（分段 + 二级缓存）两种 Store 实现
- **并发控制**：数据分片 + 分段互斥锁降低锁竞争，核心指标使用原子操作统计
- **防击穿**：singleflight 合并同一 key 的并发回源请求，避免热点失效瞬间打穿下游
- **一致性哈希**：带虚拟节点的哈希环，缓解节点增减导致的数据倾斜
- **服务发现**：基于 etcd 的节点注册与上下线感知，对端列表动态更新
- **节点通信**：gRPC + Protobuf 二进制协议，支持 Get / Set / Delete

## 结构

```
cache.go            缓存分组对外入口
group.go            分组的取值主流程：本地 -> 对端 -> 回源
byteview.go         只读缓存值封装
peers.go            对端管理、根据 key 选择节点
server.go           节点对外 gRPC 服务
client.go           访问其他节点的 gRPC 客户端
consistenthash/     一致性哈希环（虚拟节点）
registry/           etcd 服务注册与发现
singleflight/       并发请求合并
store/              LRU / LRU-2 存储实现
pb/                 Protobuf 定义与生成代码
example/            多节点示例
```

## 使用

```go
opts := store.NewOptions()
opts.MaxBytes = 1 << 20

group := locache.NewGroup("scores", 2<<10, locache.GetterFunc(
    func(ctx context.Context, key string) ([]byte, error) {
        // 回源逻辑：查数据库 / 调下游
        return []byte("value"), nil
    }))
group.NewStore(store.LRU, opts)

val, err := group.Get(context.Background(), "some-key")
```

多节点示例见 `example/multi_node_demo.go`。

## License

MIT
