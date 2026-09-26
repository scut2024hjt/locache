# locache

> 一个以内嵌 Library 形式运行的 Go 分布式缓存：一致性哈希确定 Key 的唯一 Owner，etcd 维护动态成员，gRPC 完成跨节点访问；支持显式 `Get / Set / Delete`、可选 read-through 回源，以及短 TTL Near Cache。

## 项目定位

Locache 不是数据库，也不把缓存当作业务数据的 Source of Truth。业务数据仍由 MySQL、PostgreSQL 或下游服务保存；Locache 负责把多台业务实例的内存组织成一个可动态扩缩容的缓存池。

它同时支持两种使用方式：

1. **显式缓存模式**：业务在更新 Source of Truth 后主动调用 `Set` 或 `Delete`；
2. **read-through 模式**：Owner miss 时通过 `Getter` 回源，并使用 singleflight 合并同 Key 并发加载。

适合：用户资料、商品/内容详情、配置与元数据、计算结果等**读多写少、缓存可丢失**的数据。

对于余额、库存扣减、订单状态等要求严格一致的核心状态，缓存只能作为加速层，权威读写仍应由对应业务存储承担。

## 核心模型

```text
                          etcd
                           │
                    membership view
                           │
                           ▼
                   consistent hash
                           │
                     key -> Owner
                    /            \
           owner == self      owner == remote
                │                   │
                ▼                   ▼
          Owner Cache            gRPC
          Sharded LRU               │
                │                Owner Node
             hit/miss                │
                │               Owner Cache
                │                   │
                └──── miss ─── singleflight ─── Getter(optional)

remote read result
        │
        └── optional short-TTL Near Cache on requester
```

### 三个角色必须分清

- **Owner Cache**：一致性哈希确定的唯一正式缓存副本；`Set/Delete` 最终都路由到 Owner。
- **Near Cache**：非 Owner 上可选的短 TTL 本地副本，只用于减少远程 RPC；不是权威副本。
- **Source of Truth**：缓存之外的数据库或服务。Locache 的 `Set/Delete` 只修改缓存，不替业务更新 Source。

## 为什么保留 Set / Delete

纯 read-through 缓存对于不可变或天然通过 TTL 更新的数据很合适，但对实际后端业务太受限。业务常见流程是：

```text
更新数据库
   ↓
更新 / 删除缓存
```

因此 Locache 保留显式 `Set/Delete`：

```text
Set(key, value)
   ↓
consistent hash
   ↓
unique Owner
   ↓
Owner Cache update
   ↓
best-effort invalidate peer Near Cache
```

`Delete` 同理，用于缓存失效；它不会删除数据库中的业务数据。

## Near Cache：性能与一致性的取舍

严格 Owner-only 的好处是语义简单，但热点 Key 的所有请求都会跨节点访问同一个 Owner。

开启 `WithNearCache` 后，非 Owner 可以把远程读取结果短暂保存在本地：

```text
Requester Near Cache
        │ hit
        ├──────────────> return
        │ miss
        ▼
      Owner ──> value
        │
        └────── short-TTL local copy
```

当发生 `Set/Delete` 时：

1. 先完成 Owner 上的修改；
2. 再并发、best-effort 地向其他节点发送本地 eviction；
3. 如果某个节点不可达，写操作不回滚；该节点的旧 Near Cache 最迟由 TTL 淘汰。

因此 Near Cache 模式提供的是：

> **正常情况下主动失效，异常情况下 TTL 兜底的有界陈旧。**

如果业务更看重读新值而不是跨节点读性能，可以**不启用 Near Cache**。此时所有非 Owner 读取都会访问 Owner，`Set/Delete` 完成后不会再被 requester-side 副本遮挡。

### 为什么不做强一致副本协议

要让所有 Near Cache 副本严格同步，需要引入副本目录、确认协议、版本校验或共识机制，会显著增加写放大和系统复杂度。Locache 的目标是缓存系统而不是强一致 KV，因此当前选择：

- 唯一 Owner 保证主要状态职责明确；
- Near Cache 只作为可关闭的性能优化；
- 写时主动失效降低常规陈旧窗口；
- TTL 处理失效 RPC 丢失等异常路径。

这个设计更适合**读多写少、可接受短暂陈旧**的缓存业务。

## 与 groupcache 思路的区别

项目最初参考了 groupcache / GeeCache 的 `Group + Getter + consistent hash + singleflight` 结构，但当前版本不再是简单复刻。

上游 groupcache README 明确采用较窄的不可变缓存语义：不支持 versioned values、过期时间和显式 eviction，并要求同一个 Key 始终对应相同值。Locache 在此基础上扩展为：

- 支持显式 `Set / Delete`；
- Owner Cache 支持 TTL；
- Getter 变为可选，既能作为普通分布式缓存，也能 read-through；
- etcd 动态服务发现替代静态 peer 列表；
- gRPC + Protobuf 作为节点通信；
- Near Cache 支持写时 best-effort 失效 + TTL 兜底；
- membership revision/epoch 用于处理节点变化后的路由与旧副本隔离；
- 节点内使用 Sharded LRU，而不是旧版含义不准确的“两级 LRU-2”。

参考：<https://github.com/golang/groupcache>

## 一致性哈希与动态成员

所有节点基于相同 membership 构建同一哈希环：

```text
A / B / C
   ↓
fixed virtual nodes
   ↓
same ring on every node
   ↓
same key -> same Owner
```

节点加入或退出时，只会重映射部分 Key。缓存不是权威存储，因此当前不迁移缓存数据：新 Owner 首次 miss 时重新加载或等待业务 `Set`。

项目移除了旧版“根据本节点请求量动态修改虚拟节点数”的机制。不同节点观察到的流量并不相同，各自修改 vnode 会形成不同的哈希环，破坏 Owner 一致性。

## etcd：Snapshot + Revision Watch

服务发现采用：

```text
Get(prefix) -> snapshot + revision
                  │
                  ▼
        Watch(from revision + 1)
```

避免全量快照和 Watch 建立之间发生节点变化而漏事件。

- Server 使用 lease + KeepAlive 注册；
- PUT / DELETE 更新 membership；
- Watch 中断后重新获取 snapshot 并继续；
- Server 停止时撤销 lease；
- membership 包含 self，但只有远端成员才建立 gRPC Client。

membership 变化时 epoch 递增；Near Cache key 带 epoch，因此旧拓扑下的副本不会在新拓扑中继续命中。

## singleflight：在 Owner 合并回源

如果 Group 配置了 Getter，Owner miss 后执行 read-through：

```text
Node A ─┐
Node C ─┼──> Owner B ──> singleflight ──> Getter once
Node D ─┘
```

多个入口节点对同一热点 Key 的并发请求最终汇聚到同一个 Owner，并共享一次 Source 查询。

如果 Group 没有 Getter，Owner miss 直接返回 `ErrCacheMiss`，此时 Locache 就是普通的显式 Get/Set/Delete 分布式缓存。

## 节点内缓存

Owner Cache 与 Near Cache 均使用 Sharded LRU：

```text
hash(key)
   │
   ├─ shard 0 -> LRU + mutex
   ├─ shard 1 -> LRU + mutex
   └─ shard N -> LRU + mutex
```

不同 Key 分散到独立 shard，降低全局锁竞争；每个 shard 支持容量淘汰和 TTL 清理。

## API 示例

### 显式缓存模式

```go
group := locache.NewGroup(
    "profile",
    32<<20,
    nil,
    locache.WithPeers(picker),
    locache.WithExpiration(60*time.Second),
    locache.WithNearCache(4<<20, 3*time.Second),
)

// 业务先更新数据库，再更新缓存
if err := updateProfileInDB(ctx, profile); err != nil {
    return err
}
if err := group.Set(ctx, "user:42", encodedProfile); err != nil {
    // Source 已经成功，缓存失败按业务策略记录/重试即可
}

value, err := group.Get(ctx, "user:42")

// 也可以选择只失效缓存，让下一次读重新加载/回源
_ = group.Delete(ctx, "user:42")
```

### Read-through 模式

```go
group := locache.NewGroup(
    "product",
    32<<20,
    locache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
        return loadProductFromDB(ctx, key)
    }),
    locache.WithPeers(picker),
    locache.WithExpiration(60*time.Second),
    locache.WithNearCache(4<<20, 3*time.Second),
)

value, err := group.Get(ctx, "product:1001")
```

## 请求语义

### Get

```text
Pick Owner
  ├─ self   -> Owner Cache -> [optional Getter on miss]
  └─ remote -> Near Cache -> gRPC Owner -> Owner local path
```

### Set

```text
Pick Owner
  ├─ self   -> set owner local cache
  └─ remote -> gRPC Set(owner)

Owner success
  -> best-effort invalidate non-owner copies
  -> writer may keep a fresh short-TTL Near Cache copy
```

### Delete

```text
Pick Owner
  -> evict owner copy
  -> best-effort evict copies on other nodes
```

远端 Owner 在 membership 收敛前暂时不可达时，请求会返回错误；当前不会让非 Owner 擅自变成 Owner，因为那会破坏明确的 ownership 语义。

## 项目结构

```text
group.go             Get/Set/Delete、Owner/Near Cache、singleflight
peers.go             一致性哈希、etcd snapshot/watch、成员 epoch、失效广播
server.go            Owner-local gRPC Get/Set/Delete 与 etcd lease 生命周期
client.go            gRPC Peer Client
consistenthash/      固定虚拟节点的一致性哈希环
registry/            etcd 注册、租约与地址规范化
store/               LRU / Sharded LRU / TTL
singleflight/        同 Key 并发回源合并
pb/                  Protobuf / gRPC Get/Set/Delete
example/             多节点示例
```

## 测试重点

```bash
go test ./...
go test -race ./...
```

测试重点包括：

- 相同 membership 下各节点选择同一 Owner；
- 节点加入/退出后的部分 Key 重映射；
- Sharded LRU 并发访问与 TTL；
- 显式模式下 `Set -> Get -> Delete`；
- Owner 本地 miss 的 singleflight；
- 远程读取只进入 Near Cache，不污染 requester 的 Owner Cache；
- membership epoch 变化后旧 Near Cache 不再命中；
- Near Cache TTL 形成有界陈旧窗口；
- gRPC Server 只执行 local path，不发生 peer 递归路由；
- Group / Server / registry 生命周期关闭不死锁。

CI 执行 `gofmt`、`go vet`、普通测试和 race test。

## 边界

- Locache 是缓存，不是持久化 KV；
- `Set/Delete` 只操作缓存，Source 更新仍由业务负责；
- Near Cache 开启时允许短暂陈旧，主动失效失败由 TTL 兜底；
- 写操作需要向其他节点发送失效请求，当前适合读多写少而非高写入吞吐场景；
- 节点变化时不迁移缓存数据；
- 当前没有副本 Owner、quorum、Raft 或跨机房一致性协议；
- Owner 故障到 etcd membership 收敛之间，请求可能失败。

## License

MIT
