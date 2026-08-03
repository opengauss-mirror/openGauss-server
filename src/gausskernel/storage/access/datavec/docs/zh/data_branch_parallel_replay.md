# 并行回放

## 一、功能概述

并行回放用于优化数据分支场景下 Pageserver 的页面重建性能，目标是降低多个页面同时触发 WAL redo 时的排队时间。

在存储计算分离架构中，Compute 读取某个 LSN 下的数据页时，如果 Pageserver 只有较早的 base image 和后续 WAL 记录，需要通过页面重建得到目标页面。页面重建过程中，部分记录可由 Pageserver 内置逻辑直接处理；其余需要 openGauss walredo 子进程执行回放。

walredo 本质上是一个以特殊模式启动的 openGauss 进程。它不作为普通数据库实例对外提供 SQL 服务，只接收 Pageserver 发送的页面镜像和 WAL 记录，执行 WAL 回放后将重建出的页面返回给 Pageserver。

原有模式下，每个 tenant shard 只维护一个 walredo 子进程。多个 GetPage、compaction 或其他页面重建请求同时需要 openGauss WAL redo 时，会集中到同一个 walredo 子进程上执行，容易形成排队。

该特性将每个 tenant shard 的 walredo 从单进程扩展为可配置的进程池，并通过全局额外进程额度控制资源占用。并行回放不改变 WAL 语义和页面重建结果，只提升需要外部 walredo 的页面重建并发能力。

## 二、适用场景

并行回放更适合存在高并发页面重建的场景，例如并发读请求集中访问需要 WAL redo 的页面、缺少近端 image layer 导致 WAL 链较长、后台 compaction 与读路径同时触发页面重建等。

如果页面读取主要命中 Pageserver 已有页面版本、LFC、本地缓存，或者 WAL 记录大多可由 Pageserver 进程内直接处理，则并行回放收益不明显。

## 三、并行回放机制

每个 tenant shard 维护一个 walredo 进程池，进程池大小由 `wal_redo_concurrency` 控制。进程池中的每个 slot 对应一个 walredo 子进程，子进程按需懒启动，不会在 Pageserver 启动时立即全部创建。

当请求需要 openGauss WAL redo 时，Pageserver 会按轮询方式选择一个 walredo slot。不同请求可以分散到不同子进程执行，从而减少单 walredo 子进程上的排队。

slot 0 是保底进程，不消耗全局额外进程额度。slot 1 到 slot N-1 是额外进程，启动前需要获取全局 extra permit。如果全局额外额度不足，请求会回退到 slot 0，而不是等待额外额度释放。

整体请求分发关系如下：

```text
GetPage/compaction页面重建请求
        |
        v
walredo 管理器
        |
        v
按轮询选择 walredo slot
        |
        +-- slot 0：保底 walredo 进程
        |
        +-- slot 1..N-1：额外 walredo 进程，受全局额度限制
```

## 四、约束与限制

使用并行回放时需要注意以下约束：

1. 并行回放只提升需要 openGauss walredo 子进程处理的页面重建并发能力。
2. 同一个 walredo 子进程内部仍按自身请求顺序处理，进程池提升的是多个独立 redo batch 的横向并发。
3. 全局额外额度不足时，请求会回退到 slot 0，功能不会失效，但并发收益会下降。
4. 调高 `wal_redo_concurrency` 会增加潜在进程数，需要结合 CPU、内存、文件描述符和 tenant 数量评估。

## 五、参数说明

以下参数配置在 `pageserver.toml` 中，修改后需要重启 Pageserver 生效。

| 参数名 | 说明 | 默认值 | 取值限制 |
| --- | --- | --- | --- |
| `wal_redo_concurrency` | 单个 tenant shard 的 walredo 进程池大小。 | `1` | 取值大于 0，最大值为 `8` |
| `wal_redo_global_extra_concurrency` | Pageserver 级别额外 walredo 进程总额度。slot 0 不消耗该额度。 | 未显式配置时为 `wal_redo_concurrency - 1` | 最大值为 `64` |

配置示例：

```toml
# 单个 tenant shard 最多保留 4 个 walredo 进程 slot。
wal_redo_concurrency = 4

# 所有 tenant shard 共享 16 个额外 walredo 进程额度。
wal_redo_global_extra_concurrency = 16
```

如果只希望保持接近原有单进程行为，可使用默认值，或显式设置：

```toml
wal_redo_concurrency = 1
```

## 六、观测方式

可通过 Pageserver 的 tenant status API 查看 walredo 进程状态：

```bash
curl http://127.0.0.1:<pageserver_http_port>/v1/tenant/<tenant_shard_id>
```

返回结果中的 `walredo.processes` 会列出当前已经启动的 walredo 子进程：

```json
{
  "walredo": {
    "last_redo_at": "2026-06-05T02:00:00Z",
    "processes": [
      { "pid": 12345 },
      { "pid": 12346 }
    ],
    "process": { "pid": 12345 }
  }
}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `last_redo_at` | 最近一次发生 WAL redo 的时间 |
| `processes` | 当前已启动的 walredo 子进程列表 |
| `process` | 兼容旧客户端的字段，返回 `processes` 中的第一个进程 |

也可以通过 Pageserver 日志观察 walredo 子进程启动情况：

```text
launched walredo process ... pid=<pid> pool_idx=<idx>
```

其中 `pool_idx=0` 表示保底进程，`pool_idx>0` 表示额外进程。

## 七、注意事项

- 单租户压测或少量热点 tenant 可从 `wal_redo_concurrency = 4` 开始验证。
- 多 tenant 场景建议同时设置 `wal_redo_global_extra_concurrency`，避免额外 walredo 进程数量不可控。
- 如果 `processes` 长期只有一个进程，可能是并发压力不足、未触发外部 walredo、或全局额外额度不足。
- 调高并发后建议关注 Pageserver CPU、内存、文件描述符、walredo 启动次数、redo latency 和读请求延迟。
- 该功能不改变数据可见性、分支隔离或 WAL 回放正确性。
