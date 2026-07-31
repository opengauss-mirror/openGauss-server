# LFC与预取

## 一、功能概述

LFC（Local File Cache）与 Prefetch 用于优化数据分支场景下 openGauss Compute 的读路径性能，目标是降低 Compute 同步访问 Pageserver 的等待时间。

在存储计算分离架构中，Compute 不保存完整数据文件。读取页面时，如果本地共享缓冲区中没有所需页面，Compute 需要向 Pageserver 发起 GetPage 请求。对于顺序扫描、索引扫描、VACUUM、ANALYZE 等会连续或批量访问页面的场景，逐页同步请求会带来较多远端访问等待。

该特性从两个方向优化读路径：

- Prefetch：提前识别后续可能访问的页面，并异步向 Pageserver 发起 GetPage 请求。
- LFC：在 Compute 本地维护文件缓存，保存已经读取的页面，减少重复访问 Pageserver。

整体命中优先级为：

```text
prefetch ring -> LFC -> Pageserver同步读取
```

Prefetch 与 LFC 均为性能优化能力。预取未命中、响应过期、缓存未命中或缓存不可用时，系统会回退到正常同步 GetPage 读取，不改变 SQL 查询结果正确性。

## 二、适用场景

Prefetch 更适合访问路径具有可预测性的场景，例如顺序扫描和索引扫描。LFC 更适合热点数据可复用、重复读取比例较高的场景。

## 三、Prefetch

Prefetch 用于提前从 Pageserver 拉取未来可能访问的页面。

每个 backend 维护一个私有 prefetch ring，用于保存本 backend 发起的异步预取请求及其响应。prefetch ring 中每个 slot 对应一个页面级 GetPage 请求。

prefetch ring 容量由 `neon.readahead_buffer_size` 控制。注册新预取请求时，会先检查是否已经存在相同 `BufferTag` 的可用 slot，避免重复发送相同页面的预取请求。

当 ring 已满时，会优先释放已收到但未被使用的响应。如果最旧请求仍处于等待响应状态，则需要等待该请求完成或清理后再复用 slot。

Prefetch 可由多类访问路径触发。

| 触发来源 | 说明 |
| --- | --- |
| 顺序扫描 | 根据扫描位置维护预取窗口，提前请求后续 heap 页面 |
| B-tree Index Scan | 根据后续 index tuple 中的 heap TID 预取 heap 页面 |
| B-tree Index Only Scan | 预取后续索引叶子页 |
| Bitmap Heap Scan | 按 bitmap 顺序预取后续 heap block |
| VACUUM | 在 heap 扫描、dead tuple 清理、truncate 尾页检查等阶段预取页面 |
| ANALYZE | 行存表采样时提前预取样本块，不改变采样随机序列 |

## 四、LFC

LFC 是 Compute 本地共享文件缓存，用于保存 Pageserver 返回的数据页。LFC以 chunk 为单位管理缓存空间，一个 chunk 包含多个数据库 block。

预取结果写入LFC策略

预取响应是否立即写入 LFC 由 `neon.store_prefetch_result_in_lfc` 控制。

### 1. 默认策略：消费后写入

默认值为 `off`。此时预取响应只保存在当前 backend 私有 prefetch ring 中。

后续真实读取命中该预取响应时，页面被拷贝到 shared buffer，并在 buffer lock 保护下写入 LFC。如果预取响应一直没有被消费，后续被淘汰时不会写入 LFC。

该策略可以减少未消费预取页面写入 LFC，降低本地磁盘写入和缓存污染。

### 2. 立即写入策略

设置为 `on` 时，预取响应到达后会立即写入 LFC，并在 prefetch ring slot 上设置标记，避免后续真实读取时重复写入。

该策略可以让预取页面更早进入共享 LFC，使同一 Compute 节点上的其他 backend 也可能命中这些页面。但如果预取误判较多，也可能把最终不会被访问的页面写入 LFC，增加本地写入和缓存污染。

建议在预取命中率较高、热点页面跨连接复用明显的场景中开启该策略。

## 五、约束与限制

使用 LFC 与 Prefetch 时需要注意以下约束：

1. Neon 预取只处理永久表。
2. 临时表、unlogged 表、本地构建关系等仍走 openGauss 原有本地文件路径。
3. 预取深度受 `effective_io_concurrency` 和 `neon.readahead_buffer_size` 共同影响，根据`effective_io_concurrency`转换的`target_prefetch_pages` 不应大于 `neon.readahead_buffer_size`，否则较早预取请求可能被丢弃或转为等待。

## 六、参数说明

| 参数名 | 说明 | 默认值 | 是否需要重启 |
| --- | --- | --- | --- |
| `effective_io_concurrency` | 用户可见预取并发度，openGauss 内部通过 assign hook 换算为 `target_prefetch_pages`，用于控制访问方法的预取窗口。effective_io_concurrency>1时开启预取。 | `8` | 否 |
| `enable_seqscan_prefetch` | 控制顺序扫描是否启用 Neon 预取。 | `on` | 否 |
| `enable_indexscan_prefetch` | 控制 B-tree index scan 是否预取后续 heap 页面。 | `on` | 否 |
| `enable_indexonlyscan_prefetch` | 控制 B-tree index-only scan 是否预取后续索引叶子页。 | `on` | 否 |
| `neon.readahead_buffer_size` | 控制每个 backend 私有 prefetch ring 容量，即最多保留的在途或已接收预取请求数。（修改此值需在compute的pgdata/compute_ctl_temp_override.conf里设置，无需重启，连接会话select pg_reload_conf()后修改生效） | `128` | 否 |
| `neon.readahead_getpage_pull_timeout` | 控制 backend 主动拉取已到达 GetPage 响应的间隔。`0` 表示关闭定时主动拉取。 | `50ms` | 否 |
| `neon.store_prefetch_result_in_lfc` | 控制收到预取响应后是否立即写入 LFC。 | `off` | 否 |
| `neon.max_file_cache_size` | LFC 本地文件缓存硬上限。`0` 表示禁用 LFC。（单位为kB，MB，GB） | `0kB` | 是 |
| `neon.file_cache_size_limit` | LFC 当前软限制，控制运行期实际可使用容量，不能超过 `neon.max_file_cache_size`。（单位为kB，MB，GB） | `0kB` | 否 |
| `neon.file_cache_path` | LFC 本地缓存文件路径。 | `file.cache` | 是 |
| `neon.file_cache_chunk_size` | LFC chunk 大小，单位为数据库 block，要求为 2 的幂。默认为128block，每个block为8KB，此参数修改需设置为 2 的幂。 | `1MB` | 是 |

## 七、观测方式

可通过 `neon_lfc_stats` 和 `neon_perf_counters` 观察 LFC 与 Prefetch 的运行效果。

### 1. LFC运行状态

`neon_lfc_stats` 用于观察 LFC 的使用情况，包括缓存容量、已使用空间、命中次数、未命中次数、写入次数和命中率等。

常见关注项：

| 指标 | 说明 |
| --- | --- |
| LFC used | 当前已使用缓存空间 |
| LFC hit | 从 LFC 命中的页面次数 |
| LFC miss | LFC 未命中的页面次数 |
| LFC writes | 写入 LFC 的页面次数 |
| LFC hit ratio | LFC 命中率 |

### 2. Prefetch统计

`neon_perf_counters` 用于观察 Prefetch 请求和响应情况。

常见关注项：

| 指标 | 说明 |
| --- | --- |
| `getpage_prefetch_requests_total` | 发起的预取请求数量 |
| `getpage_prefetch_misses_total` | 真实读取时未命中 prefetch ring 的次数 |
| `getpage_prefetch_discards_total` | 被丢弃的预取响应数量 |
| `getpage_prefetch_buffered_total` | 已收到并暂存在 prefetch ring 中的响应数量 |
| LFC hit 相关计数 | 页面从 LFC 命中的情况 |

如果开启预取后 `getpage_prefetch_requests_total` 增加，但 miss 或 discard 也很高，说明访问路径可能不适合当前预取深度，或预取页面在真实读取前已经过期。

## 八、注意事项

- 预取过深可能导致 prefetch ring 淘汰或等待。
- `neon.store_prefetch_result_in_lfc=on` 可能提高跨 backend 复用概率，但也可能增加无效页面写入。
- 在热点明显、重复读取较多的场景中，LFC 收益更明显。
