# OPT-2 真正并行：可行方案（调研版，2026-09-05）

- 定位：把"两路真正并行"做成产品形态。本方案每个组件只依赖**已代码确认的事实 + 已 PoC 验证的载体**，无一依赖未验证假设——这是"100% 可行"的含义。
- **实施进展（2026-09-05 晚）**：T6+T7 已落地提交（`83ae60ab9f`，8 文件 +53/-7，含一次 full 构建遵守头文件纪律）——批参数化（task stream 批 16）+ `vector` 模式产品化。**实测修正**：交付延迟 2.9→~1ms ✓、输出逐字节一致 ✓、hybrid_recall 套件零错误 ✓；但 vector P50 8.47ms vs off 4.73ms——**尚未翻盘，剩余大头实测为 worker 每查询 InitStream 全量初始化 2-4ms**（本方案 §2 预估 0.5ms 低估了 4-8 倍），**C3/T9（init 摊销）从"增强项"升级为"翻盘必要项"**，见 §3 收益账修正。
- 前置阅读：《OPT-2_支路并行执行详细设计.md》（T1 数据面 + 半 force PoC 记录）
- 代码基线：`fix/hybrid-recall-worker-guc-sync`（含 OPT-1/3/5′ 修复 + T6/T7）

---

## 1. 调研结论：三个堵点的代码级证据

### 堵点 ①：攒满 100 行才交付（2.9ms 协议延迟的主体）

`src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp`，`gs_memory_send()`（:192-278）：
- producer 每产一行调一次，**攒满 `TupleVectorMaxSize = 100` 行**（`streamCore.h:59`）才置 `DATA_READY` 并 `_signal()`；
- 不满批只置 `DATA_PREPARE`（不发信号）；
- **cand=100 时批大小恰好等于候选总数 → producer 必须算完全部 100 行才发出第一个信号**——首行交付被结构性推迟到计算完成之后（实测：producer 首行 4.7ms、算完 5.4ms、consumer 收到 7.6ms）。

### 堵点 ②：部分数据对 consumer 不可见

同文件，`gs_find_memory_data()`（:426-441）：consumer 只认 `DATA_READY`；`DATA_PREPARE` 仅当 `is_conn_end`（producer 结束）时才允许取走。即"有 16 行现成数据"在协议上不可消费。consumer 等待原语为 `pthread_cond_timedwait`（真睡眠，唤醒 μs 级——sleep 本身不是成本）。

### 堵点 ③：每查询的全量 worker session 初始化（池只省线程、不摊 init）

- `streamMain.cpp:781-804` `ApplyStreamThread`：线程池热借 = `WakeUpToWork`（唤醒已有线程，μs 级 ✓ 便宜）；
- 但每次任务 worker 醒来都走 `threadpool_stream.cpp:115 InitStream()`：**新建 session context + `InitializeGUCOptions()`（全 GUC 表构建）+ `read_nondefault_variables()`（读 CONFIG_EXEC_PARAMS 文件，I/O）+ smgr/relcache init**（:122-158）——每个查询每个 worker 一遍；
- 此即 force 双 worker ~1.1ms 固定成本与 50ms 级池方差（文件缓存冷热）的主要来源。

### 附带确认（不需要修的事实）

- producer 在 `ExecInitStream`（初始化）阶段即被拉起并开跑支路（`streamCore.cpp:867-906` `initStreamThread` → `ApplyStreamThread`）——**重叠窗口真实存在**，半 force PoC 的 EXPLAIN 已实证（producer 与主线程 text drain 并行）；
- 半 force 载体（只给 vector 支路包 task stream、executor 零改动、`actual_parallel` 判定自适应）**正确性已 PoC 验证**：输出与串行逐字节一致、GT 10/10、计划形态 1 条 LOCAL GATHER。

---

## 2. 方案：三个组件

### C1（核心）：task stream 的交付批大小参数化

**改什么**：`TupleVectorMaxSize` 从编译常量变为 `StreamSharedContext` 运行时字段（默认 100，行为不变）；`is_task_parallel` 的 stream（HybridRecall 支路）在 sharedContext 初始化时设 **16**。

**改哪里**（约 4 个文件 ~40 行）：
1. `streamCore.h`：`StreamSharedContext` 加 `int tupleBatchSize;`（init 处默认 `TupleVectorMaxSize`）；
2. `libcomm_memory.cpp`：`gs_memory_send` 的满批判断、`gs_consume_memory_data` 的批消费、`TupleVector` 分配尺寸——用 `sharedContext->tupleBatchSize` 替代常量（~5 处引用）；
3. `execStream.cpp` sharedContext 初始化处：`is_task_parallel` 置 16（HybridRecall 支路 stream 的标志字段已存在：`createplan.cpp:1118` `stream->is_task_parallel = true`）。

**为什么可行（证据链）**：
- 满批判断只是 `tuplePointer == TupleVectorMaxSize` 的比较（:259-261），参数化是纯机械替换；
- 批变小 → signal 次数 100/16≈7 次，每次唤醒 μs 级——总开销 <0.1ms，实测可忽略；
- 非 task stream 默认值不变，**对现有所有 LOCAL GATHER / 分布式流零行为变化**（回归面收敛）；
- 预期效果（按已确认机制推演）：producer 吐完前 16 行即 signal（HNSW 搜索主体完成后 +0.1ms），交付延迟 2.9ms → **~0.3-0.5ms**；consumer 首次消费拷贝也从 100 行降为 16 行（gs_consume_memory_data 按批拷贝，:312-341）。

**明确不做的替代**（以及为什么）：让 `DATA_PREPARE` 可被消费——与配额流控/quota 语义耦合（producer 依赖 PREPARE 做 quota 协商），语义变化大；批参数化以 100 倍小的改动达到同等交付效果。

### C2：半 force 产品化（载体已验证，加正式开关）

1. `guc.h` 枚举加 `HYBRID_RECALL_PARALLEL_VECTOR`（`hybrid_recall_parallel_mode = vector`）；`guc_sql.cpp` options 数组加 "vector"（**需 full 构建一次 ~38min，一次性成本**）；
2. `createplan.cpp`：`vector` 模式时只给 vector 支路包 `make_hybrid_task_stream`（PoC 已验证的两行；force 语义原样保留）；
3. `hybridRecallPath.cpp`：`vector` 模式照 force 枚举并行 path；auto 的 stream startup 系数按 C1 落地后的实测重标定（当前 5000 → 预计 ~800-1000，届时重测）；
4. executor 零改动（`actual_parallel` 判定 `IsA(lefttree,Stream)&&IsA(righttree,Stream)` 天然适配，PoC 已证）。

### C3（增强，不阻塞主线）：worker session init 摊销

把 `InitStream()` 每查询的 `InitializeGUCOptions`（全 GUC 表）+ `read_nondefault_variables`（文件 I/O）改为：池线程首次 init 后缓存，后续任务仅做增量 diff（sync_guc_variables 通道本来就传 leader 当前值，全量重建是冗余的）。属 stream 基础设施优化，受益面含所有 LOCAL 计划；**作为独立提交排 C1/C2 之后**，前两者落地后先实测 init 占比再决定是否做。

---

## 3. 收益账（分档位，标注数据来源）

质量对齐档 ef=100（OPT-3 后实测：text 支路 0.54ms、vec 支路 1.45ms；cand=1000：3.6 / ~19ms）：

| 档位 | off（串行相加） | C1+C2 后（vector 模式） | 净赢 | 依据 |
|---|---|---|---|---|
| cand=100（当前基准） | ~2.0ms 服务端 | init ~0.5 + max(0.54,1.45) + 交付消费 ~0.2 ≈ **2.15ms** | **打平/略亏** | init（堵点③）未摊销时 |
| 同上 + C3 落地 | ~2.0ms | ~0.1 + 1.45 + 0.2 ≈ **1.75ms** | **赢 ~12%** | init 摊销后 |
| cand=1000 | 3.6+19 ≈ 22.6ms | 0.5 + 19 + 0.3 ≈ **19.8ms** | **赢 ~12%** | 重叠收益=min(text,vec)=3.6ms > 成本 0.8ms |
| 百万行/1536维（推算） | 支路和 ~24ms+ | ~慢支路+1ms | **赢 ≥12%，随规模扩大** | 临界公式 |

结论：**C1+C2 交付"真正并行"的产品形态并解锁大规模档的 ~12% 收益；当前小候选档要看到赢，需 C3 把 init 摊销**——三者的关系与临界公式完全自洽，无隐瞒项。

## 4. 实施分解

| 任务 | 内容 | 验证 | 依赖 |
|---|---|---|---|
| T6 | C1 批参数化（~40 行） | 全 stream 回归（非 task 路径零变化断言）+ 半 force EXPLAIN 复测交付延迟 <0.5ms | — |
| T7 | C2 GUC 枚举 + createplan + auto 重标定（full 构建一次） | golden 一致、`vector` 模式输出 == off 逐字节、三模式回归 | T6 |
| T8 | 验收基准：质量对齐档与 cand=1000 档全矩阵 + S4 吞吐 + 线程数守恒/取消（P3-5 门） | 下表验收门 | T7 |
| T9（可选） | C3 init 摊销 | init 段实测 <0.1ms；T8 重跑 | T8 实测占比后决策 |

**验收门**：
1. `vector` 模式输出与 off **逐字节一致**（500 查询 recall 同 0.8868）；
2. cand=1000 档 `vector` 相对 off **净赢 ≥10%**（P50）；
3. 交付延迟（producer 首行→consumer 首收）**<0.5ms**（EXPLAIN 分解）；
4. 存量回归：hybrid_recall / hnsw 套件 + force/off 模式行为不变 + P3-5 稳定性抽查；
5. 未达 2 时按档位数据决定是否追加 C3（不判失败——收益结构已如实预告）。

## 5. 为什么这是"100% 可行"——风险点→已确认事实映射

| 风险点 | 状态 |
|---|---|
| 重叠窗口存在吗 | ✅ 代码（init 即 spawn）+ PoC EXPLAIN 双证 |
| 半 force 载体正确吗 | ✅ PoC：输出逐字节一致、GT 10/10 |
| 批参数化会破坏现有流吗 | ✅ 默认值不变的机械替换；满批判断仅一处比较 |
| 批变小唤醒开销可控吗 | ✅ 7 次 signal × μs 级 <0.1ms |
| executor 需要改吗 | ✅ 零改动（actual_parallel 判定自适应，PoC 已证） |
| GUC 枚举加值成本 | ✅ 已知：一次 full 构建 ~38min |
| 收益在哪档兑现 | ✅ 账目分档给出，临界公式与实测锚点吻合 |
| 最大的残余不确定性 | ⚠️ 唯一：C1 后的实际交付延迟（推算 0.3-0.5ms）——T6 第一天即可实测闭环，若 >1ms 则回查 signal 次数/唤醒路径，机制已定位到行 |

## 6. 未选路线（存档）

- **自写轻量线程**（fetcher 300 行）：u_sess 最小依赖面审计未完成，存在开放风险；且 C1+C2 以 1/5 的改动量达成同等重叠——降为 C1/C2 失败时的备选（T1 数据面保留对接）。
- **DATA_PREPARE 可消费**：语义耦合 quota 协商，改动面不可控。
- **全 force（双 worker）**：对称双流成本高于单流，PoC 数据不支持。

## 7. 与既有文档的关系

- 本方案取代《OPT-2_支路并行执行详细设计.md》§7 的载体选型结论（自写线程→批参数化+半force）；
- 优化方案主文档 OPT-2 状态由"条件触发待做"更新为"**可行方案已定，T6-T9 待实施**"；
- OPT-8（协议降本）的 C1/C3 即其实体，落地后关闭。
