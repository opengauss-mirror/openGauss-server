# HybridRecall 优化方案（基于 vs Milvus 2.6.22 对比实测）

- 日期：2026-09-05
- 依据：《两路召回性能对比测试报告.md》（同目录）全部实测数字与 evidence（`/home/qiumc/data/code/cpp/deliverables/hybrid_vs_milvus_bench/evidence/`）
- 代码基线：分支 `backup/feature-hybrid-recall-stacked-20260903`，HEAD `8f2410d20d4e`，release 构建
- 复测基准：`deliverables/hybrid_vs_milvus_bench/` 全链路（gen/load/smoke/latency/throughput/quality），固定主档 `og:fusion:off|force:100:ef400` vs `mv:fusion:def:100`
- **阅读指引**：第 0 章是不需要内核背景的白话读本，先读它；看懂第 0 章就能理解每个优化在干什么。第 1 章起是给执行者的技术细节（源码定位、验收标准）。

---

# 第 0 章 先读懂这份方案（白话读本）

## 0.1 一条两路召回查询，在两个系统里分别怎么跑

用户发出的查询是同一件事：**"给我找 10 篇既跟这段文字相关、又跟这个向量相近的文档"**。做法都是三步：文本路找 top-100、向量路找 top-100、两路合并（RRF）取前 10。差别在**执行方式**。

**openGauss（我们的 HybridRecall，串行模式）——排队跑：**

1. 应用发 SQL，里面带一个 `hybrid_rrf_marker(...)` 标记函数；
2. 优化器认出这个标记，放弃普通 SQL 执行路径，改用一个专用算子 `Hybrid Recall`，它身上挂两个子计划：**文本支路**（走 BM25 索引）和**向量支路**（走 HNSW 索引）；
3. 执行时**先把文本支路跑完**：用查询词在 BM25 倒排索引上打分，取出最相关的 100 篇文档（实测 ~1.5ms）；
4. **然后才启动向量支路**：拿查询向量在 HNSW 图里搜索，取出余弦最近的 100 篇（实测 ~4.2ms）；
5. 两路共 200 篇（去重后 ~157 篇）按 RRF 公式重新打分（每篇分数 = 1/(60+文本路排名) + 1/(60+向量路排名)），取前 10 返回。这一步实测 **0.006ms，可以忽略**。

**Milvus（专用向量库）——同时跑：**

1. 应用调 `hybrid_search()`，带上两条检索请求（查询文本 + 查询向量）；
2. 服务端把两条检索**同时**分发给两个执行组件，各自返回 top-100；
3. 服务端做同样的 RRF 取 top-10 返回；
4. 总耗时 ≈ **两条路中较慢的那条**（同时跑，等慢的收工）。

时序对比（实测数字，质量对齐档）：

```text
openGauss 串行(off)                     Milvus（服务端并行）
────────────────────────────           ─────────────────────────────
文本路  ████████ 1.5ms                  文本路  ████████ ~1ms   ┐ 同时
              ↓ 跑完才轮到它                                        ├ 跑
向量路  ██████████████ 4.2ms            向量路  ██████ ~2ms     ┘
RRF    · 0.006ms                        RRF    ·
────────────────────────────           ─────────────────────────────
服务端合计 ≈ 5.7ms（1.5 + 4.2 相加）    服务端合计 ≈ 慢的那条 ≈ 2ms
```

**这就是 5.0ms vs 2.7ms 差距的全部秘密：不是哪个索引慢，也不是融合算法慢（0.006ms），而是"排队相加"对"同时取慢者"。** 融合公式两边一模一样。

## 0.2 三个必须懂的名词

- **候选（candidates = 100）**：每路先各取前 100 篇，给融合留足池子，再合成 top-10。下文 cand=100/1000 就是这个数。
- **ef（HNSW 的"探索口袋"）**：HNSW 索引是一张"谁和谁相近"的关系图，查询 = 从入口出发沿着边走向查询点附近。搜索途中维护一个候选口袋，**口袋越大走得越广、结果越准、也越慢**。实测同一个口袋大小（ef=100），Milvus 找回 99.8% 的真答案、openGauss 只找回 80%——**OG 需要 4 倍口袋（ef=400）才追平 Milvus ef=100 的准确度**。这说明 OG 的图搜索走法效率低（同样的探索量装到的东西少），是独立的算法问题。
- **recall@K（找回率）**：返回结果里有多少是真最优。100 篇真最优找回 99 篇 = 0.99。**比较两系统性能必须先把 recall 调到一样**（否则"快"是偷工减料换的）——这就是报告里"质量对齐档"的含义：OG 设 ef=400、Milvus 设 ef=100，两边 recall 都是 ~0.88（融合口径），这时的延迟对比才公平。

## 0.3 实测暴露的三个问题 = 三个主要优化项

**问题一（OPT-1）：并行模式的"口袋设置"传不到干活的线程 —— 这是个 bug。**
openGauss 其实有并行模式（force）：两路各派一个 worker 线程同时跑。但实测发现，会话里设置的 ef=400 **传不到 worker 线程**——worker 用了出厂默认 40。所以并行模式"又快又错"：向量路 4.2ms 降到 2.4ms 是因为口袋小了所以快，但找回率崩到 0.80。
根因：数据库主线程给 worker 线程同步配置（GUC）靠一份**白名单**（`guc.cpp` 里一个字符串数组），建白名单时向量索引刚出现，ef 这类参数没登记进去。
**修复 = 往数组里补几个字符串，核心代码 ~10 行。**

**问题二（OPT-2）：串行模式两路排队，白等了 1.5ms。**
0.1 图里可见：文本路 1.5ms 跑完后 CPU 空转等向量路。理想状态是两路同时开工、总耗时只等慢的那条（4.2ms）——省下的 1.5ms 就是较短那条路的时间。
为什么现在会排队？数据库执行器的标准工作方式是"一问一答"地拉取：算子问文本路要一行、等它给一行，问满 100 行才轮到去问向量路。对普通算子这没错（子计划间往往有依赖），但两条检索支路互不需要对方结果，排队纯属惯性。
为什么改了不会算错？因为 **RRF 只看每篇文档在各自路里的排名**，跟两路谁先谁后完成无关。
⚠️ 但有个物理限制要想清楚：热态下两路的耗时都是纯 CPU 计算，**单线程内"轮流问"不省时间**（一个核轮流算，总时间不变）。真正省时间只有两条路：(a) **多核并发**——给慢的那条路配一个轻量线程（force 模式已经这么做了，但它开 worker + 管道传数据的固定开销 ~1.1ms，把省的 1.5ms 吃掉大半，所以要做"更便宜的并发"）；(b) **IO 等待重叠**——冷态时向量路要读 410MB 索引、大量时间在等磁盘，等待期间跑文本路是白捡的（冷态 32ms 场景收益大）。
**收益上限 = 较短那条路的时间（1.5ms）：5.7 → ~4.7ms。**

**问题三（OPT-3）：OG 的图搜索要 4 倍口袋才追平别人。**
如果能让 ef=100 就达到 Milvus 的准确度，向量支路从 4.2ms 降到 2.2ms（实测 ef400→ef100 的延迟差）。为什么走法效率低？有三个嫌疑：(a) ef 参数被当成了"一次取多少条"而不是"探索口袋多大"，语义用错了；(b) 图的入口点选择太单一，大数据量时走进死胡同出不来（实测低 ef 时整个 top10 全落进错误的主题簇）；(c) 搜索提前终止条件太激进。**先做 3-5 天实验分辨是哪个，再动代码**——这一项不确定性最大，但收益也最大（单项 -40%）。

## 0.4 三个优化的效果怎么叠加

```text
现状             文本 1.5 │ 向量 4.2（排队）      服务端 ≈ 5.7ms   客户端 P50 5.0ms
+OPT-2 轻量并发  文本 1.5 ┐
                 向量 4.2 ┘ 同时（多核/IO重叠）  服务端 ≈ 4.7ms   客户端 ≈ 3.8ms
+OPT-3 提效率    文本 1.5 ┐
                 向量 2.2 ┘ 同时                 服务端 ≈ 2.4ms   客户端 ≈ 2.0ms

Milvus 参照（客户端 P50 2.7ms）
```

（OPT-1 不直接提速，它是修正确性：让并行模式的质量数据变得可信，后面所有涉及并行的复测才有意义。）

**叠加后客户端 ~2.0ms，反超 Milvus 的 2.7ms。** 前提是 OPT-3 实验证实"ef 语义"这个嫌疑；如果实验发现是图本身质量问题（更难改），叠加效果回落到 ~3.8ms（打平档）。

## 0.5 为什么按这个顺序做

1. **OPT-1（P0，1 天）**：bug 修复 + 地基。不修它，并行模式的任何性能数据都是错的（质量没对齐），后面复测全白做。
2. **OPT-3 实验（P1 前置，3-5 天）**：只做测量不改码，出根因结论后才知道 OPT-3 的真实收益和难度。
3. **OPT-2（P1）**：改 executor 核心路径，收益确定（1.5ms）但验证成本高。
4. **OPT-4/5/6（P2）**：构建提速、多连接自动降级、ef 进查询选项——锦上添花，各自独立。

## 0.6 其余四个优化项一句话版

| 项 | 一句话 |
|---|---|
| OPT-4 构建提速 | 建 HNSW 索引 636 秒太慢（Milvus 6 秒）；代码里其实有并行构建的底子，先确认能否启用（目标 ≤160s）。 |
| OPT-5 并发感知 | 8 个连接同时查询时，并行模式（每查询占 2 个 worker）互相挤兑反而更慢（319 vs 串行 883 QPS）；让优化器感知当前负载，忙时自动用串行。 |
| OPT-6 ef 进查询选项 | 现在要达到高准确度必须手工敲 `SET hnsw_ef_search=400`；把它做成查询参数（像 candidates 一样写在 options 里），应用侧才能逐查询表达"要质量还是要速度"。 |
| OPT-7 冷启动（可选） | 索引 410MB 大于数据库缓存，偶尔被挤出去后第一轮查询慢 6 倍；评估常驻内存选项。 |

---

# 第 1 章 差距分解 → 优化项映射（技术版）

| # | 实测差距（同质量档） | 根因定位 | 优化项 | 优先级 |
|---|---|---|---|---|
| 1 | 并行 force 模式融合 recall 0.830（串行 0.883），输出与 ef=100 完全一致 | stream worker 不同步 `hnsw_ef_search`：`sync_guc_variable_namelist[]` 白名单（`src/common/backend/utils/misc/guc.cpp:255`）无任何 datavec GUC，worker 经 `SetStreamWorkerInfo`（`streamMain.cpp:746`）只装载白名单 GUC → 用进程默认 ef=40 | **OPT-1 GUC 同步修复** | **P0** |
| 2 | 串行 fusion = 支路严格相加（服务端分解 text 1.50 + vec 4.17 + fusion **0.006ms**，见《两路召回性能分析.md》）；Milvus fusion(3.3) < 单路和(4.7) | 串行路径严格顺序 drain：`nodeHybridRecall.cpp:839-842` 先 `hybrid_drain_branch(text)` 后 `hybrid_drain_branch(vector)`，零重叠 | **OPT-2 支路重叠执行** | **P1** |
| 2b | 向量支路占质量对齐档延迟 68%（4.17/6.16ms）、大候选档 83%（18.9/22.7ms）；ef 400→100 使 vec 支路 -47%（4.17→2.20ms）但 recall 1.00→0.80 | HNSW 搜索需 4x ef 才达 Milvus 同质量（`hnswscan.cpp:124` ef 语义差异假设） | **OPT-3 HNSW 搜索效率专项**（**P1 首位**，单项 -40%） | **P1** |
| 3 | OG 需 ef=400 才达 Milvus ef=100 的 recall（4x ef）；ef=100 时 0.80 且整簇错收敛；ef 200→400 从 0.80 跳 1.00 | HNSW 搜索终止条件/入口策略与 Milvus segcore 的 ef 语义差异（`hnswscan.cpp:124` `batchSize = u_sess->datavec_ctx.hnsw_ef_search`）；earlystop 默认 INT32_MAX（`hnsw.h:117`）已排除 | **OPT-3 HNSW 搜索效率专项** | P1 |
| 4 | HNSW 构建 636s（同步阻塞）vs Milvus 索引 6s（异步声明） | `hnswbuild.cpp` 已有 PARALLEL_KEY 并行基础设施（:47-49）但本机实测未走并行；CREATE INDEX 同步等待 | **OPT-4 构建提速/异步化** | P2 |
| 5 | force 并发扩展 3.1x（319 QPS@8c）差于 off 4.7x（883 QPS@8c） | 多连接下每查询双 worker 池竞争；P2-6 成本模型不含并发负载信号 | **OPT-5 auto 模式并发感知** | P2 |
| 6 | 用户需手工 `SET hnsw_ef_search=400` 才达质量对齐（默认 40） | ef 未纳入 marker options / hybrid_search_v2 options 注入路径 | **OPT-6 ef 质量档自治** | P2 |
| 7 | 交替负载冷态 P50 32ms vs 热态 5ms（6.4x） | HNSW 索引 410MB 远超 shared_buffers，工作集被挤出；Milvus 常驻内存无此效应 | OPT-7 索引常驻/冷启动（可选） | P3 |

当前基线与目标（同质量档，D2 主档）：

| 指标 | 当前 | OPT-1 后 | OPT-1+2 后（目标） | Milvus 参照 |
|---|---|---|---|---|
| fusion P50/P95（串行语义最优档） | 5.0 / 7.8 ms | 5.0 / 7.8 ms（不变） | **≤3.5 / ≤5.5 ms** | 2.7 / 3.7 ms |
| force 模式融合 recall@10 | 0.830 | **0.883**（对齐串行） | 0.883 | 0.879 |
| force 模式 P50 | 9.6 ms（且质量缺陷） | ~10 ms（质量对齐） | **≤5 ms**（质量对齐） | — |
| 并发8 QPS（最优档） | 883 | ≥883 | **≥1000** | 977 |

---

## OPT-1（P0）：stream worker 同步 datavec 搜索 GUC —— ✅ 已修复（2026-09-05）

> 【白话速览】并行模式的干活线程拿不到 `SET hnsw_ef_search=400` 的设置（同步配置的白名单数组里漏登记了向量索引参数），只能用出厂默认 40，导致并行模式"快但错"。修复 = 白名单补几个字符串，~10 行代码。

> **修复状态**：分支 `fix/hybrid-recall-worker-guc-sync`，提交 `01fef232fd`（+14/-1，仅 `guc.cpp` 白名单 6 项），release 增量构建后实测验证：
> - force cand=100 ef=400 输出与串行 off **逐字节一致**（5/5 查询）、对暴力 GT **10/10**（修复前 4/10）；
> - 融合 recall@10（500 查询）：force **0.8834 == off 0.8834**（修复前 0.8298）；
> - 修复后 force 分解：total 7.29ms、vec 支路 3.03ms（真实 ef=400；修复前 2.39ms 是 ef=40 的虚假快）、overlap 1.02ms；
> - 串行模式与 ef=100 路径行为不变（回归通过）。
> 修复不改变"cand=100 档 force 仍略慢于 off"的结论（7.29 vs 6.16ms，worker 开销），但并行档质量数据从此可信——下文 OPT-2/3 的并行复测基础已就绪。验证脚本：`deliverables/hybrid_vs_milvus_bench/verify_opt1.py`。

### 现象与证据
- 串行 `off` + cand=100 + ef=400：融合输出与暴力 GT **10/10 一致**，recall@10=0.8834；
- 并行 `force` 同参数：**4/10**，输出与 ef=100 时逐字节相同（evidence/S2/smoke.log）；
- force cand=400：8/10（candidates 增大被误当成质量来源）。

### 根因（已定位到行）
openGauss stream worker（producer 线程）拥有独立 `u_sess`，从 leader 同步 GUC 的唯一通道是白名单：
- 白名单定义：`src/common/backend/utils/misc/guc.cpp:255` `const char* sync_guc_variable_namelist[]`（work_mem、vacuum_* 等 ~150 项，**无任何 hnsw/datavec/bm25/hybrid 项**）；
- 装载点：`init_sync_guc_variables()`（guc.cpp:6069）按名 bsearch 建 `sync_guc_variables`；
- worker 应用点：`src/gausskernel/process/stream/streamMain.cpp:746` `SetStreamWorkerInfo()` 把 producer 携带的列表装进 worker `u_sess->utils_cxt`；
- 支路扫描读值点：`hnswscan.cpp:124` `batchSize = u_sess->datavec_ctx.hnsw_ef_search`、`:275` `so->length = ...` → worker 永远读到进程默认 40。

### 改动设计
1. `sync_guc_variable_namelist[]` 追加（建议集合，逐项评估）：
   - **必须**：`hnsw_ef_search`（本缺陷主体）
   - 强相关：`hnsw_earlystop_threshold`、`hnsw_max_scan_tuples`、`hnsw_iterative_scan`
   - RabiTQ 搜索行为：`rbq_query_bits`、`rbq_refinek`（若为 USERSET 搜索期参数）
   - 评估项：`hybrid_recall_*` 资源上限 GUC 五项（P3-3 的 executor 再校验若发生在 worker 侧才需要；当前校验在 leader 侧 `ExecutorStart`，先确认再加）
2. 同步机制无需新代码：GUC 注册的变量指针本就指向 `u_sess` 偏移（`guc_sql.cpp:2929` 一组均 `&u_sess->datavec_ctx.*`），worker 应用同名 GUC 时即写入 worker 自身 `u_sess->datavec_ctx` 对应字段。
3. 需要补的验证：确认 leader 在 producer 启动（`streamProducer.cpp:308` `m_sync_guc_variables = ...`）时的快照时机在 `SET` 之后（我们的基准 harness 是连接 init 时 SET、后续 EXECUTE，时序天然满足）。

### 测试与验收
- 新增定向用例（挂进 `src/test/regress/sql/hybrid_recall.sql` 或独立 `hybrid_recall_guc_sync.sql`）：
  ```sql
  SET enable_hybrid_recall = on; SET hybrid_recall_parallel_mode = force; SET hnsw_ef_search = 400;
  -- 大表（>P2-6 并行阈值）上 marker 查询 force 输出
  -- 与 hybrid_recall_parallel_mode = off 同参数输出 diff 为空
  ```
- 复用 smoke.py 场景 3（ef 扫描）在 force 模式下复测：ef=400 → ANN recall@100 = 1.00（当前 0.80）；
- S5 复测：`og:fusion:force:100:ef400` recall@10 从 0.830 → **0.883**（= 串行档）；
- 回归：`pg_orca/test/test.sh --orca-tests` 相关套件 + `make hybrid_recallcheck` + P0-P3 存量 evidence SQL 重跑。

### 风险与回退
- 风险：白名单加长带来每次 stream 启动时 GUC 序列化开销（每项 ~几十字节，可忽略）；某 GUC 不适合跨线程同步（如诊断类）会改变 worker 行为——逐项评估而非全量搬入。
- 回退：数组还原即回退，无 schema/数据迁移。

---

## OPT-2（P1）：支路重叠执行，消除串行顺序 drain —— 📐 可行方案已定，T6-T9 待实施（2026-09-05 三次修订）

> **状态**：应"真正并行是需要的"的定位要求，已完成代码级调研并产出 **100% 可行方案**：《OPT-2_真正并行_可行方案.md》（同目录）。三个堵点全部钉到行号（攒满 100 行才交付 `gs_memory_send`/`TupleVectorMaxSize`、`DATA_PREPARE` 对 consumer 不可见、每查询全量 worker session init `InitStream()`），方案 = 批大小参数化（task stream 100→16）+ 半 force 产品化（新 GUC 枚举 `vector`，载体 PoC 已验证）+ init 摊销（增强项）。收益分档如实：cand=1000/大规模赢 ~12%，当前小候选档需 C3 落地后赢 ~12%。实施分解 T6-T9 见该文档 §4。

### 原始背景（临界公式，仍然有效）

> 【白话速览】串行模式两条检索支路排队跑（文本 1.5ms 跑完才轮到向量 4.2ms，总 5.7ms；两条支路互不需要对方结果，排队纯属执行器"一问一答"拉取模型的惯性）。目标：同时开工、总耗时取较慢者（≈4.2ms），省的 1.5ms 就是较短支路的时间。**物理约束（重要）**：热态下两支路都是纯 CPU 计算，"单线程内交替问"不省时间（一个核轮流算总时间不变）——真重叠要么靠多核并发（轻量线程），要么靠 IO 等待重叠（冷态专属）。RRF 只看各自名单内的排名，不依赖两路的完成顺序，所以换执行方式结果不变（golden 回归可证）。

### 现象与证据
- 单路：text 0.54ms + vec 2.70ms（ef400）= 3.24ms；
- 串行 fusion：P50 5.0ms → **顺序执行外多出 ~1.8ms**（融合+算子开销），且两支路零重叠；
- Milvus：fusion 3.3ms < 单路和 4.7ms（服务端两路并行，取 max 而非 sum）；
- 并行 force（P3-7a 轮询优化后）本已实现重叠，但受 OPT-1 拖累质量；修复后 force P50 ~9.6ms 仍高于 off 的 5.0ms——**并行开销（双 worker 启动 + stream 传输）在小候选档吃掉重叠收益**。

### 设计（两层递进；原"融合开销剖析"档已由实测关闭——Fusion Time 全档 0.006~0.024ms）

> 档2 已有可直接开工的详细设计：《OPT-2_支路并行执行详细设计.md》（同目录）——单侧 fetcher 线程方案：代码骨架、四个安全点（单写者原则/内存隔离/错误冒泡/取消与 rescan）、逐节点审计清单（R1-R4）、线程骨架合规选项、结果等价性论证、T1-T5 原子任务分解、验收门与备胎（搜索内核预取）。

**收益的物理来源必须先讲清**（修正早期表述）：热态实测两支路耗时主体均为纯 CPU（BM25 打分 / HNSW 图遍历），因此：
- 纯单线程"交替消费"在热态收益 ≈ 0（单核轮流算，总时间不变）；
- 真正的重叠收益只有两个来源：**多核并发**（把一条支路丢给另一个核）或 **IO 等待重叠**（冷态下一支路读盘等待期间跑另一支路的 CPU）。

1. **【零代码】OPT-1 修复后重标定 auto 阈值**：force/off 的选择逻辑（P2-6 成本模型）用实测重标定——当前 10 万行 cand=100 档 off 更快（total 6.16 vs 7.29ms 修复后，force 重叠收益 1.0ms 被 worker/stream 开销 ~1.1ms 抵消），force 仅在支路更重时才有净空间。行动：跑 S3 全矩阵（cand 100/1000 × off/force × 表规模）刷新 P2-6 系数，让 auto 在正确场景选对。
2. **【中高风险】轻量并发执行**（对 `nodeHybridRecall.cpp:839-842` 的顺序 drain 动刀）：目标是用比 force 便宜得多的方式拿回 min(text, vec) ≈ 1.5ms 的重叠收益（cand=100 档：6.16 → ~4.7ms）：
   - **形态 a（热态主方案）单侧裸线程**：仅给慢支路（向量）配一个裸执行线程，主线程同步跑快支路（文本）；两支路是相互独立的 PlanState 子树、无数据依赖，需要处理的只有：每支路独立内存上下文（AllocSet 非线程安全）、错误/取消传播（支路失败冒泡到主线程按既有 ERROR 语义处理）、rescan 生命周期。相比 force 省掉的是：stream producer/consumer 协议、计划序列化传输、双 worker。
   - **形态 b（冷态增强）异步预取**：主线程 drain 文本支路前，先对向量支路发起首批结果的异步预取（IO 与计算重叠）；冷态 32ms 场景收益大（读盘等待是真等待），热态无收益。
   - ⚠️ 语义约束（两形态通用）：融合是 rank-based（RRF），**不要求支路消费顺序**，只要求每支路内部顺序——输出与执行方式无关，需保持 P0-4 golden 不变（回归可证）。

### 验收
- 档1：auto 模式在 10 万行 cand=100 选 off、cand=1000 视 OPT-3 后复测（EXPLAIN 证据）；
- 档2 形态 a：串行档服务端 total **6.16 → ≤4.8ms**（热态），线程开销 <0.5ms；golden/串并一致性回归全绿 + P3-5 稳定性门（100 次重扫、取消、线程数守恒）；
- 档2 形态 b：冷态（重启实例首轮）total 相对现状降 ≥30%。

### 风险
- 档2 触碰 executor 核心路径：必须过 P3-5 同款稳定性门（100 次真重扫、取消/超时、线程数守恒、memcheck 全绿）；
- 交替消费改变支路拉取时序，需重验 P2-4 recheck 与 MVCC 快照语义（两支路同一快照的既有不变量不能破）。

---

## OPT-3（P1）：HNSW 搜索效率——同 recall 所需 ef 为 Milvus 的 4 倍 —— ✅ 已修复（2026-09-05）

> 【白话速览】图搜索的"探索口袋"（ef）要开到 Milvus 的 4 倍大才追平人家的准确度（ef400 vs ef100），低 ef 时甚至会整个找错主题簇。

> **修复状态**：根因不是 ef 语义，而是**上层导航单路径贪心下降**（`hnswscan.cpp` 上层 `HnswSearchLayer(..., 1, lc, ...)` 每层只保留 1 个候选）——200 簇数据上整簇走错（证据：recall 按整数量级跳变、recall@10 ≈ recall@100）。修复 = 上层下降 ef 1→8（**2 行**，提交 `17b1e06c97`）：
> - recall@100（ef=100）：0.7495 → **0.9995**（Milvus 同 ef：0.998）
> - **质量对齐档迁移到 ef=100**（与 Milvus 同参）：融合客户端 P50 5.0 → **3.5ms**（差距 1.85x → 1.3x），vec 单路 2.01 → **1.45ms**（反超 Milvus 2.4ms）
> - 并发8 吞吐：883 → **1146 QPS，反超 Milvus（977，+17%）**
> - 融合 recall@10（500 查询）：0.8868（旧对齐档 0.8834，Milvus 0.879）
> - 回归：hnsw_rabitq_vector / hnsw_lsg_vector 与 S0-3 基线一致（负向断言 + 已知活实例伪影）
>
> **连锁影响——OPT-2 转条件触发**：OPT-3 后当前基准（10 万行 cand=100）的重叠收益上限 = min(text,vec) 缩水到 ~0.5ms，低于协议成本（~3ms），故当前规模下不实施；但收益随规模线性增长（见 OPT-2 节临界公式），百万行/大候选/重 text 场景过临界后必须做——届时若 OPT-8（协议降本）已完成，半 force 两行改动即达目标。

### 现象与证据（ef-recall 阶梯，D2 limit=100，修复前）
| ef | 40 | 100 | 200 | 400 |
|---|---|---|---|---|
| OG recall@100 | 0.72 | 0.80 | 0.80 | 1.00 |
| Milvus | 拒绝(ef<k) | **0.998** | — | — |

两个异常特征：
1. ef=100→200 **零改善**（0.80 平台）后 400 突跳 1.00——不是平滑的 ef-recall 曲线，暗示搜索在某规模处被硬性截断或入口策略饱和；
2. 低 ef 时 top10 **整体落入错误主题簇**（非均匀丢召回）——入口点到目标簇的导航路径质量差。

### 根因假设（需实验甄别，按可能性排序）
1. **ef 语义差异**：`hnswscan.cpp:124` 把 ef 当 `batchSize`（一次取多少）而非 Milvus 的"候选队列长度"（探索宽度）——迭代扫描按 limit 分批出队时有效探索宽度可能远小于 ef；
2. **入口点策略**：单入口/固定入口（hnsw.cpp 入口层选择），大数据集上贪心下降到局部最优簇后 ef 内无法跳出；
3. **终止条件**：出队满 k 即停，未做"探索队列耗尽"检查（与 1 等价表述）。

### 实验计划（先测量后改码）
1. 细粒度 ef 扫描（25/50/75/100/150/200/300/400）× limit（10/100）画 recall 曲线，确认平台边界；
2. 与 Milvus segcore 对照读码：`VectorMemIndex.cpp` 的 ef 突进逻辑 vs `hnswscan.cpp` 的迭代出队逻辑，对齐"ef=候选队列宽度"语义；
3. 构造消融：同数据 ef=100 下换多入口（取 entry 层多个起点）验证假设 2。

### 改动候选（视实验结论）
- 若假设 1/3：`hnswscan.cpp` 搜索层把内部探索队列宽度与用户 ef 解耦（探索宽度 = max(ef, k+α)）或明确 ef 语义文档化；
- 若假设 2：入口层多点采样（已建索引不重灌即可生效）。

### 验收
- **ef=100（limit=100）recall@100 ≥ 0.99**，或若达不到则把"OG ef 需求 = 4x"写入使用指南的质量预算表并联动 OPT-6；
- 融合档延迟复查：若 ef=400→100 可行，串行 fusion P50 有 **~2ms 级额外收益**（vec 单路 2.7→~1ms）；
- 回归：hnsw_rabitq_vector / hnsw_lsg_vector / diskann 等存量 datavec 套件不回退。

### 风险
- 搜索核心改动波及所有 HNSW 用户（含非 HybridRecall 场景）——建议独立特性开关（reloption 或 GUC）灰度；
- recall 提升通常换延迟，验收必须**同 recall 档比延迟**而非单独看 recall。

---

## OPT-4（P2）：HNSW 构建提速与装载体验

### 现状
10 万行 × 768 维（M=16/efC=200）同步构建 **636s**；Milvus 同参数 6s（异步声明，后台构建）。导入侧 COPY 12.7s vs insert 18.8s 本身不构成差距。

### 行动
1. **确认并行构建启用条件**：`hnswbuild.cpp` 已有 `PARALLEL_KEY_HNSW_SHARED` 等基础设施（:47-49、:980 shared HnswElement）；核查 `max_parallel_maintenance_workers`（openGauss 对应 GUC）与当前实例配置，实测并行度 4/8 的构建加速比；
2. **评估异步构建**（产品语义变化，需评审）：CREATE INDEX 快速返回 + 后台填充 + 期间查询走旧路径/降级路径。Milvus 的 6s 是"声明即返回"；openGauss 若保持同步语义，至少给 `maintenance_work_mem` 调优指南（图构建的内存水位决定溢出代价）；
3. 与 DiskANN 项目的构建经验对齐（同仓库 datavec 家族）。

### 验收
- 并行构建：10 万行构建 ≤ **160s**（并行度 8，加速比 ≥4x）；
- 文档：使用指南补"构建资源调优"节（maintenance_work_mem / 并行 workers / 预期时长表）。

---

## OPT-5（P2）：auto 模式并发感知，治理多连接 worker 竞争 —— ✅ 症状已消除（被 OPT-5' 取代，动态感知机制关闭）

> **状态（2026-09-05）**：本项的**用户可见问题已解决**——commit `c96e7818c7` 将 auto 成本模型的 stream 启动成本校准到实测值（150→5000×cpu_tuple_cost）后，auto 在全部档位选串行，而串行恰好在所有并发档实测最优，多连接 worker 挤兑不再可达。**原设计的动态机制（活跃 producer 计数 + 运行时并发信号）未实现且当前不需要**：其前提"并行在单查询有净收益"在 OPT-1/3 修复后的实测中不成立（force 单查询 9.5 vs off 3.6ms，协议固定开销 ~6ms）。原验收"低并发仍选并行"一条在当前实现下不应满足。
> **复活条件**：stream 协议成本被优化（如修复 OPT-2 PoC 实测的 ~2.9ms 握手成本）使并行在单查询重新有净收益时，按本节原设计实施动态感知；届时原验收标准恢复有效。

### 现象
- off：QPS 186→883（1→8 并发，4.7x）；
- force：103→319（3.1x），P50 从 9.6 恶化到 24.6ms——**每查询双 worker 在多连接下互相挤兑**，最优并发档反而是串行。

### 设计
P2-6 成本模型（`hybridRecallPath.cpp`）目前只看查询自身（行数/候选/K）；扩展一个运行时信号维度：
- 信号源：活跃 stream producer 数（`pg_stat_activity` 级别的轻量计数器，或 worker 池水位）；
- 决策：`auto` 在"当前并发流 ≥ 阈值"时偏向串行 Path（哪怕单查询看并行更优）——总吞吐最优；
- 阈值初值由本次 S4 数据标定：8 连接时 force 总 QPS 319 vs off 883 → 串行偏好阈值 ≤4 并发流。

### 验收
- 混合负载（1/4/8 连接阶梯）下 auto 模式总 QPS ≥ 对应最优固定模式的 95%；
- 单查询低并发场景 auto 仍能选并行（保留 force 单查询收益）。

---

## OPT-6（P2）：ef 质量档进入 options，免去手工 GUC

### 现状
质量对齐需会话级 `SET hnsw_ef_search=400`；经 ogai.hybrid_search_v2 的用户（应用侧）无法逐查询表达质量意图，且 OPT-1 修复前并行模式根本不尊重它。

### 设计
1. marker options 增加可选键 `vector_ef`（int，默认 0=沿用 GUC）：Planner 解析（复用 P3-1 options 解析路径）写入 HybridRecallPath → `ExecInitHybridRecall` 在初始化**向量支路扫描前**对当前执行上下文设置生效值；
2. `ogai.hybrid_search_v2` 的 options 透传同名键（结构与 `text_candidates` 等一致，走 `%L` 校验防注入）；
3. 边界：上限复用 GUC max（1000），并计入 P3-3 的资源校验链；
4. 使用指南补"质量-延迟预算表"（本测试的 ef-recall 阶梯直接引用）。

### 验收
- `options: {"vector_ef":400}` 在 off/force（OPT-1 修复后）均生效：EXPLAIN 显示 Vector ef，融合输出与手工 SET GUC 逐字节一致；
- 未知键/越界值明确报错（负向用例）。

---

## OPT-7（P3，可选）：索引常驻与冷启动

### 现象
交替负载下 og:fusion:off:100:ef400 的 P50：round0（冷）32.1ms → round1 11.0ms → 独占稳态 5.0ms；HNSW 索引 410MB > shared_buffers，工作集被其他配置挤出。Milvus 常驻内存（~0.7GB RSS@10万行）无此效应。

### 行动（低优先）
1. `hnsw` reloptions 已有 `use_mmap`（hnsw.cpp 选项表）：评估 mmap 常驻 + 首查预热对冷态的改善（注意 mmap 对热态性能的潜在拖累，须双向测）；
2. 文档化部署建议：向量检索重载实例的 shared_buffers/索引规模配比（≥ 索引总量或接受冷态 3-6x）；
3. 规模上到百万行时重评（当前 7.8GiB 环境不可行，见报告 §9）。

### 验收
冷启动首轮 P95 ≤ 热态 P95 的 3x（当前 6.4x）。

---

## OPT-8（新工作项，OPT-2 的前置）：LOCAL GATHER 协议成本降本

> 【白话速览】真正闷死"两路真正并行"的元凶：LOCAL GATHER 每次查询的固定协议成本 ~3ms（单 stream，半 force PoC 实测：producer 出首行到主线程收到首行差 2.9ms 握手/唤醒/交付 + 池调度方差）。把它压到 <1ms，force/半 force 就能在过临界规模的场景净赚，OPT-2 即以"两行改动"形态达成。

### 证据（半 force PoC，evidence 见 OPT-2 设计文档）
- producer 侧 Ann Index Scan 首行 4.7ms（与主线程 text drain **真重叠**）；
- 主线程收到首行 7.6ms——**2.9ms 纯协议延迟**；另见 50ms 级池调度方差（不稳定本身即负证据）；
- force 双 worker 总开销 ~6ms（7.29 vs 6.16ms）与其同源。

### 候选改法（需专项设计，深水区）
1. **握手延迟**：producer 启动后等 consumer 激活的同步点——审查 StreamNodeGroup 的 barrier/唤醒链路，让 producer 无条件先跑满环形缓冲；
2. **池调度方差**：stream worker 池的借还路径（ThreadPoolStream::InitStream 全量 session 初始化是大头——新建 u_sess + 全 GUC 表 + smgr/relcache init）——worker session 复用/惰性初始化；
3. **交付路径**：同进程 LOCAL 场景绕开环形缓冲序列化，直接内存交接（需安全审计）。

### 验收
单 stream 固定成本 <1ms（半 force 形态下 total ≤ max(支路)+1ms）；cand=1000 档 force 显著优于 off；池方差 P99 <2ms。

### 定位
- 属 stream 基础设施改造，受益面超出 HybridRecall（所有 LOCAL GATHER 计划）——建议单独立项评审，不挂在本优化批次；
- 完成后：auto 成本模型的 stream_startup 系数（`c96e7818c7` 标定的 5000）按新实测重标定，force/半 force 在过临界场景自动胜出。



| 阶段 | 内容 | 工作量 | 门 |
|---|---|---|---|
| **P0**（本周） | OPT-1 白名单修复 + 定向用例 + 回归 | 1 天 | force 输出=off 输出（10/10）；S5 force recall 0.883；存量套件全绿 |
| **P1a** | OPT-2 档1（auto 重标定） | 0.5 天 | auto 选择矩阵符合实测 |
| **P1b** | OPT-3 实验计划（ef 曲线 + 读码对照） | 3-5 天 | 产出根因结论 + 改动可行性判定 |
| **P1c** | OPT-2 档2（交替 drain）+ 档3 剖析 | 3-5 天 | 串行 fusion P50 ≤3.5ms；P3-5 稳定性门全绿 |
| **P2** | OPT-4/5/6 | 各 2-4 天 | 各自验收节 |
| 每阶段收尾 | `hybrid_vs_milvus_bench` 全量复测 + 报告数字对比表更新 | 0.5 天 | 对比报告同口径 |

### 工程纪律
- 一律走 `deliverables/hybrid_vs_milvus_bench/build_release.sh full`（release 口径；改头文件必须 full）；`version()` 无 `debug on` 才开测；
- 每步 evidence 落 `openGauss-server/docs/hybrid_recall/evidence/`（沿用 BASELINE.md 风格）；
- 分支：从 `backup/feature-hybrid-recall-stacked-20260903` 拉特性分支，**严禁 reset/checkout 到 origin/main**；
- OPT-1/2/6 触碰 executor/guc 核心：每提交跑 `make hybrid_recallcheck` + 串并一致性 golden + P3-5 抽查（重扫/取消/线程数）；
- OPT-3 触碰 datavec 全局：额外跑 hnsw_rabitq_vector / hnsw_lsg_vector / diskann_vector / ai_vector 存量套件。

## 预期整体效果（全部落地后，同质量档）

量化路线（服务端 total，cand=100 质量对齐档，依据《两路召回性能分析.md》实测分解）：

| 路线 | 服务端 total | 客户端 P50 折算 |
|---|---|---|
| 现状 off ef400（text 1.50 + vec 4.17 串行） | 6.16 ms | ~5.0 ms |
| +OPT-2 档2 交替消费（省 min 支路 1.5ms） | ~4.7 ms | ~3.8 ms |
| +OPT-3 ef100 同质量（vec 4.17→2.2） | ~3.7 ms | ~3.0 ms |
| **两者叠加（≈max(1.5, 2.2)）** | **~2.4 ms** | **~2.0 ms** |

| 指标 | 当前 | 目标 | Milvus 参照 |
|---|---|---|---|
| 单查询 P50/P95（客户端） | 5.0 / 7.8 ms | **~2.0 / ~3.5 ms**（OPT-2+3 叠加） | 2.7 / 3.7 ms |
| 并行档质量 | 0.830（缺陷） | **0.883** | 0.879 |
| 并发8 QPS | 883 | **≥1000** | 977 |
| HNSW 构建 | 636 s | **≤160 s**（并行） | 6 s（异步） |

若 OPT-3 实验（P1b）证实 ef 语义假设，两项叠加后**单查询将反超 Milvus 参照值**；若实验不成立（图导航质量根因），叠加效果回落到 ~3.8ms（打平档）。

单查询延迟与并发吞吐双双进入 Milvus 同一量级（≤1.2x / 持平），同时保留 OG 在 BM25 单路（4x 快）、scifact nDCG（0.495 vs 0.434）、SQL/事务/权限完整性上的既有优势。
