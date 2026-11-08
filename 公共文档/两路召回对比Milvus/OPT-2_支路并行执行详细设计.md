# OPT-2 支路并行执行 详细设计（单侧 fetcher 线程方案）

- 日期：2026-09-05；状态：**T1 已落地提交（`6c0886e8e2`）；T2 载体选型完成并经 PoC 实验排除"半 force 借 stream 骨架"路线；自写轻量线程路线待实施**
- 上游：《HybridRecall优化方案.md》OPT-2 节（形态 a）；依据《两路召回性能分析.md》服务端分解
- 代码基线：`fix/hybrid-recall-worker-guc-sync` 分支（含 OPT-1 修复 `01fef232fd` + T1 `6c0886e8e2`）
- 目标读者：实施者。粒度：可直接开工。

## 实施进展（2026-09-05）

| 任务 | 状态 | 结果 |
|---|---|---|
| T1 数据面 | ✅ 已提交 | fetcher（MinimalTuple 拷贝 + 私有上下文 + 回放）串行接入：输出与串行 drain **逐字节一致**，GT 10/10，500 查询 recall 0.8834 不变。后续线程版直接在已验证数据面上叠加 |
| T2 载体选型 | ✅ 完成（含否定性 PoC） | 三条路线评估：a) 自写轻量线程（待实施）；b) 池借 ThreadPoolStream（依赖 StreamProducer 构造，≈重造 force）；c) **半 force（只给 vector 包 stream）——已 PoC 实测否决**，见下 |
| T3-T5 | 待 T2 自写线程路线实施 | — |

### 半 force PoC 实验记录（否决证据）

改动（已 revert）：`createplan.cpp` force 分支只给 vector 支路包 `make_hybrid_task_stream`，text 支路内联；`actual_parallel` 判定天然为 false（左树非 Stream），串行 drain 顺序不变——**重叠确实发生了**：EXPLAIN 显示 vector producer 与主线程的 text drain 并行执行（producer 侧 Ann Index Scan 首行 4.7ms，期间主线程完成 text 1.4ms）。

**但被 stream 协议成本否决**：
- LOCAL GATHER 首行到达主线程时刻 7.6ms，与 producer 出首行（4.7ms）之间有 **~2.9ms 协议同步成本**（producer-consumer 握手/唤醒/环形缓冲交付），另观察到 50ms 级的池调度方差（3 条采样中位 50.8ms vs 单条 9.6ms，不稳定本身即负证据）；
- 半 force total 9.6~50.8ms vs 串行 6.16ms——**净亏 3.4ms 起步**；重叠收益上限只有 min(text,vec)≈1.5ms，协议成本结构性大于收益。

结论：LOCAL GATHER 的 producer/consumer 协议（为分布式流设计）对"单进程内两支路重叠"这个场景过重；`ThreadPoolStream::InitStream` 的全量 session 初始化（新建 u_sess + 全 GUC 表构建 + smgr/relcache init）也是 force 开销的真实来源。**借 stream 骨架路线（§7 选项 A）关闭，T2 剩余唯一路线为自写轻量线程（原 §4 设计）**，其难点与风险已在 §6/§7 写明；若自写线程的环境初始化（最小 u_sess 子集）也压不进 0.5ms 预算，OPT-2 整体按 §11 判据关闭、转 §9 备胎。



## 1. 问题与目标

串行（off）模式下 HybridRecall 算子把两条互不依赖的检索支路**顺序执行**，`nodeHybridRecall.cpp` BUILD 阶段 else 分支（当前 839-842 行附近）：

```c
} else {
    INSTR_TIME_SET_CURRENT(node->text_first_tuple);
    hybrid_drain_branch(node, node->text_state, true, &node->text_rows);    /* 语句① 文本路 */
    INSTR_TIME_SET_CURRENT(node->text_end);
    INSTR_TIME_SET_CURRENT(node->vector_first_tuple);
    hybrid_drain_branch(node, node->vector_state, false, &node->vector_rows); /* 语句② 向量路 */
    INSTR_TIME_SET_CURRENT(node->vector_end);
}
```

实测（质量对齐档，热态，TEXT EXPLAIN ANALYZE）：text 1.50ms + vec 4.17ms + fusion 0.006 ≈ total 6.16ms——两段工作零重叠（`text_end ≈ vector_first`，Branch Overlap=0.000）。

**改造目标：**

```text
现状：  主线程 ██████文本1.5ms██████▓▓▓▓向量4.2ms▓▓▓▓   共 ~5.7ms
改后：  主线程 ██████文本1.5ms██████░░汇合░░             共 ~4.3ms（净赚 min(text,vec)≈1.5ms）
        新线程 ░░░░░░░░向量4.2ms░░░░░░░░░↑
```

约束：输出与串行版**逐字节一致**（golden 回归零差异）；线程固定开销 <0.5ms（超过则放弃本方案，见 §9 备胎）。

## 2. 为什么现有 force 模式不够

force（stream worker + LOCAL GATHER）已实现真并行，但 OPT-1 修复后实测 total 7.29 vs off 6.16ms：重叠收益 1.0ms < worker/管道固定开销 ~1.1ms。开销大头是 stream 协议三件套：producer/consumer 管道、计划序列化传输、双 worker 生命周期。本方案保留"借内核线程骨架"的安全性，砍掉这三样。

## 3. 总体设计

- **主线程**：发起向量支路 fetcher 线程后，照旧同步 drain 文本支路（写候选哈希表）；
- **fetcher 线程**：只做一件事——反复 `ExecProcNode(vector_state)` 拉行、物化进**私有数组**；不写任何共享结构；
- **汇合点**（主线程 join 后）：若 failed 则重抛错误；否则把私有数组按拉取顺序回放进候选哈希表（rank 按回放序编号）；
- `hybrid_fuse` 及其后一切不动。

**单写者原则**：候选哈希表、输出数组、EXPLAIN 统计只由主线程触碰——共享结构零锁。

## 4. 数据结构与线程入口

```c
/* 新增：向量支路搬运工（只碰自己的东西） */
typedef struct HybridVectorFetcher {
    PlanState     *vector_state;    /* 向量支路子计划——全世界只有本线程碰它 */
    MemoryContext  fetch_cxt;       /* 本线程私有内存上下文（独立 AllocSet） */
    HybridRowCopy *rows;            /* 私有行数组（物化后的 doc_id/score/payload 副本） */
    int            n_rows, cap_rows;
    volatile bool  done;
    volatile bool  failed;          /* 出错/被取消只设标志 */
    ErrorData      saved_err;       /* 错误快照，汇合点由主线程重抛 */
    volatile bool  cancel_requested;
    instr_time     started_at;      /* 线程实际起跑时刻（EXPLAIN 计时用） */
    pthread_t      th;              /* 或内核线程骨架句柄，见 §7 */
} HybridVectorFetcher;

static void* hybrid_vector_fetch_main(void *arg)
{
    HybridVectorFetcher *f = arg;
    sigjmp_buf local_jmp;
    /* ① 本线程自己的异常栈：ERROR 截在这里，绝不跨线程 longjmp */
    if (sigsetjmp(local_jmp, 0) != 0) {
        f->failed = true;
        CaptureError(&f->saved_err);
        return NULL;
    }
    PG_exception_stack = &local_jmp;
    /* ② 切到私有内存上下文：本线程所有 palloc 与主线程分配器完全隔离 */
    SwitchToThreadLocalMemoryContext(f->fetch_cxt);
    INSTR_TIME_SET_CURRENT(f->started_at);
    /* ③ 干活：拉满向量支路候选（cap = vector_limit） */
    while (f->n_rows < f->cap_rows && fetch_one_row_into(f)) {
        if (f->cancel_requested) { f->failed = true; return NULL; }
    }
    f->done = true;
    return NULL;
}
```

`fetch_one_row_into(f)`：`= hybrid_drain_next 的私有版`——`ExecProcNode(f->vector_state)` 拿 slot，物化后把 (doc_id, score/distance, payload 各列) **深拷贝**进 `f->rows[f->n_rows++]`；不做哈希 upsert、不碰 `node->ps.ps_ExprContext`（见 §6-R1）。

## 5. BUILD 阶段改写

```c
} else {
    HybridVectorFetcher *f = start_vector_fetcher(node);  /* 起线程，立即返回 */
    INSTR_TIME_SET_CURRENT(node->text_first_tuple);
    node->vector_first_tuple = f->started_at;   /* 与 force 计时语义对齐（两路同刻起跑） */

    hybrid_drain_branch(node, node->text_state, true, &node->text_rows);  /* 照旧写哈希表 */
    INSTR_TIME_SET_CURRENT(node->text_end);

    join_vector_fetcher(f);                     /* 等副线程收工 */
    if (f->failed)
        ereport(ERROR, RestoreError(&f->saved_err));   /* 错误在汇合点统一冒泡 */
    replay_rows_into_hash(node, f);             /* rank = 1..n_rows 按回放序编号 */
    node->vector_rows = f->n_rows;
    INSTR_TIME_SET_CURRENT(node->vector_end);
    DestroyVectorFetcher(f);                    /* 释放线程 + 私有上下文 */
}
```

## 6. 四个安全点及其设计答案

| # | 问题 | 设计答案 |
|---|---|---|
| S1 | 候选哈希表并发写 | 副线程零写共享结构，攒私有数组；汇合后主线程顺序回放。**单写者，无锁** |
| S2 | AllocSet 非线程安全 | 副线程 `CurrentMemoryContext` 切入 `fetch_cxt`（独立 AllocSet 实例）；回放时主线程把行拷进 `candidate_cxt` 再销毁 `fetch_cxt` |
| S3 | ERROR=longjmp 跨线程炸 | 副线程自设 `sigsetjmp` 栈截获，快照 ErrorData、设 `failed`；主线程汇合点 `ereport` 重抛——错误码/消息与串行版一致 |
| S4 | 取消/超时/rescan | 主线程 `CHECK_FOR_INTERRUPTS` 触发处置 `cancel_requested=true`，副线程每行轮询后干净退出；语句超时同路径；`ExecReScanHybridRecall` 若 fetcher 存活先 join-再重启（复用 P3-5 已验证的重扫框架） |

**必须逐节点审计的清单**（评审重点）：
- R1：`hybrid_drain_next` 现版每行调 `ResetExprContext(node->ps.ps_ExprContext)`——这是**父节点**的 ExprContext，副线程不能碰；`fetch_one_row_into` 须用向量支路自己的 `vector_state->ps_ExprContext`（只被副线程访问）；
- R2：`ExecProcNode(vector_state)` 往下每个节点（Ann Index Scan、Limit、物化）是否触碰 estate 全局可变状态（快照/param 只读 ✓，`es_result_relation_info` 等可变项 ✗）——逐节点过；
- R3：instrumentation（`INSTRUMENT_ALL` 下 per-node 计时）与 WLM operator 槽位在线程间的归属——副线程内节点的时间戳只写自己的 `Instrumentation`（无竞争），但 WLM 上报统一在主线程汇合后做；
- R4：relcache/syscache 并发——openGauss 本就多会话并发访问，内部有锁，无新增风险（确认即可）。

## 7. 线程起动的"合规姿势"（两个选项）

- **选项 A（首选）**：借用 stream worker 的**线程骨架**（信号路由、`t_thrd`/`u_sess` 挂接、退出清理），但**不走 Gather 管道、不序列化计划**——副线程直接在同一进程内操作 `vector_state` 指针。这保留内核对线程的全部治理，砍掉 force 开销的大头；
- **选项 B（保守）**：裸 `pthread_create` + 自管信号屏蔽——更轻但要自己补齐信号路由与 threadpool 假设的审计，评审风险更高。

无论 A/B，**不修改** P0/P1 的治理边界：fetcher 不进入 Gather/GatherMerge 树、不与 PG parallel worker 混用，与现有 HybridRecall 约束一致。

## 8. 结果等价性论证（golden 零差异的依据）

RRF 与确定性 tie-break（`hybrid_score DESC, source_mask DESC, best_rank ASC, doc_id ASC`）只依赖**每路内部 rank**。串行版 rank = 向量支路拉取顺序（1..n）；本方案回放严格按拉取顺序编号 → 哈希表最终状态与串行版**完全相同** → 融合输出逐字节一致。文本路径完全未动。故 golden、串并一致性、P2-7 参数化用例全部直接复用。

## 9. 备胎方案 B（若 §7 评审不过 / 开销压不进 0.5ms）

把并行面从 SQL 层缩到**搜索内核**：`hnswscan.cpp` 里把"第一批 HNSW 搜索"（向量支路耗时大头，ef=400 时 ~4ms）交后台线程计算，主线程同时跑 BM25；SQL 层完全无线程。
- 优点：改动面小、无 estate/ExprContext 问题；
- 缺点：只救"首批主导"场景（大 ef、冷态收益大；小查询打折），且与 OPT-3 的搜索改造可能冲突（动同一个文件）。

## 10. 实施分解（原子任务粒度）

| 任务 | 内容 | 验证 |
|---|---|---|
| T1 | `HybridVectorFetcher` 结构 + 私有上下文/数组 + `fetch_one_row_into`（含 R1/R2 审计） | 编译 + 单元：串行调用 fetcher（不起线程）输出 == 现版 drain |
| T2 | 线程骨架接入（§7 选项 A）+ `start/join/Destroy` | 线程数守恒：查询前后 postmaster 线程数不变 |
| T3 | BUILD 阶段改写 + 回放 + 计时语义（`vector_first=started_at`） | golden 零差异；EXPLAIN 显示 overlap>0、total<sum |
| T4 | 取消/超时/rescan 适配（S4） | P3-5 同款：100 次真重扫、50ms 超时取消、无残留线程/锁 |
| T5 | 回归全套 + EXPLAIN ANALYZE 分解复测 | `make hybrid_recallcheck`、串并/generic plan、P0-P3 存量 evidence SQL |

预计 T1-T3 一天半，T4-T5 一天；总量 ~250-350 行 C（含注释）。

## 11. 验收门

1. golden 与串并一致性零差异（T1/T3）；
2. 热态质量对齐档服务端 total **6.16 → ≤4.8ms**，`Branch Overlap > 1.2ms`，线程开销（total − max(text,vec)）**<0.5ms**；
3. P3-5 稳定性门全绿（重扫/取消/线程数/memcheck 抽查）；
4. 若 2 不达标（开销压不进 0.5ms）→ 记录数据、关闭本方案、转 §9 备胎，优化方案文档同步。

## 12. 风险与回退

- 最大风险：R2 审计发现向量支路子树存在主线程也在写的可变状态 → 数据面污染。对策：T1 的"串行调用 fetcher"先行验证 + ASAN/memcheck 门；
- 回退：改动收敛在 else 分支 + 新文件/新结构，revert 单 commit 即回退，无 schema/数据迁移；
- 与 OPT-3 的顺序：无硬依赖。OPT-3 落地后 min(text,vec) 的绝对值不变（text 不变），本方案收益不缩水；但 OPT-3 会改变"是否还需要本方案"的性价比判断（vec 降到 2.2ms 后 total≈3.7ms，本方案再省 1.5ms 的相对占比更大）。
