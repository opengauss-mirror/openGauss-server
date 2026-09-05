# S0-1 基线固化报告（BASELINE.md）

- 日期：2026-08-21
- 执行者：Claude (GLM-5.3)
- 项目：文本+向量两路并行召回算子（HybridRecall），执行文档 §14.2 阶段 S0
- 设计文档：`/home/qiumc/data/code/cpp/公共文档/openGauss_文本与向量两路并行召回算子详细设计与实施文档.md`

## 1. 基线 commit 核对

```text
git rev-parse HEAD
→ d98de98c8016ee99ac14079597ee7d76284904dd
→ 与设计文档基线一致 ✅（分支 master）
```

## 2. 环境事故与重建（本次基线特有）

2026-08-16 最后一次成功运行后，`mppdb_temp_install`（GAUSSHOME）、`topk_test_data`
（PGDATA）与全部构建产物（`.o`）被清理。2026-08-21 完成重建：

| 步骤 | 证据 |
| --- | --- |
| 手工全量编译+首装（38 min，镜像脚本 full 动作） | `build_logs/manual_full_20260821_094605.log`，rc=0 |
| `gs_initdb`（nodename=single_node，UTF-8/C，随机密码已 shred 销毁，本地免密） | PG_VERSION=9.2 |
| 脚本首跑 incremental | ❌ jre 444 复装失败（详见 §5 坑 1） |
| 脚本重跑 incremental 端到端 | ✅ `build_logs/incremental_20260821_103535.log`，script_exit=0，实例运行中 |

实例：PGDATA=`topk_test_data`，端口 **55440**，DB 用户 `qiumc`，openGauss 7.0.0-RC3
debug+cassert 构建。增量循环实测 ~8 min/轮。

## 3. 构建配置与命令约定

- configure：`--enable-debug --enable-cassert --enable-thread-safety`，无 mot/bbox/htap
  （与 config.status 和脚本 `full` 模式一致；§16.3 手工块含 mot/bbox/htap 为陈旧变体，不采用）
- 唯一日常入口：`/home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh incremental|full|restart`
- 进度条：同目录 `opengauss_build_progress.sh [-w]`

## 4. 工作区 dirty 文件分类（1177 条，快照：evidence/S0-1_git_status.txt）

| 类别 | 条目 | 处置 |
| --- | --- | --- |
| 构建生成物（可再生） | 1156 `?? src/`、11 `?? contrib/`（parser/bison/flex 输出、树内构建产物） | 非修改源，不动 |
| 运行实例数据目录 | `?? topk_test_data/`（PGDATA） | **绝不允许修改/删除** |
| 用户文档/数据资产 | `OPENGAUSS_BUILD_INSTALL_RUN_GUIDE.md`、`opengauss_diskann_scalar_filter_topk_design.md`、`union_all_diskann_topk_test_cases.sql`、`.cac/`、`M docs/diskann_include/evidence/P2-4/LG.md` | 保留，本任务不得触碰 |
| 用户已删除 | `D opengauss_master_union_all_vector_topk_pushdown_design.md`、`D contrib/roach_api/openeuler_aarch64/roach_api.so` | 保持删除，不恢复 |
| 安装副作用 | `M contrib/shark/smartmatch.pl`（mode change） | 构建脚本自动恢复权限 |
| 本项目新增 | `?? docs/`（hybrid_recall 文档与 evidence） | 本项目工作区 |

**属于本任务的已跟踪源码修改：无。** 从零开始，无 overlap 风险。

## 5. 已知坑（本项目执行期踩到并已处理）

1. **jre 444 复装失败**：`src/gausskernel/Makefile:690` 用 `cp -r`（无 `-f`）装系统 JRE，
   产物只读，第二次 `make install` 即 Permission denied。已修：安装树 jre 下 9 个文件
   `chmod u+w`（一次性；cp 覆盖已存在文件保留目标权限）。根治（Makefile 改 `cp -rf`）
   待用户拍板，未动源码。
2. 脚本 preflight 拒绝在无 PG_VERSION 的环境运行（设计如此）；环境被清空后需手工
   make+install+initdb 引导一次。

## 6. 基线测试证据（S0-2/S0-3）

| 项 | 状态 | 证据 |
| --- | --- | --- |
| S0-2 debug/cassert 全量编译 | ✅ | `build_logs/manual_full_20260821_094605.log` |
| S0-3 定向回归 9 项全过 | ✅ | `evidence/S0-3/live_*.log` |
| S0-4 串行 Hybrid 基线（等价 SQL 形态） | ✅ | `evidence/S0-5_golden_run1.log` |
| S0-5 确定性数据 + golden | ✅ | `src/test/regress/sql/hybrid_recall_baseline.sql` + `evidence/S0-5_golden_result1.txt`（双跑 diff 空） |

**用户决定（2026-08-21）：本项目不跑 `make fastcheck` 全量回归**，G0 以活实例定向回归
为准。DiskANN INCLUDE 项目在同一 commit d98de98c 已有 fastcheck 基线
（`docs/diskann_include/evidence/G2/g2_failures.manifest`，74 项既有环境差异零新增），
如需全量参照可查该清单，不在本项目重复执行。

### S0-3 定向回归明细（2026-08-21，活实例 topk_test_data:55440）

| 测试 | exit | ERROR | 判定 |
| --- | --- | --- | --- |
| bm25_varblock_001/002/003、bm25_parallel_build、bm25_vacuum_stress | 0 | 0 | ✅ 全过 |
| hnsw_rabitq_vector | 2 | 0 | ✅（exit=2 仅因文件尾 `\c regression` 活实例伪影） |
| diskann_vector | 2 | 3 | ✅ 3 个均为设计预期负向断言（index_size 16..1000 越界×2、>1536 维），与前项目基线记录一致 |
| hnsw_lsg_vector | 2 | 6 | ✅ 6 个均为设计预期负向断言（LSG 空表建索引×3、LSW 与量化混用×3） |
| ai_vector | 2 | 1 | ✅ l2_norm 类型拒绝负向断言 |

注意：这些测试按 pg_regress 一次性数据库设计，活实例连环执行需在每个测试前
`DROP DATABASE IF EXISTS test`，否则库/表残留互相污染。

### S0-4 边界与等价基线

`ogai.hybrid_search()` 内部调用 `ogai_embedding(query, model_key, dim)` 生成查询向量，
**需要外部 embedding 模型服务，本环境无本地提供方**——无法直接录制该函数基线。
等价基线：`hybrid_recall_baseline.sql` 的手写串行 RRF 融合（rrf_k=60、权重 0.5/0.5、
完整 tie-break），即未来内核串行 HybridRecall 必须对齐的 golden 语义。

### S0-5 golden 摘要

- 数据：500 行全确定性（模运算派生，无随机数），`vector(8)`，tenant 0..3；
- 双跑结果**逐字节一致**（`S0-5_golden_result1.txt` vs `2.txt` diff 为空），run 无 ERROR；
- 融合统计：text_rows=25 / vector_rows=100 / 重叠 21（部分重叠场景天然存在）；
- tie-break 验证：id=1 与 id=5 同分 .01653005，source_mask DESC 生效。

## 7. S0 期间发现的关键技术事实（供 P0/P2 设计约束）

1. **BM25 执行期守卫**（`bm25scan.cpp:1056` `bm25_scores_text()`）：`<&>` 打分时
   `u_sess->bm25_ctx.scoreHashTable == NULL`（即本查询未执行过 BM25 索引扫描）直接
   ERROR。→ 内核 HybridRecall 文本支路**必须**包含 BM25 索引扫描节点，不可能回退
   Sort 路径（AM 强制，与设计文档 §7.2 一致）。
2. **支路级 ORDER BY 不能带 tie-break**：`ORDER BY score DESC, id ASC` 的复合排序使
   BM25/HNSW 索引有序路径失效（只能 Sort → 触发守卫/退化）。tie-break 必须放在融合层。
   与设计文档 §3.4/§8.5 的分层一致，但这是**硬约束**而非风格选择。
3. **小表上向量路默认计划是 SeqScan+Sort**（500 行，成本模型如此），`enable_seqscan=off`
   后 `Ann Index Scan using hrd_hnsw_idx` 可达。ogai.hybrid_search 自身就是靠关闭
   seqscan 保证索引路径（ogai_functions.sql:1114）。→ P2 代价模型需保证大表自然选择
   ANN，小表/测试表需可强制。
4. gsql 活实例连环跑 pg_regress 风格测试必须逐测试清库（见 S0-3 注意）。

## 8. 闸门 G0 判定

- 单路检索 9 项定向回归：**无新增失败**（全部 ERROR 均为设计预期负向断言/活实例伪影）✅
- 串行 Hybrid 基线（等价 SQL 形态）：**已录制且可重复**（双跑字节一致）✅
- 确定性数据 + golden：**已固化** ✅

**G0 关闭，允许进入 P0（新增 NodeTag/Path/Plan/State 空结构与串行 HybridRecall）。**
2026-08-21

## 9. P0 阶段完成与 G1 判定（2026-08-22）

| 任务 | commit | 判据 |
| --- | --- | --- |
| P0-1 节点脚手架 | 35e92059ce | 5 头文件纯增量，full 链验证 + golden 一致 |
| P0-2 序列化闭环 | 0c1ac63aaf | copy/out/read + equalfuncs 无需改动结论 |
| P0-3 executor 分发 | ce7578434b | init/end/rescan 分支 + 空生命周期 |
| P0-4/5 串行融合主体 | bb12d40eee | 内核输出与修正版 golden 逐行一致；边界套件全绿；golden 语义缺陷修正（coalesce→hit-only） |
| P0-6 tie-break/边界 | 7981645455 | min-rank 修复 + T1-T5 全绿（含 text 主键、NULL 拒绝、NaN） |
| P0-7 End/EarlyFree/memcheck | a3771b94dc | ASAN 全套件零报告 + 100 次循环守恒 |

**G1 关闭**：串行 HybridRecall 正确性（golden 一致、边界、内存）全部满足。
验证载具：docs/hybrid_recall/p04_test_module（真实规划器组计划 + 手工 HybridRecall
父节点 + ExecutorRun，openGauss C 函数五层适配已固化在 build.sh 与注释中）。

下一阶段：P1 Task Stream 并行（is_task_parallel 标记、1:1 本地 Stream 保留、
producer 启动、快照共享、错误联动、overlap 证明）。

## 10. P1 阶段完成与 G2 判定（2026-08-23）

| 任务 | commit | 判据 |
| --- | --- | --- |
| P1-1 Stream 标记+序列化 | 43e18fae27 | copy/out/read 闭环 |
| P1-2 折叠守卫×3 | b8689263ed | is_task_parallel 1:1 流不被删 |
| explain 改进 | adef03ca87 | 节点名注册+标准子指针（真实 EXPLAIN 可打印完整树） |
| 载具语义修复 | cf1b9b1cf5 | serial/parallel 入口分离 |
| P1-3 并行执行 | 929a17d9b8 | 双 worker 并行输出与 golden 逐行一致 |
| P1-8 overlap + 构建修复 | 495f1e0a8c | overlap ratio 1.0；/lib64 系统 LLVM 遮蔽修复 |
| P1-4 MVCC + cancel | 本 commit | 双会话快照一致 + 取消无泄漏 |

**G2 关闭**（evidence/G2_gate.log）：真实 overlap ✅ 同快照 ✅ 取消无泄漏 ✅。
P1-7 降级随 P2 planner 落地（模式决策处），串行路径已验证。
下一阶段：P2 Planner → G3。

## 11. P2 Planner 进展（2026-08-26）

| 任务 | commit | 判据 |
| --- | --- | --- |
| P2-1 marker 解析与白名单 | 69d7335109 | `ogai` namespace、单表、单 DESC sort、双索引正向识别；负向不误匹配 |
| P2-2 BM25 branch Path | 1e9a24cae5 | `Limit → BM25 Index Scan`，经 1:1 LOCAL GATHER 接入 HybridRecall |
| P2-3 ANN branch Path | 1e9a24cae5 | `Limit → Ann Index Scan`，小表嵌套规划期间局部关闭 seqscan 且调用后恢复 |
| P2-2/3 payload | 1e9a24cae5 | 统一 `(key, score, payload...)` tuple 契约；多列、by-ref、重复/乱序、无显式 id 均正确 |
| P2-4 公共 filter/recheck | 9b14a3b8d0 | 两支路等价下推；WHERE-only 隐藏列随 tuple 传递；父层去重后、Top-K heap 前防御性 recheck |
| P2-5 串/并行 Path 与模式 | 593fd734c5 | `off` 无 Stream、`force` 双 LOCAL GATHER、`auto` 枚举候选；串并行结果逐字节一致 |
| P2-6 初始成本模型 | 3b5f35a576 | 短查询自动串行、大查询自动双 Stream 并行；计划显示 HybridRecall 自身阻塞成本 |
| P2-7 参数化与计划缓存 | 86ee1ea94a | `$1..$4` 支持 custom/generic plan；结果等价；运行时 K 边界校验 |

实现与回归证据：

- `evidence/P2-2_P2-3_test.sql`
- `evidence/P2-2_P2-3_live.log`（exit=0，Top-10 与 S0 golden 逐项一致）
- 计划包含 `Hybrid Recall`、两条 `Streaming(type: LOCAL GATHER)`、BM25 `Index Scan`、
  `Ann Index Scan` 和两个 branch `Limit`；调用前后 `enable_seqscan=on`。
- 同时启动两个顶层 marker 查询：均 exit=0、各返回 10 行。
- P0 手工节点载具在本轮改动后复测，Top-3 仍为 245/165/25，旧 5 列契约保持兼容。

本轮修复的三个边界问题：

1. openGauss 计划复制/缓存会保留或恢复原表 `Var` 坐标；payload 映射必须在
   `ExecInitHybridRecall` 最终计划上完成，并保留 `varoattno` 供 cached plan 下一次重建。
2. 递归外层 setrefs 必须继续调整 nested planner branch 的 RTE offset；否则上层计划可能
   把 branch `scanrelid=1` 指向 relid=0 的 Subquery RTE。
3. V1 明确拒绝在 CTE/子查询/聚合/窗口等上层计划内注入 task stream；P06 负向
   `EXPLAIN` 为普通 Sort/SeqScan，避免尚无 rescan generation 语义时出现等待。
4. P2-7 前只接受非 NULL 的 int8 常量 `LIMIT`，并拒绝 `OFFSET`；这两类形态
   均通过负例确认保持普通 Sort/SeqScan，避免将非有限 K 或 offset 误解为 final_topk。

另将 hash callback 的类型缓存指针改为 `THR_LOCAL`，避免同一 gaussdb 进程内并发会话
使用不同主键类型时发生进程级数据竞争。

### P2-4 公共 filter/recheck 证据

- `evidence/P2-4_test.sql` / `evidence/P2-4_live.log`：exit=0；外部 `diff` 证明带
  `tenant_id = 1 AND title LIKE 'doc-2%'` 的内核 Top-5 与独立手写串行 RRF 逐字节一致。
- 查询只输出 `id/score` 时，`EXPLAIN VERBOSE` 仍显示两个支路都输出隐藏
  `tenant_id/title`，并在 BM25 `Index Scan` 与 HNSW `Ann Index Scan` 上保留相同 Filter。
- `recheck_quals` 在 `ExecInitHybridRecall` 映射到统一 branch tuple，在 doc_id 去重后、
  有界 Top-K heap 前执行；prepared plan 连续执行两次结果完全一致。
- 高选择率过滤分别验证候选数小于 final K（1 行）和零候选（0 行），无漏行或越权行；
  调用前后 `enable_seqscan=on`。
- volatile 公共过滤、SubPlan 公共过滤以及 volatile 查询参数均不生成 HybridRecall/
  LOCAL GATHER，`EXPLAIN` 保持普通 Sort/SeqScan（SubPlan 例包含 InitPlan）。
- 标准增量构建、安装、重启日志：`build_logs/incremental_20260826_094243.log`。

### P2-5 串行/并行模式证据

- 新增会话级 `hybrid_recall_parallel_mode = off|auto|force`，默认 `auto`；`off` 仅关闭
  branch task stream，保留语义相同的串行 HybridRecall，reserved marker 不会漏到逐行执行。
- `evidence/P2-5_test.sql` / `evidence/P2-5_live.log`：`off` 计划为 Hybrid Recall 下两个
  `Limit → Index Scan/Ann Index Scan`，没有 Streaming；`force` 在两支路各增加一个
  `Streaming(type: LOCAL GATHER)`；两种模式 Top-10 外部 `diff` 为空。
- `auto` 同时枚举低启动成本串行 Path 与低总成本并行 Path；P2-5 的交叉占位成本在当前
  500 行短查询上选择串行，P2-6 将替换为基于真实 branch cost、融合和 Stream 开销的模型。
- 公共头文件触发的全树增量构建、安装和重启成功：
  `build_logs/incremental_20260826_113242.log`；P2-4、P2-2/P2-3 活实例复测均 exit=0。

### P2-6 初始成本模型证据

- 串行成本按“两支路之和 + 去重/RRF/Top-K 融合”估算，并行成本按“两支路较慢者 +
  Stream 启动/传输 + 融合”估算；HybridRecall 是阻塞算子，因此自身
  `startup_cost = total_cost`。
- `evidence/P2-6_test.sql` / `evidence/P2-6_live.log`：默认 `auto` 下，500 行短查询
  选择无 Stream 的串行计划（HybridRecall cost=20.42）；20,000 行查询选择含两个
  `LOCAL GATHER` 的并行计划（HybridRecall cost=25.76）。
- `createplan.cpp` 保留所选 HybridRecallPath 的成本，不再让 EXPLAIN 显示复制自文本支路
  的成本；`plan_rows` 仍为 final K。
- 标准增量构建、安装、重启成功：`build_logs/incremental_20260826_141019.log`；
  P2-5、P2-4、P2-2/P2-3 活实例复测均 exit=0，串并行结果一致性与过滤语义无回退。
- 当前系数是保守初值；worker 紧张、I/O 竞争惩罚和冷/热缓存标定按设计留给 P3。

### P2-7 Param/custom/generic plan 证据

- planner 同时接受有限 int8 常量 K 与 `PARAM_EXTERN/int8` K；generic plan 的 Path 行数和
  成本使用保守估值 10，但 Plan 保留原始 `$4`，executor 每次执行/重扫重新求值。
- `evidence/P2-7_test.sql` / `evidence/P2-7_live.log`：`force_custom_plan` 与
  `force_generic_plan` 均生成 `Hybrid Recall` 和两条 `LOCAL GATHER`；generic 计划文本路
  保留 `$1/$3`，向量路保留 `$2/$3`。
- 三组参数结果逐项一致：tenant 1/K=3 为 `245,165,25`，K=1 为 `245`；替换文本、向量
  与 tenant 后 K=3 为 `294,42,378`，证明查询参数不是 prepare 时固化的常量。
- final K 在 `ExecutorStart` 求值并校验，因而外层 `LIMIT 0` 也不能跳过子节点校验；
  K=0、K=101（超过两个固定候选上限 100）、K=NULL 分别得到明确错误。
- generic plan 初测暴露 `ExecEvalExpr` 使用 NULL ExprContext 导致外部参数执行阻塞；改为
  HybridRecall PlanState 的表达式上下文后消除。30 秒 statement timeout 下全套 9.9 秒完成。
- 标准增量构建、安装和重启成功：`build_logs/incremental_20260826_144850.log`；P2-6、
  P2-5、P2-4、P2-2/P2-3 与 P0-4/P0-6 回归均通过。

## 12. P2 阶段完成与 G3 判定（2026-08-26）

- `off`：稳定生成串行两支路 HybridRecall；`force`：稳定生成两条 1:1 LOCAL GATHER；
  `auto`：500 行短查询选择串行，20,000 行查询选择并行。
- BM25/ANN 两支路均保持索引 Top-N、公共过滤和统一 payload 契约；融合结果与串行 golden
  一致，串行/并行、custom/generic 之间均无语义差异。
- 不安全 filter/查询参数与 V1 不支持的查询形态保持普通计划，不误注入 HybridRecall。
- 参数化 prepared statement 覆盖查询文本、向量、公共过滤和 final K，运行时边界完整校验。

**G3 关闭，P2 Planner 阶段完成。** 下一阶段进入 P3：代价标定、资源治理、可观测性与
压力/故障回归。

## 13. P3 OGAI 接口进展（2026-08-26）

### P3-1 `hybrid_search_v2` 与 options

- 新增注入安全的同表/append V1 API：任务、源关系、主键/文本/向量列、返回列均从
  `ogai.vectorize_tasks` 和系统目录解析；动态标识符使用 `quote_ident`/`format(%I)`，
  用户值仅通过外部 Param 或经类型验证后的 `%L` 字面量进入生成 SQL。
- V2 拒绝旧接口的任意 `p_where_clause` SQL，改用 `options.filters` 的结构化标量等值
  过滤；支持 `strategy`、两路候选数、`rrf_k`、`weights`、worker 短缺策略和预计算
  `query_vector`，未知键、错误类型及 V1 资源边界均明确报错。
- query embedding 在 wrapper 中只求值一次并作为 `$2::vector` 传入；离线测试可通过
  `query_vector` 跳过外部模型服务。Planner 解析同一 JSONB 常量并把候选数、RRF 参数
  写入 HybridRecallPath，不再使用固定值。
- wrapper 为 `SECURITY INVOKER`，同时校验任务归属和源表 SELECT 权限。跨角色实测分别
  得到“任务不属于当前用户”和“源表权限不足”，没有绕过普通 ACL。
- 新增 C SPI 物化 helper，一次 `SPI_execute_with_args` 完整驱动并行 HybridRecall；避免
  PL/pgSQL cursor 分批取数与 task stream 生命周期不兼容造成的 stream 元数据损坏。
- `evidence/P3-1_test.sql` / `evidence/P3-1_live.log`：封装 API 与直接 marker 在
  `weights=[1,0]`、`rrf_k=10`、tenant 过滤下均返回 `5,25,45`；权限 2 例和注入/参数
  7 例全部按预期拒绝，注入目标表仍存在。全新数据库 `CREATE EXTENSION ogai` 也已验证。
- 标准增量构建、安装和原 PGDATA 重启成功：
  `build_logs/incremental_20260826_153831.log`。最终安装后 P2-7、P2-6、P2-5、P2-4、
  P2-2/P2-3、P0-4、P0-6 均回归通过，实例保持在线。

**P3-1 完成。** 下一原子任务是 P3-2 legacy fallback：当能力关闭或 marker 形态不支持时，
按明确 option/GUC 回到旧 `ogai.hybrid_search()`，并证明旧返回契约完全兼容。

### P3-2 legacy fallback 与运行时回滚开关

- 新增 USERSET GUC `enable_hybrid_recall`，默认 `off`。关闭时 Planner 不再生成新的
  HybridRecallPath；缓存计划若在关闭后再次执行，会在初始化支路 worker 前安全拒绝。
- `hybrid_search_v2.options.on_unsupported` 支持 `legacy`（默认）和 `error`。wrapper 在
  GUC 关闭、任务不是 append 同表形态或 BM25/ANN 索引能力缺失时，分别进入旧串行实现
  或返回包含具体原因的错误；内部专用 option 不再传给 marker。
- 旧 `ogai.hybrid_search(text,text,text,integer,text)` 签名和 JSONB 投影不变。原实现抽取为
  `hybrid_search_legacy_core`，使 V2 能把只计算一次的查询向量交给兼容路径，避免回退时
  第二次调用 embedding 服务。
- V2 的结构化过滤仍通过目录类型校验和 `%I/%L` 构造。实测修复了 A 兼容模式下空字符串
  被视为 NULL 导致首个过滤谓词丢失的问题；租户过滤后的旧实现/V2 回退逐行 JSONB
  mismatch 为 0，删除 ANN 索引触发回退后的 mismatch 也为 0。
- `evidence/P3-2_test.sql` / `evidence/P3-2_live.log`：默认关闭、显式开启计划、缺索引回退、
  error 策略、缓存计划关闭后拒绝、旧接口开关无关性与原五参数签名均通过。
- 最终源文件与安装目录扩展 SQL 字节一致；新库 `hybrid_recall_p32_20260826` 上
  `CREATE EXTENSION ogai` 成功且默认开关为 `off`。P3-1、P2-1 至 P2-7、P0-4/P0-5/P0-6
  均完成回归。

**P3-2 完成。** 下一原子任务是 P3-3：补齐候选数、最终 K、RRF 参数、单查询内存和
worker 使用量的统一硬上限与可诊断错误，并把资源策略接入 GUC/options。

### P3-3 GUC 与资源硬上限

- 新增五个 USERSET GUC：最终 Top-K、单支路候选、两支路候选总数、默认 RRF `k` 和
  单查询候选内存上限；默认值分别为 `100/1000/2000/60/65536 kB`。显式 marker/V2
  option 仍优先于默认 `rrf_k`。
- wrapper 在生成 embedding 前按当前 GUC 校验最终 K 和候选数；Planner 对常量 LIMIT
  及常量 options 提前拒绝；Executor 对参数化 K 和缓存 generic plan 重新读取当前上限，
  因而不能通过先 prepare、后降低上限绕过资源控制。
- Executor 在候选哈希表创建、每个新候选 payload、最终 heap 和输出数组分配后检查
  `candidate_cxt`。64 kB 压测在 `used=65576/limit=65536` 时返回可诊断错误，随后实例
  可继续执行 SQL。最终 heap 同时迁入 `candidate_cxt`，修复 rescan 时丢弃旧指针造成的
  query-context 内存累积。
- 五个整数字段复用 session 结构既有对齐空洞；`knl_session_attr_sql=656`、
  `knl_u_datavec_context=56`、`knl_session_attr=44032`、`knl_session_context=52776`，
  既有字段 offset 均未变化。只执行目标对象编译、`gaussdb` relink、二进制/扩展 SQL
  定向安装和原 PGDATA restart，没有重建或重装整个 openGauss。
- `evidence/P3-3_test.sql` / `evidence/P3-3_live.log`：默认值、RRF 默认覆盖、wrapper/
  Planner/Executor 三层上限、缓存计划再校验和候选内存错误均通过。新库
  `hybrid_recall_p33_20260826` 可直接 `CREATE EXTENSION ogai`；P3-1/P3-2、P2-1 至
  P2-7、P0-4/P0-5/P0-6 全量回归通过。

**P3-3 完成。** 下一原子任务是 P3-4：补齐 HybridRecall EXPLAIN、非敏感诊断日志和
WLM/worker 资源观测字段。

### P3-4 EXPLAIN、日志与 WLM 可观测性

- `EXPLAIN` 新增策略、最终 K、两路候选数、RRF 参数/权重和计划 worker 数；
  `EXPLAIN ANALYZE` 进一步输出实际 worker 数、两路/去重/最终候选行数、两路耗时、
  fusion 时间、branch overlap 和候选内存。TEXT 与 JSON 格式均验证通过。
- 串行实测为 `planned/actual workers=0/0`、`overlap=0`；并行 JSON 实测为 `2/2`，
  两路耗时约 `1.988/1.974 ms`，overlap `1.960 ms`，候选内存 `11 kB`。两条
  LOCAL GATHER、BM25 Index Scan 和 Ann Index Scan 仍完整显示。
- 首次 `EXPLAIN ANALYZE` 暴露 WLM `ThreadInstrumentation::allocInstrSlot` 未识别
  `T_HybridRecall`，debug build 命中默认断言。现已映射为 `Hybrid Recall` utility
  operator；`resource_track_level=operator` 下串行、并行执行均不再断言。
- 新增默认关闭的 USERSET GUC `hybrid_recall_log_summary`。开启时仅写 strategy、K、
  候选行数和耗时数字，且使用 `errhidestmt(true)`；带敏感测试标记的 SQL 成功执行后，
  服务端日志中的标记计数为 0。该 bool 复用 offset 630 的既有 padding，session 结构
  大小和后续字段 offset 均不变。
- 仅编译四个目标对象、relink/install `gaussdb` 并重启原 PGDATA，没有全量构建或重装。
  `evidence/P3-4_test.sql` / `evidence/P3-4_live.log` 记录完整结果；P3-1 至 P3-3、
  P2-1 至 P2-7、P0-4/P0-5/P0-6 全量回归通过。

**P3-4 完成。** 下一原子任务是 P3-5：debug/release/memcheck 构建与循环执行、取消和
rescan 内存稳定性验证。

### P3-5 debug/release/memcheck 稳定性

- 测试模块新增 `hybrid_recall_debug_rescan`：在同一个 `QueryDesc` 上执行 1 次
  `ExecutorRun` 和 99 次 `ExecutorRewind -> ExecReScanHybridRecall -> ExecutorRun`，
  100 轮共返回 300 行，证明不是 cursor 物化伪重扫。并行测试异常路径通过
  `PG_CATCH` 恢复测试专用 producer delay 后原样重抛 timeout/cancel。
- 主 debug+cassert 实例完成 100 次真重扫、100 次交替 options 的真实 OGAI 并行执行，
  以及两个 producer 同时工作时的确定性 statement timeout。取消后并行结果仍为
  `245,165,25`，实例存活，postmaster 线程数 `32 -> 32`，服务端无断言/PANIC/崩溃。
- 提交 `4c032d0fb7` 在隔离 worktree 完成干净 release 构建：`-O2`、
  `enable_debug=no`、`enable_cassert=no`，退出 0，耗时 3044 秒。独立 55541 实例执行
  同口径测试退出 0，线程数 `33 -> 33`，取消后查询正常。
- 官方 `build.sh -m memcheck` 干净构建退出 0，耗时 2358 秒；产物包含
  `-O0 -fsanitize=address -fsanitize=leak`、debug/cassert，并正确解析 GCC 10.3
  `libasan.so.6`。ASAN `gaussdb` 在 55641 完成同口径测试，退出 0；测试后连续五次
  线程采样均为 29，无残留 stream/producer，ASAN 报告文件为 0，服务端无
  ASAN/LSAN/断言/PANIC/段错误。
- 官方 memcheck `gs_initdb` 自身在本环境即使 `--version` 也会递归输出
  `AddressSanitizer:DEADLYSIGNAL` 且无符号化报告；运行验证因此使用同提交 release
  实例停机后复制的 PGDATA，由当前 memcheck `gaussdb` 启动。该工具边界已如实记录，
  未使用或修改主实例 PGDATA。完整命令、时间和日志判据见
  `evidence/P3-5_test.sql` / `evidence/P3-5_live.log`。

**P3-5 完成。** 下一原子任务是 P3-6：主备查询与故障切换后重试语义验证。

### P3-6 主备查询与故障切换后重试

- 从已停机的 P3-5 release PGDATA 克隆两个隔离节点，按官方
  `--single_node -M primary|standby` 方式启动；主备达到 `Normal/Streaming`、同步进度
  100%。原 `topk_test_data` 未参与本测试。
- 主节点和 `hot_standby` 只读备节点均以两个实际 branch worker 执行
  `hybrid_search_v2`，冻结数据集都返回 `245,165,25`。V1 的承诺范围仍以主节点为准，
  备读能力不扩张为延迟或可用性 SLA。
- 故障注入时，旧主语句已完成真实两路召回、但在首行发送前保持 active；立即停主后旧
  客户端退出 2，唯一终态为 `connection to server was lost`，没有收到结果行。备节点
  `failover` 后成为 `Normal` 新主，客户端整句重试返回同一结果，随后 20 次重试全部通过。
- 新主 `EXPLAIN ANALYZE` 仍显示 `Parallel RRF`、planned/actual workers=`2/2`、
  `Parallel Fallback=false`、BM25/HNSW 两条索引支路和正 overlap；故障会话残留为 0，
  服务端无断言/PANIC/段错误。
- 明确语义：不提供透明会话迁移；客户端遇到连接错误必须丢弃该次可能已接收的所有行，
  在确认 read-write 的新主上重试完整只读 SELECT。重试使用新快照，数据/WAL 已变化时结果
  允许变化；session GUC 和 prepared plan 需重新建立。推荐预传 `query_vector`，避免外部
  embedding 服务被重复调用。
- 可复现脚本和完整证据：`evidence/P3-6_ha_test.sh`、`evidence/P3-6_live.log`。

**P3-6 完成。** 下一原子任务是 P3-7：按第 18 节性能矩阵标定串行/并行收益与资源门槛。

### P3-7a HNSW 10 万行 Release 性能标定

- 定位到已启动两个 worker 但父节点仍顺序 drain 两个本地 Stream：先读完文本再读向量会
  让向量 producer 填满有界缓冲后等待，墙钟收益退化。新增窄接口
  `localStreamPoll.h`，只为多输入协调者提供本地 Stream 非阻塞 poll；并行
  `HybridRecall` 同轮轮询两路并交替消费，串行和普通 Stream 路径不变。
- 最终 `-O2` Release 二进制完成定向 `install-bin`，源码/安装文件 SHA-256 一致。10 万行
  表的 BM25/HNSW 均有效；典型计划为 `Hybrid Recall -> 两个 LOCAL GATHER`，实际 worker
  `2/2`、fallback=false、BM25/HNSW 各 1000 行，overlap ratio=1.00。
- 串并常规 Top-10 和文本支路零命中场景逐行 mismatch 均为 0；50 ms timeout 后线程数
  恢复 33，非 idle 会话和遗留锁均为 0，实例继续可用。
- 5 组 warm-up 后执行 60 组交替顺序配对：120 次结果 100% 一致；串行/并行 P50 为
  `899.084/642.882 ms`，P95 为 `1441.934/803.964 ms`，P99 为
  `1641.037/1057.671 ms`，分别降低 `28.50%/44.24%/35.55%`。并行 60/60 获胜，
  满足第 18.4 节 P95 推荐通过线。
- 本轮按用户范围只测 HNSW 10 万行；DiskANN 排除，1000 万行取消，100 万行暂缓。
  完整环境、30 轮小样本波动和 60 轮数据见
  `evidence/P3-7_hnsw_100k_result.md`。

**P3-7a 完成。** 下一步仅在确认需要扩大规模时执行 HNSW 100 万行标定；不启动
DiskANN 或 1000 万行测试。

## 14. 对标 Milvus 驱动的优化批次（2026-09-05，分支 fix/hybrid-recall-worker-guc-sync）

依据 10 万行 hybrid 对标基准（`deliverables/hybrid_vs_milvus_bench/`，与 Milvus 2.6.22 同机同参数，
完整报告见 `公共文档/两路召回对比Milvus/`）定位并修复四个问题，四个 commit：

| commit | 修复 | 效果 |
| --- | --- | --- |
| `01fef232fd` | stream worker GUC 同步白名单缺 datavec 搜索参数（OPT-1：并行支路静默用默认 ef=40） | force 输出与 off 逐字节一致，recall@10 0.830→0.8834 |
| `6c0886e8e2` | OPT-2 T1：vector 支路 fetcher 数据面（MinimalTuple 私有上下文 + 回放），供后续并行化复用 | 输出与串行 drain 逐字节一致（golden 不变） |
| `17b1e06c97` | HNSW 上层下降 ef 1→8（OPT-3 根因：单路径贪心下降整簇走错，recall 按 0.75 平台/整簇量化跳变） | ef=100 recall@100 0.7495→0.9995；质量对齐档 ef400→ef100 |
| `c96e7818c7` | auto 成本模型 stream 启动成本校准 150→5000×cpu_tuple_cost（实测固定开销 ~6ms≈54 单位，旧值让默认配置选 2.6x 次优并行） | auto 默认选串行（各档实测最优）；并行路径仍可达（cpu_operator_cost≥1.0） |

优化后对标（质量对齐档：融合 recall@10 OG 0.8868 / MV 0.879）：

| 指标 | 优化前 | 优化后 | Milvus 2.6.22 |
| --- | --- | --- | --- |
| 融合客户端 P50 | 5.0 ms | **3.5 ms** | 2.7 ms |
| 向量单路 P50 | 2.01 ms | **1.45 ms** | 2.41 ms |
| 并发8 QPS | 883 | **1146** | 977 |
| 融合 recall@10 | 0.8834 | 0.8868 | 0.879 |

附带结论：出厂 ef=40 修复后即满召回（recall@100=1.0）；EXPLAIN JSON 格式有 ~20x 计时 artifact（须用 TEXT）；
半 force（单支路 stream）PoC 否决——LOCAL GATHER 协议固定成本 ~2.9ms > 支路重叠收益上限；
OPT-2 并行 drain 主方案关闭（OPT-3 后收益上限缩水至 ~0.5ms）。evidence：
`evidence/S2/smoke.log`、`evidence/S3/opt3_opt1_results_20260905.md`、OPT-2 设计文档 PoC 记录。

回归：hybrid_recall 套件 exit=0 零 ERROR；hnsw_rabitq_vector PASS；hnsw_lsg_vector 与 S0-3 基线同构（负向断言 + 活实例伪影）。
