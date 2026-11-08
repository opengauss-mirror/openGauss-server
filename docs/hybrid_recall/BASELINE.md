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

## 14. 对标 Milvus 驱动的优化批次与分支收敛（2026-09-05～2026-09-07）

依据 10 万行 hybrid 对标基准（`deliverables/hybrid_vs_milvus_bench/`，与 Milvus 2.6.22 同机同参数，
完整报告见 `公共文档/两路召回对比Milvus/`）完成以下提交。表中为纯功能分支
`feature/hybrid-recall` 的 commit；原堆叠提交保留在 `integration/multimodal-stacked` 和 backup 分支中：

| commit | 修复 | 效果 |
| --- | --- | --- |
| `aa39527a48` | stream worker GUC 同步白名单缺 datavec 搜索参数（OPT-1：并行支路静默用默认 ef=40） | force 输出与 off 逐字节一致，recall@10 0.830→0.8834 |
| `17f2a92b93` | OPT-2 T1：vector 支路 fetcher 数据面（MinimalTuple 私有上下文 + 回放），供后续并行化复用 | 输出与串行 drain 逐字节一致（golden 不变） |
| `39c47295d8` | HNSW 上层下降 ef 1→8（OPT-3 根因：单路径贪心下降整簇走错，recall 按 0.75 平台/整簇量化跳变） | ef=100 recall@100 0.7495→0.9995；质量对齐档 ef400→ef100 |
| `5417434c2e` | auto 成本模型 stream 启动成本校准 150→5000×cpu_tuple_cost（实测固定开销 ~6ms≈54 单位，旧值让默认配置选 2.6x 次优并行） | auto 默认选串行（各档实测最优）；并行路径仍可达（cpu_operator_cost≥1.0） |
| `159f3cc2e3` | 记录 Milvus 对标驱动的初始优化批次 | 文档基线 |
| `891941e164` | task stream 批交付参数化为 16，并增加仅 vector 支路并行模式 | 首批交付约 2.9→1 ms；小候选端到端仍不如串行 off |
| `2dd1883b2a` | 内置 vector 距离直达、discarded 延迟物化、同页邻接 TID 预取 | 结合 all-PLAIN 与参数寻优后，D2 95.02%/99.04% 等召回单连接档均领先 Milvus；生产并发语义门待补 |

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

2026-09-07 分支收敛时，从迁移前纯分支 `7f765945ac` 依次迁入上述 7 个提交，
`git range-diff` 确认与原堆叠分支的 `01fef232fd`、`6c0886e8e2`、`17b1e06c97`、
`c96e7818c7`、`cd2f005ea4`、`83ae60ab9f`、`bf59a66d8b` 逐项补丁等价。
纯分支使用 `-O2`、`enable_debug=no`、`enable_cassert=no` 完成全量编译；其 regression golden
曾错误依赖只有 DiskANN 堆叠代码才输出的 `Filter Mode: Post-Filter`，提交 `675b0326b6`
去除该跨分支依赖后，Release/O2 `hybrid_recallcheck` 1/1 通过。该提交不改运行代码。

当前分支职责：`feature/hybrid-recall` 是纯功能权威分支；
`integration/multimodal-stacked@bf59a66d8b` 只用于和 DiskANN 等多模改动联调；
旧 `fix/hybrid-recall-worker-guc-sync` 已删除。迁移前纯分支和原优化堆叠头分别由
`backup/feature-hybrid-recall-pre-optimization-20260907`、
`backup/hybrid-recall-optimized-stacked-20260907` 保留。上述分支均未 push。

## 15. SciFact 驱动的 RRF 支路分数投影消除（2026-10-13）

SciFact 全部 1,109 条查询的火焰图发现：索引已经确定候选顺序后，支路投影仍重复计算
RRF 不使用的原始 BM25 分数和余弦距离。后者会重新读取 heap 的 EXTERNAL 向量；
这不是 HNSW 图搜索内部的 TOAST 解压。

- `createplan.cpp:hybrid_elide_unused_score()` 仅处理 RRF 的直接
  `Limit -> IndexScan/AnnIndexScan`；校验唯一排序表达式、原始分数算子及类型，拒绝
  volatile 表达式。遇到 Sort/Result 等其他结构保留原路径。
- 仅将两层 targetlist 第 2 列替换为同类型 NULL，保留列位置；索引 ORDER BY、过滤、
  MVCC、候选数、ef 与最终 RRF 分数均不变。该步骤位于嵌套规划之后、task stream 包装之前，
  不修改页格式，不重建索引。
- 按规定脚本完成 Release/O2 增量编译与安装；`hybrid_recallcheck` 1/1 通过。
  新增 prepared/generic plan、多列/乱序 payload、可见常量及最终分数的 golden 回归。
- 另外在现有 D1/D2 表上执行 96 次语义验证，覆盖 off/auto/force/vector、custom/generic
  plan、过滤及变更参数；包含全部 payload 和融合分数的结果 hash 与旧二进制完全相同。
  四个正式性能配置的全部查询有序 ID 也与修改前一致。
- ef400、text100/vector80 的诊断采样中，每请求用户态 CPU 从 5.3454 ms 降至
  4.8404 ms（9.45%）；原始分数投影子树消失。优化后 HNSW 占 CPU 样本约 75.89%。
  这是单次前后采样的 CPU 证据，不等于端到端时延同比下降，也不是已反超 Milvus。
- 安装二进制 SHA-256：`fa47501098b3b9b54dcad559120048ed611c7866116471b9fb29409e92ef27a2`。
  本次增量构建未重编版本字符串所在对象，`version()` 仍显示旧 build/date，不能据此
  判断新代码未安装；需同时核对运行路径、二进制 hash、构建日志和实际执行计划。

完整证据位于工作区 `公共文档/两路召回对比Milvus/验证/SciFact投影优化_20261013/`；
最新正式 OG/Milvus 差距及环境验收状态以同主题总报告 §4.5 为准。

## 16. RRF 的 BM25 名次扫描（2026-10-13）

投影消除后，BM25 AM 仍为每批候选建立正文字符串到原始分数的查找表；其中包含
额外回表、正文提取/解压、ExecutorState/IndexInfo 构建。RRF 不消费这张表。

- `BM25ScanOpaqueData::skipScoreKeys` 默认 false，只在 `ExecInitHybridRecall()`
  证明文本支路是直接、本线程、无过滤的 `Limit -> IndexScan`，原始分数列为 typed
  NULL，投影无其他表达式，且实际扫描是普通 heap/BM25 时开启。文本 Stream、过滤、
  分区/桶表和其他形态继续使用原路径；普通 `<&>` 查询行为不变。
- `BM25IndexScan()` 的打分、排序、候选扩批仍执行，只跳过 `ConstructScanScoreKeys()`。
  正常 IndexScan 的回表和 MVCC 检查保留。没有持久 GUC、页格式或索引重建变化。
- Limit 的 targetlist 已被 `setrefs` 改写为 OUTER_VAR；typed NULL 必须在真正执行
  投影的子 IndexScan 上验证，不能在 Limit 上要求 NULL Const，否则快路径不会启用。
- 跳过分数表后，翻倍扩批和最终 TAAT 扩批的清理必须检查 NULL：`DELETE_EX`
  自身无空指针保护。开发期回归抓到了遗漏，最终版本已补齐两处检查。
- 头文件改动后按脚本完成全量 Release/O2；后续 `.cpp` 修正采用增量构建。
  最终安装二进制 SHA-256：
  `57c457ea0de83b77a7502ef524a5809b35c9c65908cd7306412e5c9025067e0e`。
  `version()` 显示构建时 HEAD `7546c32f`，应与构建日志及 hash 联合核验。
- 原生 `hybrid_recallcheck` 1/1 通过；672 次 D1/D2、custom/generic、四种并行模式、
  初始批量 5/100、过滤/无过滤、原始分数查询穿插、无文本命中及重复执行对照均通过，
  96 个有序 payload/原始分数/RRF 分数 hash 与修改前一致。
  四个主性能配置的全部查询有序 ID 也与历史正式结果一致；OG-only 诊断计时中多数
  窗口受换页影响，不能代替同期 OG/Milvus 的严格验收。
- 独立 240 行/3000 行小表的 20 组 MVCC/扩批/VACUUM 检查通过，比较快路径和全真
  过滤回退路径；3000 行用例以 text2800/bm25_topk5 覆盖超过八轮后的 TAAT 清理。
  测试表已清理，不替代已有 HNSW 并发 VACUUM/槽位复用的完整语义门。

证据根：`公共文档/两路召回对比Milvus/验证/SciFact名次扫描优化_20261013/`。
最终正确性文件均为 `v3`；首版保护过严未启用、v2 缺 NULL 保护均不能作为验收版本。
性能与火焰图以同主题总报告及交接文档的最新章节为准，不用原生回归计时作为性能结果。

SciFact 高档全查询火焰图（未启用快路径的同代 Release 对照 → v3）：BM25 扫描占比
11.8664%→4.9988%，该子树估算 user CPU 0.60242→0.21809 ms/请求；正文解压样本
89→0，BM25 内额外回表/CreateExecutorState/BuildIndexInfo 样本 69/11/3→0/0/0。
两图 4222/4281 样本，完整图质量检查无 warning。整请求采样 user CPU 5.0766→4.3628 ms
（观测下降 14.06%），但未修改的 HNSW 子树耗时也有波动，不能将整项变化全部归因于本次
代码，更不能当作与 Milvus 的端到端差距。新图 HNSW 80.85%，仍是主要剩余瓶颈。

## 17. HNSW 会话级 buffer 编号提示与有界预取（2026-10-13）

高维普通 vector 扫描的热点仍是逐候选页面定位和内积。本次不缓存向量、页内容或
查询结果，不改变距离算术、候选处理次序、页格式、索引或 MVCC：

- `knl_u_datavec_context::hnsw_buffer_hints` 在 SessionCacheMemoryContext 按需分配，
  每会话至多4096个页号/Buffer编号、约32 KiB，只记一个物理索引，随会话释放。
  不新增独立线程局部变量，不使用被GSC按B-tree元数据解释的 `rd_amcache`。
- 编号只是提示。使用前仍执行原 `PinBuffer` 和 ResourceOwner登记，再检查
  `BM_VALID` 及完整物理BufferTag；不匹配立即释放并回退 `ReadBuffer`。
  缓冲槽位驱逐/复用无需额外失效线程；物理索引改变则清空提示。
  保留读取/命中统计、会话计数及VACUUM成本计数。
- 同一邻接批最多提前pin四页；短共享内容锁内定位tuple，只预取前512字节。
  不跨解锁保留tuple指针/内容，真正计算距离时重新持锁读取line pointer/tuple。
  正常退出释放全部pin，ERROR由原ResourceOwner清理；无长期pin。
- 仅内置vector负内积、维数至少256、非插入/early-stop、非PQ/RabitQ/LSG/mmap
  的扫描启用预取。临时/UNLOGGED、段页、DMS/DSS、NVM/SMB、加密、恢复/NEON
  等不走buffer编号快路径，仍经标准ReadBuffer。

构建与验证：

- 涉及会话头文件，已用统一脚本完成Release/O2 **full**：
  `build_logs/release_full_20261013_211240.log`；实验二进制SHA256
  `254c9725ef8be1d110d90d121e3601f686fe371656a28d38fc8cf20c8bafc7cc`。
- 原生 `hybrid_recallcheck` **2/2**。新增 `hnsw_buffer_hints` 用256维向量对比
  快路径与通用读取的有序ID/距离，覆盖过滤、重复查询、UPDATE/DELETE/VACUUM、
  REINDEX及TRUNCATE后重装。
- 独立16MB buffer实例对约64MB、4096行×2000维索引：11阶段44组有序ID/payload
  相等；预热后仍持续读页，覆盖淘汰压力；5次1ms超时取消后正常恢复。
- 2线程池实例：32/2000维各300次读、100轮UPDATE/DELETE/INSERT及100轮VACUUM，
  游标续扫、快照payload验证通过；同一读会话实际记录到两个worker TID。
- L2/IP/cosine、UNLOGGED/TEMP、halfvec、RabitQ sq8及LSG共24组对照通过。
  **PQ初始化与mmap在当前x86受限，不能记作实测通过；分布式/恢复/NVM/加密也
  未运行。** 这些路径不启用新编号快路径，不能据此宣称全平台生产准入。
- D1/D2的672次语义/96 hash与先前基线相同；新SciFact参数两档的全部1109条
  实际融合结果重现离线验证，有效调参的召回/人工指标另见工作区报告。

同一最终二进制、SciFact text100/vector80/ef400、全1109查询的相邻独占火焰图：
关闭→开启整请求采样user CPU为4.73940→3.64142 ms（观测下降23.17%），HNSW子树
估算3.83419→2.73972 ms（28.55%）。开启图HNSW子树中buffer哈希未采到样本；
其他SQL/BM25/回表路径仍有该调用。内积占完整请求25.74%，并非新的TOAST瓶颈。
两图3932/3574样本、无质量warning、有序ID相同。关闭组用既有early-stop排除门
（INT32_MAX-1，远高于表规模，不会真的提前终止），仍有附加条件检查。
**上述采样CPU证据不是无采样正式时延，更不是对Milvus的胜出百分比。**

证据根：`公共文档/两路召回对比Milvus/验证/HNSW缓存预取_20261013/`，最终版本
使用 `提示v4*` 和 `火焰图_会话提示v4{关闭}_独占/`。整页8KB预取v1因CPU退化
否决；独立线程局部提示v3因原生规范门被替换，不能恢复。后续需继续完成双方独立
寻优及零换页、对称同期性能验收，目标尚未宣称达到。

## 18. HNSW 有界空闲扫描工作区（2026-10-13）

含内核火焰图及目标线程计数发现，高档SciFact每请求约362次minor fault，
主要来自HNSW临时对象首次触页；这不是TOAST解压或磁盘major fault。
缩小常规分配块到96KiB的方案无收益，已撤回。本次只复用逻辑清空的工作内存，
不缓存结果、候选、向量、索引页或pin，不改变距离、候选/ef、续扫、MVCC及页格式。

- 新增会话参数 `hnsw_scan_cache_size`，单位kB，0～16384，**默认0（关闭）**；
  诊断/性能测试显式设2048。设置变化在下一次扫描应用，不重置活跃cursor。
- 每会话最多保留一个空闲上下文，其keeper块不超过设置值，另有少量固定元数据。
  活跃扫描仍可按原机制申请额外块；本参数不是搜索内存或访问量硬上限。
- 借出前清空会话槽并reparent到当前查询。异常/子事务回滚时仍由查询父上下文
  清理，无需保存指向可能已释放上下文的会话指针。多个活跃扫描不共享上下文。
- 正常结束先ResetAndDeleteChildren，再在空闲槽未占用且块大小不超限时归还；
  否则直接删除。仅普通非共享AllocSet、相同session_id且无详细MemoryTracking
  节点时复用；normal/executor/fullexec跟踪及其他分配器回退原路径。
- 槽位在 `knl_u_datavec_context`，随会话结束释放，支持线程池迁移；参数加入
  stream worker同步清单，不新增独立THR_LOCAL。

构建与验证：

- 头文件改动使用Release/O2 full `release_full_20261013_225526.log`，后续CPP
  安全回退增量 `release_incremental_20261013_231826.log`，最终安装SHA256
  `dbfe20b5c0fd3160ae826de65da519865942f90981a9877d466d9f9d808817e6`。
- 原生回归2/2，扩展 `hnsw_buffer_hints` 独立比较工作区开/关的有序ID及精确距离，
  含过滤、UPDATE/DELETE/VACUUM、REINDEX、TRUNCATE后复用。
- `工作区v2生命周期.json`：24阶段，100次重复扫描、双活跃游标、子事务错误、
  整事务回滚、5次超时、1KiB～16MiB/关闭切换、五种跟踪模式及活跃cursor切换；
  空闲块最多一块且只剩分配器元数据，取消后可恢复。
- `工作区v2语义.json`：672查询/96 hash与原基线相同；新SciFact两档各1109查询
  的实际融合ID、双参考召回和人工指标不变。D2完整500查询ID亦复现旧基线。
- 私有16MB buffer、2线程池：32/2000维各300读、100轮写、100轮VACUUM，
  每读后检查空闲工作区且实际观察到同一会话跨两个worker。临时fixture已清理。
- L2/IP/cosine、UNLOGGED/TEMP、halfvec、RabitQ、LSG共24组工作区开/关一致；
  当前PQ动态库不可用、mmap关闭，不计作实测通过，也不外推生产稳定性。

最终代码同二进制0→2048→2048→0四档诊断，全窗口汇总的P50观测降幅：
SciFact低/高档16.85%/18.55%，D2 95%/99%档9.76%/9.42%。存在主机换页，
**只作代码收益证据，不是正式Milvus PK**。高档目标线程minor fault约361.63→0.34
次/请求，system CPU约0.525→0.108 ms/请求；这些百分比不能与前轮收益相加。
v1同二进制含内核图的缺页处理占比9.37%→0.052%；v2新增的是诊断模式回退，
最终性能和火焰图证据以工作区 `验证/HNSW锁与内核_20261013/工作区v2*` 为准。

## 19. HybridRecall 协议级 Bind 基准适配器（2026-10-14）

`contrib/ogai/bench/libpq_bind.py` 为有限查询集提供标准libpq
`PQexecPrepared`文本/二进制参数绑定；服务端C++仍为`4c6383e5d5`，
这不是新的HNSW算法或结果缓存。通过psycopg2自身扩展解析libpq符号，
只用于单调用者、空闲UTF8/autocommit连接，有限参数集合在计时前编码，
实际调用/响应解码在计时内。所有PGresult均释放，错误不转为成功或换计划。
不是通用DB-API或生产连接池实现，不支持在同一连接上并行/异步操作。

- 输入沿用已有十进制vector，通过C locale下的`strtof`直接舍入为float32，
  避免先转Python float造成二次舍入；显式网络字节序、保留负零、不量化。
  全部查询均以服务器`vector_send`校验文本/二进制输入字节相同，再校验结果ID。
- 实测发现当前服务端默认`session_timeout=10min`会覆盖扩展协议的语句定时器：
  `statement_timeout=5ms`下SQL EXECUTE取消0.2秒sleep，而Bind未取消。
  本适配器拒绝非零`session_timeout`或`idle_in_transaction_session_timeout`，
  不偷偷修改GUC；调用方必须显式将两个空闲计时器设为0，同时保留语句超时。
  关闭的是当前测试连接的空闲断连，不是执行超时；未改持久配置或服务端定时器代码。
  该限制必须随结果披露，不能将适配器当作已覆盖任意生产会话配置的客户端。
- `test_libpq_bind.py --out <新JSON>`在现有55440上只创建会话级prepared statement。
  17项通过：Unicode/引号/空文本、负零/极值/舍入中点、非法输入、服务端类型错误、
  超时与错误后恢复、非零空闲计时器拒绝、关闭连接拒绝；不建持久对象。
- 最终实现四档全部输入与有序ID通过，证据
  `验证/HNSW锁与内核_20261013/协议绑定_安全配置质量/results.json`。
  SciFact各1109条、D2各500条；候选/ef/召回/人工质量不变。
- 相同二进制、2048KiB工作区、相同零空闲/10秒语句超时配置的连续采样：
  SQL3898样本、binary3654样本；lexer/parser55→0、vector_in73→0、
  vector_recv0→5；user+system CPU/请求3.37370→3.16244ms（观测下降6.26%）。
  HNSW估算CPU2.38876→2.41813ms基本相近，不能把整体降幅称作HNSW提速。
  原始图`协议绑定_{SQL,二进制}高档全CPU/`及`协议绑定_火焰对照.json`。
  这是顺序采样诊断，不是正式Milvus领先结论；不同阶段降幅不能相加。

工作区原`同期稳态ABBA.py`与`四档同期复测.py`默认仍用SQL EXECUTE。
选择新路径必须显式传`--og-protocol binary_bind --og-session-timeout 0`
及`--og-scan-cache-kb 2048`，环境记录实际超时、协议及适配器源码hash；
比较协议收益的SQL对照也必须使用相同空闲计时器配置。仍按原候选参数、
最低双参考recall、人工质量不退化、完整查询周期和零换页对称轮次验收。
首次原型的`协议绑定_高档/`、`协议绑定_其余三档/`未加空闲计时器约束，
只作方向初筛，不当作最终受保护配置的正式性能结果。

## 20. 内积返回的 AVX/SSE 边界清理（2026-10-14）

`VectorInnerProduct` 在 x86 `__AVX__` 构建的原计算循环之后执行
`_mm256_zeroupper()`。本机旧 Release 反汇编中该函数返回前没有清理
YMM 上半部，而 HNSW 调用方接着执行传统 SSE 指令；新二进制已确认
生成 `vzeroupper`。不改乘加/水平归约顺序、FMA 选择或标量尾部，
不改候选、ef、索引、MVCC、页格式或构建参数；非 AVX 与 ARM 路径不变。
手写同序 AVX helper 仅作为早期对照，已撤下，没有随本补丁提交。

- 统一 Release/O2 增量日志 `release_incremental_20261014_015303.log`，
  01:56:49 完成。安装及实际进程二进制 SHA-256：
  `fd233e19ff83fbcbd7f86533a6a87d97c82a1542984bbcd37e79c8f9ea7067aa`。
- 独立三核对照中，仅补清理与旧循环的 568000 组输入逐位相同。
  传统 SSE 调用方下，旧核约 275～286 ns/次，仅补清理约 132～141 ns/次；
  这是核级控制实验，不是整请求加速倍数，也不是所有硬件的性能保证。
- 本版 672 查询/96 个语义 hash 与前版完全一致；四档完整查询的输入
  `vector_send` 字节和有序 ID 不变；原生 `hybrid_recallcheck` 2/2 通过。
  8 种普通/特殊形态共 24 组回退开关一致性检查通过，PQ/mmap 不可用不计通过。
  独立测试对象已清理，现有基准索引未重建。
- 同参数顺序代码诊断中，SciFact 低/高档 P50 观测下降 19.72%/16.99%，
  P99 下降 17.58%/14.45%；D2 两档 P50 下降 4.39%/2.83%。
  包含全部窗口，存在主机换页，不是干净对称性能验收。
- 同参数完整 CPU 图：旧 3654 样本、新 3882 样本，unknown 叶节点均为 0，
  DSO-only 各 1 样本，无采集/解栈 warning。整请求 user+system CPU
  3.16244→2.61597 ms；HnswSearchLayer 自身估算 0.26397→0.02965 ms，
  内积子树估算 0.85941→0.74126 ms。包含关系的占比不能相加，
  顺序采样降幅不能完全归因于代码，更不能直接换算为领先 Milvus 的幅度。

工作区证据根 `公共文档/两路召回对比Milvus/验证/HNSW锁与内核_20261013/`：
`AVX_SSE边界独立对照.json`、`AVX边界_语义.json`、`AVX边界_质量/results.json`、
`AVX边界_特殊路径.json`、`AVX边界_收益审计.json` 与 `AVX边界_高档全CPU/cpu.svg`。
新代码的同期 Milvus 复测须另跑；继续遵守 §19 的协议/计时器前提和
等质量、零换页、对称顺序验收，不用上述单端诊断替代正式结果。

## 21. 可选 BM25 统一分词模式（2026-10-14）

新增索引级 `WITH(analyzer='standard_v1')`，用于英文/Unicode 混合文本的
共同检索语义测试。默认或显式 `analyzer='jieba'` 保留原有行为，**不会修写
旧索引的词频或改变已有查询结果**。这是一项质量语义能力，不是已验收的
性能优化，也不表示 SciFact 人工相关性 Recall 已达到 95%/99%。

- `standard_v1` 固定 Unicode 16.0.0 Alphabetic 与 Nd/Nl/No 为词字符，
  按非词字符切分，再逐字符完整小写；保留停用词、单字符和数字，不做
  stemming、Unicode normalization 或上下文相关的希腊词尾 sigma 转换。
  规则表及 UCD SHA-256 随代码保存，构建时不需要网络；升级 Unicode 必须
  新增 analyzer/version，不能原地改变已存索引的语义。
- 建索引、插入和查询共享同一路径；无匹配时不回退 Jieba。
  仅支持 UTF8 数据库，禁止同时指定 `dict_path`。当前索引词项上限为
  99 个 UTF8 字节；超限报错，不截断、跳过或悄悄产出空文档。
- analyzer 在创建后不可 SET/RESET。新模式用不同 metapage magic，旧二进制
  应拒绝访问新模式索引；原 Jieba 页布局/magic 不变。不能仅修改 catalog
  选项来切换分词。不同 analyzer 应使用隔离表或独立列，**不要在同一列上
  混建不同语义的 BM25 索引依赖规划器选择**；本次未新增查询级 analyzer 选择 API。

构建使用统一 Release/O2 `full`（修改了头文件），日志
`build_logs/release_full_20261014_084319.log`，08:54:04 完成。安装/运行 SHA-256：
`81d2014bb87f7908a481a3086a0364142eaf794efdb6508cfa7abc282e915a79`。
banner 为构建前的 f17b4508，不能仅凭 banner 推断是否含本节代码。

验证结果：

- 独立遍历 0～0x10FFFF 的 1,114,112 个码点，分类/小写映射全部符合冻结 UCD。
  SciFact 5,183 文档和 1,109 查询的词频与实测 Milvus 2.6.22 analyzer 全等；
  D2 全部 22,000 个词通过。
- 隔离 BM25 文本库全文字节与源文件一致；原生 110,900 个分数独立重算，
  最大相对误差 3.121e-7。全部查询 BM25 Top-10 与 Milvus 及精确公式均 100% 一致。
  4 个并行构建 worker 和 4 个 reorder worker 实际启动，抽验 30 查询分数通过。
- `make bm25_standardcheck EXTRA_TESTS='hybrid_recall hnsw_buffer_hints'` **3/3 通过**。
  新用例含 28 项断言：Unicode/词频、选项不可变、长词边界、在线写入、
  UPDATE/DELETE/VACUUM/REINDEX/TRUNCATE，以及默认 Jieba 行为。
  独立 SQL_ASCII 库验证非 UTF8 拒绝。
- 旧四档全部输入字节/有序 ID hash 不变。原数据、BM25、HNSW 未重建。

仅在**精确向量离线融合**下，相对同词项独立 BM25/RRF 参考的一致率为
OG 100%、Milvus 99.99098%。对应人工 Recall@10 为 BM25 78.7444%、融合
66.1222%；融合仍低于 BM25 单路，也比旧 OG 精确融合 67.4722% 低，不能称为
人工质量全面提升。该首轮验证不包含真实 HNSW 融合的 95%/99% 性能，
后续定标和实测见本节末。

工作区证据：`公共文档/两路召回对比Milvus/验证/SciFact统一召回口径_20261014/`，
重点为 `standard_v1_token_validation.json`、`standard_v1_native_02/`、
`standard_v1_legacy_four_cases/results.json`。首次辅助脚本的 TSV 列读取和
`parallel_workers=0` 参数错误均已修正；首次 native golden 建立时旧 Jieba
全停用词回退的断言样例已修正，随后全部通过，未为此改动旧分词代码。

最终头文件注释空白规范化后按规则再次 Release full：
`build_logs/release_full_20261014_090552.log`，09:16:53 完成，安装/实际进程
SHA-256 为 `24ae5253a5719bcfb48e89192975b87392a75851062b1bd0fa051dfe93d121b4`。
09:26 重跑上述三项原生用例，三个结果文件与 golden 均逐字节一致，
临时实例已停止。产品提交为 `dfe48f9706`，banner 仍是 configure 时的 f17b4508。

共同参考已在工作区生成，旧参考保留。D2 新隔离表的全部 100000 行、SciFact
全部 5183 行与 Milvus 都已回读核对正文和原始 float32 向量逐位相同；不再把
旧 6 位小数 TSV 向量混用。参考固定精确 BM25/cosine 各 Top100、RRF k60、
最终 Top10、数字 ID 同分，SciFact 1109 查询、D2 前 500 查询。
原生 HNSW 融合已验证两数据集双方均可达到 95%/99%，不是仅离线精确向量：
SciFact OG M16/efC200 的 ef258 与 Milvus M64/efC800 的 ef190 均为
10542/11090；ef1760 与 ef1150 均为 10984/11090。
内部 100+100 是这些可行点的配置，不是强制约束，仍在探索其他参数。

Milvus 原有 M16/efC200 索引在 ef6000/32768 下融合只有 97.8810%；
自身向量 Top1 有 196/5183 篇不命中（返回分数均非约1的重复向量同分），
新建隔离同参数对照仍有 208 篇，因此未将其作为对手不可达99%的结论。
M32/efC400 达98.9360%，M64/efC800 已达99%以上。所有旧索引保留；
OG 也有新的 M64 隔离表用于独立调优。这是索引配置与质量口径工作，
不能写成新增 C++ 性能收益；后续等质量同期测量如下。

### 21.1 共同参考下四档原生融合及同期测量

2026-10-14 10:05 完成一轮四档 ABBA。SciFact 两端均使用隔离的
M64/efConstruction800 索引，D2 两端均为 M16/efConstruction200；没有重建
旧基准索引。每个引擎的候选和 ef 独立定标，以下是已验证可行点，不是穷尽
全部参数的全局最优。OG 的 bm25_topk 等于本行 text 候选。

| 数据集/档位 | OG text/vector/ef | Milvus text/dense/ef | 双方共同参考命中 |
|---|---|---|---|
| SciFact 95% | 94/91/91 | 100/100/199 | 10558/11090 = 95.2029% |
| SciFact 99% | 100/100/350 | 100/100/1125 | 10981/11090 = 99.0171% |
| D2 95% | 60/70/70 | 91/70/70 | 4753/5000 = 95.06% |
| D2 99% | 100/100/106 | 100/100/144 | 4960/5000 = 99.20% |

配置为工作区证据根的 `matched_common_cases_m64.json`，原始测量及审计为
`common_abba_m64_01/`。每档包含 OG→Milvus、Milvus→OG 两个配对，每窗
至少预热20秒、测量30秒，在完整查询周期结束；所有16个窗口及原生/Bind
有序ID校验通过。计时端到端、热态、单连接、无过滤；OG使用2048KiB空闲
扫描工作区、串行融合、relaxed_order、二进制Bind，查询向量取原始float32。

全部完整窗口的探索性结果（正数表示OG领先，非代码改动前后加速比）：

| 数据集/等召回 | OG延迟降幅 P50/P95/P99 | OG QPS提升 |
|---|---|---|
| D2 95.06% | +49.46% / +45.79% / +41.73% | +96.15% |
| D2 99.20% | +45.76% / +42.91% / +38.54% | +83.71% |
| SciFact 95.2029% | +31.43% / +23.71% / +21.02% | +44.72% |
| SciFact 99.0171% | -4.11% / -9.70% / -11.91% | -3.60% |

**四档均为环境未通过，8个配对全部受换页污染，不能作为正式验收。**
新口径下SciFact 99%仍未观察到领先，不能继续引用旧80%/81%结果说四档全胜。
人工标注指标也必须独立报告：SciFact 95%档OG/Milvus人工Recall@10为
65.7889%/64.1222%，99%档为64.7889%/66.1222%，不是95%/99%人工召回，
也不是严格等人工相关性质量。统一分词、输入精度与新索引配置的变化不能
全部归因于新增产品性能优化。

该轮的下一诊断点是新SciFact 99%配置下的目标线程CPU/minor fault和火焰图。
该档OG窗口宿主机pgfault约179万/6654请求，其他窗口通常约4～6万；
这只是线索，尚不能归因于OG。现有工作区只保留设置大小的keeper，溢出块
仍在reset时释放；应先验证M64/ef350是否超出2048KiB，而非直接扩大默认值。
原有零swap-in/out/major-fault正式门不变。测后服务恢复停止，新旧数据保留。

## 22. 小索引 visited 初始容量按统计量收敛（2026-10-14）

新共同参考 SciFact 99% 的 CPU/轻缺页采样确认了一项过度分配：M64/ef350
按 `ef * m * 2 = 44800` 预分配 visited，simplehash 实际得到 65536 个桶、
512KiB 数组，而索引只有 5183 条。这个独立大块不会随扫描 keeper 一起保留。
轻缺页事件图中 `tidhash_create` 包含占比为 50.31%；这是轻缺页事件比例，
不是 CPU 比例。将 keeper 从 2MiB 增至 4MiB 可减少另一部分分配，但不能
消除该大数组的问题；8MiB 相比 4MiB 未观察到进一步收益。

`InitVisited` 仅在磁盘查询、索引 `reltuples` 为正且小于原初始容量时，
用其向上取整值作为初始容量提示。当前 SciFact 数组变为 8192 桶、64KiB。
**统计量不限制可访问节点数；simplehash 仍按需扩容。** 插入路径、内存构建、
未知/非正统计量保留旧策略；未改变 ef、距离、候选、页格式或索引，不需要重建。
本次仅改 `.cpp`，Release/O2 incremental 日志为
`build_logs/release_incremental_20261014_113436.log`，11:38:18 完成；安装及实际
运行 SHA-256 均为 `a1ee51773854ebbb662a5d9428eee8ed95ca8296cf2284648619fdb5f3de298d`。

同为 4MiB keeper、相同完整 1109 查询集、相同候选/ef/结果 ID：

- 采样窗口各 7763 请求，目标线程 user+system CPU 为 **4.056→3.861ms/请求，
  下降4.83%**；线程轻缺页 **1001550→16**，major fault 均为0。
- 不带 perf 的两次 4MiB 窗口、各3327请求，平均 CPU **3.939→3.769ms/请求，
  下降4.31%**。这是非交错的前后诊断，不是正式 Milvus PK 或跨场景收益承诺。
- 修改后 CPU 图为4462样本，unknown leaf 0%、DSO-only 0.0672%，无采样告警；
  `VectorInnerProduct` self 23.29%、visited 插入 self 11.77%、邻接展开 self
  10.42%。这些占比不能与包含子调用的比例相加。

验证：原生 `bm25_standardcheck EXTRA_TESTS='hybrid_recall hnsw_buffer_hints
hnsw_visited_sizing'` **4/4通过**；新增用例覆盖统计仍为4而实际256行时扩容、
过滤精确结果、删除/VACUUM、TRUNCATE/未知统计，cache=0/4096均检查。四个
新共同参考档位全量原生有序ID hash与改前一致；旧语义672查询/96hash一致。
32/2000维各300并发读、100写轮次、100VACUUM轮次及快照续扫通过；8类可用
特殊路径各3查询通过。PQ动态库未加载、mmap未启用，**两者未计入通过项**。

上述证据均在工作区 `验证/SciFact统一召回口径_20261014/`：
`sf99_m64_cache4096_cpu_before/`、`sf99_m64_cache4096_cpu_after/`、
`sf99_m64_cache2048_minor_01/`、`sf99_m64_workspace_sweep_01/`、
`visited_sizing_workspace_after_01/`、`visited_sizing_quality_*_01/`、
`visited_sizing_legacy_semantics.json`、`visited_sizing_concurrent.json`、
`visited_sizing_fallbacks.json`。CPU变化与 keeper 调参分开归因；正式性能门仍
要求同期双方、等共同参考召回、完整周期和双向配对全部零宿主机换页。

### 22.1 新代码与工作区参数的四档同期复测

产品提交`292303d8ea`、同一上述二进制于12:04完成
`common_abba_visited_sizing_01/all_cases_audit.json`。沿用§21.1数据、索引、
共同参考和所有候选/ef；只有SciFact99%的会话keeper设为4096KiB，其余三档
仍为2048KiB。两个引擎在同轮重新测量，2个反序配对/档、每窗至少预热20秒、
测量30秒并完成整轮查询；16窗完整，四档整数命中和有序ID均与定标一致。

| 数据集/等召回 | OG延迟降幅 P50/P95/P99 | OG/Milvus QPS | OG QPS提升 |
|---|---|---|---|
| D2 95.06% | +56.85% / +54.34% / +51.33% | 868.47 / 378.03 | +129.73% |
| D2 99.20% | +50.30% / +43.15% / +34.89% | 684.89 / 349.88 | +95.75% |
| SciFact 95.2029% | +36.31% / +30.47% / +27.65% | 468.87 / 299.81 | +56.39% |
| SciFact 99.0171% | +12.84% / +8.71% / +7.01% | 249.89 / 218.10 | +14.58% |

**样本四档均领先，正式验收仍未完成：**15/16窗口被swap-in或major fault
污染，完整干净配对0/8。上表是聚合全部完整窗口的探索性值，不能作为正式
胜出或SLA，也不能把代码、参数及环境变化都记为此次C++补丁收益。
尤其D2未触发容量缩减，绝对时延变化不能直接归因于该分支。
SciFact人工指标不变，不表示95%/99%人工召回或等人工质量。

12:06测试服务恢复停止，新旧数据/基准索引保留。停库后40秒观察仍有18次
swap-in/15次major，进程快照对应Codex；只能说明该空闲观察窗口背景换页，
不反推计时窗口归因、不豁免原门。复跑使用工作区
`matched_common_cases_m64_workspace4096.json`及`run_common_bench.py`，先取得
安静环境，不无限重跑。完整延迟表、构建及安全边界见工作区交接§0.35和
四部分总报告§4.5.14，产品/测试已本地提交、未push。

## 23. BM25 task-worker参数同步与AUTO计划验收（2026-10-14）

用户要求将性能主入口从固定OFF改为AUTO，并实测验证代价排序。四模式
检查发现：`sync_guc_variable_namelist`已有HNSW设置，但遗漏了BM25的
`bm25_topk`、`bm25_k1`、`bm25_b`。FORCE下的文本worker静默使用默认值；
VECTOR文本在主线程执行，所以不受该问题影响。D2的第36条查询出现名次
差异，非默认评分参数还会改变结果集合，不能以“只是同分”忽略。

本次只补入上述三项同步，不修改成本系数、候选、ef、表、索引或页格式。
原生新增`hybrid_bm25_worker_guc`使用独立200行fixture，覆盖custom/generic、
初始批5/60、分别修改k1/b及AUTO/VECTOR/FORCE，共24组有序ID比较，并检查
非默认参数确实改变fixture排序。旧二进制断言失败、新二进制通过。
本轮固定`enable_bm25_taat=off`；不宣称覆盖所有BM25非默认设置。

- Release/O2 incremental日志`build_logs/release_incremental_20261014_151117.log`，
  15:14:57完成；安装及实际进程SHA256均为
  `a74feea43addf3829a63310650a4e9608ed76af23a517662b49a8ed6e7ba9067`。
- 完整原生`bm25_standardcheck EXTRA_TESTS='hybrid_recall hybrid_bm25_worker_guc
  hnsw_buffer_hints hnsw_visited_sizing'` **5/5通过**，五个结果文件与golden
  逐字节一致。首轮golden多一行空白，核实断言均true后仅修空白并完整重跑。
- 工作区`验证/SciFact统一召回口径_20261014/auto_bm25_worker_before.json`
  与`auto_bm25_worker_after.json`保存四档各60查询、三组参数、三种执行模式
  的独立对照。修改后2160请求中所有同参数串并行有序ID均一致；FORCE不再
  忽略非默认k1/b。新用例实际输出保存在`auto_bm25_native_worker_guc.out`。

AUTO验证以同一场景下低代价对应低延迟/高QPS为准，代价单位不是毫秒。
当前AUTO只枚举串行和双worker，VECTOR为显式诊断，尚未被AUTO枚举。
工作区`validate_auto_plans.py`为四模式独立连接、每条查询custom/generic
计划、实际worker检查及正反序计时。主ABBA CLI已新增`--og-parallel-mode`并默认AUTO，保存
实际SHOW结果；复跑历史OFF必须显式传off。未修改全局默认设置。

### 23.1 四档四模式控制与逐查询代价排序

`auto_plan_validation_01/`已完成（22:08～22:34）：每模式独立连接及预编译
语句，10秒预热、30秒测量、完整查询周期，两轮顺序相反；无并发构建或
其他压测，Milvus此阶段未启动，**不是与Milvus的同期PK**。

| 等召回档位 | 串行/双worker估计代价 | OFF P50 ms | AUTO P50 ms | VECTOR P50 ms | FORCE P50 ms |
|---|---|---|---|---|---|
| D2 95.06% | 33.07 / 77.64 | 1.048 | 1.051 | 7.494 | 8.327 |
| D2 99.20% | 35.07 / 79.79 | 1.313 | 1.261 | 8.271 | 9.784 |
| SciFact 95.2029% | 22.85 / 71.46 | 2.086 | 2.084 | 9.806 | 10.888 |
| SciFact 99.0171% | 23.28 / 71.93 | 3.741 | 3.767 | 12.456 | 13.321 |

四档AUTO均选串行。`per_query_choice_audit.json`检查3218个“查询×档位”
组合的custom/generic计划（四模式合计25744次计划检查）：全部AUTO选择
低代价串行；每条查询跨周期中位数、以及分别按两轮中位数比较，都没有
VECTOR/FORCE更快的查询。TEXT EXPLAIN ANALYZE首条查询实际worker为
OFF/AUTO/VECTOR/FORCE=0/0/1/2，均无fallback；VECTOR的planned字段仍显示2，
是其共享双worker计划标注的局限，不代表实际启动2个。所有请求有序ID
均与各档冻结原生结果一致，未削减候选或改变质量。

**控制实验32/32计时窗口均被换页污染**（5281次swap-in、3399次major，
swap-out为0），所以仅为探索性排序证据，不是正式性能验收。OFF与AUTO
选中相同物理策略，二者数值差异不能记为新代码加速。成本系数未改动；
该模型未直接计入ef/维数/M，VECTOR与FORCE还共用估计值，不能把本四档
选对推广为跨场景精确耗时预测。后续须在安静窗口复测，并补非默认TAAT、
max_score_ratio等未覆盖的worker参数；本轮两者保持默认off/1.05。

### 23.2 AUTO与Milvus同期复测

22:49完成`common_abba_auto_01/all_cases_audit.json`，使用相同四档参数、
共同参考、同一二进制及AUTO与Milvus2.6.22同期比较。两端同期在线，
每窗至少20秒预热、30秒测量并完成500/1109查询周期，两个顺序均保留。
四档SHOW均为AUTO且实际选择Serial RRF/BM25/Ann；双方各自有序ID与原生
定标完全复现，整数命中严格相等。下表汇总全部16个窗口，不按速度选择。

| 等召回档位 | OG AUTO P50/P95/P99（ms） | Milvus P50/P95/P99（ms） | OG延迟降幅（P50/P95/P99） | OG/Milvus QPS | OG QPS提升 |
|---|---|---|---|---|---|
| D2 95.06% | 1.041 / 1.549 / 1.908 | 2.340 / 3.394 / 3.928 | +55.53% / +54.37% / +51.41% | 911.73 / 402.62 | +126.45% |
| D2 99.20% | 1.232 / 1.840 / 2.292 | 2.579 / 3.633 / 4.211 | +52.21% / +49.36% / +45.57% | 768.52 / 368.47 | +108.57% |
| SciFact 95.2029% | 2.015 / 3.089 / 3.738 | 3.077 / 4.272 / 5.010 | +34.50% / +27.69% / +25.40% | 472.34 / 311.38 | +51.70% |
| SciFact 99.0171% | 3.798 / 5.138 / 5.945 | 4.197 / 5.489 / 6.316 | +9.50% / +6.39% / +5.88% | 255.59 / 230.10 | +11.08% |

**全部16个窗口未过环境门，干净配对0/8**（5656次swap-in、2355次major，
swap-out为0）。以上四档均观察领先，但不是正式性能验收；不把新旧窗口
波动记成AUTO或此次参数同步补丁的加速。质量档位是共同RRF参考的复现率，
不是人工相关性召回；人工指标与原定标相同。后续需要安静/独占窗口，
不无限重试污染窗口、不擅停用户任务。原生结果正确性与代价选择观察已完成，
正式性能门仍未通过，当前不能宣布整个优化目标已验收。

22:50已恢复正式OG55440/Milvus停止，Oracle/私有55450/原生25632均未运行，
无活动构建或测试。Milvus正常stop等待超时后exit137、OOM=false；容器、
数据卷与所有基准表/索引均保留。仅本地提交产品、原生回归及本记录，
不push；原`contrib/shark/smartmatch.pl`权限dirty不属于本次，保持原样。

## 24. Worker生命周期与Hybrid成本第一阶段修订（2026-10-15）

以下§4.5.*节号指工作区四部分总报告；本节同步其本轮验证记录。当前成本修订只覆盖Hybrid根节点，不代表底层独立ANN成本或跨场景准确性已经完成。

本轮按Release/O2编译并验证了四项产品变更，没有修改基准数据、等召回PK的候选/ef参数或索引格式，也没有重建基准索引：

- `StreamNodeGroup::deInit()`仅在仍有worker未退出时sleep，不再在已满足退出条件后额外睡1ms。独立追踪的收尾阶段均值约2.24→1.11～1.12ms；这不是整请求或Milvus PK的提升百分比。
- `ExecPollLocalStream()`在FINISH时补报已保存的worker错误。旧版FORCE曾把无效向量维数错误吞成文本单支路结果；修复前20/20复现，修复后80次四模式错误/恢复检查通过。属于正确性修复。
- `ThreadPoolStream::InitStream()`为无前端连接的Port初始化无效socket哨兵，消除误查fd0的keepalive错误；新生命周期日志不再出现该类错误。没有通过压低日志等级掩盖。
- `hybridRecallPath.cpp`的Hybrid根节点成本加入ef、M、维数、上层ef=8下降、邻接检查与距离计算的不同增长形态，并按实际池化路径和1/2个worker区分启动/传输成本。保留BM25原估算；过滤、多个ANN、分区及特殊编码等未覆盖路径回退。**这是部分成本修正，不是把底层所有ANN成本模型都改好了。**

既有线程池确实可复用Stream线程：576请求/1152任务由同2个Stream TID完成。但每个任务仍重建session；`enable_thread_pool`也会让普通非HA连接进入会话线程池，不是只影响Hybrid worker的独立开关。本轮只临时启用实例配置，未更改产品默认值；收尾已恢复原配置。

**成本修订的独立验证。** 固定候选，训练诊断用ef100/350/700；验收用未重叠的60条查询、ef200/500、正反两序，比较耗时比例与成本比例，不将cost当毫秒。以下均汇总全部窗口，没有按速度选择；部分窗口换页，所以不是精确标定或误差上界。

| 执行环境 | ef变化的平均相对误差：旧→新 | 同ef串并行比例的平均相对误差：旧→新 |
|---|---|---|
| 线程池OFF | 20.18% → 5.96% | 29.65% → 25.58% |
| 线程池ON | 23.30% → 7.14% | 33.96% → 15.48% |

四个正式参数档的同版本模式对照另覆盖3218个查询×档位组合，全量有序ID一致；逐查询中位数未发现VECTOR/FORCE更快，实际worker数为OFF/AUTO/VECTOR/FORCE=0/0/1/2。SciFact99的P50为4.578/4.651/9.967/10.535ms，支持AUTO选串行。95%→99%时，SciFact成本增长73.82%、AUTO平均耗时增长78.55%；D2分别增长17.06%/19.31%。这些是同数据集内的短诊断，不证明跨数据集标尺、BM25词频/扩批、缓存越界或并发负载已经准确建模。

**线程池ON＋AUTO的四档同期ABBA。** SciFact为5183文档/1109查询/768维词袋随机投影，HNSW M64、efConstruction800；D2为10万合成文档/500查询/768维，M16、efConstruction200。双方沿用§4.5.13的统一分词/共同参考与各自寻优配置，最终Top-10、等权RRF k60。质量是共同参考复现率，不是人工相关性召回。每窗口预热至少20s、测量至少30s，完整查询周期、双端同期在线、两个顺序均保留。

| 等召回档位 | OG P50/P95/P99（ms） | Milvus P50/P95/P99（ms） | OG 延迟降幅（P50/P95/P99） | OG/Milvus QPS | OG QPS 提升 |
|---|---|---|---|---|---|
| D2 95.06% | 1.312 / 1.931 / 2.335 | 2.822 / 3.952 / 4.526 | +53.50% / +51.13% / +48.40% | 724.10 / 340.73 | +112.52% |
| D2 99.20% | 1.553 / 2.304 / 2.784 | 2.706 / 3.779 / 4.413 | +42.60% / +39.03% / +36.92% | 609.56 / 352.00 | +73.17% |
| SciFact 95.2029% | 2.636 / 3.672 / 4.272 | 3.220 / 4.255 / 4.853 | +18.14% / +13.69% / +11.97% | 367.52 / 299.91 | +22.55% |
| SciFact 99.0171% | 4.657 / 5.909 / 6.605 | 4.505 / 5.699 / 6.383 | -3.37% / -3.68% / -3.47% | 210.25 / 215.49 | -2.43% |

双方各自冻结的有序ID和相等的整数命中全部复现。**16/16窗口受换页污染，合格配对0/8**（swap-in3518、major2879、swap-out0）。本轮ON配置下SciFact99仍观察落后，不能沿用10月14日“最新四档都领先”的说法，也不能将本表当正式验收。

**恢复原线程池OFF后的SciFact99补验，仍为AUTO、同一新二进制和质量参数：**

| 等召回档位 | OG P50/P95/P99（ms） | Milvus P50/P95/P99（ms） | OG 延迟降幅（P50/P95/P99） | OG/Milvus QPS | OG QPS 提升 |
|---|---|---|---|---|---|
| SciFact 99.0171% | 4.317 / 5.884 / 6.666 | 4.480 / 5.681 / 6.310 | +3.65% / -3.58% / -5.64% | 224.25 / 216.64 | +3.51% |

这次QPS/P50观察领先，但P95/P99仍落后；4/4窗口换页、合格配对0/2（swap-in388、major317）。不能与ON另三档拼成“一套统一配置四档均赢”的表，不能把不同时间窗口之差直接归因给线程池。**当前尚未达到全指标、干净窗口下全面超过Milvus的目标。**

验收与风险：标准Release增量日志`release_incremental_20261015_093851.log`；正式/原生临时安装二进制SHA256均为`f05b9fd863436c03ac80906a14bc00b19bd86bb17fff116550e2d80a5cab9dc4`。原生五项通过；最终池ON/OFF各12872正常请求、20错误恢复及4连接400请求通过，池ON另有80短超时/恢复通过。非池化取消偶发FATAL关连接已在精确旧二进制400次中14次复现，未解决，不计为该门通过；池化短测试也不等于长期稳定性验收。一次池化成本计时与正确性测试尾部重叠，已整轮标记作废并独占重跑，未纳入上述数字。

证据：[池ON四档同期](../../../公共文档/两路召回对比Milvus/验证/并行瓶颈分析_20261015/auto_pool_on_common_abba_01/all_cases_audit.json)、[恢复池OFF的SciFact99](../../../公共文档/两路召回对比Milvus/验证/并行瓶颈分析_20261015/auto_pool_off_scifact99_abba_01/all_cases_audit.json)、[最终逐查询模式选择](../../../公共文档/两路召回对比Milvus/验证/并行瓶颈分析_20261015/final_four_modes_pool_on/choice_audit.json)、[非池化成本误差](../../../公共文档/两路召回对比Milvus/验证/并行瓶颈分析_20261015/cost_model_pool_off_comparison.json)、[池化成本误差](../../../公共文档/两路召回对比Milvus/验证/并行瓶颈分析_20261015/cost_model_pool_on_comparison.json)、[原生五项日志](../../../公共文档/两路召回对比Milvus/验证/并行瓶颈分析_20261015/cost_model_native5_final.log)。完整失败记录、代码位置和下一步见[交接§0.38](../../../公共文档/两路召回对比Milvus/交接文档_继续优化.md)。

## 25. 精确新旧串行核查与SciFact99复测（2026-10-15）

产品代码仍为`183eeda3de`，本节仅记录诊断，没有新增产品优化或回退已提交修复。
临时恢复4个.cpp并用标准Release增量脚本重建旧产品`b47f4fb9a5`，
精确复现旧SHA256 `a74feea43addf3829a63310650a4e9608ed76af23a517662b49a8ed6e7ba9067`；
随后恢复源码并重编，新SHA256仍为
`f05b9fd863436c03ac80906a14bc00b19bd86bb17fff116550e2d80a5cab9dc4`。

按预先固定旧→新→新→旧、每阶段重启、20秒预热/30秒测量/完整查询周期，
线程池OFF、AUTO实际串行、同候选/ef/工作区、Milvus停止：
SciFact99旧/新P99为5.525/5.590ms（新版高1.18%），CPU/请求3.682/3.731ms；
D2 99旧/新P99为2.212/2.210ms。全量有序ID一致，8窗均有host换页，目标线程major为0。
没有复现约12%的持续版本回退，但不把约1%直接判定为稳定退化或噪声。

新旧CPU图13566/13405样本，unknown0%，HNSW占82.25%/82.02%；
内积和visited热点接近，未采到额外规划热点或HNSW内detoast。
样本缺失不证明函数零调用；带perf时延不进PK，包含占比不能相加。

同一新版随后单独同期复测SciFact99，池OFF＋AUTO串行、两端M64/efConstruction800，
OG候选/ef=100/100/350、4MiB工作区，Milvus=100/100/1125；
最终Top10、RRF k60，共同参考命中10981/11090、双方各自冻结ID全复现。
OG P50/P95/P99=3.971/5.197/5.831ms，Milvus=4.553/5.657/6.214ms，
OG延迟低12.79%/8.12%/6.16%，QPS245.68/214.76（高14.40%）。
4窗均污染、合格配对0/2，仅观察领先；保留上午慢样本，不与池ON其他档拼统一表，
不改写旧正式表，不宣称已严格通过无回退/稳定胜出门。

源码/默认安装已恢复新版，配置hash仍`8c83e997…`、线程池默认OFF；
OG/Milvus停止，数据/索引保留。Milvus stop退出134、OOM=false，下次启动检查恢复。
下一步先稳定隔离环境、追查约1%版本差异及跨窗口慢尾，不盲目撤销正确性修复。

完整参数、原始证据、图及限定见[总报告§4.5.18](../../../公共文档/两路召回对比Milvus/HybridRecall与Milvus测试数据、有效优化及结果总结.md)，
复跑与恢复点见[交接§0.39](../../../公共文档/两路召回对比Milvus/交接文档_继续优化.md)。

## 26. 长连接核验、事件式收尾与worker名次快路径（2026-10-15）

本轮基于产品`183eeda3de`，没有修改数据、索引、候选、ef或成本系数。
客户端此前就是长连接；新增审计在计时窗口外核对连接对象、socket inode、
libpq启动标识和会话application_name，确认预热、正反序及全部查询周期复用同一连接。
这不等于内部Stream worker session跨查询复用，本轮没有实现后者。

### 26.1 两项代码改动及安全边界

- `StreamNodeGroup::deInit()`用已有mutex/cond等待原有生命周期谓词，
  最后一个worker在`restoreStreamEnter()`持锁通知；消除1ms轮询sleep，
  不是跳过真实清理等待。循环处理虚假唤醒，零/部分创建仍用原计数条件，
  取消不能提前释放group。此为公共Stream逻辑，普通SMP也必须回归。
- `ExecEnableHybridRecallWorkerRankOnly()`在worker的`PortalStart`之后、首次执行之前，
  只对HybridRecall文本task分支重新验证串行路径的名次扫描条件，
  在worker自己拥有的BM25 descriptor上设置`skipScoreKeys`。
  必须是直接无过滤的`Limit→IndexScan`、第二列为planner消除的typed NULL，
  Stream类型/组/分支/DOP/投影均匹配；有过滤、表达式消费原始分数、
  分区/桶、普通Stream等均保留原路径。没有共享leader descriptor或新增全局GUC。

### 26.2 构建及验证

头文件有变更，已用统一脚本Release/O2 **full**，日志
`build_logs/release_full_20261015_114648.log`，11:57:50完成。
安装、实际测量进程和原生临时安装的SHA256均为
`e954f9db341c66a02366b3407d862f75ab7118eb5b4f6b0af8024b943658d09d`；
banner是configure时的`7d49dcae`，不能以此代替源码/二进制核验。

- 原生5/5通过；新增custom/generic、过滤/无过滤、参数重扫、ID/payload/融合分数对照，
  并穿插普通BM25查询，确认原始分数仍可用。
- 池OFF和ON各12872个正常请求、20次运行时错误恢复、4连接400个并行请求通过，
  所有有序ID复现；ON另有80次短超时尝试及同会话恢复通过。
- 普通非Hybrid SMP在池OFF/ON各90次结果与串行一致、10次除零错误恢复，
  实际覆盖LOCAL GATHER/REDISTRIBUTE。
- 非池化取消旧问题仍存在：新版本160次诊断中5次FORCE关闭连接，无实例重启；
  旧版已在400次中复现14次。不能用不同样本发生率证明等价，不能宣称取消门通过。

证据根为`公共文档/两路召回对比Milvus/验证/并行事件等待与快路径_20261015/`。
OFF追踪666请求的deInit内sleep均为0，均值0.192ms；之前条件轮询版本记录1.114ms。
这是带探针阶段诊断，不是严格配对的端到端收益。BM25调用点探针对无过滤/过滤各120请求：
重复分数表构建为0/520次，结果均与串行相同，证明快路径生效且过滤回退保留。
池ON的无过滤/过滤各120请求也分别为0/520次。池ON另828请求的deInit内sleep全部为0，
均值0.123ms；3个Stream线程执行1656次任务初始化，证实线程复用存在，
但每请求的两个任务仍各自初始化session。

### 26.3 观察结果与未完成事项

同一长连接脚本的新旧池OFF对照中，FORCE的P50观察降低8.79%～16.15%；
32个窗口仅3个无换页，且版本间经历构建/重启，不作为严格净收益或新Milvus PK。
首轮D2串行变慢，追加固定旧→新→新→旧复核而非删掉慢样本：
SciFact99 P50/平均时延为+0.37%/+0.57%，D2 99为−4.93%/−5.19%；
8窗仍有host换页，没有重现持续7.5%的D2 P50退化，也未严格证明无回退。

随后池ON四模式长连接对照（2秒预热/3秒测量下限、完整查询周期、正反两序）
实际保存的OFF/AUTO/VECTOR/FORCE计划worker数为0/0/1/2，无fallback。
3218个查询×档位的逐查询中位数比较未发现并行更快，支持AUTO保留串行；
没有重新调成本。16个长连接共84924个预热/测量请求均无重连、ID相同。
32窗中8窗无换页，仍只作短诊断，不与池OFF跨窗口数据拼成线程池净收益。

新FORCE全进程CPU图13054样本，unknown为0%；互斥归因中worker初始化39.44%、
清理10.20%、HNSW29.91%、BM25 2.34%。这是CPU样本分布，不是墙钟阶段占比。
下一重点是安全的内部worker会话/只读缓存复用，必须处理权限、GUC、事务快照、
资源所有者、失效和内存上限，不能用客户端长连接冒充已解决。
本轮未运行Milvus，不改写§25的旧版本同期结果；正式环境门和非池化取消风险仍未完成。

完整窗口、参数及后续池化模式对照见[总报告§4.5.19](../../../公共文档/两路召回对比Milvus/HybridRecall与Milvus测试数据、有效优化及结果总结.md)，
复跑、探针清理及收尾状态见[交接§0.40](../../../公共文档/两路召回对比Milvus/交接文档_继续优化.md)。

## 27. 池化HNSW空存储复用与单路收尾接收（2026-10-15）

这一轮针对执行区间，而非再次压低并行计划代价。未改候选、ef、融合规则、
索引格式或全局线程池默认值，未重建任何基准索引。业务/性能主入口仍为AUTO，
FORCE只用于同参数的真实双worker对照；执行中段改善不等于端到端或Milvus胜出。

### 27.1 所有权与回退

- `knl_t_storage_context`只新增一个按需分配的`HnswWorkerCache`指针。
  不增加独立`THR_LOCAL`，不放宽原生宏数量门；头文件变化已全量编译。
- 仅不迁移的`THREADPOOL_STREAM`保留线程所有的空工作区。普通backend、
  非池化Stream和可跨worker迁移的客户端session仍走既有session路径。
  不修改`session_id`、不把session上下文重新挂到线程、不保留整个worker session。
- 每个参与线程至多一个空闲keeper，受现有`hnsw_scan_cache_size`限制，最多16MiB。
  换父session、改变尺寸、关闭缓存或开启详细跟踪时丢弃；嵌套扫描在借出期间回退。
  正常结束先清空全部扫描内容，查询上下文上的reset callback负责ERROR/取消时
  删除借出的存储，包含keeper分配尚未完成的失败。线程退出按原内存树回收。
- buffer提示只保存约32KiB的物理页/缓冲编号，不保留页内容、Relation/tuple指针、
  pin、锁、快照或权限判断。每次仍先pin、校验完整BufferTag和BM_VALID、
  按当前ResourceOwner记账；不命中或不适用时回到原ReadBuffer。
  `hnsw_scan_cache_size=0`只关闭空工作区，并不关闭独立的buffer编号提示。

### 27.2 执行区间内的背压修正

工作区修正后，追踪仍发现SciFact99/D2 99的HNSW worker每请求约6次发送等待，
在ExecutorRun内合计约0.640/0.671ms。两路活跃时仍采用原交替非阻塞轮询，
避免阻塞一条支路而饿住另一条。**只有一路已真实EOF**，才对剩余一路使用
既有`ExecProcNode → ScanMemoryStream → gs_memory_recv`阻塞通知路径。
仍逐条维护原rank和candidate cap、读至EOF、传播生产者错误并响应取消；
不因首批达到LIMIT提前终止，也不改变普通Stream接收实现。

### 27.3 构建、正确性与测量边界

Release full日志为`build_logs/release_full_20261015_165652.log`；仅接收端.cpp的
后续增量日志为`release_incremental_20261015_173123.log`，17:35:13完成。
最终二进制SHA256：
`5e5fa8443f6ae6ddcc5ab8d7422734049dcebbcd57fb8f2a6c8ea4dc51c19945`。
banner仍显示configure时的4db9d8d4，不用它替代hash验证，也不因仅提交Git再重编。

- 原生五项5/5通过，包含BM25 worker GUC、buffer hints和visited sizing。
- 池ON四模式12872查询的冻结有序ID一致；另有4连接400请求、20次worker错误恢复、
  80次短超时尝试及同连接恢复通过。实际worker为0/0/1/2，无fallback。
- 工作区65个生命周期阶段通过，含12次worker维数错误、40次取消后恢复，
  尺寸0/1/64/2048/4096/16384KiB与跟踪模式切换；空闲上下文有界且逻辑清空。
- 私有fixture共376次完整ID/RRF分数对照，通过更新/删除/VACUUM、重建索引、
  TRUNCATE/重填、换物理索引及四种空支路形态。只清理自己的schema，基准对象未动。
- 非池化取消旧风险、全面OOM注入、多平台/PQ/mmap和长期生产稳定性仍不声称通过。

证据根：`公共文档/两路召回对比Milvus/验证/worker执行复用对照_20261015/`。
最终版本为`*_drain`，`*_final`是较早的仅工作区版本，不能混作同一二进制。
CPU采样、时间线和无探针2×2对照分开；保持长连接、最终Top-10和冻结结果ID。
线程池中的其他简单SQL按每个Bind/Sync请求的实际TID排除归属，但保留全部客户端
时延及原始trace，不通过删慢请求消除其干扰。中段与端到端分开验收，不加总并行worker时间。

最新数值及环境门见[总报告§4.5.21](../../../公共文档/两路召回对比Milvus/HybridRecall与Milvus测试数据、有效优化及结果总结.md)，
收尾和续做入口见[交接§0.42](../../../公共文档/两路召回对比Milvus/交接文档_继续优化.md)。

本轮最终诊断均值：SciFact99串行/并行执行区间4.036/4.313ms，D2 99为
1.240/1.449ms，仍分别慢6.85%/16.81%。HNSW worker发送等待观察由
0.640/0.671ms降至0.214/0.348ms，未消除所有传输或图搜索差额。
无探针旧/新FORCE均值为9.244→6.713ms、4.355→3.563ms；AUTO为
4.449→4.426ms、1.551→1.612ms（D2观察慢3.96%，尚未排除回退）。
新16个无探针窗仅1个干净，追踪相关12窗5个干净；不是严格净收益或Milvus PK。
下一步验证16行批次是否适合阻塞融合，并核对图搜索工作量/缓存/调度；
不能只把等待挪到ExecutorRun外来满足阶段指标。17:57:45 fast停库并完成停止，
临时配置精确恢复，36个自有探针清理，原tracefs和全部基准数据保留。

## 28. 按候选上限确定有界交付批次（2026-10-16）

### 28.1 依据及实现边界

上轮100个候选每16行交付一次，HNSW worker约6次等待共享缓冲可用。
HybridRecall必须收齐两路候选后融合，首批提前到达不会缩短这个依赖，
却会增加生产者与消费者交接。在同一实验ELF、同一持久FORCE连接内比较
16/32/64/100行批次，100行时两路发送配额等待均为0。

`execStream.cpp:buildLocalStreamContext`仅对`is_task_parallel`且直接子节点为
`Limit`、上限为非空正值INT8按值常量的路径，设置
`tupleBatchSize = min(limit, TupleVectorMaxSize)`，当前上限100。
大于100的候选仍分批，未知任务形态保持16，普通Stream保持既有100。
生产者缓冲按该有界批次分配，接收者既有容量为100；不改变候选上限、
ef、rank、EOF/错误/取消或元组所有权，尾部不足一批仍用原EOF刷新。
没有新增GUC、头文件或页格式；实验的`hybrid_batch_experiment.rows`
读取钩子已从最终产品删除。`createplan.cpp`只同步说明注释。

### 28.2 编译与验证

最终Release incremental日志`build_logs/release_incremental_20261016_084342.log`，
08:47:31完成；安装、运行、回归与诊断的二进制SHA256均为
`0b2a00cf09f2aaaa2e72f14dffafe58e45b2c8cb7ce333fde2758679e35ecca4`。
本轮没有头文件变化，不需要full。banner仍为configure时4db9d8d4，不能作源码身份。

- 原生5/5；四模式12872次冻结有序ID核对、4连接400请求、20次worker错误恢复、
  80次短超时尝试及恢复通过，实际worker为0/0/1/2，无fallback。
- 私有315行fixture的候选上限1～400，覆盖16/32/64/100边界、非整批、
  超过表行数和不对称支路；有/无过滤共200组，完整ID/payload/RRF分数及
  两路实际候选行数与OFF一致。首次脚本误设bm25_topk=1被合法范围5～200拒绝，
  原记录保留，清理后修正独立初始批量参数并在新目录完整重跑，未放宽产品限制。
- 376次私有fixture更新/删除/VACUUM/REINDEX/TRUNCATE/换索引/空支路检查通过；
  普通实际Stream的90次查询及10次错误恢复通过。自建schema清理，基准索引未重建。
- 不扩展声称全面OOM、非池化取消、PQ/mmap或长期生产稳定性已通过。

### 28.3 完整执行区间与结果局限

旧§27并行中段只计worker包络，遗漏末worker结束后leader的接收/融合/输出尾部。
本轮串行取leader ExecutorRun；并行取首worker开始至
`max(末worker结束, leader ExecutorRun结束)`，不通过把尾部挪出执行段制造收益。
准备＋完整执行＋收尾可加；worker包络、发送等待、图搜索是子区间，不能重复相加。

同一最终ELF、线程池ON、持久连接、最终Top-10与冻结质量下的诊断均值ms：

| 档位 | AUTO串行完整执行 | FORCE并行完整执行 | 并行相对串行 | AUTO/FORCE Bind→Sync |
|---|---:|---:|---|---:|
| SciFact 99.0171% | 4.164 | 4.328 | 慢3.94% | 4.444 / 6.601 |
| D2 99.20% | 1.190 | 1.111 | 快6.68% | 1.393 / 3.113 |

100候选时两路发送配额等待均为0；但端到端FORCE仍慢，不改cost强迫AUTO并行。
无探针正反序长连接均值为SciFact AUTO/FORCE 4.556/6.898ms、D2 1.477/2.989ms。
最终追踪相关12窗仅1窗、无探针8窗仅2窗过host分页门，全部窗口保留，
这些是机制验证和诊断观察，不是正式净收益、SLA或新的Milvus PK。

SciFact图搜索层墙钟串行/并行3.541/3.968ms，独立CPU窗口估算HNSW
3.644/4.316ms CPU/请求。下一步先核对有效ef、展开/距离/访问次数，工作量相同
再查缓存/调度；有序ID相同不证明工作量相同，不预先认定NUMA或频率为根因。
证据根`公共文档/两路召回对比Milvus/验证/交付批次对照_20261016/`，入口
`final_audit.json`、`experiment_audit.json`、`work_state.json`；详细表见总报告§4.5.22，
续做见交接§0.43。09:05 fast停止OG，配置hash精确恢复8c83e997…、线程池默认OFF，
自有探针清理，原tracefs/用户dirty/基准数据保留，Milvus和Oracle未启动。

## 29. HNSW工作量与反向单worker诊断（2026-10-16）

### 29.1 核对真实工作量

在§28的0b2产品二进制上，对SciFact全部1109查询做AUTO/FORCE正反序，
共4436请求；冻结有序ID、底层ef350/上层ef8/M64、3层搜索和邻接展开次数一致。
平均展开359.970243次；平均距离入口为4028.412985/4028.412083次。
约893万次入口/模式中有2次孤立的串行额外计数，原证据保留，
不称全部计数逐位相等；不能用这个规模解释worker约10%的图搜索差额。

计数只在HnswSearchLayer范围归属；leader scan生命周期还包着BM25/SQL，
不能误当纯HNSW。FORCE的2218次leader空scan初始化另存，不是重复图搜索。
两数据集各32查询的抽样另外核对预取、pin、ReadBuffer；首个worker冷hint
与后续热态分开。重探针计数时延不作为性能数据。

### 29.2 未保留的反向分工原型

临时59c3412b ELF只改createplan.cpp和nodeHybridRecall.cpp：VECTOR模式加
实验占位参数，改成BM25 worker＋leader本地HNSW，并先做本地向量再接收文本。
不是产品新增TEXT模式，不改AUTO/成本/候选/ef/索引。原型已撤回，补丁与ELF归档。

同一原型、池ON、每数据集同一长连接切换方案，正反序两轮。
每窗口至少预热10秒、测量10秒、完整查询周期；切换在计时外重新PREPARE，
plan_cache_mode仍auto，实际计划0/2/1/1worker、无fallback、冻结ID不变。

| 档位 | 串行完整执行ms | 双worker完整执行ms | 反向单worker完整执行ms | 无探针串行/双worker/反向均值ms |
|---|---:|---:|---:|---|
| SciFact99 | 4.333 | 4.502 | 4.278 | 4.919 / 6.857 / 5.111 |
| D2 99 | 1.242 | 1.112 | 1.824 | 1.631 / 3.272 / 2.447 |

完整执行含最后leader接收/融合/输出。单worker还包含leader本地搜索，
不能把末worker结束后的全部时间标作纯融合尾部。SciFact反向分工阶段仅观察
快1.27%，端到端仍慢3.92%；D2反向方案端到端慢50.10%，因此不合入。
独立长连接的四方案对照同方向；两个无探针对照各1/16窗干净，
追踪1/12窗、独立CPU1/3窗干净，无合格对称组合，不是正式收益或Milvus PK。

### 29.3 证据支持到哪里

SciFact串行HNSW/BM25估算3.990/0.283ms CPU/请求，负载极不均衡。
双worker及反向方案的HNSW约4.250/4.302ms CPU/请求，额外执行成本抵消
大部分文本重叠。内积self差较小，预取、buffer pin/锁和候选堆管理仍是方向。
反向方案实测图搜索与BM25执行重叠0.442ms，但图搜索也变慢0.304ms；
省下0.055ms完整执行，额外准备/收尾却多0.336ms（同一trace边界）。

SciFact图搜索AUTO/FORCE/反向墙钟3.719/4.142/4.022ms，guest可见
off-CPU仅0.053/0.894/0.152微秒。WSL sched payload是kernel PID，perf样本
是另一命名空间TID，必须按同一switch-out事件验证映射；首次分析失败保留，
修正后另做完整平衡追踪。不能用未匹配事件制造零等待。
硬件cycles/instructions/cache-misses不可用，尚不能把差额具体归因缓存、
频率、NUMA或宿主机vCPU调度。CPU采样不是墙钟，不跨窗口相减。

下一步分别验证add_guc_variable的重复全表排序与HNSW预取/pin/内容锁热路径；
前者是初始化优化，不能改称图搜索加速。不能省略GUC继承、保留脏session、
跳过页锁或降低成本强推端到端更慢的并行。

### 29.4 验证与复跑入口

原型Release incremental日志release_incremental_20261016_092736.log。
原生5/5、四档四方案12872有序ID、并发400、10次leader维数错误恢复、
40次超时尝试/恢复、200组候选/过滤/批次完整结果通过，基准索引未重建。
这不等于原型所有worker错误、OOM、多平台和长期稳定性门已验收。

证据根`公共文档/两路召回对比Milvus/验证/HNSW工作量核对_20261016/`，
入口orientation_audit.json、work_state.json；脚本count_work.py、
branch_orientation.py、run_orientation_diagnostics.py、summarize_orientation.py。
prototype补丁orientation_prototype.patch仅为历史实验，不要自动重施。
原型ELF保存在`.runtime/hybrid-leader-vector-20261016.xsw496/gaussdb_orientation`。
恢复编译日志release_incremental_20261016_100531.log；最终恢复/收尾状态见
该证据根的completion.json及[交接§0.44](../../../公共文档/两路召回对比Milvus/交接文档_继续优化.md)。

恢复编译于10:09:25完成，安装和实际PID2093889的SHA256均精确恢复为§28的
0b2a00cf09f2aaaa2e72f14dffafe58e45b2c8cb7ce333fde2758679e35ecca4。
恢复后的原生5/5、四模式12872有序ID、并发400请求、20次运行期worker错误
恢复、80次短超时尝试/恢复均通过。源码只有本节文档新增，产品C++保持099656b971，
原smartmatch.pl权限dirty保留，未push；不需因本次文档提交再次编译。

## 30. 同虚拟CPU执行对照（2026-10-16，无产品改动）

功能代码仍099656b971，安装及实际进程仍为§28的0b2 SHA256；
未重施§29反向原型、未改成本/候选/ef/索引，没有新Milvus PK或编译。
本轮用池ON、同一长连接、SciFact99既定参数和冻结结果，临时约束本实例
TPLworker/TPLstream的CPU亲和性，并在每组finally中恢复。

- 双worker允许CPU2/4，leader CPU6；串行分别CPU2/4。
  4436次同查询同实际CPU配对：HNSW串行参考3.240694、worker3.482349ms，
  观察慢7.46%。换CPU与不换CPU的914个query/CPU配对差0.051986ms，
  这是观察性分组，不能当作缓存失效或调度的因果贡献。
- 既有VECTOR单HNSW worker固定CPU2，leader在CPU6做BM25；串行也CPU2。
  两个窗口等权HNSW3.376151/3.720642ms，慢10.20%；
  底层3.316159/3.650515ms，占0.344491ms图搜索增量97.06%。
  因此不能仅归给两个worker竞争、静态CPU位置、初始化或上层下降。
  guest可见off-CPU仍很小；同guest CPU不等于固定物理核/频率/宿主机排他。
- 独立ABBA火焰图估算HNSW3.562229/3.782473ms CPU，差额分散在
  内积、visited、邻接、预取及pin/内容锁，未证明某个单独微架构原因。
  CPU估算不与另一个窗口的墙钟相减，锁CPU不等于已证实锁争用。
- 默认亲和性完整执行正序串行/双worker4.068/3.838ms，反序3.787/3.893ms，
  方向不同；等窗3.927/3.865，Bind→Sync4.179/5.959ms。
  不能把此前“完整执行慢4%”当固定规律，也不能靠阶段数字强推AUTO。
- 14追踪窗36597请求、4CPU窗；各阶段预热/控制/采样合计133080次有序ID一致
  （不含三个initial warmup），实际0/2/1worker及Hybrid Recall/ANN计划核验。
  追踪仅5/14、CPU0/4环境门通过，没有干净对称组合，不发布正式收益。
  原始请求加权summary保留，另给等窗均值展示完整查询周期数的权重影响。

11:14:25正常停库，三个控制的线程亲和性已恢复，自有探针清理、原tracefs保留；
实例配置hash恢复8c83e997…、默认池OFF，Milvus/Oracle未启动，基准数据未改。
下一步仅对有证据的底层热点做最小对照；GUC重复排序是独立初始化课题，
不能预先算作图搜索提速。已否决的全页预取/页内位置提示不要重复。

证据根`公共文档/两路召回对比Milvus/验证/HNSW执行位置对照_20261016/`，
入口diagnosis_audit.json、work_state.json、completion.json。
复跑脚本placement_control.py、single_worker_control.py、capture_fixed_cpu.py，
以及analyze_transitions.py、summarize_diagnostics.py；必须新输出目录和核验ELF。
这些工作区脚本和证据不属于源码Git提交。

## 31. Local Stream 提前 EOF 竞态：原版实证及最小修复（2026-10-19）

在产品099656b971、HEAD5bde98afc0、原Release二进制0b2上，使用GDB
non-stop只暂停接收线程，不修改数据、寄存器值或状态标志：接收端刚读到
DATA_EMPTY后，worker正常发布100条BM25候选并设置结束标记；恢复接收端，
`gs_find_memory_data()`返回FINISH时队列仍有100条未读。SciFact ordinal1038
最终结果精确等于此前异常的纯向量Top10。这证明原产品存在接收竞态，不是
HNSW近似搜索噪声，也不是只有旧临时插桩版才可能出现。

根因是先读队列状态、再读结束标记，可能拼出“旧EMPTY＋新结束”的组合。
本提交只修改`libcomm_memory.cpp`：先读结束标记，经`pg_read_barrier()`再读
队列状态；producer以`pg_write_barrier()`在最后一批数据/消息之后发布结束。
未看到结束则本轮不能宣告EOF；看到结束后则检查已发布的最终队列。
不新增锁、睡眠、GUC或头文件字段，不改搜索候选、索引、RRF和成本。
此修复覆盖共用接收函数，而不是只在HybridRecall融合层补候选。

统一Release/O2增量构建`release_incremental_20261019_113044.log`于11:35:41
成功，安装及实际PID3010992 SHA256均为：

```text
efe0796ebbca2cf4f9ecc16372aba28fe2b8b77a1e313f5acccca6f0304dda8e
```

修复版同样暂停接收端：在两次读取之间发布时先返回DATA、读空后才FINISH；
在两次读取之后发布时先返回WAIT、下轮取走数据，两种结果都匹配冻结Top10。
原版的两个历史自然失败请求没有接收现场，不能追溯保证其唯一原因，亦不能
用本次人为延长时序估计自然发生率。该修复不证明HNSW worker差额已消除。

验证结果（线程池ON，长连接；非性能测试）：

- 提取真实接收函数的85种确定性交错：旧代码18失败，新代码85/85通过。
- 原生回归5/5；四档四模式12872个有序ID、4客户端并发400请求通过。
- 20次worker维数错误、80次超时尝试及会话恢复通过。
- 200组候选/批次/过滤边界，完整有序ID、payload、RRF分数及支路行数一致。
- 普通行式/向量化Local Gather/Redistribute，DOP2/4共160次完整结果比较
  及4次worker除零报错/恢复通过，覆盖多生产者与空/多批队列。
- 两个历史异常查询各5000次，共10000次原生FORCE请求通过。

可独立运行的源码回归（无需启动数据库）：

```bash
python3 src/test/ut/local_stream_eof/check.py
python3 src/test/ut/local_stream_eof/check.py --git-ref 5bde98afc0  # 预期退出1，18失败
```

该单测验证协议交错，不能替代弱内存架构实机验证；ARM未实测。
全部原始GDB状态、失败的两次调试器接入尝试、构建和回归证据在
`公共文档/两路召回对比Milvus/验证/LocalStream提前EOF验证_20261019/`，
入口`audit.json`与`completion.json`，总报告§4.5.32、交接§0.53。
调试断点已移除，无新perf探针或CPU绑定。原配置恢复后停止测试实例，
不启动Milvus/Oracle，不重建基准表或索引；仅自建批次夹具已清理。
下一步回到无BM25重叠条件下HNSW worker首次页访问/执行状态差额，
不把正确性修复包装成新Milvus性能收益。
