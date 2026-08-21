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
