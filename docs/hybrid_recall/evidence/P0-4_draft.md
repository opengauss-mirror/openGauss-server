# P0-4 设计草案：串行收集 + RRF 融合主体（P0-3 落地后细化）

> 状态：仅记录关键决策点（2026-08-21）。完整实现设计在 P0-3 提交后补全。

## 核心问题：执行器先于 Planner，如何验证？（待用户知悉）

设计文档 §14.3 要求 P0-4 "串行收集两路候选，与两个单路 SQL 候选一致"——但 P2 之前
没有 Planner 入口能产出 HybridRecall 计划，纯执行器改动不可从 SQL 触达。

### 候选方案

| 方案 | 说明 | 评估 |
| --- | --- | --- |
| A. 调试执行函数（推荐） | `#ifdef USE_ASSERT_CHECKING` 下新增 `hybrid_recall_debug_execute(plan_text)`：手写 HYBRIDRECALL 序列化计划文本 → `stringToNode` → ExecutorStart/Run，返回融合行 | 与 P0-2 的 out/read 闭环正好接上（手写计划即 round-trip 测试）；仅 debug 构建存在，不进 release；与文档 §12.3 测试 hook 先例一致 |
| B. 提前做 P2-1 marker | 把 planner 识别提前 | 违反阶段闸门顺序（G1 前不动 planner） |
| C. 只做编译级验证 | 无运行时证据 | 不满足 "先加失败测试" 纪律 |

**拟定：方案 A**。P0-4 提交内容 = nodeHybridRecall.cpp 主体（Drain/Upsert/RRF/TopK/输出）
+ 调试函数 + 回归用例（debug 构建跑，golden 对照 S0 手写 RRF）。

## 实现要点（占位，P0-3 后细化）

1. 分支 targetlist schema 固定：`(doc_id, branch_score, payload...)`，resno 来自 Plan；
2. by_doc_id：HashCreate 在 candidate_cxt，key=datumCopy 后的 doc_id（TypeCache
   hash/eq，禁止字节序假设，设计文档 §6.3/§8.4）；
3. RRF + 容量 final_top_k 的 binaryheap，比较器实现完整 tie-break
   （score DESC, source_mask DESC, best_rank ASC, doc_id ASC——doc_id 用 B-tree
   comparator）；
4. StoreHybridResult：从 MinimalTuple 物化输出行；
5. ValidateRuntimeOptions：ereport 版（P0-3 的 Assert 升级）；
6. NaN 策略：非法分数按最低相关处理并固定测试（文档 §8.5）。
