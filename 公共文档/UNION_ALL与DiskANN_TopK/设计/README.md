# UNION ALL LIMIT 下推与列裁剪：设计总入口

整理与源码核对日期：2026-09-12。

本目录集中收纳两个特性的设计：UNION ALL 分支级 DiskANN `ORDER BY + LIMIT` 下推，以及 UNION ALL 必需列源头裁剪。这里的“LIMIT 下推”不是任意 UNION ALL 查询的通用 LIMIT 下推。

另收纳与上述目标计划关联的后续优化设计：仅适配 DiskANN 的 AM 距离结果复用（L2 精确开方、Cosine 显式近似），HNSW 不做适配。两份原文档保留 ANN 文件名，共用接口通过实际 AM 身份限定为 DiskANN。这部分目前只有开发设计和实施指南，尚未开发或验收，不代表现有两个特性已经具备距离复用能力。

## 1. 先读哪份

| 阅读目的 | 文档 | 使用口径 |
|---|---|---|
| 理解当前两个特性如何协同、两个 GUC 如何组合、改写入口和计划变化 | [LIMIT 下推与列裁剪协同设计说明](UNION_ALL_LIMIT下推与列裁剪协同设计说明.md) | 当前实现对照入口，优先阅读 |
| 深入了解字段最后消费者、列号压紧、四种目标 SQL、测试矩阵 | [必需列源头裁剪详细设计](UNION_ALL必需列源头裁剪详细设计.md) | 保留初期方案与实施记录；按文首说明区分设计建议和已实现内容 |
| 开发 DiskANN 距离返回与执行器复用，理解数值契约、显式引用、缓存寿命和排序边界 | [DiskANN 距离结果复用详细开发设计](ANN距离结果复用详细开发设计.md) | 仅适配 DiskANN 的拟开发方案；GUC、引用节点和计划展示均未实现 |
| 按文件和阶段实施 DiskANN 距离复用，并接入原生回归 | [DiskANN 距离结果复用开发实施指南](ANN距离结果复用开发实施指南.md) | 包含修改清单、伪代码、DiskANN 小数据 SQL 骨架、构建及验收门；不是已有通过报告 |
| 追溯最初的 Top-K 下推方案 | [Top-K 详细设计全文](历史参考/openGauss_UNION_ALL下推DiskANN_TopK详细设计.md) | 历史设计，不代表当前全部安全边界 |
| 追溯外层 WHERE 与分支 Top-K 的联合设计、表达式映射、EC 修复 | [分支过滤与 Top-K 协同下推设计全文](历史参考/openGauss_UNION_ALL分支过滤与DiskANN_TopK协同下推设计.md) | 历史设计，不能覆盖后续列裁剪和六类安全修复 |
| 查历史限制与当时的源码实现 | [下推限制分析全文](历史参考/openGauss_UNION_ALL与DiskANN_TopK下推限制分析.md)、[Top-K 完整代码解读](历史参考/openGauss_UNION_ALL与DiskANN_TopK完整代码解读.md) | 基于旧提交；其中“当前”“尚未修复”等用语均指原文版本 |

当前核对对象为 `openGauss-server` 的 `integration/multimodal-stacked` 工作树：HEAD 是 `098d77c8b76d56eace4af8fddaf8d94b394b996c`，另有尚未提交的六类正确性修复。只取得这个 commit 或旧特性 patch，不能认为已取得修复后的完整实现。

文档冲突时：当前工作树源码及对应测试证据 → 协同设计说明 → 详细设计的实现状态说明 → 历史方案。目标迁移分支有差异时，必须在目标分支重新核验，不把本目录中的旧 OID、类型名称、行号或计划形状当成固定接口。

## 2. 源码、测试、修复与迁移资料

以下资料保持原有专题分层，通过本入口统一关联；没有为了汇总设计而搬动源码、重跑测试或改写已有 ZIP。

| 类别 | 入口 | 说明 |
|---|---|---|
| 列裁剪关键代码逐段解读 | [源码解读](../源码解读/UNION_ALL必需列源头裁剪源码解读.md) | 配合当前三阶段入口和安全修复阅读 |
| 初始开发与验收 | [开发与测试报告](../测试/UNION_ALL必需列源头裁剪开发与测试报告.md) | 初始交付快照，不替代后续修复验收 |
| 六类缺陷的现象、根因、修复、验收 | [深度复核与缺陷修复报告](../测试/UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md) | 包含列裁剪与既有 Top-K 调用链的正确性问题 |
| 修复后的原生回归证据 | [最终回归日志](../测试/深度复核证据/原生回归_最终.log) | 已有 5/5 联跑证据，不是本次文档整理新跑的结果 |
| Join 类型变化的独立归因 | [根因分析](../测试/Join类型变化验证/根因分析.md)、[复现 SQL](../测试/Join类型变化验证/复现.sql)、[完整输出](../测试/Join类型变化验证/实测完整输出.out) | 63 行源数据；含主对照和反事实对照 |
| 跨同源分支迁移质量检查 | [迁移审查指南](../迁移/UNION_ALL必需列源头裁剪跨分支迁移质量审查指南.md) | 语义、Query/Var 元数据、入口顺序、测试框架核对 |
| 迁移用全文件快照 | [源码与测试比对包 ZIP](../执行包/UNION_ALL列裁剪源码与测试比对包.zip)、[包入口](../执行包/列裁剪迁移比对临时目录.Q9Z9tX/00_README_迁移比对入口.md)、[源文件清单](../执行包/列裁剪迁移比对临时目录.Q9Z9tX/MANIFEST.tsv) | 完整文件，不是仅截取差异；按清单比对，不能盲目覆盖异源工作树 |
| 六类问题的完整复现与修复交付 | [修复包 ZIP](../执行包/UNION_ALL六类正确性问题复现与修复.zip)、[给大模型的入口](../执行包/UNION_ALL六类正确性问题复现与修复/00_入口_给大模型.md)、[修复 patch](../执行包/UNION_ALL六类正确性问题复现与修复/02_问题修复/union_all_safety_fixes.patch) | 问题说明、修复、SQL/expected 和证据配套 |
| 初始列裁剪 patch | [初始特性 patch](../执行包/opengauss_union_all_required_columns_pushdown.patch) | 历史交付，单独使用不包含六类修复 |

## 3. 当前代码导航

路径相对于本工作区；链接指向活动源码，不是冻结快照。需要冻结材料时使用上一节比对包。

| 文件 | 需要看的内容 |
|---|---|
| [preptopk.cpp](../../../src/gausskernel/optimizer/prep/preptopk.cpp) | 三个入口；全部 UNION ALL 叶子发现；WHERE 映射；必需列压紧；Top-K；排序键输出；安全检查 |
| [planner.cpp](../../../src/gausskernel/optimizer/plan/planner.cpp) | `subquery_planner()` 中三阶段顺序；本 Query 成功布尔值与可继承扫描策略的区分 |
| [createplan.cpp](../../../src/gausskernel/optimizer/plan/createplan.cpp) | `use_physical_tlist()` 防止源扫描恢复无用列；有序索引 `indexorderbyops` 的表达式类型/EC 相关修复 |
| [prep.h](../../../src/include/optimizer/prep.h) | 入口声明，特别是列裁剪入口返回 `bool` |
| [relation.h](../../../src/include/nodes/relation.h) | `PlannerInfo::union_all_required_columns_pruned` |
| [guc_sql.cpp](../../../src/common/backend/utils/misc/guc/guc_sql.cpp)、[knl_session_attr_sql.h](../../../src/include/knl/knl_guc/knl_session_attr_sql.h) | 两个独立 GUC 的注册、默认值和会话存储 |
| [prep/Makefile](../../../src/gausskernel/optimizer/prep/Makefile)、[prep/CMakeLists.txt](../../../src/gausskernel/optimizer/prep/CMakeLists.txt) | 核对目标分支实际使用的构建系统是否包含 `preptopk.cpp` |
| [主回归 SQL](../../../src/test/regress/sql/union_all_required_columns.sql)、[expected](../../../src/test/regress/expected/union_all_required_columns.out) | 典型计划与查询结果看护 |
| [安全回归 SQL](../../../src/test/regress/sql/union_all_required_columns_safety.sql)、[expected](../../../src/test/regress/expected/union_all_required_columns_safety.out) | 六类正确性缺陷看护；当前为未跟踪文件，迁移时不可漏掉 |
| [回归 GNUmakefile](../../../src/test/regress/GNUmakefile)、[专用 schedule](../../../src/test/regress/parallel_schedule.union_all_required_columns)、[主 schedule](../../../src/test/regress/parallel_schedule0) | 原生 `pg_regress` 接入，专用目标 `union_all_required_columns_check` |

## 4. 汇总范围与维护规则

- 两份历史 Top-K 设计、限制分析、Top-K 完整代码解读均以完整正文归入 `历史参考/`，不是仅提供摘要或原路径。来源、原文 SHA-256 和复制规则见[历史参考清单](历史参考/README.md)。
- 原列裁剪详细设计继续保存在本目录；新增协同说明统一解释实现现状，不再另建同名“最终版”。
- 历史来源文件保留原位，避免破坏已有引用。本次只给归档副本增加版本提示、修正因目录层级变化失效的相对链接。
- 后续功能变化优先更新协同说明和现有详细设计；历史正文保留溯源价值，不持续伪装成最新实现。
- 单表 DiskANN 自适应过滤/补搜、INCLUDE、HybridRecall 等不是这两个特性本身，不合并进本设计集合。
- DiskANN 距离结果复用作为关联后续优化单列维护，不覆盖前两特性的当前实现说明；特别不得把“免重复计算”写成“整个查询不再回表”，也不得把“距离值已转换”写成“跨批输出已证明有序”。
