# GV 距离复用与列裁剪排查：关键函数源码比对档案

## 0. 给目标仓 AI 的使用说明

本文件把本轮「UNION ALL＋ORDER BY＋LIMIT＋Anti Join＋KGN 扫描不注入距离引用」排查涉及的源仓函数集中在一起。**归档某个函数不表示已确认它有 BUG**。这里记录当前 openGauss 工作树的真实源码，不是 GV 源码，也不是可以直接套用的迁移 patch。

请先对照入口、调用顺序、实际计划节点和表达式树，找出**同一条 SQL 中第一个阻断复用的位置**，再提出修复。不要直接删安全检查，不要把返回 false 的不同函数理解成相同语义，也不要靠 EXPLAIN 的括号推断内部节点类型。

使用顺序：

1. 先读第 1～4 节，明确源仓与 GV 的映射及比对契约。
2. 用第 5 节函数索引定位源码。A 组完整收录 preptopk.cpp；B～F 组包含完整入口函数、计划构造、setrefs 和 walker，不能只核对注入函数。
3. 对每个相关函数按「等价 / 缺失 / 有差异且合理 / 差异待验证」记录。目标仓可重命名或重构；名称不同不等于漏迁，行为不同也不自动等于错误。
4. 先核对实际运行二进制、源码版本、GUC 和 SQL；所有断点必须对应同一次执行及同一计划对象。禁止把一个单表 SQL 的注入成功与另一条 UNION SQL 的拒绝日志拼成因果。
5. 输出修复前的明确证据后再改代码；保留各项语义检查并补真实计划、结果正确性和 GUC 组合回归。目标 KGN 必须实际命中其执行器取值路径，不能只修改 EXPLAIN 显示。

这是一个可单独复制的 Markdown 文件；涉及的函数全部给出完整函数体，包含较大的规划入口、setrefs 分发、表达式初始化、解释执行和反解析函数，**不以局部 case 代替完整函数**。另附结构、字段、宏及 GUC 注册项的定义上下文；这些不是被截断的函数。它不是整个数据库的可独立编译源码树，也不替代 KGN 自己的 AM 内部实现。

## 1. 快照范围与已知状态

- 归档时间：`2026-10-30T15:48:28+08:00`。
- 源工作树：`/home/qiumc/data/code/cpp/openGauss-server`。
- 分支：`integration/multimodal-stacked`；HEAD：`ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4`。
- 来源口径：当前工作树，包含已编译测试但尚未提交的无 WHERE 增强；不是仅从 HEAD 导出。
- 收录：139 个函数定义（含 A 组整文件中的函数），114 个源码块。
- GV 仓库及其运行环境不在本地；本文的 GV 路径与现象来自用户提供的信息。

当前已经放开的条件是 `required_columns_outer_query_is_safe()` 中「必须有外层 WHERE」的限制，实际代码现在只检查 `query->jointree == NULL`。对应日志也已修正。**未放开** `ann_orderby_node_allows_approx()` 的 Var/Const 检查；未增加 VRACNODE；未实施「列裁剪与距离复用解耦」。

preptopk.cpp 的历史函数注释仍有 `mandatory outer WHERE` 文案；归档保留原文，不在摘录中偷偷改写。判断行为必须看实际条件。无 WHERE 时 `prepare_outer_where_pushdown()` 返回 false 仅表示没有推谓词，不等于列裁剪整体失败。

此前本地已完成编译和 7 项原生回归、60 组联动矩阵、12 组消费者正反验证；这些是 DiskANN 源仓测试，不是 GV/KGN 实测。修复后 Result 仍在，扫描距离引用已生效。两档 `enable_expr_fusion` 设置都做过测试，但实际断点都命中传统取值函数，不能据此声称展平引擎 opcode 已实际覆盖。本轮仅生成源码归档，没有重新启动数据库或修改内核。

## 2. 源仓与 GV 应比对的语义，而不是照抄的名字

| 项目 | 此归档中的源仓 | GV/KGN 比对要求 |
|---|---|---|
| 向量类型 | vector，typmod 按源仓维度表示 | floatvector 及其 typmod 编码必须核对；不能直接假定数值相同 |
| L2 操作符 | `<->`，本地 OID 8311 | 按目标实际 OID/支持函数匹配；KGN L2 也是近似值 |
| Cosine 操作符 | `<=>`，本地 OID 8313 | 目标是 `<+>`，必须适配；源仓 `<+>` 是 L1，不是 cosine |
| 索引 AM / 扫描 | DiskANN / AnnIndexScan | KGN / 普通 IndexScan；注入、安全检查、EXPLAIN、状态绑定与两套表达式引擎要一致适配 |
| 距离质量 | exact L2、approximate cosine | KGN 两种距离都按目标近似契约处理；不能让近似 L2 偷走源仓 exact-L2 安全豁免或 heap recheck 分支 |
| 转换公式 | 源仓 DiskANN 约定 | 遵守 KGN 已明确的公式；这里不是要求更换目标算法 |
| Result 类型 | T_BaseResult；SPQ 另有条件编译的 T_Result | 记录实际 nodeTag，不能仅按 EXPLAIN 的 Result 字符串猜测 |
| 部署角色 | VSINGLENODE 才进入源仓注入 | 按真实运行角色和本地执行能力适配；VRACNODE 仅是此前的假设，当前源仓不存在此类型 |
| delta 分支 | 本轮实测为 SeqScan | 用户 GV 计划可为 ForeignScan；必须检查其输出引用和 walker 行为，不能机械套用源仓堆扫描资格 |
| Query 改写 | preptopk.cpp 三个 preprocess 入口 | 用户报告映射为 GSRboUnionAllRequiredColumns / GSRboUnionAllOrderKeyProjection；仍需核对真实 RBO 阶段与成功返回值 |

## 3. 完整调用顺序与必须保持的性质

```text
planner
  standard_planner
    subquery_planner
      初始化 root->union_all_required_columns_pruned（继承 parent_root）
      preprocess_union_all_required_columns
        本 Query 安全检查、全部叶子验证、列号重映射、一次性安装改写结果
        成功后 pruned=true（不要求新列数小于旧列数）
      preprocess_union_all_diskann_topk
        基于原始距离 ORDER BY 决定是否做分支 Top-K
      preprocess_union_all_order_key_projection
        由本 Query 的列裁剪成功返回值触发
        分支导出 __ua_order_N 标量列
        外层距离排序项改为引用该列的 Var
      pull_up_subqueries / flatten_simple_union_all / 规划路径与创建计划
    push_down_union_all_source_projections
      pruned 标志控制；安全 Anti Join 保留侧投影下沉到 Scan
      这时仍是 setrefs 之前的表达式域
    top_plan = set_plan_references(root, top_plan)
      Limit / Sort / MergeAppend：dummy tlist → OUTER_VAR
      Result：匹配下层表达式 → 槽位引用；匹配不到则可能仍执行 OpExpr
      Join：按上下侧与可空语义建立 OUTER_VAR / INNER_VAR
    各独立 subplan 的 set_plan_references
    rewrite_ann_orderby_distance_refs
      角色、版本、全计划执行域检查
      rewrite_ann_orderby_distance_walker
        按祖先传播 approximatePlanSafe，递归返回后恢复
        识别目标 AM / 算子 / 类型 / quality
        匹配 Scan.targetlist 与 indexorderbyorig[0]
        完整匹配的 TLE.expr 替换为 AnnOrderByRef
```

`unionAllColumnsRewritten` 表示本 Query 是否通过改写检查；继承来的 `root->union_all_required_columns_pruned` 用于后续窄投影约束。两者不能相互替代。仅设置 pruned=true 不能证明排序键已导出，更不能代替安全验证。

Top-K 给分支添加 resjunk 排序表达式，不等于已把它作为 UNION 可见接口传给上层。排序键物化和物理源投影下推是两步：Query 分支的 targetList 有距离，物理 Scan 的 targetlist 仍可能没有距离，因此两步都要核对。

`union_all_replace_source_chain()` 必须先找齐源 TargetEntry、按指针去重、完整验证后统一修改。Sort 等节点可能共享 targetlist；不能恢复成「边改边重新查找 Var」的旧实现。

## 4. 针对当前 GV 断点的排查顺序

### 4.1 三个布尔条件的含义

`ann_orderby_plan_is_unsafe()` 为 false 表示没有命中其禁止执行域，通常允许继续；它不是「是否成功注入」。`ann_orderby_node_allows_approx()` 为 false 才表示当前节点不允许近似值继续穿透。入口 if 中的 return 是退出复用改写，不能把 unsafe 调用取反。

目标已报告入口三个条件均通过，应将排查集中到后续实际节点和质量判定；不要继续靠改入口碰运气。

### 4.2 顶层 Limit 真有 OpExpr：先查时序与表示

若实际断点确认 `Limit.targetlist` 含 OpExpr，且因此 `approximatePlanSafe=false → NOQUALITY`，这个直接拒绝链可以成立。但「UNION ALL 上层必须保留距离 OpExpr，因此与注入器根本冲突」不成立。

源仓 Limit 在 setrefs 中对全部输出调用 `set_dummy_tlist_references()`，无条件建立 `OUTER_VAR`；它不执行 targetlist 投影。Result 则可以真正计算表达式，不能与 Limit 一起无条件放行。GV 如果采用不同表示，需证明哪些字段仅为描述、哪些参与执行，再适配节点检查。

请记录同一次 SQL、同一计划在以下三个时点的数据：

```text
时点：setrefs 前 / setrefs 后 / 注入入口
topPlan 地址、节点 tag、Limit 与 Result 地址
每个 TLE：resno、resjunk、ressortgroupref、nodeTag(expr)
Var：varno、varattno、varlevelsup、vartype、vartypmod
OpExpr：opno、opfuncid、opresulttype、opcollid、inputcollid、完整 args
调用栈：setrefs 的调用点、返回计划的接收点、注入调用点
```

据此依次排除：注入过早；检查了旧 topPlan；setrefs 后新建/覆盖 targetlist；GV setrefs 故意保留表达式而源仓检查未适配。EXPLAIN 反解析为距离文本不等于真实 expr 是 OpExpr。

### 4.3 Result / Join 的真实消费者与扫描注入

1. 若 Result 的距离仍为 OpExpr，核对分支标量接口、外层 `rewrite_outer_order_projection()`、下层完整表达式匹配与 setrefs。没有 qual 不代表 Result 不计算距离。
2. 检查普通 IndexScan 是否在目标 `ann_orderby_node_allows_approx()` 正确适配；仅改注入类型而保留源仓 AnnIndexScan 白名单会在扫描本身拒绝。
3. 在目标 KGN 扫描入口记录祖先传入的 approximatePlanSafe、当前节点检查值、最终 quality、每个 TLE 与 orderExpr 的 node_equal 结果；这能区分消费者限制、AM 识别和结构不匹配。
4. 外层 Limit/Result 拒绝后，所有后代继承拒绝是有意的安全行为。ForeignScan 分支单独拒绝则不能污染其兄弟 KGN 分支；比较递归返回处是否恢复父上下文，以及 walker 的字段/列表遍历方式。
5. 原始 embedding 还被其他输出、Join 条件、排序键、参数消费时，当前源下推可以保守拒绝；不能强行覆盖这个列。别把正确拒绝直接当成漏迁。
6. 若只在 indexorderby 有距离、Scan.targetlist 完全没有匹配项，当前注入机制不会成功。单表「成功」结论也必须检查真实 AnnOrderByRef，不能漏看 resjunk 或只看有序 AM 路径。
7. 若真实 AnnOrderByRef 已存在但显示单括号，再核对 EXPLAIN 对普通 IndexScan 的显示适配；若计划没有引用，改显示不能解决计算复用。

对相等判定要比对实际 helper：源仓 `_equalOpExpr()` 转调 `_euqalCommonExprPart()`（原始拼写如此）。一边 opfuncid 为 0、另一边已填充时仍允许相等；location 比较是空操作。因此不能仅凭 opfuncid 填充时机或 SQL 文本位置不同就断定 node_equal 必然失败。

### 4.4 必须给出的比对结果

目标 AI 应提交一张表，至少列出：源函数 → GV 对应函数 → 是否执行/完整迁移 → 第一处行为差异 → 实际节点/字段证据 → 拟修复与验证方式。

对当前故障至少给出：首次拒绝节点与真实 TLE；Scan 注入前后节点类型；AM/操作符/quality 判定；同一查询开关前后结果列数、重复 id 列、完整候选范围内结果；已有近似消费者负向用例是否保持拒绝。Source 的 L2 精确结果断言不能照搬到近似 KGN。

不要删除 `leaf_local_expr_is_valid()`，它只检查映射后的表达式；分支 WHERE 的 NOT EXISTS 并不自动进入这个距离表达式。不要无条件允许所有 Result/Join/ForeignScan。不要把 source pruned 标志强置 true。不要通过先注入再做 setrefs 来规避匹配问题。

## 5. 函数与片段索引

| 组 | 函数或片段 | 原始路径与行号 | 范围 |
|---|---|---|---|
| A | [preptopk.cpp 全文（全部 Query 改写函数与局部结构）](#source-001) | `src/gausskernel/optimizer/prep/preptopk.cpp:1-2025` | 完整源文件 |
| B | [planner()](#source-002) | `src/gausskernel/optimizer/plan/planner.cpp:413-459` | 完整函数 |
| B | [standard_planner()](#source-003) | `src/gausskernel/optimizer/plan/planner.cpp:1139-1667` | 完整函数 |
| B | [subquery_planner()](#source-004) | `src/gausskernel/optimizer/plan/planner.cpp:2142-2924` | 完整函数 |
| C | [三个 walker 上下文结构](#source-005) | `src/gausskernel/optimizer/plan/planner.cpp:378-391` | 明确标注的源码片段 |
| C | [距离操作符与 Var 匹配上下文](#source-006) | `src/gausskernel/optimizer/plan/planner.cpp:564-572` | 明确标注的源码片段 |
| C | [union_all_expr_uses_var_walker()](#source-007) | `src/gausskernel/optimizer/plan/planner.cpp:573-583` | 完整函数 |
| C | [union_all_expr_uses_var()](#source-008) | `src/gausskernel/optimizer/plan/planner.cpp:585-590` | 完整函数 |
| C | [union_all_safe_vector_distance()](#source-009) | `src/gausskernel/optimizer/plan/planner.cpp:599-643` | 完整函数 |
| C | [union_all_find_source_tle()](#source-010) | `src/gausskernel/optimizer/plan/planner.cpp:645-660` | 完整函数 |
| C | [union_all_collect_source_chain()](#source-011) | `src/gausskernel/optimizer/plan/planner.cpp:663-690` | 完整函数 |
| C | [union_all_join_uses_source_elsewhere()](#source-012) | `src/gausskernel/optimizer/plan/planner.cpp:692-727` | 完整函数 |
| C | [union_all_replace_source_chain()](#source-013) | `src/gausskernel/optimizer/plan/planner.cpp:729-778` | 完整函数 |
| C | [push_down_union_all_source_projection_walker()](#source-014) | `src/gausskernel/optimizer/plan/planner.cpp:780-817` | 完整函数 |
| C | [push_down_union_all_source_projections()](#source-015) | `src/gausskernel/optimizer/plan/planner.cpp:819-832` | 完整函数 |
| C | [ann_orderby_safety_walker()](#source-016) | `src/gausskernel/optimizer/plan/planner.cpp:834-856` | 完整函数 |
| C | [ann_orderby_plan_is_unsafe()](#source-017) | `src/gausskernel/optimizer/plan/planner.cpp:858-872` | 完整函数 |
| C | [ann_orderby_join_expr_consumes_distance()](#source-018) | `src/gausskernel/optimizer/plan/planner.cpp:883-910` | 完整函数 |
| C | [ann_orderby_join_allows_approx()](#source-019) | `src/gausskernel/optimizer/plan/planner.cpp:912-957` | 完整函数 |
| C | [ann_orderby_node_allows_approx()](#source-020) | `src/gausskernel/optimizer/plan/planner.cpp:959-1025` | 完整函数 |
| C | [rewrite_ann_orderby_distance_walker()](#source-021) | `src/gausskernel/optimizer/plan/planner.cpp:1033-1110` | 完整函数 |
| C | [rewrite_ann_orderby_distance_refs()](#source-022) | `src/gausskernel/optimizer/plan/planner.cpp:1112-1137` | 完整函数 |
| D | [set_plan_references()](#source-023) | `src/gausskernel/optimizer/plan/setrefs.cpp:209-300` | 完整函数 |
| D | [set_plan_refs()](#source-024) | `src/gausskernel/optimizer/plan/setrefs.cpp:337-1177` | 完整函数 |
| D | [set_subqueryscan_references()](#source-025) | `src/gausskernel/optimizer/plan/setrefs.cpp:1267-1353` | 完整函数 |
| D | [trivial_subqueryscan()](#source-026) | `src/gausskernel/optimizer/plan/setrefs.cpp:1430-1481` | 完整函数 |
| D | [fix_expr_common()](#source-027) | `src/gausskernel/optimizer/plan/setrefs.cpp:1510-1569` | 完整函数 |
| D | [fix_scan_expr()](#source-028) | `src/gausskernel/optimizer/plan/setrefs.cpp:1579-1600` | 完整函数 |
| D | [fix_scan_expr_mutator()](#source-029) | `src/gausskernel/optimizer/plan/setrefs.cpp:1619-1659` | 完整函数 |
| D | [fix_scan_expr_walker()](#source-030) | `src/gausskernel/optimizer/plan/setrefs.cpp:1661-1668` | 完整函数 |
| D | [set_join_references()](#source-031) | `src/gausskernel/optimizer/plan/setrefs.cpp:1678-1765` | 完整函数 |
| D | [has_same_groupby()](#source-032) | `src/gausskernel/optimizer/plan/setrefs.cpp:1768-1785` | 完整函数 |
| D | [set_upper_references()](#source-033) | `src/gausskernel/optimizer/plan/setrefs.cpp:1806-1852` | 完整函数 |
| D | [set_dummy_tlist_references()](#source-034) | `src/gausskernel/optimizer/plan/setrefs.cpp:1867-1895` | 完整函数 |
| D | [build_tlist_index()](#source-035) | `src/gausskernel/optimizer/plan/setrefs.cpp:1910-1947` | 完整函数 |
| D | [build_tlist_index_other_vars()](#source-036) | `src/gausskernel/optimizer/plan/setrefs.cpp:1957-1992` | 完整函数 |
| D | [search_indexed_tlist_for_var()](#source-037) | `src/gausskernel/optimizer/plan/setrefs.cpp:2002-2025` | 完整函数 |
| D | [search_indexed_tlist_for_non_var()](#source-038) | `src/gausskernel/optimizer/plan/setrefs.cpp:2038-2106` | 完整函数 |
| D | [search_indexed_tlist_for_sortgroupref()](#source-039) | `src/gausskernel/optimizer/plan/setrefs.cpp:2118-2137` | 完整函数 |
| D | [fix_join_expr()](#source-040) | `src/gausskernel/optimizer/plan/setrefs.cpp:2169-2180` | 完整函数 |
| D | [fix_join_expr_mutator()](#source-041) | `src/gausskernel/optimizer/plan/setrefs.cpp:2182-2249` | 完整函数 |
| D | [fix_upper_expr()](#source-042) | `src/gausskernel/optimizer/plan/setrefs.cpp:2279-2288` | 完整函数 |
| D | [fix_upper_expr_mutator()](#source-043) | `src/gausskernel/optimizer/plan/setrefs.cpp:2290-2327` | 完整函数 |
| D | [set_foreignscan_references()](#source-044) | `src/gausskernel/optimizer/plan/setrefs.cpp:2861-2907` | 完整函数 |
| D | [indexed_tlist 与引用改写上下文结构定义](#source-045) | `src/gausskernel/optimizer/plan/setrefs.cpp:47-82` | 明确标注的源码片段 |
| E | [create_plan()](#source-046) | `src/gausskernel/optimizer/plan/createplan.cpp:528-602` | 完整函数 |
| E | [create_plan_recurse()](#source-047) | `src/gausskernel/optimizer/plan/createplan.cpp:608-732` | 完整函数 |
| E | [create_scan_plan()](#source-048) | `src/gausskernel/optimizer/plan/createplan.cpp:1232-1425` | 完整函数 |
| E | [build_relation_tlist()](#source-049) | `src/gausskernel/optimizer/plan/createplan.cpp:1462-1490` | 完整函数 |
| E | [build_path_tlist()](#source-050) | `src/gausskernel/optimizer/plan/createplan.cpp:1499-1528` | 完整函数 |
| E | [use_physical_tlist()](#source-051) | `src/gausskernel/optimizer/plan/createplan.cpp:1570-1629` | 完整函数 |
| E | [disuse_physical_tlist()](#source-052) | `src/gausskernel/optimizer/plan/createplan.cpp:1668-1750` | 完整函数 |
| E | [create_append_plan()](#source-053) | `src/gausskernel/optimizer/plan/createplan.cpp:1925-1992` | 完整函数 |
| E | [create_merge_append_plan()](#source-054) | `src/gausskernel/optimizer/plan/createplan.cpp:2001-2127` | 完整函数 |
| E | [create_result_plan()](#source-055) | `src/gausskernel/optimizer/plan/createplan.cpp:2187-2224` | 完整函数 |
| E | [create_projection_plan()](#source-056) | `src/gausskernel/optimizer/plan/createplan.cpp:2233-2282` | 完整函数 |
| E | [create_indexscan_plan()](#source-057) | `src/gausskernel/optimizer/plan/createplan.cpp:3275-3528` | 完整函数 |
| E | [make_annindexscan()](#source-058) | `src/gausskernel/optimizer/plan/createplan.cpp:7093-7115` | 完整函数 |
| E | [make_limit()](#source-059) | `src/gausskernel/optimizer/plan/createplan.cpp:9693-9738` | 完整函数 |
| E | [make_sort()](#source-060) | `src/gausskernel/optimizer/plan/createplan.cpp:8405-8448` | 完整函数 |
| E | [make_result()](#source-061) | `src/gausskernel/optimizer/plan/createplan.cpp:10224-10278` | 完整函数 |
| E | [is_projection_capable_plan()](#source-062) | `src/gausskernel/optimizer/plan/createplan.cpp:10884-10949` | 完整函数 |
| E | [is_projection_capable_path()](#source-063) | `src/gausskernel/optimizer/plan/createplan.cpp:12327-12365` | 完整函数 |
| E | [build_base_rel_tlists()](#source-064) | `src/gausskernel/optimizer/plan/initsplan.cpp:122-130` | 完整函数 |
| E | [add_vars_to_targetlist()](#source-065) | `src/gausskernel/optimizer/plan/initsplan.cpp:147-183` | 完整函数 |
| E | [recurse_set_operations()](#source-066) | `src/gausskernel/optimizer/prep/prepunion.cpp:210-394` | 完整函数 |
| E | [generate_union_plan()](#source-067) | `src/gausskernel/optimizer/prep/prepunion.cpp:629-701` | 完整函数 |
| E | [generate_setop_tlist()](#source-068) | `src/gausskernel/optimizer/prep/prepunion.cpp:1140-1234` | 完整函数 |
| E | [generate_append_tlist()](#source-069) | `src/gausskernel/optimizer/prep/prepunion.cpp:1249-1338` | 完整函数 |
| F | [walk_plan_node_fields()](#source-070) | `src/gausskernel/optimizer/util/planmem_walker.cpp:97-128` | 完整函数 |
| F | [walk_scan_node_fields()](#source-071) | `src/gausskernel/optimizer/util/planmem_walker.cpp:145-153` | 完整函数 |
| F | [walk_join_node_fields()](#source-072) | `src/gausskernel/optimizer/util/planmem_walker.cpp:169-178` | 完整函数 |
| F | [plan_tree_walker()](#source-073) | `src/gausskernel/optimizer/util/planmem_walker.cpp:190-663` | 完整函数 |
| F | [plan_tree_base_subplan_get_plan()](#source-074) | `src/gausskernel/optimizer/util/planmem_walker.cpp:672-691` | 完整函数 |
| G | [PlannerInfo 的列裁剪标志](#source-075) | `src/include/nodes/relation.h:524-534` | 明确标注的源码片段 |
| G | [AnnOrderByRef 定义](#source-076) | `src/include/nodes/primnodes.h:982-999` | 明确标注的源码片段 |
| G | [AM 距离质量与发布通道](#source-077) | `src/include/access/relscan.h:30-59` | 明确标注的源码片段 |
| G | [距离复用枚举 GUC 取值](#source-078) | `src/common/backend/utils/misc/guc/guc_sql.cpp:369-376` | 明确标注的源码片段 |
| G | [两个 UNION ALL 布尔 GUC 注册](#source-079) | `src/common/backend/utils/misc/guc/guc_sql.cpp:853-874` | 明确标注的源码片段 |
| G | [距离复用 GUC 注册](#source-080) | `src/common/backend/utils/misc/guc/guc_sql.cpp:3855-3868` | 明确标注的源码片段 |
| G | [距离复用工作版本门槛](#source-081) | `src/include/miscadmin.h:876-880` | 明确标注的源码片段 |
| G | [距离缓存、绑定状态与表达式运行时上下文](#source-082) | `src/include/nodes/execnodes.h:2188-2218` | 明确标注的源码片段 |
| G | [ANN_ORDERBY_REUSE_OFF 定义上下文](#source-083) | `src/include/utils/guc.h:458-466` | 明确标注的源码片段 |
| G | [preprocess_union_all_required_columns 定义上下文](#source-084) | `src/include/optimizer/prep.h:117-125` | 明确标注的源码片段 |
| G | [ann_orderby_distance_reuse 定义上下文](#source-085) | `src/include/knl/knl_guc/knl_session_attr_sql.h:293-301` | 明确标注的源码片段 |
| G | [enable_union_all_required_columns_pushdown 定义上下文](#source-086) | `src/include/knl/knl_guc/knl_session_attr_sql.h:68-76` | 明确标注的源码片段 |
| H | [show_plan_tlist()](#source-087) | `src/gausskernel/optimizer/commands/explain.cpp:3690-3804` | 完整函数 |
| H | [get_rule_expr()](#source-088) | `src/common/backend/utils/adt/ruleutils.cpp:10069-11056` | 完整函数 |
| H | [AnnIndexFindOrderByRef()](#source-089) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:54-65` | 完整函数 |
| H | [AnnIndexReuseModeAllows()](#source-090) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:67-76` | 完整函数 |
| H | [AnnIndexRuntimeDistanceQuality()](#source-091) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:81-105` | 完整函数 |
| H | [AnnIndexInvalidateOrderByValue()](#source-092) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:107-114` | 完整函数 |
| H | [AnnIndexBindOrderByValue()](#source-093) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:119-172` | 完整函数 |
| H | [AnnIndexNext()](#source-094) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:181-271` | 完整函数 |
| H | [AnnIndexSortPut()](#source-095) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:276-309` | 完整函数 |
| H | [AnnIndexOrderedNext()](#source-096) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:311-407` | 完整函数 |
| H | [ExecAnnIndexScan()](#source-097) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:433-479` | 完整函数 |
| H | [ExecReScanAnnIndexScan()](#source-098) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:492-564` | 完整函数 |
| H | [ExecEndAnnIndexScan()](#source-099) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:570-652` | 完整函数 |
| H | [ExecInitAnnIndexScan()](#source-100) | `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:802-1023` | 完整函数 |
| H | [ExecEvalAnnOrderByRef()](#source-101) | `src/gausskernel/runtime/executor/execQual.cpp:4308-4350` | 完整函数 |
| H | [ExecInitExprByRecursion()](#source-102) | `src/gausskernel/runtime/executor/execQual.cpp:6042-6051` | 完整函数 |
| H | [ExecInitExprByRecursionInternal()](#source-103) | `src/gausskernel/runtime/executor/execQual.cpp:6053-6859` | 完整函数 |
| H | [ExecInitExprRec()](#source-104) | `src/gausskernel/runtime/executor/execExpr.cpp:532-2192` | 完整函数 |
| H | [ExecInterpExpr()](#source-105) | `src/gausskernel/runtime/executor/execExprInterp.cpp:565-2314` | 完整函数 |
| I | [_equalVar()](#source-106) | `src/common/backend/nodes/equalfuncs.cpp:187-200` | 完整函数 |
| I | [_equalConst()](#source-107) | `src/common/backend/nodes/equalfuncs.cpp:202-220` | 完整函数 |
| I | [_euqalCommonExprPart()](#source-108) | `src/common/backend/nodes/equalfuncs.cpp:356-379` | 完整函数 |
| I | [_equalOpExpr()](#source-109) | `src/common/backend/nodes/equalfuncs.cpp:380-383` | 完整函数 |
| I | [_equalAnnOrderByRef()](#source-110) | `src/common/backend/nodes/equalfuncs.cpp:592-599` | 完整函数 |
| I | [字段比较宏定义（包含 location 的空操作语义）](#source-111) | `src/common/backend/nodes/equalfuncs.cpp:40-102` | 明确标注的源码片段 |
| I | [_copyAnnOrderByRef()](#source-112) | `src/common/backend/nodes/copyfuncs.cpp:3360-3369` | 完整函数 |
| I | [_outAnnOrderByRef()](#source-113) | `src/common/backend/nodes/outfuncs.cpp:3215-3222` | 完整函数 |
| I | [_readAnnOrderByRef()](#source-114) | `src/common/backend/nodes/readfuncs.cpp:3028-3037` | 完整函数 |

### A 组完整文件内的函数定位

| 函数 | 原始定义行 |
|---|---|
| `preprocess_union_all_required_columns()` | 143 |
| `required_columns_outer_query_is_safe()` | 238 |
| `count_visible_tlist_entries()` | 274 |
| `add_required_columns_from_expr()` | 288 |
| `collect_required_output_columns()` | 321 |
| `preserve_semantically_required_outputs()` | 350 |
| `validate_required_column_shape()` | 379 |
| `prepare_outer_where_pushdown()` | 426 |
| `compact_name_list()` | 458 |
| `compact_leaf_targetlist()` | 478 |
| `compact_union_targetlist()` | 527 |
| `compact_setop_metadata()` | 581 |
| `required_column_var_mutator()` | 622 |
| `remap_outer_query_vars()` | 660 |
| `compact_union_all_query()` | 685 |
| `preprocess_union_all_order_key_projection()` | 740 |
| `find_union_all_order_key()` | 801 |
| `prepare_union_all_order_keys()` | 821 |
| `validate_order_projection_shape()` | 886 |
| `collect_order_projection_raw_columns()` | 921 |
| `leaf_expr_is_order_key()` | 954 |
| `rewrite_leaf_order_projection()` | 968 |
| `append_setop_order_key_metadata()` | 1049 |
| `rewrite_outer_order_projection()` | 1071 |
| `rewrite_union_order_projection()` | 1110 |
| `preprocess_union_all_diskann_topk()` | 1211 |
| `diskann_topk_outer_query_is_safe()` | 1292 |
| `is_diskann_vector_order_expr()` | 1434 |
| `get_single_union_subquery_rte()` | 1506 |
| `collect_pure_union_all_leaves()` | 1531 |
| `collect_setop_leaves()` | 1541 |
| `leaf_query_is_safe()` | 1580 |
| `map_sort_expr_to_leaf()` | 1611 |
| `build_visible_output_map_tlist()` | 1668 |
| `validate_union_leaf_output_types()` | 1693 |
| `leaf_local_expr_is_valid()` | 1729 |
| `make_prepared_leaf()` | 1771 |
| `prepare_leaf_diskann_topk()` | 1787 |
| `commit_prepared_leaves()` | 1863 |
| `check_outer_qual_pushdown_safe()` | 1883 |
| `map_outer_qual_to_leaf()` | 1948 |
| `append_outer_qual_to_leaf()` | 2007 |

## 6. 原文源码归档

以下每段标注来源、原始行范围和内容 SHA256。完整函数不插入中文行内注释、不改名、不改 OID，方便直接做差异比较。函数中的依赖可在本文件中按名搜索；基础 List/节点库等通用设施仍属于目标内核自身，不在这里复制整个数据库。

### A. 查询改写、必要列与排序键物化

<a id="source-001"></a>

#### preptopk.cpp 全文（全部 Query 改写函数与局部结构）

来源：`src/gausskernel/optimizer/prep/preptopk.cpp:1-2025`；范围：**完整源文件**。

内容 SHA256：`deea19a3c0155100912dbbc9db7b68a85a5dd9d1ff637f85f8d1b711382c33fd`。

包含本地尚未提交的无 WHERE 放行和日志修正。保留原文；入口旧注释仍写 mandatory outer WHERE，实际判定已放开。

```cpp
/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * preptopk.cpp
 *    UNION ALL + DiskANN Vector Top-K pushdown optimization
 *
 * This module implements logical query tree rewriting to push ORDER BY + LIMIT
 * down to each branch of a UNION ALL when the ordering involves DiskANN vector
 * distance operators. This reduces the amount of data flowing into the global
 * sort from ΣMi to at most N×K.
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/prep/preptopk.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_guc.h"

#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amop.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "rewrite/rewriteManip.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "access/datavec/diskann.h"

/* Temporary structure for prepared leaf queries during two-phase commit */
typedef struct DiskAnnTopKPreparedLeaf {
    RangeTblEntry *ownerRte;    /* Original RTE owning the leaf subquery */
    Query *rewrittenQuery;       /* Rewritten copy with ORDER BY + LIMIT */
} DiskAnnTopKPreparedLeaf;

typedef struct RequiredColumnVarMapContext {
    Index unionRti;
    int oldNcols;
    AttrNumber *oldToNew;
    bool failed;
} RequiredColumnVarMapContext;

typedef struct UnionAllOrderKey {
    Index sortRef;
    List *leafExprs;
    Oid type;
    int32 typmod;
    Oid collation;
    AttrNumber newAttno;
} UnionAllOrderKey;

/* Forward declarations */
static bool diskann_topk_outer_query_is_safe(Query *query);
static bool is_diskann_vector_order_expr(Expr *expr, Oid *opfamilyOut);
static RangeTblEntry *get_single_union_subquery_rte(Query *outer, Index *unionRtiOut);
static bool collect_pure_union_all_leaves(Query *query, List **leaves);
static bool collect_setop_leaves(Node *node, List *rtable, List **leaves);
static bool leaf_query_is_safe(Query *leaf);
static Expr *map_sort_expr_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                    Query *leaf, Expr *outerExpr);
static List *build_visible_output_map_tlist(Query *leaf);
static bool validate_union_leaf_output_types(Query *unionQuery, List *leafMapTlist, List *vars);
static bool leaf_local_expr_is_valid(Query *leaf, Expr *mappedExpr);
static DiskAnnTopKPreparedLeaf *make_prepared_leaf(RangeTblEntry *ownerRte, Query *rewrittenQuery);
static bool prepare_leaf_diskann_topk(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                       Query *unionQuery, Query *leafCopy, Node *outerQual);
static void commit_prepared_leaves(List *prepared);
static bool check_outer_qual_pushdown_safe(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                             List *leaves);
static Expr *map_outer_qual_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                      Query *leaf, Expr *outerQual);
static bool append_outer_qual_to_leaf(Query *leafCopy, Expr *mappedQual);
static bool required_columns_outer_query_is_safe(Query *query);
static int count_visible_tlist_entries(List *targetList);
static bool add_required_columns_from_expr(Node *expr, Index unionRti, int oldNcols,
                                           Bitmapset **required);
static bool collect_required_output_columns(Query *outer, Index unionRti, int oldNcols,
                                             Bitmapset **required);
static void preserve_semantically_required_outputs(List *leaves, int oldNcols, Bitmapset **required);
static bool validate_required_column_shape(Query *unionQuery, List *leaves, int oldNcols);
static bool prepare_outer_where_pushdown(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                         List *leaves);
static List *compact_name_list(List *names, AttrNumber *oldToNew, int oldNcols, bool *ok);
static List *compact_leaf_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok);
static List *compact_union_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok);
static bool compact_setop_metadata(Node *node, AttrNumber *oldToNew, int oldNcols);
static Node *required_column_var_mutator(Node *node, void *context);
static bool remap_outer_query_vars(Query *outer, Index unionRti, AttrNumber *oldToNew, int oldNcols);
static bool compact_union_all_query(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                    List *leaves, Bitmapset *required, int oldNcols);
static bool validate_order_projection_shape(Query *unionQuery, List *leaves, int oldNcols);
static UnionAllOrderKey *find_union_all_order_key(List *keys, Index sortRef);
static bool prepare_union_all_order_keys(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                         List *leaves, List **keys);
static bool collect_order_projection_raw_columns(Query *outer, Index unionRti, int oldNcols,
                                                 List *keys, Bitmapset **required);
static bool leaf_expr_is_order_key(Expr *expr, List *keys, int leafIndex);
static bool rewrite_leaf_order_projection(RangeTblEntry *leafRte, AttrNumber *oldToNew,
                                          int oldNcols, List *keys, int leafIndex);
static void append_setop_order_key_metadata(Node *node, List *keys);
static bool rewrite_outer_order_projection(Query *outer, Index unionRti,
                                           AttrNumber *oldToNew, int oldNcols, List *keys);
static bool rewrite_union_order_projection(Query *outer, Index unionRti,
                                           RangeTblEntry *unionRte, List *leaves,
                                           Bitmapset *rawRequired, List *keys, int oldNcols);

/*
 * preprocess_union_all_required_columns
 *     Push a safe mandatory outer WHERE into every pure UNION ALL leaf and
 *     physically compact the set-operation output to columns needed above it.
 *
 * The rewrite is performed on a private copy of the outer Query.  The copied
 * rtable/targetlist/jointree pointers are installed only after all positional
 * targetlist and type metadata checks succeed.
 * Return true only when this Query was rewritten; the inherited planner flag
 * must not authorize expression projection in an unchecked descendant Query.
 */
bool
preprocess_union_all_required_columns(PlannerInfo *root)
{
    Query *outer = root->parse;
    Query *work = NULL;
    RangeTblEntry *unionRte = NULL;
    Index unionRti = 0;
    List *leaves = NIL;
    Bitmapset *required = NULL;
    int oldNcols = 0;
    int newNcols = 0;
    bool wherePushed = false;

    if (!u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown)
        return false;

    if (!required_columns_outer_query_is_safe(outer)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=outer_query_shape");
        return false;
    }

    /*
     * Work on a complete copy so that a late validation failure cannot leave
     * only some UNION ALL leaves rewritten.
     */
    work = (Query *)copyObject(outer);
    unionRte = get_single_union_subquery_rte(work, &unionRti);
    if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=unsafe_union_rte");
        return false;
    }

    if (!collect_pure_union_all_leaves(unionRte->subquery, &leaves)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=non_union_all");
        return false;
    }

    oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
    if (oldNcols <= 0 || !validate_required_column_shape(unionRte->subquery, leaves, oldNcols)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=inconsistent_output_shape");
        return false;
    }

    /*
     * Mapping must happen before column numbers are compacted.  If every leaf
     * accepts the exact predicate, remove the redundant outer recheck so that
     * columns used only by WHERE stop at the leaf.
     */
    wherePushed = prepare_outer_where_pushdown(work, unionRti, unionRte, leaves);

    if (!collect_required_output_columns(work, unionRti, oldNcols, &required)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=unsupported_outer_reference");
        return false;
    }

    preserve_semantically_required_outputs(leaves, oldNcols, &required);

    /*
     * An empty set-operation tuple is poorly supported by several legacy plan
     * paths.  Keep the first column as a harmless carrier for SELECT constants.
     */
    if (bms_is_empty(required))
        required = bms_add_member(required, 1);

    newNcols = bms_num_members(required);
    if (!compact_union_all_query(work, unionRti, unionRte, leaves, required, oldNcols)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=compact_validation_failed");
        return false;
    }

    /*
     * No planner structures referring to these fields have been built yet.
     * Installing the copied fields keeps root->parse and the caller's Query
     * object identity stable.
     */
    outer->targetList = work->targetList;
    outer->jointree = work->jointree;
    outer->rtable = work->rtable;
    root->union_all_required_columns_pruned = true;

    elog(DEBUG1,
         "union_all_required_columns: old=%d new=%d where_pushed=%s outer_recheck=%s",
         oldNcols,
         newNcols,
         wherePushed ? "true" : "false",
         outer->jointree != NULL && outer->jointree->quals != NULL ? "true" : "false");

    return true;
}

/*
 * Keep the first implementation deliberately narrow: it covers the target
 * SELECT-over-one-UNION-view shape, while declining constructs whose row or
 * evaluation semantics need more than positional output pruning.
 */
static bool
required_columns_outer_query_is_safe(Query *query)
{
    FromExpr *fromExpr = NULL;

    if (query == NULL || query->commandType != CMD_SELECT)
        return false;

    /* An absent predicate needs no pushdown; keep the same projection checks. */
    if (query->jointree == NULL)
        return false;

    if (query->setOperations != NULL || query->hasAggs || query->groupClause != NIL ||
        query->havingQual != NULL || query->distinctClause != NIL ||
        query->hasWindowFuncs || query->windowClause != NIL || query->cteList != NIL ||
        query->hasForUpdate || query->hasTargetSRFs || query->hasSubLinks ||
        query->hasRecursive || query->hasModifyingCTE || query->hasRowSecurity ||
        query->limitOffset != NULL || query->limitIsPercent || query->limitWithTies)
        return false;

    /* The early rewrite cannot rely on hasTargetSRFs being populated yet. */
    if (expression_returns_set((Node *)query->targetList) ||
        expression_contains_rownum((Node *)query->targetList) || ContainRownumQual(query))
        return false;

    if (list_length(query->rtable) != 1 || !IsA(query->jointree, FromExpr))
        return false;

    fromExpr = (FromExpr *)query->jointree;
    if (list_length(fromExpr->fromlist) != 1 ||
        !IsA(linitial(fromExpr->fromlist), RangeTblRef))
        return false;

    return ((RangeTblRef *)linitial(fromExpr->fromlist))->rtindex == 1;
}

static int
count_visible_tlist_entries(List *targetList)
{
    ListCell *lc = NULL;
    int count = 0;

    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (!tle->resjunk)
            count++;
    }
    return count;
}

static bool
add_required_columns_from_expr(Node *expr, Index unionRti, int oldNcols,
                               Bitmapset **required)
{
    List *vars = NIL;
    ListCell *lc = NULL;

    if (expr == NULL)
        return true;

    vars = pull_vars_of_level(expr, 0);
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        Var *var = NULL;

        if (!IsA(node, Var)) {
            list_free_ext(vars);
            return false;
        }

        var = (Var *)node;
        if (var->varlevelsup != 0 || var->varno != unionRti ||
            var->varattno <= 0 || var->varattno > oldNcols) {
            list_free_ext(vars);
            return false;
        }
        *required = bms_add_member(*required, var->varattno);
    }

    list_free_ext(vars);
    return true;
}

static bool
collect_required_output_columns(Query *outer, Index unionRti, int oldNcols,
                                Bitmapset **required)
{
    ListCell *lc = NULL;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (!add_required_columns_from_expr((Node *)tle->expr, unionRti, oldNcols, required))
            return false;
    }

    if (outer->jointree != NULL &&
        !add_required_columns_from_expr((Node *)outer->jointree->quals,
                                        unionRti, oldNcols, required))
        return false;

    if (!add_required_columns_from_expr(outer->limitCount, unionRti, oldNcols, required) ||
        !add_required_columns_from_expr(outer->limitOffset, unionRti, oldNcols, required))
        return false;

    return true;
}

/*
 * Dropping an unused volatile/SRF output can alter observable evaluation or
 * row multiplicity.  Since UNION ALL columns are positional, preserve that
 * output position in every leaf when any leaf requires it.
 */
static void
preserve_semantically_required_outputs(List *leaves, int oldNcols, Bitmapset **required)
{
    ListCell *leafCell = NULL;

    foreach (leafCell, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(leafCell);
        Query *leaf = leafRte->subquery;
        ListCell *lc = NULL;
        int pos = 0;

        foreach (lc, leaf->targetList) {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            if (tle->resjunk)
                continue;

            pos++;
            if (pos > oldNcols)
                break;

            if (tle->ressortgroupref != 0 ||
                expression_returns_set((Node *)tle->expr) ||
                contain_volatile_functions((Node *)tle->expr) ||
                checkExprHasSubLink((Node *)tle->expr))
                *required = bms_add_member(*required, pos);
        }
    }
}

static bool
validate_required_column_shape(Query *unionQuery, List *leaves, int oldNcols)
{
    SetOperationStmt *topop = NULL;
    ListCell *lc = NULL;

    if (unionQuery == NULL || unionQuery->setOperations == NULL ||
        unionQuery->commandType != CMD_SELECT || unionQuery->sortClause != NIL ||
        unionQuery->limitCount != NULL || unionQuery->limitOffset != NULL ||
        unionQuery->hasAggs || unionQuery->groupClause != NIL ||
        unionQuery->havingQual != NULL || unionQuery->distinctClause != NIL ||
        unionQuery->hasWindowFuncs || unionQuery->windowClause != NIL ||
        unionQuery->cteList != NIL || unionQuery->hasForUpdate ||
        unionQuery->hasTargetSRFs || unionQuery->hasRecursive ||
        unionQuery->hasModifyingCTE || unionQuery->hasRowSecurity)
        return false;

    foreach (lc, unionQuery->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk)
            return false;
    }

    topop = (SetOperationStmt *)unionQuery->setOperations;
    if (!IsA(topop, SetOperationStmt) ||
        list_length(topop->colTypes) != oldNcols ||
        list_length(topop->colTypmods) != oldNcols ||
        list_length(topop->colCollations) != oldNcols)
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Query *leaf = leafRte->subquery;

        if (leafRte->security_barrier || leafRte->lateral ||
            !leaf_query_is_safe(leaf) || leaf->hasTargetSRFs ||
            count_visible_tlist_entries(leaf->targetList) != oldNcols)
            return false;

        if (leafRte->eref == NULL ||
            list_length(leafRte->eref->colnames) != oldNcols)
            return false;
    }

    return true;
}

static bool
prepare_outer_where_pushdown(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                             List *leaves)
{
    Node *outerQual = outer->jointree != NULL ? outer->jointree->quals : NULL;
    List *mappedQuals = NIL;
    ListCell *lc = NULL;
    ListCell *mq = NULL;

    if (outerQual == NULL ||
        !check_outer_qual_pushdown_safe(outer, unionRti, unionRte, leaves))
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Expr *mapped = map_outer_qual_to_leaf(outer, unionRti, unionRte,
                                             leafRte->subquery, (Expr *)outerQual);
        if (mapped == NULL)
            return false;
        mappedQuals = lappend(mappedQuals, mapped);
    }

    forboth(lc, leaves, mq, mappedQuals) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        if (!append_outer_qual_to_leaf(leafRte->subquery, (Expr *)lfirst(mq)))
            return false;
    }

    outer->jointree->quals = NULL;
    return true;
}

static List *
compact_name_list(List *names, AttrNumber *oldToNew, int oldNcols, bool *ok)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int pos = 0;

    if (names == NIL || list_length(names) != oldNcols) {
        *ok = false;
        return NIL;
    }

    foreach (lc, names) {
        pos++;
        if (oldToNew[pos] > 0)
            result = lappend(result, copyObject(lfirst(lc)));
    }
    return result;
}

static List *
compact_leaf_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int oldPos = 0;
    int nextResno = 1;

    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (tle->resjunk)
            continue;

        oldPos++;
        if (oldPos > oldNcols) {
            *ok = false;
            return NIL;
        }

        if (oldToNew[oldPos] == 0)
            continue;

        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        result = lappend(result, copy);
    }

    if (oldPos != oldNcols) {
        *ok = false;
        return NIL;
    }

    /* Resjunk items are internal to the leaf and remain after visible output. */
    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (!tle->resjunk)
            continue;
        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        result = lappend(result, copy);
    }

    return result;
}

static List *
compact_union_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int oldPos = 0;

    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;
        Var *var = NULL;

        if (tle->resjunk) {
            *ok = false;
            return NIL;
        }

        oldPos++;
        if (oldPos > oldNcols) {
            *ok = false;
            return NIL;
        }
        if (oldToNew[oldPos] == 0)
            continue;

        /*
         * A set-operation Query targetlist references the leftmost leaf
         * output.  Keep this restriction explicit so unexpected parse shapes
         * safely fall back instead of silently remapping the wrong Var.
         */
        if (!IsA(tle->expr, Var)) {
            *ok = false;
            return NIL;
        }
        var = (Var *)tle->expr;
        if (var->varlevelsup != 0 || var->varattno != oldPos) {
            *ok = false;
            return NIL;
        }

        copy = (TargetEntry *)copyObject(tle);
        copy->resno = oldToNew[oldPos];
        var = (Var *)copy->expr;
        var->varattno = oldToNew[oldPos];
        if (var->varoattno == oldPos)
            var->varoattno = oldToNew[oldPos];
        result = lappend(result, copy);
    }

    if (oldPos != oldNcols)
        *ok = false;
    return result;
}

static bool
compact_setop_metadata(Node *node, AttrNumber *oldToNew, int oldNcols)
{
    SetOperationStmt *op = NULL;
    List *types = NIL;
    List *typmods = NIL;
    List *collations = NIL;
    int pos = 0;

    if (node == NULL)
        return false;
    if (IsA(node, RangeTblRef))
        return true;
    if (!IsA(node, SetOperationStmt))
        return false;

    op = (SetOperationStmt *)node;
    if (op->op != SETOP_UNION || !op->all || op->groupClauses != NIL ||
        list_length(op->colTypes) != oldNcols ||
        list_length(op->colTypmods) != oldNcols ||
        list_length(op->colCollations) != oldNcols)
        return false;

    for (pos = 1; pos <= oldNcols; pos++) {
        if (oldToNew[pos] == 0)
            continue;
        types = lappend_oid(types, list_nth_oid(op->colTypes, pos - 1));
        typmods = lappend_int(typmods, list_nth_int(op->colTypmods, pos - 1));
        collations = lappend_oid(collations, list_nth_oid(op->colCollations, pos - 1));
    }

    if (!compact_setop_metadata(op->larg, oldToNew, oldNcols) ||
        !compact_setop_metadata(op->rarg, oldToNew, oldNcols))
        return false;

    op->colTypes = types;
    op->colTypmods = typmods;
    op->colCollations = collations;
    return true;
}

static Node *
required_column_var_mutator(Node *node, void *context)
{
    RequiredColumnVarMapContext *ctx = (RequiredColumnVarMapContext *)context;

    if (node == NULL)
        return NULL;

    if (IsA(node, Var)) {
        Var *oldVar = (Var *)node;
        Var *newVar = NULL;
        AttrNumber newAttno = 0;

        if (oldVar->varlevelsup != 0 || oldVar->varno != ctx->unionRti)
            return (Node *)copyObject(oldVar);

        if (oldVar->varattno <= 0 || oldVar->varattno > ctx->oldNcols) {
            ctx->failed = true;
            return (Node *)copyObject(oldVar);
        }

        newAttno = ctx->oldToNew[oldVar->varattno];
        if (newAttno <= 0) {
            ctx->failed = true;
            return (Node *)copyObject(oldVar);
        }

        newVar = (Var *)copyObject(oldVar);
        newVar->varattno = newAttno;
        if (newVar->varnoold == ctx->unionRti &&
            newVar->varoattno == oldVar->varattno)
            newVar->varoattno = newAttno;
        return (Node *)newVar;
    }

    return expression_tree_mutator(node, required_column_var_mutator, context);
}

static bool
remap_outer_query_vars(Query *outer, Index unionRti, AttrNumber *oldToNew, int oldNcols)
{
    RequiredColumnVarMapContext ctx;
    ListCell *lc = NULL;

    ctx.unionRti = unionRti;
    ctx.oldNcols = oldNcols;
    ctx.oldToNew = oldToNew;
    ctx.failed = false;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        tle->expr = (Expr *)required_column_var_mutator((Node *)tle->expr, &ctx);
    }

    if (outer->jointree != NULL)
        outer->jointree->quals = required_column_var_mutator(
            (Node *)outer->jointree->quals, &ctx);
    outer->limitCount = required_column_var_mutator(outer->limitCount, &ctx);
    outer->limitOffset = required_column_var_mutator(outer->limitOffset, &ctx);

    return !ctx.failed;
}

static bool
compact_union_all_query(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                        List *leaves, Bitmapset *required, int oldNcols)
{
    Query *unionQuery = unionRte->subquery;
    AttrNumber *oldToNew = NULL;
    ListCell *lc = NULL;
    int pos = 0;
    int nextAttno = 1;
    bool ok = true;

    oldToNew = (AttrNumber *)palloc0((oldNcols + 1) * sizeof(AttrNumber));
    for (pos = 1; pos <= oldNcols; pos++) {
        if (bms_is_member(pos, required))
            oldToNew[pos] = (AttrNumber)nextAttno++;
    }

    if (!remap_outer_query_vars(outer, unionRti, oldToNew, oldNcols))
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        leafRte->subquery->targetList = compact_leaf_targetlist(
            leafRte->subquery->targetList, oldToNew, oldNcols, &ok);
        if (!ok)
            return false;

        leafRte->eref->colnames = compact_name_list(
            leafRte->eref->colnames, oldToNew, oldNcols, &ok);
        if (!ok)
            return false;
    }

    unionQuery->targetList = compact_union_targetlist(
        unionQuery->targetList, oldToNew, oldNcols, &ok);
    if (!ok || !compact_setop_metadata(unionQuery->setOperations, oldToNew, oldNcols))
        return false;

    if (unionRte->eref == NULL)
        return false;
    unionRte->eref->colnames = compact_name_list(
        unionRte->eref->colnames, oldToNew, oldNcols, &ok);

    return ok;
}

/*
 * preprocess_union_all_order_key_projection
 *     Materialize non-Var ORDER BY expressions in each UNION ALL leaf.
 *
 * This runs after optional Top-K rewriting, because Top-K eligibility must be
 * checked while the outer expression is still the original DiskANN operator.
 * The outer query subsequently sorts a compact scalar key rather than carrying
 * the key's wide inputs (notably vector) through Append.
 */
void
preprocess_union_all_order_key_projection(PlannerInfo *root)
{
    Query *outer = root->parse;
    Query *work = NULL;
    RangeTblEntry *unionRte = NULL;
    Index unionRti = 0;
    List *leaves = NIL;
    List *keys = NIL;
    Bitmapset *rawRequired = NULL;
    int oldNcols = 0;

    if (!u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown ||
        !root->union_all_required_columns_pruned || outer->sortClause == NIL)
        return;

    /*
     * A residual filter must run before evaluation of the outer sort key.
     * Even an immutable expression can throw (e.g. 1 / x for a filtered-out
     * zero).  Keep its original evaluation boundary unless WHERE was pushed
     * successfully into every leaf.
     */
    if (outer->jointree != NULL && outer->jointree->quals != NULL)
        return;

    work = (Query *)copyObject(outer);
    unionRte = get_single_union_subquery_rte(work, &unionRti);
    if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral ||
        !collect_pure_union_all_leaves(unionRte->subquery, &leaves))
        return;

    oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
    if (oldNcols <= 0 ||
        !validate_order_projection_shape(unionRte->subquery, leaves, oldNcols))
        return;

    if (!prepare_union_all_order_keys(work, unionRti, unionRte, leaves, &keys) ||
        keys == NIL)
        return;

    if (!collect_order_projection_raw_columns(work, unionRti, oldNcols,
                                              keys, &rawRequired))
        return;
    preserve_semantically_required_outputs(leaves, oldNcols, &rawRequired);

    if (!rewrite_union_order_projection(work, unionRti, unionRte, leaves,
                                        rawRequired, keys, oldNcols)) {
        elog(DEBUG1, "union_all_required_columns: order projection skipped reason=rewrite_failed");
        return;
    }

    outer->targetList = work->targetList;
    outer->jointree = work->jointree;
    outer->rtable = work->rtable;

    elog(DEBUG1,
         "union_all_required_columns: order_keys=%d raw_columns=%d materialized=true",
         list_length(keys),
         bms_num_members(rawRequired));
}

static UnionAllOrderKey *
find_union_all_order_key(List *keys, Index sortRef)
{
    ListCell *lc = NULL;

    if (sortRef == 0)
        return NULL;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        if (key->sortRef == sortRef)
            return key;
    }
    return NULL;
}

/*
 * Direct Vars are already compact and are useful as tie-break keys, so only
 * derived expressions need a synthetic scalar output.
 */
static bool
prepare_union_all_order_keys(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                             List *leaves, List **keys)
{
    ListCell *scCell = NULL;

    foreach (scCell, outer->sortClause) {
        SortGroupClause *sc = (SortGroupClause *)lfirst(scCell);
        TargetEntry *outerTle = get_sortgroupclause_tle(sc, outer->targetList);
        UnionAllOrderKey *key = NULL;
        List *vars = NIL;
        ListCell *lc = NULL;

        if (outerTle == NULL || outerTle->ressortgroupref == 0)
            return false;

        if (IsA(outerTle->expr, Var))
            continue;

        vars = pull_vars_of_level((Node *)outerTle->expr, 0);
        if (vars == NIL)
            continue; /* A constant ordering key needs no branch output. */

        foreach (lc, vars) {
            Node *node = (Node *)lfirst(lc);
            Var *var = NULL;

            if (!IsA(node, Var)) {
                list_free_ext(vars);
                return false;
            }
            var = (Var *)node;
            if (var->varlevelsup != 0 || var->varno != unionRti ||
                var->varattno <= 0) {
                list_free_ext(vars);
                return false;
            }
        }
        list_free_ext(vars);

        key = (UnionAllOrderKey *)palloc0(sizeof(UnionAllOrderKey));
        key->sortRef = outerTle->ressortgroupref;
        key->type = exprType((Node *)outerTle->expr);
        key->typmod = exprTypmod((Node *)outerTle->expr);
        key->collation = exprCollation((Node *)outerTle->expr);

        foreach (lc, leaves) {
            RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
            Expr *mapped = map_sort_expr_to_leaf(outer, unionRti, unionRte,
                                                 leafRte->subquery,
                                                 (Expr *)outerTle->expr);
            if (mapped == NULL ||
                exprType((Node *)mapped) != key->type ||
                exprTypmod((Node *)mapped) != key->typmod ||
                exprCollation((Node *)mapped) != key->collation)
                return false;
            key->leafExprs = lappend(key->leafExprs, mapped);
        }

        *keys = lappend(*keys, key);
    }

    return true;
}

static bool
validate_order_projection_shape(Query *unionQuery, List *leaves, int oldNcols)
{
    SetOperationStmt *topop = NULL;
    ListCell *lc = NULL;

    if (unionQuery == NULL || unionQuery->setOperations == NULL ||
        count_visible_tlist_entries(unionQuery->targetList) != oldNcols)
        return false;

    foreach (lc, unionQuery->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk)
            return false;
    }

    topop = (SetOperationStmt *)unionQuery->setOperations;
    if (!IsA(topop, SetOperationStmt) ||
        list_length(topop->colTypes) != oldNcols ||
        list_length(topop->colTypmods) != oldNcols ||
        list_length(topop->colCollations) != oldNcols)
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        if (leafRte->security_barrier || leafRte->lateral ||
            leafRte->eref == NULL ||
            list_length(leafRte->eref->colnames) != oldNcols ||
            count_visible_tlist_entries(leafRte->subquery->targetList) != oldNcols)
            return false;
    }

    return true;
}

static bool
collect_order_projection_raw_columns(Query *outer, Index unionRti, int oldNcols,
                                     List *keys, Bitmapset **required)
{
    ListCell *lc = NULL;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);

        /*
         * The expression is replaced by its synthetic leaf output even when
         * it is a user-visible SELECT item, so its raw Vars are not needed.
         */
        if (find_union_all_order_key(keys, tle->ressortgroupref) != NULL)
            continue;

        if (!add_required_columns_from_expr((Node *)tle->expr,
                                            unionRti, oldNcols, required))
            return false;
    }

    if (outer->jointree != NULL &&
        !add_required_columns_from_expr((Node *)outer->jointree->quals,
                                        unionRti, oldNcols, required))
        return false;

    if (!add_required_columns_from_expr(outer->limitCount, unionRti, oldNcols, required) ||
        !add_required_columns_from_expr(outer->limitOffset, unionRti, oldNcols, required))
        return false;

    return true;
}

static bool
leaf_expr_is_order_key(Expr *expr, List *keys, int leafIndex)
{
    ListCell *lc = NULL;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        Expr *keyExpr = (Expr *)list_nth(key->leafExprs, leafIndex);
        if (equal(expr, keyExpr))
            return true;
    }
    return false;
}

static bool
rewrite_leaf_order_projection(RangeTblEntry *leafRte, AttrNumber *oldToNew,
                              int oldNcols, List *keys, int leafIndex)
{
    Query *leaf = leafRte->subquery;
    List *oldTarget = leaf->targetList;
    List *newTarget = NIL;
    List *newNames = NIL;
    ListCell *lc = NULL;
    int oldPos = 0;
    int nextResno = 1;
    int keyNumber = 0;
    bool ok = true;

    newNames = compact_name_list(leafRte->eref->colnames,
                                 oldToNew, oldNcols, &ok);
    if (!ok)
        return false;

    foreach (lc, oldTarget) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (tle->resjunk)
            continue;
        oldPos++;
        if (oldPos > oldNcols)
            return false;
        if (oldToNew[oldPos] == 0)
            continue;

        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        newTarget = lappend(newTarget, copy);
    }
    if (oldPos != oldNcols)
        return false;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        Expr *mapped = (Expr *)list_nth(key->leafExprs, leafIndex);
        TargetEntry *oldTle = tlist_member((Node *)mapped, oldTarget);
        TargetEntry *newTle = NULL;
        char name[NAMEDATALEN];
        errno_t rc;

        keyNumber++;
        rc = snprintf_s(name, NAMEDATALEN, NAMEDATALEN - 1,
                        "__ua_order_%d", keyNumber);
        securec_check_ss(rc, "\0", "\0");

        newTle = makeTargetEntry((Expr *)copyObject(mapped),
                                 (AttrNumber)nextResno++,
                                 pstrdup(name),
                                 false);
        if (oldTle != NULL)
            newTle->ressortgroupref = oldTle->ressortgroupref;
        newTarget = lappend(newTarget, newTle);
        newNames = lappend(newNames, makeString(pstrdup(name)));
    }

    /*
     * Keep unrelated leaf-internal junk entries, but remove the old resjunk
     * copy of a key that has just become a visible UNION output.
     */
    foreach (lc, oldTarget) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (!tle->resjunk || leaf_expr_is_order_key(tle->expr, keys, leafIndex))
            continue;
        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        newTarget = lappend(newTarget, copy);
    }

    leaf->targetList = newTarget;
    leafRte->eref->colnames = newNames;
    return true;
}

static void
append_setop_order_key_metadata(Node *node, List *keys)
{
    SetOperationStmt *op = NULL;
    ListCell *lc = NULL;

    if (node == NULL || IsA(node, RangeTblRef))
        return;

    Assert(IsA(node, SetOperationStmt));
    op = (SetOperationStmt *)node;
    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        op->colTypes = lappend_oid(op->colTypes, key->type);
        op->colTypmods = lappend_int(op->colTypmods, key->typmod);
        op->colCollations = lappend_oid(op->colCollations, key->collation);
    }

    append_setop_order_key_metadata(op->larg, keys);
    append_setop_order_key_metadata(op->rarg, keys);
}

static bool
rewrite_outer_order_projection(Query *outer, Index unionRti,
                               AttrNumber *oldToNew, int oldNcols, List *keys)
{
    RequiredColumnVarMapContext ctx;
    ListCell *lc = NULL;

    ctx.unionRti = unionRti;
    ctx.oldNcols = oldNcols;
    ctx.oldToNew = oldToNew;
    ctx.failed = false;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        UnionAllOrderKey *key =
            find_union_all_order_key(keys, tle->ressortgroupref);

        if (key != NULL) {
            tle->expr = (Expr *)makeVar(unionRti,
                                       key->newAttno,
                                       key->type,
                                       key->typmod,
                                       key->collation,
                                       0);
        } else {
            tle->expr = (Expr *)required_column_var_mutator(
                (Node *)tle->expr, &ctx);
        }
    }

    if (outer->jointree != NULL)
        outer->jointree->quals = required_column_var_mutator(
            (Node *)outer->jointree->quals, &ctx);
    outer->limitCount = required_column_var_mutator(outer->limitCount, &ctx);
    outer->limitOffset = required_column_var_mutator(outer->limitOffset, &ctx);

    return !ctx.failed;
}

static bool
rewrite_union_order_projection(Query *outer, Index unionRti,
                               RangeTblEntry *unionRte, List *leaves,
                               Bitmapset *rawRequired, List *keys, int oldNcols)
{
    Query *unionQuery = unionRte->subquery;
    AttrNumber *oldToNew = NULL;
    List *unionTarget = NIL;
    List *unionNames = NIL;
    ListCell *lc = NULL;
    Var *leftmostVar = NULL;
    Index leftmostRti = 0;
    int pos = 0;
    int nextAttno = 1;
    int leafIndex = 0;
    int keyNumber = 0;
    bool ok = true;

    if (unionQuery->targetList == NIL ||
        !IsA(((TargetEntry *)linitial(unionQuery->targetList))->expr, Var))
        return false;
    leftmostVar = (Var *)((TargetEntry *)linitial(unionQuery->targetList))->expr;
    leftmostRti = leftmostVar->varno;

    oldToNew = (AttrNumber *)palloc0((oldNcols + 1) * sizeof(AttrNumber));
    for (pos = 1; pos <= oldNcols; pos++) {
        if (bms_is_member(pos, rawRequired))
            oldToNew[pos] = (AttrNumber)nextAttno++;
    }

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        key->newAttno = (AttrNumber)nextAttno++;
    }

    if (!rewrite_outer_order_projection(outer, unionRti,
                                        oldToNew, oldNcols, keys))
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        if (!rewrite_leaf_order_projection(leafRte, oldToNew,
                                           oldNcols, keys, leafIndex++))
            return false;
    }

    unionTarget = compact_union_targetlist(unionQuery->targetList,
                                           oldToNew, oldNcols, &ok);
    if (!ok)
        return false;

    unionNames = compact_name_list(unionRte->eref->colnames,
                                   oldToNew, oldNcols, &ok);
    if (!ok)
        return false;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        TargetEntry *tle = NULL;
        Var *var = NULL;
        char name[NAMEDATALEN];
        errno_t rc;

        keyNumber++;
        rc = snprintf_s(name, NAMEDATALEN, NAMEDATALEN - 1,
                        "__ua_order_%d", keyNumber);
        securec_check_ss(rc, "\0", "\0");

        var = makeVar(leftmostRti,
                      key->newAttno,
                      key->type,
                      key->typmod,
                      key->collation,
                      0);
        tle = makeTargetEntry((Expr *)var,
                              key->newAttno,
                              pstrdup(name),
                              false);
        unionTarget = lappend(unionTarget, tle);
        unionNames = lappend(unionNames, makeString(pstrdup(name)));
    }

    unionQuery->targetList = unionTarget;
    unionRte->eref->colnames = unionNames;

    if (!compact_setop_metadata(unionQuery->setOperations,
                                oldToNew, oldNcols))
        return false;
    append_setop_order_key_metadata(unionQuery->setOperations, keys);

    return true;
}


/*
 * preprocess_union_all_diskann_topk
 *     Main entry point for UNION ALL + DiskANN Top-K pushdown.
 *
 * Called from subquery_planner() after reduce_orderby/preprocess_rownum
 * and before pull_up_subqueries.
 */
void
preprocess_union_all_diskann_topk(PlannerInfo *root)
{
    Query *outer = root->parse;
    List *leaves = NIL;
    List *prepared = NIL;
    ListCell *lc = NULL;
    RangeTblEntry *unionRte = NULL;
    Index unionRti = 0;
    Node *outerQual = NULL;

    /* Check GUC switch */
    if (!u_sess->attr.attr_sql.enable_union_all_diskann_topk_pushdown) {
        elog(DEBUG1, "preprocess_union_all_diskann_topk: GUC disabled");
        return;
    }

    elog(DEBUG1, "preprocess_union_all_diskann_topk: starting...");

    /* Validate outer query structure */
    if (!diskann_topk_outer_query_is_safe(outer)) {
        elog(DEBUG1, "preprocess_union_all_diskann_topk: outer query unsafe");
        return;
    }

    /* Get the single UNION ALL subquery RTE */
    unionRte = get_single_union_subquery_rte(outer, &unionRti);
    if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral)
        return;

    /* Collect all leaves from pure UNION ALL tree */
    if (!collect_pure_union_all_leaves(unionRte->subquery, &leaves))
        return;

    /*
     * The UNION container is a semantic boundary too.  An existing LIMIT or
     * OFFSET there selects rows before the outer predicate/order; pushing a
     * new local Top-K beneath it would change which rows can be selected.
     * Reuse the common container and leaf checks, including barrier RTEs.
     */
    int oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
    if (oldNcols <= 0 || !validate_required_column_shape(unionRte->subquery, leaves, oldNcols))
        return;

    /* Extract outer WHERE clause */
    outerQual = (outer->jointree != NULL) ? outer->jointree->quals : NULL;

    /* Check if outer WHERE can be safely pushed down to all leaves */
    if (!check_outer_qual_pushdown_safe(outer, unionRti, unionRte, leaves))
        return;

    /* Phase 1: Copy all leaf queries without modifying originals */
    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Query *copy = (Query *)copyObject(leafRte->subquery);

        prepared = lappend(prepared, make_prepared_leaf(leafRte, copy));
    }

    /* Phase 2: Prepare each copy; abort if any fails */
    foreach (lc, prepared) {
        DiskAnnTopKPreparedLeaf *item = (DiskAnnTopKPreparedLeaf *)lfirst(lc);

        if (!prepare_leaf_diskann_topk(outer, unionRti, unionRte,
                                        unionRte->subquery, item->rewrittenQuery, outerQual)) {
            /* Failure: discard all and keep original query */
            return;
        }
    }

    /* Phase 3: Commit all rewritten queries atomically */
    commit_prepared_leaves(prepared);

    elog(DEBUG1, "preprocess_union_all_diskann_topk: successfully pushed ORDER BY + LIMIT to %d leaves",
         list_length(prepared));
}

/*
 * diskann_topk_outer_query_is_safe
 *     Check if the outer query meets all safety conditions for pushdown.
 */
static bool
diskann_topk_outer_query_is_safe(Query *query)
{
    ListCell *lc = NULL;
    bool hasDiskAnnOrderKey = false;

    /* Must be a simple SELECT */
    if (query->commandType != CMD_SELECT)
        return false;

    /* Must not have aggregation, grouping, DISTINCT, window, etc. */
    if (query->hasAggs || query->groupClause || query->havingQual ||
        query->distinctClause || query->hasWindowFuncs || query->windowClause || query->cteList ||
        query->hasForUpdate || query->hasSubLinks || query->setOperations ||
        query->hasTargetSRFs || query->hasRecursive || query->hasModifyingCTE ||
        query->hasRowSecurity || query->limitIsPercent || query->limitWithTies)
        return false;

    /* A zero-row SRF invalidates input-row Top-K even if the flag is unset. */
    if (expression_returns_set((Node *)query->targetList) ||
        expression_contains_rownum((Node *)query->targetList) || ContainRownumQual(query))
        return false;

    /* Must have exactly one FROM item (the UNION ALL subquery) */
    if (list_length(query->rtable) != 1 || query->jointree == NULL ||
        !IsA(query->jointree, FromExpr))
        return false;

    FromExpr *fromExpr = (FromExpr *)query->jointree;
    if (list_length(fromExpr->fromlist) != 1)
        return false;

    /* Must have non-empty ORDER BY */
    if (query->sortClause == NIL)
        return false;

    /* Each leaf receives the exact sort operators and NULL ordering below. */

    /* Must have valid LIMIT (not NULL, not OFFSET, not PERCENT, not WITH TIES) */
    if (query->limitCount == NULL || query->limitOffset != NULL)
        return false;

    /*
     * Validate LIMIT expression:
     * - Accept Const (e.g., LIMIT 2::bigint)
     * - Accept Param (e.g., LIMIT $1)
     * - Accept FuncExpr wrapping a Const (e.g., int8(2::int4) for plain LIMIT 2)
     * - Reject complex expressions (SubLink, volatile functions, etc.)
     *
     * For FuncExpr, verify it's a simple type coercion (int8, int4, int2) with
     * a constant argument. This handles the common case where parser wraps
     * integer literals with implicit casts.
     */
    Node *limitNode = query->limitCount;

    if (IsA(limitNode, Const)) {
        Const *c = (Const *)limitNode;
        if (c->constisnull)
            return false;
        /* Should be int8 >= 1 */
        if (c->consttype != INT8OID)
            return false;
        int64 k = DatumGetInt64(c->constvalue);
        if (k <= 0)
            return false;
    } else if (IsA(limitNode, Param)) {
        Param *p = (Param *)limitNode;
        if (p->paramkind != PARAM_EXTERN)
            return false;
        /* Runtime parameter: accept and let execution validate */
    } else if (IsA(limitNode, FuncExpr)) {
        FuncExpr *fe = (FuncExpr *)limitNode;

        /* Must be a simple type coercion function (int8, int4, int2, etc.) */
        /* Check if it's a cast-like function with one argument */
        if (list_length(fe->args) != 1)
            return false;

        Node *arg = (Node *)linitial(fe->args);

        /* Argument must be a Const */
        if (!IsA(arg, Const))
            return false;

        Const *c = (Const *)arg;
        if (c->constisnull)
            return false;

        /* Verify the constant value is a positive integer */
        int64 k = 0;
        switch (c->consttype) {
            case INT2OID:
                k = DatumGetInt16(c->constvalue);
                break;
            case INT4OID:
                k = DatumGetInt32(c->constvalue);
                break;
            case INT8OID:
                k = DatumGetInt64(c->constvalue);
                break;
            default:
                /* Non-integer type: reject */
                return false;
        }

        if (k <= 0)
            return false;

        /* Additional safety: reject if function contains volatile operations */
        if (contain_volatile_functions(limitNode))
            return false;
    } else {
        /* Complex expression (SubLink, Aggref, etc.): reject */
        return false;
    }

    /* Check for at least one DiskANN vector ordering key */
    foreach (lc, query->sortClause) {
        SortGroupClause *sc = (SortGroupClause *)lfirst(lc);
        TargetEntry *tle = get_sortgroupclause_tle(sc, query->targetList);
        if (tle == NULL)
            return false;

        if (is_diskann_vector_order_expr((Expr *)tle->expr, NULL)) {
            hasDiskAnnOrderKey = true;
            break;
        }
    }

    if (!hasDiskAnnOrderKey)
        return false;

    return true;
}

/*
 * is_diskann_vector_order_expr
 *     Check if an expression is a DiskANN vector distance operator.
 *
 * Returns true if expr is an OpExpr with signature vector × vector → float8
 * and the operator belongs to a DiskANN opfamily.
 */
static bool
is_diskann_vector_order_expr(Expr *expr, Oid *opfamilyOut)
{
    /* Strip RelabelType */
    while (expr && IsA(expr, RelabelType))
        expr = ((RelabelType *)expr)->arg;

    if (!IsA(expr, OpExpr))
        return false;

    OpExpr *op = (OpExpr *)expr;

    /* Check signature: vector, vector -> float8 */
    if (list_length(op->args) != 2)
        return false;

    Oid leftType = exprType((Node *)linitial(op->args));
    Oid rightType = exprType((Node *)lsecond(op->args));
    Oid resType = op->opresulttype;

    /* VECTOROID is defined in vector.h; approximate check */
    /* We need to verify these are vector types and result is float8 */
    if (resType != FLOAT8OID)
        return false;

    /* Look up operator in syscache */
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
    if (!HeapTupleIsValid(opertup))
        return false;

    Form_pg_operator operform = (Form_pg_operator)GETSTRUCT(opertup);

    /* Verify input types match */
    if (operform->oprleft != leftType || operform->oprright != rightType) {
        ReleaseSysCache(opertup);
        return false;
    }

    ReleaseSysCache(opertup);

    /* Check if this operator belongs to a DiskANN opfamily */
    /* We need to query pg_amop for amopfamily where amopmethod = DISKANN_AM_OID */
    CatCList *catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(op->opno));

    bool found = false;
    for (int i = 0; i < catlist->n_members; i++) {
        HeapTuple amoptup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(amoptup);

        /* Check if this opfamily belongs to DiskANN AM */
        HeapTuple opftup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(aform->amopfamily));
        if (HeapTupleIsValid(opftup)) {
            Form_pg_opfamily opfform = (Form_pg_opfamily)GETSTRUCT(opftup);
            if (opfform->opfmethod == DISKANN_AM_OID && aform->amoppurpose == AMOP_ORDER) {
                found = true;
                if (opfamilyOut != NULL)
                    *opfamilyOut = aform->amopfamily;
                ReleaseSysCache(opftup);
                break;
            }
            ReleaseSysCache(opftup);
        }
    }

    ReleaseSysCacheList(catlist);
    return found;
}

/*
 * get_single_union_subquery_rte
 *     Extract the single RTE_SUBQUERY from outer query's FROM clause.
 */
static RangeTblEntry *
get_single_union_subquery_rte(Query *outer, Index *unionRtiOut)
{
    if (list_length(outer->rtable) != 1)
        return NULL;

    RangeTblEntry *rte = (RangeTblEntry *)linitial(outer->rtable);
    if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
        return NULL;

    if (rte->subquery->setOperations == NULL)
        return NULL;

    if (unionRtiOut != NULL)
        *unionRtiOut = 1; /* The only RTE */

    return rte;
}

/*
 * collect_pure_union_all_leaves
 *     Recursively collect all leaf RTEs from a pure UNION ALL tree.
 *
 * Returns false if the tree is not pure UNION ALL or if leaves are not subqueries.
 */
static bool
collect_pure_union_all_leaves(Query *query, List **leaves)
{
    if (query->setOperations == NULL)
        return false;

    return collect_setop_leaves(query->setOperations, query->rtable, leaves);
}

/* Helper to recursively traverse SetOperationStmt */
static bool
collect_setop_leaves(Node *node, List *rtable, List **leaves)
{
    if (node == NULL)
        return false;

    if (IsA(node, SetOperationStmt)) {
        SetOperationStmt *setop = (SetOperationStmt *)node;

        /* Must be UNION ALL */
        if (setop->op != SETOP_UNION || !setop->all)
            return false;

        /* Recursively process both sides */
        if (!collect_setop_leaves(setop->larg, rtable, leaves))
            return false;
        if (!collect_setop_leaves(setop->rarg, rtable, leaves))
            return false;

        return true;
    } else if (IsA(node, RangeTblRef)) {
        RangeTblRef *rtr = (RangeTblRef *)node;
        RangeTblEntry *rte = rt_fetch(rtr->rtindex, rtable);

        /* Leaf must be a subquery */
        if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
            return false;

        *leaves = lappend(*leaves, rte);
        return true;
    }

    return false;
}

/*
 * leaf_query_is_safe
 *     Check if a leaf query can safely accept ORDER BY + LIMIT pushdown.
 */
static bool
leaf_query_is_safe(Query *leaf)
{
    /* WHERE/order movement changes the numbering of a ROWNUM-producing leaf. */
    if (leaf == NULL || leaf->commandType != CMD_SELECT ||
        expression_returns_set((Node *)leaf->targetList) ||
        expression_contains_rownum((Node *)leaf->targetList) || ContainRownumQual(leaf))
        return false;

    /* Must not already have ORDER BY, LIMIT, or setOperations */
    if (leaf->sortClause != NIL || leaf->limitCount != NULL ||
        leaf->limitOffset != NULL || leaf->setOperations != NULL)
        return false;

    /* Must not have aggregation, grouping, DISTINCT, window, CTE, row locks, SRF */
    if (leaf->hasAggs || leaf->groupClause || leaf->havingQual ||
        leaf->distinctClause || leaf->hasWindowFuncs || leaf->cteList ||
        leaf->hasForUpdate || leaf->hasTargetSRFs || leaf->hasRecursive ||
        leaf->hasModifyingCTE || leaf->hasRowSecurity)
        return false;

    return true;
}

/*
 * map_sort_expr_to_leaf
 *     Map an outer sort expression to the corresponding leaf expression.
 *
 * This uses ReplaceVarsFromTargetList to replace UNION output Vars with
 * the actual leaf expressions.
 */
static Expr *
map_sort_expr_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                      Query *leaf, Expr *outerExpr)
{
    /* First, reject SubLinks and outer-level Vars */
    if (checkExprHasSubLink((Node *)outerExpr))
        return NULL;

    if (contain_vars_of_level_or_above((Node *)outerExpr, 1))
        return NULL;

    /* Collect all level-0 Vars */
    List *vars = pull_vars_of_level((Node *)outerExpr, 0);
    ListCell *lc = NULL;

    /* All Vars must reference the UNION RTE and be normal column refs */
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            return NULL;

        Var *v = (Var *)node;
        Assert(v->varlevelsup == 0);

        if (v->varno != unionRti || v->varattno <= 0)
            return NULL;
    }

    /* Build a temporary mapping targetlist for this leaf */
    List *leafMapTlist = build_visible_output_map_tlist(leaf);

    /* Validate types match between UNION and leaf */
    if (!validate_union_leaf_output_types(unionRte->subquery, leafMapTlist, vars))
        return NULL;

    /* Perform the replacement */
    Expr *mapped = (Expr *)ReplaceVarsFromTargetList((Node *)outerExpr,
                                                      unionRti,
                                                      0,
                                                      unionRte,
                                                      leafMapTlist,
                                                      REPLACEVARS_REPORT_ERROR,
                                                      0,
                                                      NULL);

    /* Validate the mapped expression */
    if (!leaf_local_expr_is_valid(leaf, mapped))
        return NULL;

    return mapped;
}

/*
 * build_visible_output_map_tlist
 *     Build a normalized targetlist containing only non-resjunk entries
 *     with resno = 1..N for ReplaceVarsFromTargetList.
 */
static List *
build_visible_output_map_tlist(Query *leaf)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int resno = 1;

    foreach (lc, leaf->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk)
            continue;

        TargetEntry *newtle = makeTargetEntry(tle->expr, resno, NULL, false);
        result = lappend(result, newtle);
        resno++;
    }

    return result;
}

/*
 * validate_union_leaf_output_types
 *     Verify that for each Var referencing a UNION output column,
 *     the corresponding leaf expression has matching type/typmod/collation.
 */
static bool
validate_union_leaf_output_types(Query *unionQuery, List *leafMapTlist, List *vars)
{
    ListCell *lc = NULL;

    foreach (lc, vars) {
        Var *v = (Var *)lfirst(lc);
        int attno = v->varattno; /* 1-based */

        /* Find the UNION output column */
        TargetEntry *unionTle = get_tle_by_resno(unionQuery->targetList, attno);
        if (unionTle == NULL)
            return false;

        /* Find the corresponding leaf column */
        TargetEntry *leafTle = get_tle_by_resno(leafMapTlist, attno);
        if (leafTle == NULL)
            return false;

        /* Check type compatibility */
        if (exprType((Node *)unionTle->expr) != exprType((Node *)leafTle->expr))
            return false;
        if (exprTypmod((Node *)unionTle->expr) != exprTypmod((Node *)leafTle->expr))
            return false;
        if (exprCollation((Node *)unionTle->expr) != exprCollation((Node *)leafTle->expr))
            return false;
    }

    return true;
}

/*
 * leaf_local_expr_is_valid
 *     Validate that the mapped expression only contains local Vars resolvable
 *     in the leaf's rtable, and no volatile functions or sublinks.
 */
static bool
leaf_local_expr_is_valid(Query *leaf, Expr *mappedExpr)
{
    /* Check for volatile functions */
    if (contain_volatile_functions((Node *)mappedExpr))
        return false;

    /* Check for SubLinks */
    if (checkExprHasSubLink((Node *)mappedExpr))
        return false;

    /* Collect all Vars and verify they are local */
    List *vars = pull_var_clause((Node *)mappedExpr, PVC_RECURSE_AGGREGATES,
                                  PVC_INCLUDE_PLACEHOLDERS);
    ListCell *lc = NULL;

    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            continue;

        Var *v = (Var *)node;
        if (v->varlevelsup != 0)
            return false;

        /* Verify varno is within leaf's rtable */
        if (v->varno < 1 || v->varno > list_length(leaf->rtable))
            return false;

        /* Verify it can be fetched */
        RangeTblEntry *rte = rt_fetch(v->varno, leaf->rtable);
        if (rte == NULL)
            return false;
    }

    return true;
}

/*
 * make_prepared_leaf
 *     Create a prepared leaf structure.
 */
static DiskAnnTopKPreparedLeaf *
make_prepared_leaf(RangeTblEntry *ownerRte, Query *rewrittenQuery)
{
    DiskAnnTopKPreparedLeaf *prep = (DiskAnnTopKPreparedLeaf *)palloc(sizeof(DiskAnnTopKPreparedLeaf));
    prep->ownerRte = ownerRte;
    prep->rewrittenQuery = rewrittenQuery;
    return prep;
}

/*
 * prepare_leaf_diskann_topk
 *     Prepare a single leaf query by adding outer WHERE + ORDER BY + LIMIT.
 *
 * This operates on the rewritten copy, not the original.
 * Execution order: map outer WHERE -> merge with leaf quals -> map ORDER BY -> set LIMIT
 */
static bool
prepare_leaf_diskann_topk(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                          Query *unionQuery, Query *leafCopy, Node *outerQual)
{
    ListCell *lc = NULL;

    /* Validate leaf safety */
    if (!leaf_query_is_safe(leafCopy))
        return false;

    /* Step 1: Map and append outer WHERE to leaf if present */
    if (outerQual != NULL) {
        Expr *mappedQual = map_outer_qual_to_leaf(outer, unionRti, unionRte,
                                                    leafCopy, (Expr *)outerQual);
        if (mappedQual == NULL)
            return false;

        if (!append_outer_qual_to_leaf(leafCopy, mappedQual))
            return false;
    }

    /* Step 2: Map each sort clause */
    List *mappedSortClause = NIL;

    foreach (lc, outer->sortClause) {
        SortGroupClause *outerSc = (SortGroupClause *)lfirst(lc);
        TargetEntry *outerTle = get_sortgroupclause_tle(outerSc, outer->targetList);
        if (outerTle == NULL)
            return false;

        /* Map the sort expression to leaf */
        Expr *mappedExpr = map_sort_expr_to_leaf(outer, unionRti, unionRte,
                                                  leafCopy, (Expr *)outerTle->expr);
        if (mappedExpr == NULL)
            return false;

        /* Find or add this expression in leaf's targetList */
        TargetEntry *leafTle = tlist_member((Node *)mappedExpr, leafCopy->targetList);
        if (leafTle == NULL) {
            /* Add as resjunk */
            leafTle = makeTargetEntry(mappedExpr,
                                      list_length(leafCopy->targetList) + 1,
                                      NULL,
                                      true);
            leafCopy->targetList = lappend(leafCopy->targetList, leafTle);
        }

        /* Assign sortgroupref if needed */
        if (leafTle->ressortgroupref == 0)
            leafTle->ressortgroupref = assignSortGroupRef(leafTle, leafCopy->targetList);

        /* Copy the SortGroupClause, updating tleSortGroupRef */
        SortGroupClause *leafSc = (SortGroupClause *)copyObject(outerSc);
        leafSc->tleSortGroupRef = leafTle->ressortgroupref;

        mappedSortClause = lappend(mappedSortClause, leafSc);
    }

    /* Step 3: Assign the ORDER BY and LIMIT */
    leafCopy->sortClause = mappedSortClause;
    leafCopy->limitCount = (Node *)copyObject(outer->limitCount);
    leafCopy->limitOffset = NULL;
    leafCopy->limitIsPercent = false;
    leafCopy->limitWithTies = false;

    elog(DEBUG1, "prepare_leaf_diskann_topk: set leaf LIMIT, sortClause length=%d, limitCount=%s",
         list_length(leafCopy->sortClause),
         leafCopy->limitCount ? "SET" : "NULL");

    return true;
}

/*
 * commit_prepared_leaves
 *     Atomically replace all original leaf subqueries with rewritten versions.
 */
static void
commit_prepared_leaves(List *prepared)
{
    ListCell *lc = NULL;

    foreach (lc, prepared) {
        DiskAnnTopKPreparedLeaf *item = (DiskAnnTopKPreparedLeaf *)lfirst(lc);
        item->ownerRte->subquery = item->rewrittenQuery;
    }
}

/*
 * check_outer_qual_pushdown_safe
 *     Check if the outer WHERE clause can be safely pushed down to all UNION ALL leaves.
 *
 * Returns false if:
 * - Outer WHERE contains SubLink, volatile functions, SRF, aggregates, or window functions
 * - Any Var in outer WHERE does not reference the UNION RTE's normal output columns
 * - Type/typmod/collation mismatch between UNION output and any leaf's corresponding column
 */
static bool
check_outer_qual_pushdown_safe(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                 List *leaves)
{
    Node *outerQual = (outer->jointree != NULL) ? outer->jointree->quals : NULL;
    ListCell *lc = NULL;

    /* No WHERE clause is a valid case - nothing to push down */
    if (outerQual == NULL)
        return true;

    /* Reject SubLinks - correlated subqueries need special handling */
    if (checkExprHasSubLink(outerQual))
        return false;

    /* Reject volatile functions - they may have side effects or change results */
    if (contain_volatile_functions(outerQual))
        return false;

    /* Reject SRF, aggregates, and window functions */
    if (expression_returns_set(outerQual) ||
        contain_agg_clause(outerQual) ||
        contain_window_function(outerQual))
        return false;

    /* Collect all level-0 Vars from outer WHERE */
    List *vars = pull_vars_of_level((Node *)outerQual, 0);

    /* Verify all Vars reference the UNION RTE and are normal column refs */
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            return false;

        Var *v = (Var *)node;
        Assert(v->varlevelsup == 0);

        /* Must reference unionRti and be a normal output column (varattno > 0) */
        if (v->varno != unionRti || v->varattno <= 0)
            return false;
    }

    /* Verify type compatibility for each leaf */
    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Query *leaf = leafRte->subquery;

        /* Build temporary mapping targetlist for this leaf */
        List *leafMapTlist = build_visible_output_map_tlist(leaf);

        /* Validate types match between UNION and leaf */
        if (!validate_union_leaf_output_types(unionRte->subquery, leafMapTlist, vars))
            return false;
    }

    return true;
}

/*
 * map_outer_qual_to_leaf
 *     Map the outer WHERE expression to the corresponding leaf expression.
 *
 * This replaces UNION output Vars with the actual leaf expressions using
 * ReplaceVarsFromTargetList.
 */
static Expr *
map_outer_qual_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                        Query *leaf, Expr *outerQual)
{
    /* Reject SubLinks and outer-level Vars */
    if (checkExprHasSubLink((Node *)outerQual))
        return NULL;

    if (contain_vars_of_level_or_above((Node *)outerQual, 1))
        return NULL;

    /* Collect all level-0 Vars */
    List *vars = pull_vars_of_level((Node *)outerQual, 0);
    ListCell *lc = NULL;

    /* All Vars must reference the UNION RTE and be normal column refs */
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            return NULL;

        Var *v = (Var *)node;
        Assert(v->varlevelsup == 0);

        if (v->varno != unionRti || v->varattno <= 0)
            return NULL;
    }

    /* Build a temporary mapping targetlist for this leaf */
    List *leafMapTlist = build_visible_output_map_tlist(leaf);

    /* Validate types match between UNION and leaf */
    if (!validate_union_leaf_output_types(unionRte->subquery, leafMapTlist, vars))
        return NULL;

    /* Perform the replacement */
    Expr *mapped = (Expr *)ReplaceVarsFromTargetList((Node *)outerQual,
                                                      unionRti,
                                                      0,
                                                      unionRte,
                                                      leafMapTlist,
                                                      REPLACEVARS_REPORT_ERROR,
                                                      0,
                                                      NULL);

    /* Validate the mapped expression */
    if (!leaf_local_expr_is_valid(leaf, mapped))
        return NULL;

    return mapped;
}

/*
 * append_outer_qual_to_leaf
 *     Append the mapped outer WHERE to the leaf's existing quals using AND.
 *
 * This preserves the leaf's original WHERE conditions (e.g., NOT EXISTS for deletions)
 * and adds the outer filter condition.
 */
static bool
append_outer_qual_to_leaf(Query *leafCopy, Expr *mappedQual)
{
    if (mappedQual == NULL)
        return false;

    /* Merge with existing leaf quals using AND */
    if (leafCopy->jointree == NULL) {
        leafCopy->jointree = makeNode(FromExpr);
        leafCopy->jointree->fromlist = NIL;
        leafCopy->jointree->quals = NULL;
    }

    leafCopy->jointree->quals = (Node *)make_and_qual(
        (Node *)leafCopy->jointree->quals,
        (Node *)mappedQual);

    return true;
}
```

### B. 规划入口、调用顺序与标志继承

<a id="source-002"></a>

#### planner()

来源：`src/gausskernel/optimizer/plan/planner.cpp:413-459`；范围：**完整函数**。

内容 SHA256：`b64ba649270e363aba951081ca0092501365b7c7fe1c26487a0f651385fffbf3`。

保留完整函数以核对真实调用位置、返回的计划指针、主计划/子计划 setrefs 以及 pruned 的继承。

```cpp
PlannedStmt* planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt* result = NULL;
    instr_time starttime;
    double totaltime = 0;

#ifdef USE_SPQ
    if (spq_planner_hook) {
        return (*spq_planner_hook) (parse, cursorOptions, boundParams);
    }
#endif

    INSTR_TIME_SET_CURRENT(starttime);

#ifdef PGXC
    /*
     * streaming engine hook for agg rewrite.
     */
    if (t_thrd.streaming_cxt.streaming_planner_hook)
        (*(planner_hook_type) t_thrd.streaming_cxt.streaming_planner_hook)\
                 (parse, cursorOptions, boundParams);
    /*
     * A Coordinator receiving a query from another Coordinator
     * is not allowed to go into PGXC planner.
     */
    if (u_sess->hook_cxt.pluginPlannerHook){
        result = (*(planner_hook_type) u_sess->hook_cxt.pluginPlannerHook)\
                 (parse, cursorOptions, boundParams);
    } else if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !IsConnFromCoord()){
        result = pgxc_planner(parse, cursorOptions, boundParams);
    } else
#endif
        result = (planner_hook != NULL)?
		(*planner_hook)(parse, cursorOptions, boundParams) : standard_planner(parse, cursorOptions, boundParams);

    totaltime += elapsed_time(&starttime);
    result->plannertime = totaltime;
    if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        GetRemoteQuery(result, NULL);
    }

    if (ndp_pushdown_hook) {
        (*ndp_pushdown_hook)(parse, result);
    }

    return result;
}
```

<a id="source-003"></a>

#### standard_planner()

来源：`src/gausskernel/optimizer/plan/planner.cpp:1139-1667`；范围：**完整函数**。

内容 SHA256：`64f35ce7ed1f4ef598499a236e9977564af4037969e8b21a9de49ca249bd5d4f`。

保留完整函数以核对真实调用位置、返回的计划指针、主计划/子计划 setrefs 以及 pruned 的继承。

```cpp
PlannedStmt* standard_planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt* result = NULL;
    PlannerGlobal* glob = NULL;
    double tuple_fraction;
    PlannerInfo* root = NULL;
    Plan* top_plan = NULL;
    ListCell* lp = NULL;
    ListCell* lr = NULL;
    int max_mem = 0;
    int available_mem = 0;
    int esti_op_mem = 0;
    bool use_query_mem = false;
    bool use_tenant = false;
    List* parse_hint_warning = NIL;

    if (cursorOptions & CURSOR_OPT_SPQ_OK)
        cursorOptions &= ~CURSOR_OPT_SPQ_OK;
    //if it is pgxc plan for tsstore delete sql.errport
    if((!u_sess->attr.attr_sql.enable_stream_operator || !u_sess->opt_cxt.is_stream) && IS_PGXC_COORDINATOR) {
        checkTsstoreQuery(parse);
    }

    /*
     * Dynamic smp
     */
    if (IsDynamicSmpEnabled()) {
        InitDynamicSmp();
        int hashTableCount = 0;

        if (isIntergratedMachine) {
            GetHashTableCount(parse, parse->cteList, &hashTableCount);
        }

        ChooseStartQueryDop(hashTableCount);
    }

    if (enable_check_implicit_cast())
        find_implicit_cast_var(parse);

    /* Initilizing the work mem used by optimizer */
    if (IS_STREAM_PLAN) {
        dywlm_client_get_memory_info(&max_mem, &available_mem, &use_tenant);

        if (max_mem != 0) {
            use_query_mem = true;
            esti_op_mem = (double)available_mem / 2.0;
            u_sess->opt_cxt.op_work_mem = Min(esti_op_mem, OPT_MAX_OP_MEM);
            AssertEreport(u_sess->opt_cxt.op_work_mem > 0,
                MOD_OPT,
                "invalid operator work memory when initilizing the work memory used by optimizer");
        } else {
            u_sess->opt_cxt.op_work_mem = u_sess->attr.attr_memory.work_mem;
            esti_op_mem = u_sess->opt_cxt.op_work_mem;
        }
    } else {
        u_sess->opt_cxt.op_work_mem = u_sess->attr.attr_memory.work_mem;
        esti_op_mem = u_sess->opt_cxt.op_work_mem;
    }

    /* Cursor options may come from caller or from DECLARE CURSOR stmt */
    if (parse->utilityStmt && IsA(parse->utilityStmt, DeclareCursorStmt))
        cursorOptions = (uint32)cursorOptions | (uint32)(((DeclareCursorStmt*)parse->utilityStmt)->options);

    else if (parse->utilityStmt && IsA(parse->utilityStmt, CursorExpression))
        cursorOptions = (uint32)cursorOptions | (uint32)(((CursorExpression*)parse->utilityStmt)->options);

    /*
     * Set up global state for this planner invocation.  This data is needed
     * across all levels of sub-Query that might exist in the given command,
     * so we keep it in a separate struct that's linked to by each per-Query
     * PlannerInfo.
     */
    glob = makeNode(PlannerGlobal);

    glob->boundParams = boundParams;
    glob->subplans = NIL;
    glob->subroots = NIL;
    glob->rewindPlanIDs = NULL;
    glob->finalrtable = NIL;
    glob->finalrowmarks = NIL;
    glob->resultRelations = NIL;
    glob->relationOids = NIL;
    glob->invalItems = NIL;
    glob->nParamExec = 0;
    glob->lastPHId = 0;
    glob->lastRowMarkId = 0;
    glob->transientPlan = false;
    glob->dependsOnRole = false;
    glob->insideRecursion = false;
    glob->bloomfilter.bloomfilter_index = -1;
    glob->bloomfilter.add_index = true;
    glob->estiopmem = esti_op_mem;
    
    if (IS_STREAM_PLAN)
        glob->vectorized = !vector_engine_preprocess_walker((Node*)parse, parse->rtable);
    else
        glob->vectorized = false;
    /* Assume work mem is at least 1/4 of query mem */
    glob->minopmem = Min(available_mem / 4, OPT_MAX_OP_MEM);
    if (u_sess->parser_cxt.has_hintwarning) {
        parse_hint_warning = retrieve_query_hint_warning((Node*)parse);
    }

    /*
     * Set up default exec_nodes, we fist build re-cursively iterate parse->rtable
     * to check if we are refering base relations from different node group(error-out),
     * then fetch 1st RTE entry to build default ExecNodes and put the reference to
     * the top-most PlannerInfo->glob
     */
    bool ngbk_is_multiple_nodegroup_scenario = false;
    int ngbk_different_nodegroup_count = 1;
    Distribution* ngbk_in_redistribution_group_distribution = NULL;
    Distribution* ngbk_compute_permission_group_distribution = NULL;
    Distribution* ngbk_query_union_set_group_distribution = NULL;
    Distribution* ngbk_single_node_distribution = NULL;
    ng_backup_nodegroup_options(&ngbk_is_multiple_nodegroup_scenario,
        &ngbk_different_nodegroup_count,
        &ngbk_in_redistribution_group_distribution,
        &ngbk_compute_permission_group_distribution,
        &ngbk_query_union_set_group_distribution,
        &ngbk_single_node_distribution);
    ng_init_nodegroup_optimizer(parse);

    /* Must assign value after call ng_init_nodegroup_optimizer(). */
    u_sess->opt_cxt.is_dngather_support = 
        u_sess->opt_cxt.is_dngather_support && ng_get_single_node_distribution() != NULL;

    /* Determine what fraction of the plan is likely to be scanned */
    if ((uint32)cursorOptions & CURSOR_OPT_FAST_PLAN) {
        /*
         * We have no real idea how many tuples the user will ultimately FETCH
         * from a cursor, but it is often the case that he doesn't want 'em
         * all, or would prefer a fast-start plan anyway so that he can
         * process some of the tuples sooner.  Use a GUC parameter to decide
         * what fraction to optimize for.
         */
        tuple_fraction = u_sess->attr.attr_sql.cursor_tuple_fraction;

        /*
         * We document cursor_tuple_fraction as simply being a fraction, which
         * means the edge cases 0 and 1 have to be treated specially here.	We
         * convert 1 to 0 ("all the tuples") and 0 to a very small fraction.
         */
        if (tuple_fraction >= 1.0) {
            tuple_fraction = 0.0;
        } else if (tuple_fraction <= 0.0) {
            tuple_fraction = 1e-10;
        }
    } else {
        /* Default assumption is we need all the tuples */
        tuple_fraction = 0.0;
    }

    /* reset u_sess->analyze_cxt.need_autoanalyze */
    u_sess->analyze_cxt.need_autoanalyze = false;

    MemoryContext old_context = CurrentMemoryContext;
    init_optimizer_context(glob);
    old_context = MemoryContextSwitchTo(glob->plannerContext->plannerMemContext);

    /* primary planning entry point (may recurse for subqueries) */
    top_plan = subquery_planner(glob, parse, NULL, false, tuple_fraction, &root);

    MemoryContextSwitchTo(old_context);

#ifdef ENABLE_MULTIPLE_NODES
    /* Are there OBS/HDFS ForeignScan node(s) in the plan tree? */
    u_sess->opt_cxt.srvtype = T_INVALID;
    u_sess->opt_cxt.has_obsrel = has_dfs_node(top_plan, glob);

    /*
     * try to accelerate the query for HDFS/OBS foreign table by pushing
     * scan/agg node down to the compute pool.
     */
    if (u_sess->opt_cxt.has_obsrel) {
        AssertEreport(u_sess->opt_cxt.srvtype != T_INVALID,
            MOD_OPT,
            "invalid server type when push scan/agg node down to the compute pool to the accelerate the query.");

        top_plan = try_accelerate_plan(top_plan, root, glob);
    }
#endif
    /*
     * If creating a plan for a scrollable cursor, make sure it can run
     * backwards on demand.  Add a Material node at the top at need.
     */
    if ((unsigned int)cursorOptions & CURSOR_OPT_SCROLL) {
        if (!ExecSupportsBackwardScan(top_plan))
            top_plan = materialize_finished_plan(top_plan);
    }

    /* final cleanup of the plan */
    AssertEreport(glob->finalrtable == NIL,
        MOD_OPT,
        "finalrtable is not empty when finish creating a plan for a scrollable cursor");
    AssertEreport(glob->finalrowmarks == NIL,
        MOD_OPT,
        "finalrowmarks is not empty when finish creating a plan for a scrollable cursor");
    AssertEreport(glob->resultRelations == NIL,
        MOD_OPT,
        "resultRelations is not empty when finish creating a plan for a scrollable cursor");

    if ((IS_STREAM_PLAN || (IS_PGXC_DATANODE && (!IS_STREAM || IS_STREAM_DATANODE))) && root->query_level == 1) {
        /* remote query and windowagg do not support vectorize rescan, so fallback to row plan */
        top_plan = try_vectorize_plan(top_plan, parse, cursorOptions & CURSOR_OPT_HOLD);
    }

    /*
     * Required-column rewriting has already replaced the wide UNION key with
     * a scalar leaf projection.  For a safe Anti Join leaf, evaluate that
     * projection in the preserved-side scan as well, so the raw vector does
     * not occupy the scan-to-join tuple slot.
     */
    if (root->union_all_required_columns_pruned) {
        push_down_union_all_source_projections(glob, top_plan);
    }

    top_plan = set_plan_references(root, top_plan);
    delete_redundant_streams_of_remotequery((RemoteQuery *)top_plan);

    /*
     * just for cooperation analysis on client cluster,
     * try deparse agg node to remote sql in ForeignScan node.
     * NOTE: call try_deparse_agg() must be after set_plan_references().
     */
    top_plan = try_deparse_agg(top_plan, root, glob);

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * just for cooperation analysis on source data cluster,
     * reassign dn list scaned of RemoteQuery node for the request from client cluster.
     */
    find_remotequery(top_plan, root);

    if (IS_PGXC_COORDINATOR && root->query_level == 1) {
        bool materialize = false;
        bool sort_to_store = false;
        /*
         * if is with hold cursor, remotequery tuplestore should be used,
         * and because we do not rescan sortstore in execRemoteQueryResacn,
         * tuples in sortstore should be stored into tuplestore, to avoid missing tuples.
         */
        if (cursorOptions & CURSOR_OPT_HOLD) {
            materialize = true;
            sort_to_store = true;
        }
        materialize_remote_query(top_plan, &materialize, sort_to_store);
    }
#endif
    /*
     * Handle subplan situation.
     * We have to put this under set_plan_references() function,
     * otherwise we will mis-identify the subplan.
     */
    if (u_sess->opt_cxt.query_dop > 1) {
        List* subplan_list = NIL;
        (void)has_subplan(top_plan, NULL, NULL, true, &subplan_list, true);
    }
    confirm_parallel_info(top_plan, 1);

#ifdef STREAMPLAN
    /*
     * Mark plan node id and parent node id for all the plan nodes.
     */
    int parent_node_id = INITIAL_PARENT_NODE_ID; /* beginning with INITIAL_PARENT_NODE_ID */
    int plan_node_id = INITIAL_PLAN_NODE_ID;     /* beginning with INITIAL_PLAN_NODE_ID */
    int num_streams = 0;
    int num_plannodes = 0;
    int total_num_streams = 0;

    /* mark gather count for query */
    int gather_count = 0;
    /*
     * When enable_stream_operator = off;
     * The current mechanism has such a problem that a CN will split complex SQL into multiple simple SQL and send it to
     * the DN for execution. In order to ensure data consistency, the DN needs to use the same Snapshot for visibility
     * judgment of such SQL. Therefore, the CN side sends such SQL identifier to the DN. Use
     * PlannedStmt->Max_push_sql_num records the maximum number of SQL statements split by a SQL statement.
     */
    int max_push_sql_num = 0;
    /*
     * the array to store parent plan node id of each subplan, start from 1.
     * First item is for parent id when traverse the tree.
     */
    int* subplan_ids = (int*)palloc0(sizeof(int) * (list_length(glob->subplans) + 1));
    List* init_plan = NIL;
    int i = 1;

    NodeGroupInfoContext* node_group_info_context = (NodeGroupInfoContext*)palloc0(sizeof(NodeGroupInfoContext));

    /*
     * MPP with-recursive support
     *
     * Vectorize the each plan nodes under RecursiveUnion
     */
    Assert(list_length(glob->subplans) == list_length(glob->subroots));
    forboth(lp, glob->subplans, lr, glob->subroots)
    {
        Plan* subplan = (Plan*)lfirst(lp);
        PlannerInfo* subroot = (PlannerInfo*)lfirst(lr);

        /* Vectorize the subplan with RecursiveUnion plan node */
        if (STREAM_RECURSIVECTE_SUPPORTED && IsA(subplan, RecursiveUnion)) {
            subplan = try_vectorize_plan(subplan, subroot->parse, true);
            lfirst(lp) = subplan;
        }
    }

    /* Assign plan node id for each plan node */
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && root->query_level == 1) {
#else
    if (root->query_level == 1) {
#endif
        finalize_node_id(top_plan,
            &plan_node_id,
            &parent_node_id,
            &num_streams,
            &num_plannodes,
            &total_num_streams,
            &max_push_sql_num,
            &gather_count,
            glob->subplans,
            glob->subroots,
            &init_plan,
            subplan_ids,
            true,
            false,
            false,
            queryIsReadOnly(parse),
            node_group_info_context);
    }
#endif

    /* ... and the subplans (both regular subplans and initplans) */
    AssertEreport(list_length(glob->subplans) == list_length(glob->subroots),
        MOD_OPT,
        "The length of subplans is not equal to that of subroots when standardize planner");
    forboth(lp, glob->subplans, lr, glob->subroots)
    {
        Plan* subplan = (Plan*)lfirst(lp);
        PlannerInfo* subroot = (PlannerInfo*)lfirst(lr);

#ifdef STREAMPLAN
        /* We set reference of some plans in finalize_node_id. For undone plan, set plan references */
        if (subplan_ids[i] == 0 || IsA(subplan, RecursiveUnion) ||
            IsA(subplan, StartWithOp)) {
            if (STREAM_RECURSIVECTE_SUPPORTED && IsA(subplan, RecursiveUnion)) {
                RecursiveRefContext context;
                errno_t rc = EOK;
                rc = memset_s(&context, sizeof(RecursiveRefContext), 0, sizeof(RecursiveRefContext));
                securec_check(rc, "\0", "\0");
                context.join_type = T_Invalid;
                context.ru_plan = (RecursiveUnion*)subplan;
                context.control_plan = subplan;
                context.nested_stream_depth = 0;
                context.set_control_plan_nodeid = false;
                context.initplans = init_plan;
                context.subplans = glob->subplans;

                /* EntryPoint for iterating the underlying plan node */
                set_recursive_cteplan_ref(subplan, &context);
            } else {
                subplan = try_vectorize_plan(subplan, subroot->parse, true);
                lfirst(lp) = set_plan_references(subroot, subplan);
            }

            /* for start with processing */
            if (IsA(subplan, StartWithOp)) {
                ProcessStartWithOpMixWork(root, top_plan, subroot, (StartWithOp *)subplan);
            }

            /*
             * When enable_stream_operator = off, Subquery SQL  is not processed by finalize_node_id.
             * In this case we default each subquery to a SQL statement pushed down to the DN.
             * Here we may misjudge the subquery executed only on the CN,
             * but in order to maintain The independence of the set_plan_references function,
             * there is no further judgment on such subqueries, and it is considered that the sub-query is issued to the
             * DN. This operation does not affect the correctness.
             */
            max_push_sql_num++;
        }
#endif
        i++;
    }

    /*
     * The reference must be introduced after every plan/subplan has passed
     * setrefs, but before the optional copyObject boundary and PlannedStmt
     * construction.  Default-off is a zero-cost compatibility path.
     */
    if (u_sess->attr.attr_sql.ann_orderby_distance_reuse != ANN_ORDERBY_REUSE_OFF) {
        rewrite_ann_orderby_distance_refs(glob, top_plan);
    }

    /* Juse copy these fields only when the memory context total size meets the dropping condition. */
    uint64 optimize_memory_size = ((AllocSetContext*)(glob->plannerContext->plannerMemContext))->totalSpace;
    uint64 totalsize = u_sess->optimizer_query_memory + optimize_memory_size;
    if (totalsize >= MEMORY_MAX_TOTAL_CONTEXT_THRESHOLD ||
        IS_NEED_FREE_MEMORY_CONTEXT(glob->plannerContext->plannerMemContext)) {
        top_plan = (Plan*)copyObject(top_plan);
        glob->finalrtable = (List*)copyObject(glob->finalrtable);
        glob->resultRelations = (List*)copyObject(glob->resultRelations);
        glob->subplans = (List*)copyObject(glob->subplans);
        glob->rewindPlanIDs = bms_copy(glob->rewindPlanIDs);
        glob->finalrowmarks = (List*)copyObject(glob->finalrowmarks);
        glob->relationOids = (List*)copyObject(glob->relationOids);
        glob->invalItems = (List*)copyObject(glob->invalItems);
        init_plan = (List*)copyObject(init_plan);
    }

    glob->hint_warning = list_concat(parse_hint_warning, (List*)copyObject(glob->hint_warning));

    /* build the PlannedStmt result */
    result = makeNode(PlannedStmt);

    result->commandType = parse->commandType;
    result->queryId = parse->queryId;
    result->uniqueSQLId = parse->uniqueSQLId;
    result->hasReturning = (parse->returningList != NIL);
    result->hasModifyingCTE = parse->hasModifyingCTE;
    result->canSetTag = parse->canSetTag;
    result->transientPlan = glob->transientPlan;
    result->dependsOnRole = glob->dependsOnRole;
    result->planTree = top_plan;
    result->rtable = glob->finalrtable;
    result->resultRelations = glob->resultRelations;
    result->utilityStmt = parse->utilityStmt;
    result->subplans = glob->subplans;
    result->rewindPlanIDs = glob->rewindPlanIDs;
    result->rowMarks = glob->finalrowmarks;
    result->relationOids = glob->relationOids;
    result->invalItems = glob->invalItems;
    result->nParamExec = glob->nParamExec;
    result->noanalyze_rellist = (List*)copyObject(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList);
    result->hasIgnore = parse->hasIgnore;

    if (ENABLE_CACHEDPLAN_MGR && u_sess->pcache_cxt.is_plan_exploration) {
        ereport(DEBUG2,
                (errmodule(MOD_OPT),
                 errmsg(" ThreadId: %d: append PlannerInfo to session. PlannerInfo's memorycxt is \"%s\" and parent is \"%s\".",
                        gettid(),  root->planner_cxt->name, root->planner_cxt->parent->name)));
        u_sess->pcache_cxt.explored_plan_info = root;
    }

    if (IS_PGXC_COORDINATOR &&
        (t_thrd.proc->workingVersionNum < 92097 || total_num_streams > 0)) {
        result->nodesDefinition = get_all_datanodes_def();
    }
    result->num_nodes = u_sess->pgxc_cxt.NumDataNodes;
    result->num_streams = total_num_streams;
    result->max_push_sql_num = max_push_sql_num;
    result->gather_count = gather_count;
    result->num_plannodes = num_plannodes;

    FillPlanBucketmap(result, node_group_info_context);

    result->query_string = NULL;
    result->MaxBloomFilterNum = root->glob->bloomfilter.bloomfilter_index + 1;
    if (instr_stmt_plan_need_report_cause_type())
        result->cause_type = instr_stmt_plan_get_cause_type();
    /* record which suplan belongs to which thread */
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN) {
#else
    if (result->num_streams > 0) {
#endif
        for (i = 1; i <= list_length(result->subplans); i++)
            result->subplan_ids = lappend_int(result->subplan_ids, subplan_ids[i]);
        result->initPlan = init_plan;
    }
    pfree_ext(subplan_ids);

    /* dynamic query dop main entry */
    if (IsDynamicSmpEnabled()) {
        /* the main plan */
        OptimizePlanDop(result);
    }

    /* Query mem calculation and control main entry */
    if (IS_STREAM_PLAN && use_query_mem) {
        result->assigned_query_mem[1] = max_mem;
        result->assigned_query_mem[0] = available_mem;

        ereport(DEBUG2,
            (errmodule(MOD_MEM),
                errmsg("[standard_planner]Passing in max mem %d and available mem %d", max_mem, available_mem)));
        CalculateQueryMemMain(result, use_tenant, false);
        ereport(DEBUG2,
            (errmodule(MOD_MEM),
                errmsg("[standard_planner]Calucated query max %d and min mem %d",
                    result->query_mem[0],
                    result->query_mem[1])));
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* data redistribution for DFS table. */
    if (u_sess->attr.attr_sql.enable_cluster_resize && root->query_level == 1 &&
        root->parse->commandType == CMD_INSERT) {
        result->dataDestRelIndex = root->dataDestRelIndex;
    } else
#endif
    {
        result->dataDestRelIndex = 0;
    }

    result->query_dop = u_sess->opt_cxt.query_dop;

    if (u_sess->opt_cxt.has_obsrel) {
        result->has_obsrel = true;
    }
    result->plan_hint_warning = glob->hint_warning;

    ng_restore_nodegroup_options(ngbk_is_multiple_nodegroup_scenario,
        ngbk_different_nodegroup_count,
        ngbk_in_redistribution_group_distribution,
        ngbk_compute_permission_group_distribution,
        ngbk_query_union_set_group_distribution,
        ngbk_single_node_distribution);

    deinit_optimizer_context(glob);

    if (enable_check_implicit_cast() && g_index_vars != NIL)
        check_index_column();

    result->isRowTriggerShippable = parse->isRowTriggerShippable;
    return result;
}
```

<a id="source-004"></a>

#### subquery_planner()

来源：`src/gausskernel/optimizer/plan/planner.cpp:2142-2924`；范围：**完整函数**。

内容 SHA256：`2a01f89236f0a0528e1b067fde839e3c43de0b4cfae5ce048bf2ca6b59fdea06`。

保留完整函数以核对真实调用位置、返回的计划指针、主计划/子计划 setrefs 以及 pruned 的继承。

```cpp
Plan* subquery_planner(PlannerGlobal* glob, Query* parse, PlannerInfo* parent_root, bool hasRecursion,
    double tuple_fraction, PlannerInfo** subroot, int options, ItstDisKey* diskeys, List* subqueryRestrictInfo)
{
    int num_old_subplans = list_length(glob->subplans);
    PlannerInfo* root = NULL;
    Plan* plan = NULL;
    List* newWithCheckOptions = NIL;
    List* newHaving = NIL;
    bool hasOuterJoins = false;
    bool hasResultRTEs = false;
    bool unionAllColumnsRewritten = false;
    ListCell* l = NULL;
    StringInfoData buf;
    char RewriteContextName[NAMEDATALEN] = {0};
    MemoryContext QueryRewriteContext = NULL;
    MemoryContext oldcontext = NULL;
    errno_t rc = EOK;

/* We used DEBUG5 log to print SQL after each rewrite */
#define DEBUG_QRW(message)                                                                      \
    do {                                                                                        \
        if (log_min_messages <= DEBUG5 && module_logging_is_on(MOD_OPT_REWRITE)) {              \
            initStringInfo(&buf);                                                               \
            deparse_query(root->parse, &buf, NIL, false, false, NULL, true);                    \
            ereport(DEBUG5, (errmodule(MOD_OPT_REWRITE), errmsg("%s: %s", message, buf.data))); \
            pfree_ext(buf.data);                                                                \
        }                                                                                       \
    } while (0)

    /* Create a PlannerInfo data structure for this subquery */
    root = makeNode(PlannerInfo);
    root->parse = parse;
    root->glob = glob;
    root->query_level = parent_root ? parent_root->query_level + 1 : 1;
    root->parent_root = parent_root;
    root->plan_params = NIL;
    root->planner_cxt = CurrentMemoryContext;
    root->init_plans = NIL;
    root->cte_plan_ids = NIL;
    root->eq_classes = NIL;
    root->append_rel_list = NIL;
    root->rowMarks = NIL;
    root->hasInheritedTarget = false;
    root->grouping_map = NULL;
    root->subquery_type = options;
    root->param_upper = NULL;
	root->hasRownumQual = false;
    root->union_all_required_columns_pruned =
        parent_root != NULL && parent_root->union_all_required_columns_pruned;

    /*
     * Apply memory context for query rewrite in optimizer.
     * OptimizerContext is NULL in PBE condition which we need to consider.
     */
    rc = snprintf_s(RewriteContextName, NAMEDATALEN, NAMEDATALEN - 1, "QueryRewriteContext_%d", root->query_level);
    securec_check_ss(rc, "\0", "\0");

    QueryRewriteContext = AllocSetContextCreate(CurrentMemoryContext,
        RewriteContextName,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    oldcontext = MemoryContextSwitchTo(QueryRewriteContext);

    /*
     * Mark the current PlannerInfo is working for a query-block in recursive CTE planning,
     * in general we want to let the sub-planning stages to know we are under a recursive-cte,
     * planning, two case need
     * [1]. Call subquery_planner() to planning the query block inside of with-block
     * [2]. Call subquery_planner() to planning each query-block consists of the union
     *      operation, so consequtially inherit the "is_recursive_cte" properties from
     *      parent root
     */
    if (hasRecursion || (parent_root && parent_root->is_under_recursive_cte)) {
        root->is_under_recursive_cte = true;
        root->is_under_recursive_tree = parent_root->is_under_recursive_tree;
    } else {
        root->is_under_recursive_cte = false;
    }

    check_is_support_recursive_cte(root);

#ifdef PGXC
    root->rs_alias_index = 1;
#endif
    root->hasRecursion = hasRecursion;
    if (hasRecursion)
        root->wt_param_id = SS_assign_special_param(root);
    else
        root->wt_param_id = -1;
    root->qualSecurityLevel = 0;
    root->non_recursive_plan = NULL;
    root->subqueryRestrictInfo = subqueryRestrictInfo;

    /* Mark current planner root is correlated as well */
    if (parent_root != NULL && parent_root->is_under_recursive_cte && parent_root->is_correlated) {
        root->is_correlated = true;
    }

    preprocess_ru_is_under_start_with(root);

    DEBUG_QRW("Before rewrite");

    preprocess_const_params(root, (Node*)parse->jointree);

    DEBUG_QRW("After const params replace ");

    /*
     * If there is a WITH list, process each WITH query and build an initplan
     * SubPlan structure for it. For stream plan, it's already be replaced, so
     * no need to do this.
     *
     * For recursive cte we still process this in same way
     */
    if (parse->cteList) {
        SS_process_ctes(root);
    }

    DEBUG_QRW("After CTE substitution");

    /*
     * If the FROM clause is empty, replace it with a dummy RTE_RESULT RTE, so
     * that we don't need so many special cases to deal with that situation.
     */
    replace_empty_jointree(parse);

#ifdef STREAMPLAN
    /*
     * Since count(distinct) conversion can push down subquery, for sake of
     * duplicate of sublink pullup, we put it ahead of sublink pullup
     */
    if (IS_STREAM_PLAN && parse->hasAggs) {
        convert_multi_count_distinct(root);
        DEBUG_QRW("After multi count distinct rewrite");
    }
#endif

    /*
     * Look for ANY and EXISTS SubLinks in WHERE and JOIN/ON clauses, and try
     * to transform them into joins.  Note that this step does not descend
     * into subqueries; if we pull up any subqueries below, their SubLinks are
     * processed just before pulling them up.
     */
    if (parse->hasSubLinks) {
        pull_up_sublinks(root);
        DEBUG_QRW("After sublink pullup");
    }

    /* Reduce orderby clause in subquery for join */
    reduce_orderby(parse, false);

    DEBUG_QRW("After order by reduce");

    if (u_sess->attr.attr_sql.enable_constraint_optimization) {
        removeNotNullTest(root);
        DEBUG_QRW("After soft constraint removal");
    }

    /*
     * Scan the rangetable for set-returning functions, and inline them if
     * possible (producing subqueries that might get pulled up next).
     * Recursion issues here are handled in the same way as for SubLinks.
     */
    inline_set_returning_functions(root);

    if ((LAZY_AGG & u_sess->attr.attr_sql.rewrite_rule) && permit_from_rewrite_hint(root, LAZY_AGG)) {
        lazyagg_main(parse);
        DEBUG_QRW("After lazyagg");
    }

    /*
     * Here we only control the select permission for pan_table_data. Details see in checkPTRelkind().
     * The flag will be used in ExecCheckRTEPerms.
     */
    if (parse->commandType == CMD_SELECT && checkSelectStmtForPlanTable(parse->rtable)) {
        OnlySelectFromPlanTable = true;
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* Change ROWNUM to LIMIT if possible */
    preprocess_rownum(root, parse);
    DEBUG_QRW("After preprocess rownum");
#endif

    /*
     * Push safe outer WHERE clauses into pure UNION ALL leaves, and compact
     * the set-operation output to columns that are actually needed above it.
     * This must run before Top-K rewriting while column numbers still describe
     * the original view output.
     */
    unionAllColumnsRewritten = preprocess_union_all_required_columns(root);
    DEBUG_QRW("After union all required-column pushdown");

    /* Push ORDER BY + LIMIT to UNION ALL leaves for DiskANN vector TopK */
    preprocess_union_all_diskann_topk(root);
    DEBUG_QRW("After union all diskann top-k pushdown");

    /*
     * Top-K recognition above needs the original ORDER BY expression.  Once
     * that decision is made, replace wide ordering inputs crossing UNION ALL
     * with branch-local scalar keys.  The inherited pruned flag controls
     * physical tlists only: it does not mean this Query passed the rewrite
     * safety checks (a descendant may contain aggregates, SRFs, or LIMIT).
     */
    if (unionAllColumnsRewritten)
        preprocess_union_all_order_key_projection(root);
    DEBUG_QRW("After union all order-key projection");

    /*
     * Check to see if any subqueries in the jointree can be merged into this
     * query.
     */
    parse->jointree = (FromExpr*)pull_up_subqueries(root, (Node*)parse->jointree);

    DEBUG_QRW("After simple subquery pull up");

    /*
     * If this is a simple UNION ALL query, flatten it into an appendrel. We
     * do this now because it requires applying pull_up_subqueries to the leaf
     * queries of the UNION ALL, which weren't touched above because they
     * weren't referenced by the jointree (they will be after we do this).
     */
    if (parse->setOperations) {
        flatten_simple_union_all(root);
        DEBUG_QRW("After simple union all flatten");
    }

    /* Transform hint.*/
    transform_hints(root, parse, parse->hintState);

    DEBUG_QRW("After transform hint");

    /*
     * Detect whether any rangetable entries are RTE_JOIN kind; if not, we can
     * avoid the expense of doing flatten_join_alias_vars().  Likewise check
     * whether any are RTE_RESULT kind; if not, we can skip
     * remove_useless_result_rtes().  Also check for outer joins --- if none,
     * we can skip reduce_outer_joins().  And check for LATERAL RTEs, too.
     * This must be done after we have done pull_up_subqueries(), of course.
     */
    root->hasJoinRTEs = false;
    root->hasLateralRTEs = false;
    hasOuterJoins = false;

    foreach (l, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);

        if (rte->swAborted) {
            continue;
        }

        if (rte->rtekind == RTE_JOIN) {
            root->hasJoinRTEs = true;
            if (IS_OUTER_JOIN(rte->jointype)) {
                hasOuterJoins = true;
            }
        } else if (rte->rtekind == RTE_RESULT) {
            hasResultRTEs = true;
        }
        if (rte->lateral)
            root->hasLateralRTEs = true;
    }

    /*
     * Preprocess RowMark information.	We need to do this after subquery
     * pullup (so that all non-inherited RTEs are present) and before
     * inheritance expansion (so that the info is available for
     * expand_inherited_tables to examine and modify).
     */
    preprocess_rowmarks(root);

#ifdef PGXC
    /*
     * In Coordinators we separate row marks in two groups
     * one comprises of row marks of types ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE
     * and the other contains the rest of the types of row marks
     * The former is handeled on Coordinator in such a way that
     * FOR UPDATE/SHARE gets added in the remote query, whereas
     * the later needs to be handeled the way pg does
     *
     * Notice : This is not a very efficient way of handling row marks
     * Consider this join query
     * select * from t1, t2 where t1.val = t2.val for update
     * It results in this query to be fired at the Datanodes
     * SELECT val, val2, ctid FROM ONLY t2 WHERE true FOR UPDATE OF t2
     * We are locking the complete table where as we should have locked
     * only the rows where t1.val = t2.val is met
     *
     * We won't really call separate_rowmarks before we support for update with
     * reomtequery.
     */
    if (!IS_STREAM_PLAN)
        separate_rowmarks(root);
#endif

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * When the SQL dose not support stream mode in coordinator node, must send remotequery
     * to datanode, and need not expand dfs table into dfs main table and delta table.
     * Always support dfs table to expanding in data node.
     */
    if (u_sess->opt_cxt.is_stream || IS_PGXC_DATANODE) {
        /*
         * Expand the Dfs table.
         */
        expand_dfs_tables(root);
    }
#endif
    /*
     * Expand any rangetable entries that are inheritance sets into "append
     * relations".  This can add entries to the rangetable, but they must be
     * plain RTE_RELATION entries, so it's OK (and marginally more efficient)
     * to do it after checking for joins and other special RTEs.  We must do
     * this after pulling up subqueries, else we'd fail to handle inherited
     * tables in subqueries.
     */
    expand_inherited_tables(root);

    /*
     * Set hasHavingQual to remember if HAVING clause is present.  Needed
     * because preprocess_expression will reduce a constant-true condition to
     * an empty qual list ... but "HAVING TRUE" is not a semantic no-op.
     */
    root->hasHavingQual = (parse->havingQual != NULL);

    /* Clear this flag; might get set in distribute_qual_to_rels */
    root->hasPseudoConstantQuals = false;

    /*
     * Calculate how many tables in current query level, and give a
     * rought estimation of work mem for each relation
     */
    int work_mem_orig = u_sess->opt_cxt.op_work_mem;
    int esti_op_mem_orig = root->glob->estiopmem;
    if (root->glob->minopmem > 0) {
        int num_rel = 0;
        foreach (l, parse->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);

            if (rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY) {
                num_rel++;
            }
        }
        if (num_rel <= 1) {
            if ((parse->groupClause || parse->sortClause || parse->distinctClause))
                num_rel = 2;
            else
                num_rel = 1;
        }
        root->glob->estiopmem = Max(root->glob->minopmem, (double)root->glob->estiopmem / ceil(LOG2(num_rel + 1)));
        u_sess->opt_cxt.op_work_mem = Min(root->glob->estiopmem, OPT_MAX_OP_MEM);
        AssertEreport(u_sess->opt_cxt.op_work_mem > 0,
            MOD_OPT,
            "invalid operator work mem when roughtly estimating the work memory for each relation");
    }

    /*
     * Do expression preprocessing on targetlist and quals, as well as other
     * random expressions in the querytree.  Note that we do not need to
     * handle sort/group expressions explicitly, because they are actually
     * part of the targetlist.
     */
    parse->targetList = (List*)preprocess_expression(root, (Node*)parse->targetList, EXPRKIND_TARGET);

    /* Constant-folding might have removed all set-returning functions */
    if (parse->is_flt_frame && parse->hasTargetSRFs) {
        parse->hasTargetSRFs = expression_returns_set((Node *) parse->targetList);
    }

    foreach(l, parse->withCheckOptions) {
        WithCheckOption* wco = (WithCheckOption*)lfirst(l);

        wco->qual = preprocess_expression(root, wco->qual, EXPRKIND_QUAL);
        if (wco->qual != NULL)
            newWithCheckOptions = lappend(newWithCheckOptions, wco);
    }
    parse->withCheckOptions = newWithCheckOptions;

    parse->returningList = (List*)preprocess_expression(root, (Node*)parse->returningList, EXPRKIND_TARGET);

    preprocess_qual_conditions(root, (Node*)parse->jointree);

    parse->havingQual = preprocess_expression(root, parse->havingQual, EXPRKIND_QUAL);

    foreach (l, parse->windowClause) {
        WindowClause* wc = (WindowClause*)lfirst(l);

        /* partitionClause/orderClause are sort/group expressions */
        wc->startOffset = preprocess_expression(root, wc->startOffset, EXPRKIND_LIMIT);
        wc->endOffset = preprocess_expression(root, wc->endOffset, EXPRKIND_LIMIT);
    }

    parse->limitOffset = preprocess_expression(root, parse->limitOffset, EXPRKIND_LIMIT);
    if (parse->limitCount != NULL && !IsA(parse->limitCount, Const)) {
        parse->limitCount = preprocess_expression(root, parse->limitCount, EXPRKIND_LIMIT);
    }

    foreach (l, parse->mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(l);

        action->targetList = (List*)preprocess_expression(root, (Node*)action->targetList, EXPRKIND_TARGET);

        action->pulluped_targetList =
            (List*)preprocess_expression(root, (Node*)(action->pulluped_targetList), EXPRKIND_TARGET);

        action->qual = preprocess_expression(root, (Node*)action->qual, EXPRKIND_QUAL);
    }

    parse->mergeSourceTargetList =
        (List*)preprocess_expression(root, (Node*)parse->mergeSourceTargetList, EXPRKIND_TARGET);

    if (parse->upsertClause) {
        parse->upsertClause->updateTlist = (List*)
            preprocess_expression(root, (Node*)parse->upsertClause->updateTlist, EXPRKIND_TARGET);
        parse->upsertClause->upsertWhere = (Node*)
            preprocess_expression(root, (Node*)parse->upsertClause->upsertWhere, EXPRKIND_QUAL);
    }
    root->append_rel_list = (List*)preprocess_expression(root, (Node*)root->append_rel_list, EXPRKIND_APPINFO);

    /* Also need to preprocess expressions for function and values RTEs */
    foreach (l, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);
        int kind;

        if (rte->rtekind == RTE_RELATION) {
            if (rte->tablesample) {
                rte->tablesample =
                    (TableSampleClause*)preprocess_expression(root, (Node*)rte->tablesample, EXPRKIND_TABLESAMPLE);
            }
            if (rte->timecapsule) {
#ifndef ENABLE_MULTIPLE_NODES
                if (IS_STREAM) {
                    mark_stream_unsupport();
                }
#endif
                rte->timecapsule =
                    (TimeCapsuleClause*)preprocess_expression(root, (Node*)rte->timecapsule, EXPRKIND_TIMECAPSULE);
            }
        } else if (rte->rtekind == RTE_SUBQUERY) {
            /*
             * We don't want to do all preprocessing yet on the subquery's
             * expressions, since that will happen when we plan it.  But if it
             * contains any join aliases of our level, those have to get
             * expanded now, because planning of the subquery won't do it.
             * That's only possible if the subquery is LATERAL.
             */
            if (rte->lateral && root->hasJoinRTEs)
                rte->subquery = (Query *)flatten_join_alias_vars(root, (Node *) rte->subquery);
        } else if (rte->rtekind == RTE_FUNCTION) {
            /* Preprocess the function expression fully */
            kind = rte->lateral ? EXPRKIND_RTFUNC_LATERAL : EXPRKIND_RTFUNC;
            rte->funcexpr = preprocess_expression(root, rte->funcexpr, kind);
        } else if (rte->rtekind == RTE_TABLEFUNC) {
            /* Preprocess the function expression(s) fully */
            kind = rte->lateral ? EXPRKIND_TABLEFUNC_LATERAL : EXPRKIND_TABLEFUNC;
            rte->tablefunc = (TableFunc*)preprocess_expression(root, (Node*) rte->tablefunc, kind);
        } else if (rte->rtekind == RTE_VALUES) {
            /* Preprocess the values lists fully */
            kind = rte->lateral ? EXPRKIND_VALUES_LATERAL : EXPRKIND_VALUES;
            rte->values_lists = (List*)preprocess_expression(root, (Node*)rte->values_lists, kind);
        }

        /*
         * Process each element of the securityQuals list as if it were a
         * separate qual expression (as indeed it is).  We need to do it this
         * way to get proper canonicalization of AND/OR structure.  Note that
         * this converts each element into an implicit-AND sublist.
         */
        ListCell* cell = NULL;
        foreach (cell, rte->securityQuals) {
            lfirst(cell) = preprocess_expression(root, (Node*)lfirst(cell), EXPRKIND_QUAL);
        }
    }

    DEBUG_QRW("After preprocess expressions");

    u_sess->opt_cxt.op_work_mem = work_mem_orig;
    root->glob->estiopmem = esti_op_mem_orig;

    /*
     * In some cases we may want to transfer a HAVING clause into WHERE. We
     * cannot do so if the HAVING clause contains aggregates (obviously) or
     * volatile functions (since a HAVING clause is supposed to be executed
     * only once per group).  Also, it may be that the clause is so expensive
     * to execute that we're better off doing it only once per group, despite
     * the loss of selectivity.  This is hard to estimate short of doing the
     * entire planning process twice, so we use a heuristic: clauses
     * containing subplans are left in HAVING.	Otherwise, we move or copy the
     * HAVING clause into WHERE, in hopes of eliminating tuples before
     * aggregation instead of after.
     *
     * If the query has explicit grouping then we can simply move such a
     * clause into WHERE; any group that fails the clause will not be in the
     * output because none of its tuples will reach the grouping or
     * aggregation stage.  Otherwise we must have a degenerate (variable-free)
     * HAVING clause, which we put in WHERE so that query_planner() can use it
     * in a gating Result node, but also keep in HAVING to ensure that we
     * don't emit a bogus aggregated row. (This could be done better, but it
     * seems not worth optimizing.)
     *
     * Note that both havingQual and parse->jointree->quals are in
     * implicitly-ANDed-list form at this point, even though they are declared
     * as Node *.
     * Also, HAVING quals should not be transfered into WHERE clauses, since
     * the rownum is expected to be assigned to tuples before filtered by
     * other HAVING quals.
     */
    if (!parse->unique_check && !expression_contains_rownum((Node*)parse->havingQual))  {
        newHaving = NIL;
        foreach(l, (List *) parse->havingQual)  {
            Node       *havingclause = (Node *)lfirst(l);

            /* 
             * For groupingSets, having clause can only be calculate in havingQual, can not push-down to lefttree's qual.
             * 
             * For example:
             * select sum(a), b from group by rollup(a, b) having b > 10;
             * this mean: group by a, b
             *            group by a
             *            group by ()
             *
             * For "group by ()", we need calculate sum(a) for all lefttree's rows.
             */
            if (contain_agg_clause(havingclause) ||
                contain_volatile_functions(havingclause) ||
                contain_subplans(havingclause)
                || parse->groupingSets) {
                /* keep it in HAVING */
                newHaving = lappend(newHaving, havingclause);
            } else if (parse->groupClause)  {
                /* move it to WHERE */
                parse->jointree->quals = (Node *)
                    lappend((List *) parse->jointree->quals, havingclause);
            } else {
                /* put a copy in WHERE, keep it in HAVING */
                parse->jointree->quals = (Node *)
                    lappend((List *) parse->jointree->quals,
                            copyObject(havingclause));
                newHaving = lappend(newHaving, havingclause);
            }
        }
        parse->havingQual = (Node *) newHaving;
    }


    DEBUG_QRW("After having qual rewrite");

    passdown_itst_keys_to_subroot(root, diskeys);

    /*
     * If we have any outer joins, try to reduce them to plain inner joins.
     * This step is most easily done after we've done expression
     * preprocessing.
     */
    if (hasOuterJoins) {
        reduce_outer_joins(root);
        DEBUG_QRW("After outer-to-inner conversion");
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_STREAM_PLAN)
#endif
        {
            bool support_rewrite = true;
            do {
                if (contain_system_column((Node*)root->parse->targetList)) {
                    support_rewrite = false;
                    break;
                }
                if (join_equal_walker((Node*)root->parse->jointree, NULL)) {
                    support_rewrite = false;
                    break;
                }
                if (!fulljoin_2_left_union_right_anti_support(root->parse)) {
                    support_rewrite = false;
                    break;
                }
                if (contain_volatile_functions((Node*)root->parse)) {
                    support_rewrite = false;
                    break;
                }
                contain_func_context context =
                    init_contain_func_context(list_make3_oid(ECEXTENSIONFUNCOID, ECHADOOPFUNCOID, RANDOMFUNCOID));
                if (contains_specified_func((Node*)root->parse, &context)) {
                    char* func_name = get_func_name(((FuncExpr*)linitial(context.func_exprs))->funcid);
                    ereport(DEBUG2,
                        (errmodule(MOD_OPT_REWRITE),
                            (errmsg("[Not rewrite full Join on true]: %s functions contained.", func_name))));
                    pfree_ext(func_name);
                    list_free_ext(context.funcids);
                    context.funcids = NIL;
                    list_free_ext(context.func_exprs);
                    context.func_exprs = NIL;
                    support_rewrite = false;
                    break;
                }
                if (has_foreign_table_in_rtable(root->parse)) {
                    support_rewrite = false;
                    break;
                }
            } while (0);
            if (support_rewrite) {
                reduce_inequality_fulljoins(root);
                DEBUG_QRW("After full join conversion");
            }
        }
    }

    /*
     * If we have any RTE_RESULT relations, see if they can be deleted from
     * the jointree.  This step is most effectively done after we've done
     * expression preprocessing and outer join reduction.
     */
    if (hasResultRTEs)
        remove_useless_result_rtes(root);

    /*
     * Check if need auto-analyze for current query level.
     * No need to do auto-analyze for query on one table without Groupby.
     */
    if (u_sess->attr.attr_sql.enable_autoanalyze && !u_sess->analyze_cxt.need_autoanalyze && IS_STREAM_PLAN &&
        (list_length(parse->rtable) > 1 || parse->groupClause)) {
        /* inherit upper level and check for current query level */
        u_sess->analyze_cxt.need_autoanalyze = true;
    }
    (void)MemoryContextSwitchTo(oldcontext);

    /*
     * Do the main planning.  If we have an inherited target relation, that
     * needs special processing, else go straight to grouping_planner.
     */
    if (parse->resultRelations && parse->commandType != CMD_INSERT &&
        rt_fetch(linitial_int(parse->resultRelations), parse->rtable)->inh)
    {
        plan = inheritance_planner(root);
        plan->isinherit = true;
    } else {
        plan = internal_grouping_planner(root, tuple_fraction);
        /*
        * Make sure the topmost plan node's targetlist exposes the original
        * column names and other decorative info.  Targetlists generated within
        * the planner don't bother with that stuff, but we must have it on the
        * top-level tlist seen at execution time. 
        */
        if (parse->is_flt_frame && parse->hasTargetSRFs && !IsA(plan, ModifyTable)) {
            apply_tlist_labeling(plan->targetlist, root->processed_tlist);
        }

        /* Change the targetlist chain */
        ExtensiblePlan* tsdbExtensiblePlan;
        bool planForInsertTsdb = u_sess->hook_cxt.forTsdbHook && parse->commandType == CMD_INSERT && IsA(plan, ExtensiblePlan);

        if(planForInsertTsdb) {
            tsdbExtensiblePlan = (ExtensiblePlan*)plan;

            ListCell* lc;
            foreach(lc,tsdbExtensiblePlan->extensible_plans) {
                plan = (Plan*)lfirst(lc);
                break;
            }
        }

        /* If it's not SELECT, we need a ModifyTable node */
        if (parse->commandType != CMD_SELECT) {
            List* withCheckOptionLists = NIL;
            List* returningLists = NIL;
            List* rowMarks = NIL;
            Relation mainRel = NULL;
            Oid taleOid = rt_fetch(linitial_int(parse->resultRelations), parse->rtable)->relid;
            bool partKeyUpdated;
            bool isPartitioned;

            if (OidIsValid(taleOid)) {
                mainRel = RelationIdGetRelation(taleOid);
                isPartitioned = RELATION_IS_PARTITIONED(mainRel);
                RelationClose(mainRel);

                if (isPartitioned) {
                    partKeyUpdated = targetListHasPartitionKey(parse->targetList, taleOid);
                } else {
                    partKeyUpdated = false;
                }
            }

            /*
             * Set up the WITH CHECK OPTION and RETURNING lists-of-lists, if
             * needed.
             */
            if (parse->withCheckOptions)
                withCheckOptionLists = list_make1(parse->withCheckOptions);
            else
                withCheckOptionLists = NIL;

            if (parse->returningList)
                returningLists = list_make1(parse->returningList);
            else
                returningLists = NIL;

            /*
             * If there was a FOR [KEY] UPDATE/SHARE clause, the LockRows node will
             * have dealt with fetching non-locked marked rows, else we need
             * to have ModifyTable do that.
             */
            if (parse->rowMarks)
                rowMarks = NIL;
            else
                rowMarks = root->rowMarks;
#ifdef STREAMPLAN
            plan = (Plan*)make_modifytable(root,
                parse->commandType,
                parse->canSetTag,
                list_make1(parse->resultRelations),
                list_make1(plan),
                withCheckOptionLists,
                returningLists,
                rowMarks,
                SS_assign_special_param(root),
                partKeyUpdated,
                parse->mergeTarget_relation,
                parse->mergeSourceTargetList,
                parse->mergeActionList,
                parse->upsertClause);
#else
            plan = (Plan*)make_modifytable(parse->commandType,
                parse->canSetTag,
                list_make1(parse->resultRelations),
                list_make1(plan),
                withCheckOptionLists,
                returningLists,
                rowMarks,
                SS_assign_special_param(root),
                partKeyUpdated,
                parse->mergeTarget_relation,
                parse->mergeSourceTargetList,
                parse->mergeActionList,
                parse->upsertClause);
#endif
#ifdef PGXC
            plan = pgxc_make_modifytable(root, plan);
#endif
            if(planForInsertTsdb) {
                tsdbExtensiblePlan->extensible_plans = list_make1(plan);
                plan = (Plan*)tsdbExtensiblePlan;
            }
            if (plan->type == T_Stream) {
                ((ModifyTable*)(plan->lefttree))->isReplace = parse->isReplace;
            } else {
                ((ModifyTable*)plan)->isReplace = parse->isReplace;
            }
        }
    }

    /*
     * If any subplans were generated, or if there are any parameters to worry
     * about, build initPlan list and extParam/allParam sets for plan nodes,
     * and attach the initPlans to the top plan node.
     */
    if (plan == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Fail to generate subquery plan."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));

    if (list_length(glob->subplans) != num_old_subplans || root->glob->nParamExec > 0)
        SS_finalize_plan(root, plan, true);

    /* Return internal info if caller wants it */
    if (subroot != NULL)
        *subroot = root;

    /* add not-used hints information to warning string */
    if (parse->hintState)
        desc_hint_in_state(root, parse->hintState);

    /* Fix var's if we have changed var */
    if (root->var_mappings != NIL) {
        fix_vars_plannode(root, plan);
        root->parse->is_from_inlist2join_rewrite = true;
    }

    return plan;
}
```

### C. 源投影下推、近似消费者与引用注入

<a id="source-005"></a>

#### 三个 walker 上下文结构

来源：`src/gausskernel/optimizer/plan/planner.cpp:378-391`；范围：**明确标注的源码片段**。

内容 SHA256：`05c7712020203f7da8123d9b48e034b3c40c4edac358a108f5f936dfb98c2337`。

mpwc 必须在结构开头；approximatePlanSafe 必须按递归作用域保存和恢复。

```cpp
typedef struct AnnOrderByReuseWalkerContext {
    MethodPlanWalkerContext mpwc;
    int mode;
    bool approximatePlanSafe;
} AnnOrderByReuseWalkerContext;

typedef struct AnnOrderBySafetyWalkerContext {
    MethodPlanWalkerContext mpwc;
} AnnOrderBySafetyWalkerContext;

typedef struct UnionAllSourceProjectionContext {
    MethodPlanWalkerContext mpwc;
} UnionAllSourceProjectionContext;

```

<a id="source-006"></a>

#### 距离操作符与 Var 匹配上下文

来源：`src/gausskernel/optimizer/plan/planner.cpp:564-572`；范围：**明确标注的源码片段**。

内容 SHA256：`2c7a491cd3b02b6b9a9b92cc485e922eec7756d56d01c509689f676d41150a7b`。

数字仅属于源仓，不能照搬为 KGN 的 OID。

```cpp
/* Vector distance operator OIDs from the built-in vector catalog entries.
 * The support procedure check below is independent and remains mandatory. */
static const Oid VECTOR_L2_DISTANCE_OPERATOR_OID = 8311;
static const Oid VECTOR_COSINE_DISTANCE_OPERATOR_OID = 8313;

typedef struct UnionAllVarMatchContext {
    const Var* target;
} UnionAllVarMatchContext;

```

<a id="source-007"></a>

#### union_all_expr_uses_var_walker()

来源：`src/gausskernel/optimizer/plan/planner.cpp:573-583`；范围：**完整函数**。

内容 SHA256：`66f0422d32e541efb0e7cf70afd3a4d7c483d3f5b7613f69dad171fb8610aab5`。

```cpp
static bool union_all_expr_uses_var_walker(Node* node, UnionAllVarMatchContext* context)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, Var)) {
        return equal(node, context->target);
    }
    return expression_tree_walker(
        node, (bool (*)())union_all_expr_uses_var_walker, (void*)context);
}
```

<a id="source-008"></a>

#### union_all_expr_uses_var()

来源：`src/gausskernel/optimizer/plan/planner.cpp:585-590`；范围：**完整函数**。

内容 SHA256：`4925e98d7fa176bb4b4c56cc5a6300e827de5d1ff75b851e4b7440732a2a07c6`。

```cpp
static bool union_all_expr_uses_var(Node* node, const Var* target)
{
    UnionAllVarMatchContext context;
    context.target = target;
    return union_all_expr_uses_var_walker(node, &context);
}
```

<a id="source-009"></a>

#### union_all_safe_vector_distance()

来源：`src/gausskernel/optimizer/plan/planner.cpp:599-643`；范围：**完整函数**。

内容 SHA256：`8614f40c4d55bd69b6c2060e7ca958d3a468b4671583d4f2bf41482cbe4e30a1`。

```cpp
static bool union_all_safe_vector_distance(Expr* expr, Index scanrelid, Var** sourceVar)
{
    OpExpr* op = NULL;
    Node* first = NULL;
    Node* second = NULL;
    Var* var = NULL;
    Const* query = NULL;
    Vector* queryVector = NULL;

    if (!IsA(expr, OpExpr)) {
        return false;
    }
    op = (OpExpr*)expr;
    if ((op->opno != VECTOR_L2_DISTANCE_OPERATOR_OID &&
         op->opno != VECTOR_COSINE_DISTANCE_OPERATOR_OID) ||
        op->opresulttype != FLOAT8OID || list_length(op->args) != 2) {
        return false;
    }

    first = (Node*)linitial(op->args);
    second = (Node*)lsecond(op->args);
    if (IsA(first, Var) && IsA(second, Const)) {
        var = (Var*)first;
        query = (Const*)second;
    } else if (IsA(first, Const) && IsA(second, Var)) {
        var = (Var*)second;
        query = (Const*)first;
    } else {
        return false;
    }

    if (var->varno != scanrelid || var->varlevelsup != 0 ||
        var->vartype != VECTOROID || var->vartypmod <= 0 ||
        query->consttype != VECTOROID || query->constisnull) {
        return false;
    }

    queryVector = DatumGetVector(query->constvalue);
    if (queryVector->dim != var->vartypmod) {
        return false;
    }

    *sourceVar = var;
    return true;
}
```

<a id="source-010"></a>

#### union_all_find_source_tle()

来源：`src/gausskernel/optimizer/plan/planner.cpp:645-660`；范围：**完整函数**。

内容 SHA256：`207e62eeacbe5a9fc66c106d8b5c6ef68ee99f34c62f855709d1df0abbda64d8`。

```cpp
static TargetEntry* union_all_find_source_tle(Plan* plan, const Var* sourceVar)
{
    TargetEntry* found = NULL;
    ListCell* lc = NULL;

    foreach (lc, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (IsA(tle->expr, Var) && equal(tle->expr, sourceVar)) {
            if (found != NULL) {
                return NULL;
            }
            found = tle;
        }
    }
    return found;
}
```

<a id="source-011"></a>

#### union_all_collect_source_chain()

来源：`src/gausskernel/optimizer/plan/planner.cpp:663-690`；范围：**完整函数**。

内容 SHA256：`229ef7d6f2ad1afec9dee7ff2783bd320d4a974273bb207dd1129bdf6d3d6891`。

```cpp
static bool union_all_collect_source_chain(Plan* plan, List** chain, Index* scanrelid)
{
    if (plan == NULL) {
        return false;
    }

    *chain = lappend(*chain, plan);
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_IndexScan:
        case T_AnnIndexScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_TidRangeScan:
            *scanrelid = ((Scan*)plan)->scanrelid;
            return *scanrelid > 0;
        case T_Hash:
        case T_Material:
        case T_Sort:
            if (plan->righttree == NULL) {
                return union_all_collect_source_chain(plan->lefttree, chain, scanrelid);
            }
            break;
        default:
            break;
    }
    return false;
}
```

<a id="source-012"></a>

#### union_all_join_uses_source_elsewhere()

来源：`src/gausskernel/optimizer/plan/planner.cpp:692-727`；范围：**完整函数**。

内容 SHA256：`c00082f6fb8d300d6645df61670735819612f1a10c188ff831b5fa08124c2302`。

```cpp
static bool union_all_join_uses_source_elsewhere(Join* join, TargetEntry* candidate, const Var* sourceVar)
{
    ListCell* lc = NULL;

    foreach (lc, join->plan.targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle != candidate && union_all_expr_uses_var((Node*)tle->expr, sourceVar)) {
            return true;
        }
    }
    if (union_all_expr_uses_var((Node*)join->plan.qual, sourceVar) ||
        union_all_expr_uses_var((Node*)join->plan.var_list, sourceVar) ||
        union_all_expr_uses_var((Node*)join->plan.distributed_keys, sourceVar) ||
        union_all_expr_uses_var((Node*)join->joinqual, sourceVar) ||
        union_all_expr_uses_var((Node*)join->nulleqqual, sourceVar)) {
        return true;
    }

    if (IsA(join, MergeJoin) &&
        union_all_expr_uses_var((Node*)((MergeJoin*)join)->mergeclauses, sourceVar)) {
        return true;
    }
    if (IsA(join, HashJoin) &&
        union_all_expr_uses_var((Node*)((HashJoin*)join)->hashclauses, sourceVar)) {
        return true;
    }
    if (IsA(join, NestLoop)) {
        foreach (lc, ((NestLoop*)join)->nestParams) {
            NestLoopParam* param = (NestLoopParam*)lfirst(lc);
            if (union_all_expr_uses_var((Node*)param->paramval, sourceVar)) {
                return true;
            }
        }
    }
    return false;
}
```

<a id="source-013"></a>

#### union_all_replace_source_chain()

来源：`src/gausskernel/optimizer/plan/planner.cpp:729-778`；范围：**完整函数**。

内容 SHA256：`7bb055b2019e8089f05b40cd6413973c8672548b686009b3385bb6b0b6ba28a5`。

```cpp
static bool union_all_replace_source_chain(List* chain, TargetEntry* candidate, const Var* sourceVar)
{
    ListCell* lc = NULL;
    List* sources = NIL;

    /*
     * Validate the complete chain and remember the matching entries before
     * changing any node.  Projection-transparent nodes can share their
     * targetlist verbatim (make_sort(), for example, assigns the child's
     * targetlist pointer).  Re-finding a Var after changing an earlier alias
     * would therefore fail and could leave a partially rewritten plan.
     */
    foreach (lc, chain) {
        Plan* plan = (Plan*)lfirst(lc);
        TargetEntry* source = union_all_find_source_tle(plan, sourceVar);
        ListCell* tlc = NULL;

        if (source == NULL) {
            list_free(sources);
            return false;
        }
        foreach (tlc, plan->targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(tlc);
            if (tle != source && union_all_expr_uses_var((Node*)tle->expr, sourceVar)) {
                list_free(sources);
                return false;
            }
        }
        if (IsA(plan, Sort)) {
            Sort* sort = (Sort*)plan;
            for (int i = 0; i < sort->numCols; i++) {
                if (sort->sortColIdx[i] == source->resno) {
                    list_free(sources);
                    return false;
                }
            }
        }

        if (!list_member_ptr(sources, source)) {
            sources = lappend(sources, source);
        }
    }

    foreach (lc, sources) {
        TargetEntry* source = (TargetEntry*)lfirst(lc);
        source->expr = (Expr*)copyObject(candidate->expr);
    }
    list_free(sources);
    return true;
}
```

<a id="source-014"></a>

#### push_down_union_all_source_projection_walker()

来源：`src/gausskernel/optimizer/plan/planner.cpp:780-817`；范围：**完整函数**。

内容 SHA256：`fb47f8545bf91a71daa2383d9a182d5faf22f056a45d27a5f738e6686e8f41b8`。

```cpp
static bool push_down_union_all_source_projection_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, NestLoop) || IsA(node, MergeJoin) || IsA(node, HashJoin)) {
        Join* join = (Join*)node;
        Plan* preservedSide = NULL;
        List* chain = NIL;
        Index scanrelid = 0;
        ListCell* lc = NULL;

        if (join->jointype == JOIN_ANTI || join->jointype == JOIN_LEFT_ANTI_FULL) {
            preservedSide = join->plan.lefttree;
        } else if (join->jointype == JOIN_RIGHT_ANTI || join->jointype == JOIN_RIGHT_ANTI_FULL) {
            preservedSide = join->plan.righttree;
        }

        if (preservedSide != NULL &&
            union_all_collect_source_chain(preservedSide, &chain, &scanrelid)) {
            foreach (lc, join->plan.targetlist) {
                TargetEntry* candidate = (TargetEntry*)lfirst(lc);
                Var* sourceVar = NULL;

                if (union_all_safe_vector_distance(candidate->expr, scanrelid, &sourceVar) &&
                    !union_all_join_uses_source_elsewhere(join, candidate, sourceVar) &&
                    union_all_replace_source_chain(chain, candidate, sourceVar)) {
                    break;
                }
            }
        }
        list_free(chain);
    }

    return plan_tree_walker(
        node, (MethodWalker)push_down_union_all_source_projection_walker, context);
}
```

<a id="source-015"></a>

#### push_down_union_all_source_projections()

来源：`src/gausskernel/optimizer/plan/planner.cpp:819-832`；范围：**完整函数**。

内容 SHA256：`c0687fb1195a124f2b41a22140bb235a2f64721488bd42a317fc289ba105ca85`。

```cpp
static void push_down_union_all_source_projections(PlannerGlobal* glob, Plan* topPlan)
{
    UnionAllSourceProjectionContext context;
    errno_t rc = memset_s(&context, sizeof(context), 0, sizeof(context));
    securec_check(rc, "\0", "\0");

    context.mpwc.base.node = (Node*)glob;
    context.mpwc.base.init_plans = NIL;
    context.mpwc.base.traverse_flag =
        (bool*)palloc0(sizeof(bool) * Max(1, list_length(glob->subplans)));
    (void)push_down_union_all_source_projection_walker((Node*)topPlan, &context);
    list_free_ext(context.mpwc.base.init_plans);
    pfree_ext(context.mpwc.base.traverse_flag);
}
```

<a id="source-016"></a>

#### ann_orderby_safety_walker()

来源：`src/gausskernel/optimizer/plan/planner.cpp:834-856`；范围：**完整函数**。

内容 SHA256：`ab839fe0e1b34389b73cdbfb3be536b0f887637db0287fe2f2e1c5dca5729951`。

```cpp
static bool ann_orderby_safety_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    /* These execution domains do not support AnnOrderByRef in V1. */
    switch (nodeTag(node)) {
        case T_Stream:
        case T_VecStream:
        case T_RemoteQuery:
        case T_VecRemoteQuery:
        case T_LockRows:
        case T_VecLockRows:
        case T_ModifyTable:
        case T_VecModifyTable:
            return true;
        default:
            break;
    }

    return plan_tree_walker(node, (MethodWalker)ann_orderby_safety_walker, context);
}
```

<a id="source-017"></a>

#### ann_orderby_plan_is_unsafe()

来源：`src/gausskernel/optimizer/plan/planner.cpp:858-872`；范围：**完整函数**。

内容 SHA256：`0117b90770afe5bb30992d33387ad2a81d98c1aad8826c8113da926d7e1305a9`。

```cpp
static bool ann_orderby_plan_is_unsafe(PlannerGlobal* glob, Plan* topPlan)
{
    AnnOrderBySafetyWalkerContext safety;
    errno_t rc = memset_s(&safety, sizeof(safety), 0, sizeof(safety));
    securec_check(rc, "\0", "\0");

    safety.mpwc.base.node = (Node*)glob;
    safety.mpwc.base.init_plans = NIL;
    safety.mpwc.base.traverse_flag =
        (bool*)palloc0(sizeof(bool) * Max(1, list_length(glob->subplans)));
    bool unsafe = ann_orderby_safety_walker((Node*)topPlan, &safety);
    list_free_ext(safety.mpwc.base.init_plans);
    pfree_ext(safety.mpwc.base.traverse_flag);
    return unsafe;
}
```

<a id="source-018"></a>

#### ann_orderby_join_expr_consumes_distance()

来源：`src/gausskernel/optimizer/plan/planner.cpp:883-910`；范围：**完整函数**。

内容 SHA256：`400b8258f2a5acf6d59f352887e627df7cecda5ea8c204adecaab25258b37d0f`。

```cpp
static bool ann_orderby_join_expr_consumes_distance(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Var) || IsA(node, Param)) {
        Oid typeId = getBaseType(exprType(node));
        /* After setrefs a whole-row value can occupy an ordinary slot, so
         * checking only Var.varattno == 0 would still miss the dependency. */
        if (typeId == FLOAT8OID || type_is_rowtype(typeId)) {
            return true;
        }
    }
    if (IsA(node, AnnOrderByRef)) {
        return true;
    }
    if (IsA(node, OpExpr)) {
        Oid opno = ((OpExpr*)node)->opno;
        if (opno == VECTOR_L2_DISTANCE_OPERATOR_OID ||
            opno == VECTOR_COSINE_DISTANCE_OPERATOR_OID) {
            return true;
        }
    }

    return expression_tree_walker(
        node, (bool (*)())ann_orderby_join_expr_consumes_distance, context);
}
```

<a id="source-019"></a>

#### ann_orderby_join_allows_approx()

来源：`src/gausskernel/optimizer/plan/planner.cpp:912-957`；范围：**完整函数**。

内容 SHA256：`4af5dd843d8238e46b6fe0152e51a31d9f83dd7c0e27ec63dec473e2ca63cc59`。

```cpp
static bool ann_orderby_join_allows_approx(Node* node)
{
    Join* join = (Join*)node;

    /* plan.qual filters output rows; joinqual/nulleqqual determine matches. */
    if (ann_orderby_join_expr_consumes_distance((Node*)join->plan.qual, NULL) ||
        ann_orderby_join_expr_consumes_distance((Node*)join->joinqual, NULL) ||
        ann_orderby_join_expr_consumes_distance((Node*)join->nulleqqual, NULL)) {
        return false;
    }

    switch (nodeTag(node)) {
        case T_NestLoop: {
            ListCell* lc = NULL;
            foreach (lc, ((NestLoop*)node)->nestParams) {
                NestLoopParam* param = (NestLoopParam*)lfirst(lc);
                if (ann_orderby_join_expr_consumes_distance((Node*)param->paramval, NULL)) {
                    return false;
                }
            }
            break;
        }
        case T_MergeJoin:
            if (ann_orderby_join_expr_consumes_distance(
                    (Node*)((MergeJoin*)node)->mergeclauses, NULL)) {
                return false;
            }
            break;
        case T_HashJoin:
            if (ann_orderby_join_expr_consumes_distance(
                    (Node*)((HashJoin*)node)->hashclauses, NULL)) {
                return false;
            }
#ifdef USE_SPQ
            if (ann_orderby_join_expr_consumes_distance(
                    (Node*)((HashJoin*)node)->hashqualclauses, NULL)) {
                return false;
            }
#endif
            break;
        default:
            return false;
    }

    return true;
}
```

<a id="source-020"></a>

#### ann_orderby_node_allows_approx()

来源：`src/gausskernel/optimizer/plan/planner.cpp:959-1025`；范围：**完整函数**。

内容 SHA256：`0262d1f903eb44ea18442b0efd7ef1c6288638f169d58513434fe59fb160476b`。

```cpp
static bool ann_orderby_node_allows_approx(Node* node)
{
    /* Only transparent consumers may inherit an approximate distance.
     * A blacklist of joins/aggregates misses SubqueryScan.qual and Result
     * expressions, which can turn rounding into a different query result.
     * This runs after setrefs: pass-through outputs are Vars, not copies
     * of the original ORDER BY expression. Unknown consumers fail closed.
     */
    switch (nodeTag(node)) {
        case T_List:
            /* Append/MergeAppend children are visited through a List. */
            return true;
        case T_AnnIndexScan:
            /* Scan quals and other scan outputs still evaluate their own
             * exact expressions; only whole matching outputs are replaced. */
            return true;
        case T_BaseResult:
            if (((BaseResult*)node)->resconstantqual != NULL) {
                return false;
            }
            break;
        case T_Limit:
        case T_Sort:
        case T_Material:
        case T_Append:
        case T_MergeAppend:
        case T_SubqueryScan:
            break;
        case T_Hash:
            /* Hash keys belong to the parent HashJoin, whose checks have
             * already restricted the inherited safety context.  Hash stores
             * child tuples (including the distance) unchanged, even across
             * batches; retain the common qual/transparent-tlist checks below.
             * Never restore a context rejected by an ancestor consumer. */
            if (((Plan*)node)->lefttree == NULL || ((Plan*)node)->righttree != NULL) {
                return false;
            }
            break;
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin:
            if (!ann_orderby_join_allows_approx(node)) {
                return false;
            }
            break;
        default:
            /* Includes Param: plan_tree_walker can reach an InitPlan via
             * its PARAM_EXEC without invoking our T_SubPlan callback. */
            return false;
    }

    Plan* plan = (Plan*)node;
    /* A qualifying Join has already proved that plan.qual is independent of
     * the distance.  Other transparent nodes still require an empty qual. */
    if (!IsA(node, NestLoop) && !IsA(node, MergeJoin) && !IsA(node, HashJoin) &&
        plan->qual != NIL) {
        return false;
    }
    ListCell* lc = NULL;
    foreach (lc, plan->targetlist) {
        Expr* expr = ((TargetEntry*)lfirst(lc))->expr;
        if (!IsA(expr, Var) && !IsA(expr, Const)) {
            return false;
        }
    }
    return true;
}
```

<a id="source-021"></a>

#### rewrite_ann_orderby_distance_walker()

来源：`src/gausskernel/optimizer/plan/planner.cpp:1033-1110`；范围：**完整函数**。

内容 SHA256：`987d47be3529a80b981f06f10df964430cd297d64d3e40cc525ab2c5b9d4f80c`。

```cpp
static bool rewrite_ann_orderby_distance_walker(Node* node, void* context)
{
    AnnOrderByReuseWalkerContext* walkerCxt = (AnnOrderByReuseWalkerContext*)context;
    bool parentApproximatePlanSafe = walkerCxt->approximatePlanSafe;

    if (node == NULL) {
        return false;
    }

    if (!ann_orderby_node_allows_approx(node)) {
        walkerCxt->approximatePlanSafe = false;
    }

    if (IsA(node, AnnIndexScan)) {
        AnnIndexScan* scan = (AnnIndexScan*)node;

        if ((walkerCxt->mpwc.status & UNDER_STREAM) == 0 &&
            !scan->index_only_scan && !scan->is_ustore && !scan->scan.isPartTbl &&
            scan->scan.plan.dop <= 1 && !ScanDirectionIsBackward(scan->indexorderdir) &&
            list_length(scan->indexorderbyorig) == 1 && scan->scan.plan.targetlist != NIL) {
            Relation indexRel = index_open(scan->indexid, AccessShareLock);
            AnnOrderByDistanceQuality quality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;

            Expr* orderExpr = (Expr*)linitial(scan->indexorderbyorig);

            if (indexRel->rd_rel->relam == DISKANN_AM_OID &&
                TupleDescAttr(indexRel->rd_att, 0)->atttypid == VECTOROID &&
                !RELATION_HAS_BUCKET(indexRel) && IsA(orderExpr, OpExpr)) {
                DiskAnnOptions* opts = (DiskAnnOptions*)indexRel->rd_options;
                Oid distanceProc = index_getprocid(indexRel, 1, DISKANN_DISTANCE_PROC);
                Oid normProc = index_getprocid(indexRel, 1, DISKANN_NORM_PROC);
                OpExpr* distanceOp = (OpExpr*)orderExpr;
                bool operatorShapeMatches = list_length(distanceOp->args) == 2 &&
                    distanceOp->opresulttype == FLOAT8OID &&
                    distanceOp->opcollid == InvalidOid &&
                    distanceOp->inputcollid == InvalidOid &&
                    exprType((Node*)linitial(distanceOp->args)) == VECTOROID &&
                    exprType((Node*)lsecond(distanceOp->args)) == VECTOROID;

                /* PQ is excluded until its final rerank/value provenance is
                 * independently covered by the reuse contract. */
                if (operatorShapeMatches && (opts == NULL || !opts->enablePQ)) {
                    if ((walkerCxt->mode & ANN_ORDERBY_REUSE_L2) != 0 &&
                        distanceProc == L2_FUNC_OID && !OidIsValid(normProc) &&
                        distanceOp->opno == VECTOR_L2_DISTANCE_OPERATOR_OID) {
                        quality = ANN_ORDERBY_DISTANCE_EXACT_L2;
                    } else if ((walkerCxt->mode & ANN_ORDERBY_REUSE_COSINE) != 0 &&
                               walkerCxt->approximatePlanSafe &&
                               distanceProc == IP_FUNC_OID && OidIsValid(normProc) &&
                               distanceOp->opno == VECTOR_COSINE_DISTANCE_OPERATOR_OID) {
                        quality = ANN_ORDERBY_DISTANCE_APPROX_COSINE;
                    }
                }
            }
            index_close(indexRel, AccessShareLock);

            if (quality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
                ListCell* lc = NULL;

                foreach (lc, scan->scan.plan.targetlist) {
                    TargetEntry* tle = (TargetEntry*)lfirst(lc);

                    if (equal(tle->expr, orderExpr)) {
                        AnnOrderByRef* ref = makeNode(AnnOrderByRef);
                        ref->orderby_index = 0;
                        ref->quality = (int)quality;
                        ref->original_expr = tle->expr;
                        tle->expr = (Expr*)ref;
                    }
                }
            }
        }
    }

    bool found = plan_tree_walker(node, (MethodWalker)rewrite_ann_orderby_distance_walker, context);
    walkerCxt->approximatePlanSafe = parentApproximatePlanSafe;
    return found;
}
```

<a id="source-022"></a>

#### rewrite_ann_orderby_distance_refs()

来源：`src/gausskernel/optimizer/plan/planner.cpp:1112-1137`；范围：**完整函数**。

内容 SHA256：`42715a894f55164d8b839fc6f9ef51b3c42a2283cc66c9d7ef55846c512e5234`。

```cpp
static void rewrite_ann_orderby_distance_refs(PlannerGlobal* glob, Plan* topPlan)
{
    /* V1 is local-only: do not introduce the new executor node into a plan
     * that can be shipped to another server version. */
    if (g_instance.role != VSINGLENODE ||
        t_thrd.proc->workingVersionNum < ANN_ORDERBY_DISTANCE_REUSE_VERSION_NUM ||
        ann_orderby_plan_is_unsafe(glob, topPlan)) {
        return;
    }

    AnnOrderByReuseWalkerContext context;
    errno_t rc = memset_s(&context, sizeof(context), 0, sizeof(context));
    securec_check(rc, "\0", "\0");

    context.mpwc.base.node = (Node*)glob;
    context.mpwc.base.init_plans = NIL;
    context.mpwc.base.traverse_flag =
        (bool*)palloc0(sizeof(bool) * Max(1, list_length(glob->subplans)));
    context.mode = u_sess->attr.attr_sql.ann_orderby_distance_reuse;
    context.approximatePlanSafe = true;

    (void)rewrite_ann_orderby_distance_walker((Node*)topPlan, &context);

    list_free_ext(context.mpwc.base.init_plans);
    pfree_ext(context.mpwc.base.traverse_flag);
}
```

### D. setrefs 与真实槽位引用

<a id="source-023"></a>

#### set_plan_references()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:209-300`；范围：**完整函数**。

内容 SHA256：`fc9f1146b00b9f2d4c80d71cd1260dab070b687e16abceee5b87d85daa1b6e27`。

```cpp
Plan* set_plan_references(PlannerInfo* root, Plan* plan)
{
    PlannerGlobal* glob = root->glob;
    int rtoffset = list_length(glob->finalrtable);
    ListCell* lc = NULL;

    /*
     * In the flat rangetable, we zero out substructure pointers that are not
     * needed by the executor; this reduces the storage space and copying cost
     * for cached plans.  We keep only the alias and eref Alias fields, which
     * are needed by EXPLAIN, and the selectedCols and modifiedCols bitmaps,
     * which are needed for executor-startup permissions checking and for
     * trigger event checking.
     */
    foreach (lc, root->parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        RangeTblEntry* newrte = NULL;

        /* flat copy to duplicate all the scalar fields */
        newrte = (RangeTblEntry*)palloc(sizeof(RangeTblEntry));
        errno_t rc = memcpy_s(newrte, sizeof(RangeTblEntry), rte, sizeof(RangeTblEntry));
        securec_check(rc, "\0", "\0");

        /*
         * zap unneeded sub-structure, keep nerte->securityQuals here
         * because we need this information when execute explain query.
         */
        newrte->subquery = NULL;
        newrte->joinaliasvars = NIL;
        newrte->funcexpr = NULL;
        newrte->funccoltypes = NIL;
        newrte->funccoltypmods = NIL;
        newrte->funccolcollations = NIL;
        newrte->tablefunc = NULL;
        newrte->values_lists = NIL;
        newrte->values_collations = NIL;
        newrte->ctecoltypes = NIL;
        newrte->ctecoltypmods = NIL;
        newrte->ctecolcollations = NIL;

        glob->finalrtable = lappend(glob->finalrtable, newrte);

        /*
         * If it's a plain relation RTE, add the table to relationOids.
         *
         * We do this even though the RTE might be unreferenced in the plan
         * tree; this would correspond to cases such as views that were
         * expanded, child tables that were eliminated by constraint
         * exclusion, etc.	Schema invalidation on such a rel must still force
         * rebuilding of the plan.
         *
         * Note we don't bother to avoid duplicate list entries.  We could,
         * but it would probably cost more cycles than it would save.
         */
        if (newrte->rtekind == RTE_RELATION)
            glob->relationOids = lappend_oid(glob->relationOids, newrte->relid);
    }

    /*
     * Check for RT index overflow; it's very unlikely, but if it did happen,
     * the executor would get confused by varnos that match the special varno
     * values.
     */
    if (IS_SPECIAL_VARNO(list_length(glob->finalrtable)))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("too many range table entries when set plan reference."))));

    /*
     * Adjust RT indexes of PlanRowMarks and add to final rowmarks list
     */
    foreach (lc, root->rowMarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(lc);
        PlanRowMark* newrc = NULL;

        AssertEreport(IsA(rc, PlanRowMark), MOD_OPT, "type plan row mark is required.");

        /* flat copy is enough since all fields are scalars */
        newrc = (PlanRowMark*)palloc(sizeof(PlanRowMark));
        errno_t ret = memcpy_s(newrc, sizeof(PlanRowMark), rc, sizeof(PlanRowMark));
        securec_check(ret, "\0", "\0");
        /* adjust indexes ... but *not* the rowmarkId */
        newrc->rti += rtoffset;
        newrc->prti += rtoffset;

        glob->finalrowmarks = lappend(glob->finalrowmarks, newrc);
    }

    /* Now fix the Plan tree */
    return set_plan_refs(root, plan, rtoffset);
}
```

<a id="source-024"></a>

#### set_plan_refs()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:337-1177`；范围：**完整函数**。

内容 SHA256：`1535a9175978e79347fe15464d123f5eab5f9313c529d0ef88187e316281e4ba`。

```cpp
static Plan* set_plan_refs(PlannerInfo* root, Plan* plan, int rtoffset)
{
    ListCell* l = NULL;

    if (plan == NULL)
        return NULL;

    /*
     * Plan-type-specific fixes
     */
    switch (nodeTag(plan)) {
        case T_SeqScan:
#ifdef USE_SPQ
        case T_SpqSeqScan:
#endif
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
#ifdef ENABLE_HTAP
        case T_IMCStoreScan:
#ifdef USE_SPQ
        case T_SpqCStoreScan:
#endif
#endif
        case T_CStoreScan: {
            Scan* splan = (Scan*)plan;
            splan->scanrelid += rtoffset;
            splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
            splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            if (splan->plan.distributed_keys != NIL) {
                splan->plan.distributed_keys = fix_scan_list(root, splan->plan.distributed_keys, rtoffset);
            }
            if (splan->tablesample) {
                splan->tablesample = (TableSampleClause*)fix_scan_expr(root, (Node*)splan->tablesample, rtoffset);
            }
        } break;
#ifdef USE_SPQ
        case T_SpqIndexScan:
#endif
        case T_IndexScan: {
            IndexScan* splan = (IndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indexorderby = fix_scan_list(root, splan->indexorderby, rtoffset);
            splan->indexorderbyorig = fix_scan_list(root, splan->indexorderbyorig, rtoffset);
        } break;
#ifdef USE_SPQ
        case T_SpqIndexOnlyScan:
#endif
        case T_IndexOnlyScan: {
            IndexOnlyScan* splan = (IndexOnlyScan*)plan;
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            return set_indexonlyscan_references(root, splan, rtoffset);
        } break;
        case T_AnnIndexScan: {
            AnnIndexScan* splan = (AnnIndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indexorderby = fix_scan_list(root, splan->indexorderby, rtoffset);
            splan->indexorderbyorig = fix_scan_list(root, splan->indexorderbyorig, rtoffset);
        } break;
        case T_CStoreIndexScan: {
            CStoreIndexScan* splan = (CStoreIndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indexorderby = fix_scan_list(root, splan->indexorderby, rtoffset);
            splan->indexorderbyorig = fix_scan_list(root, splan->indexorderbyorig, rtoffset);
            splan->cstorequal = fix_scan_list(root, splan->cstorequal, rtoffset);
            splan->baserelcstorequal = fix_scan_list(root, splan->baserelcstorequal, rtoffset);
            splan->indextlist = fix_scan_list(root, splan->indextlist, rtoffset);
        } break;
#ifdef USE_SPQ
        case T_SpqBitmapHeapScan:
#endif
        case T_BitmapIndexScan: {
            BitmapIndexScan* splan = (BitmapIndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            /* no need to fix targetlist and qual */
            AssertEreport(splan->scan.plan.targetlist == NIL && splan->scan.plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
        } break;
        case T_BitmapHeapScan: {
            BitmapHeapScan* splan = (BitmapHeapScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->bitmapqualorig = fix_scan_list(root, splan->bitmapqualorig, rtoffset);
        } break;
        case T_CStoreIndexCtidScan: {
            CStoreIndexCtidScan* splan = (CStoreIndexCtidScan*)plan;

            splan->scan.scanrelid += rtoffset;
            /* no need to fix targetlist and qual */
            AssertEreport(splan->scan.plan.targetlist == NIL && splan->scan.plan.qual == NIL,
                MOD_OPT,
                "targetlist and quals hould be null");
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->cstorequal = fix_scan_list(root, splan->cstorequal, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indextlist = fix_scan_list(root, splan->indextlist, rtoffset);
        } break;
        case T_CStoreIndexHeapScan: {
            CStoreIndexHeapScan* splan = (CStoreIndexHeapScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->bitmapqualorig = fix_scan_list(root, splan->bitmapqualorig, rtoffset);
        } break;
        case T_TidScan: {
            TidScan* splan = (TidScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->tidquals = fix_scan_list(root, splan->tidquals, rtoffset);
        } break;
        case T_TidRangeScan: {
                TidRangeScan *splan = (TidRangeScan *) plan;

                splan->scan.scanrelid += rtoffset;
                splan->scan.plan.targetlist =
                    fix_scan_list(root, splan->scan.plan.targetlist,
                                  rtoffset);
                splan->scan.plan.qual =
                    fix_scan_list(root, splan->scan.plan.qual,
                                  rtoffset);
                splan->tidrangequals =
                    fix_scan_list(root, splan->tidrangequals,
                                  rtoffset);
            }
            break;
        case T_SubqueryScan:
        case T_VecSubqueryScan:
            /* Needs special treatment, see comments below */
            return set_subqueryscan_references(root, (SubqueryScan*)plan, rtoffset);
        case T_FunctionScan: {
            FunctionScan* splan = (FunctionScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->funcexpr = fix_scan_expr(root, splan->funcexpr, rtoffset);
        } break;
        case T_TableFuncScan: {
                TableFuncScan *splan = (TableFuncScan *) plan;

                splan->scan.scanrelid += rtoffset;
                splan->scan.plan.targetlist =
                    fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
                splan->scan.plan.qual =
                    fix_scan_list(root, splan->scan.plan.qual, rtoffset);
                splan->tablefunc = (TableFunc *)
                    fix_scan_expr(root, (Node *) splan->tablefunc, rtoffset);
            } break;
        case T_ValuesScan: {
            ValuesScan* splan = (ValuesScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->values_lists = fix_scan_list(root, splan->values_lists, rtoffset);
        } break;
        case T_CteScan: {
            CteScan* splan = (CteScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);

            /* set plan ref for the subplan of recursive union */
            if (STREAM_RECURSIVECTE_SUPPORTED) {
                CteScan* cte_scan = (CteScan*)plan;

                RecursiveUnion* ru_plan = (RecursiveUnion*)list_nth(root->glob->subplans, cte_scan->ctePlanId - 1);
                PlannerInfo* subroot = (PlannerInfo*)list_nth(root->glob->subroots, cte_scan->ctePlanId - 1);

                if (IsA(ru_plan, RecursiveUnion)) {
                    set_plan_references(subroot, (Plan*)ru_plan);
                }
            }
        } break;
        case T_WorkTableScan: {
            WorkTableScan* splan = (WorkTableScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
        } break;
#ifdef PGXC
        case T_RemoteQuery:
        case T_VecRemoteQuery: {
            RemoteQuery* splan = (RemoteQuery*)plan;

            /*
             * If base_tlist is set, it means that we have a reduced remote
             * query plan. So need to set the var references accordingly.
             */
            if (splan->base_tlist)
                set_remote_references(root, splan, rtoffset);
#ifdef STREAMPLAN
            if (splan->is_simple && !splan->rq_need_proj) {
                set_dummy_tlist_references(plan, rtoffset);
            } else {
                splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            }
#endif
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->base_tlist = fix_scan_list(root, splan->base_tlist, rtoffset);
            splan->scan.scanrelid += rtoffset;
        } break;
#endif
        case T_ExtensiblePlan:
            set_extensibleplan_references(root, (ExtensiblePlan*)plan, rtoffset);
            break;
        case T_ForeignScan:
        case T_VecForeignScan: {
            set_foreignscan_references(root, (ForeignScan*) plan, rtoffset);
        } break;
        case T_NestLoop:
        case T_VecNestLoop:
        case T_MergeJoin:
        case T_VecMergeJoin:
        case T_AsofJoin:
        case T_VecAsofJoin:
        case T_HashJoin:
        case T_VecHashJoin:
            set_join_references(root, (Join*)plan, rtoffset);
            break;

        case T_PartIterator:
        case T_VecPartIterator: {
            PartIterator* splan = (PartIterator*)plan;
            switch (nodeTag(splan->plan.lefttree)) {
                case T_SeqScan:
                case T_CStoreScan:
#ifdef ENABLE_HTAP
                case T_IMCStoreScan:
#ifdef USE_SPQ
                case T_SpqCStoreScan:
#endif
#endif
#ifdef ENABLE_MULTIPLE_NODES
                case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
                case T_IndexScan:
                case T_IndexOnlyScan:
                case T_AnnIndexScan:
                case T_BitmapHeapScan:
                case T_TidScan:
                case T_CStoreIndexScan:
                case T_CStoreIndexCtidScan:
                case T_CStoreIndexHeapScan:
                    splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                    if (splan->plan.distributed_keys != NIL) {
                        splan->plan.distributed_keys = fix_scan_list(root, splan->plan.distributed_keys, rtoffset);
                    }
                    break;
                default:
                    set_dummy_tlist_references(plan, rtoffset);
            }
        } break;

        case T_StartWithOp: {
            set_dummy_tlist_references(plan, rtoffset);
        } break;

        case T_Hash:
        case T_Material:
        case T_Memoize:
        case T_VecMaterial:
        case T_Sort:
        case T_SortGroup:
        case T_VecSort:
        case T_Unique:
        case T_VecUnique:
        case T_SetOp:
        case T_VecSetOp:
        case T_VecToRow:
        case T_RowToVec:

            /*
             * These plan types don't actually bother to evaluate their
             * targetlists, because they just return their unmodified input
             * tuples.	Even though the targetlist won't be used by the
             * executor, we fix it up for possible use by EXPLAIN (not to
             * mention ease of debugging --- wrong varnos are very confusing).
             */
            set_dummy_tlist_references(plan, rtoffset);

            /*
             * Since these plan types don't check quals either, we should not
             * find any qual expression attached to them.
             */
            AssertEreport(plan->qual == NIL, MOD_OPT, "qual should be null");
            break;
        case T_LockRows: {
            LockRows* splan = (LockRows*)plan;

            /*
             * Like the plan types above, LockRows doesn't evaluate its
             * tlist or quals.	But we have to fix up the RT indexes in
             * its rowmarks.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");

            foreach (l, splan->rowMarks) {
                PlanRowMark* rc = (PlanRowMark*)lfirst(l);

                rc->rti += rtoffset;
                rc->prti += rtoffset;
            }
        } break;
        case T_HybridRecall: {
            HybridRecall* splan = (HybridRecall*)plan;

            /*
             * The fusion node owns its query targetlist and materializes
             * the slot itself (Var->candidate doc key, Const->value or
             * fused score); nothing above evaluates these expressions, so
             * the raw planner-format tlist is deliberately left as-is.
             */
            splan->final_topk = (Expr*)fix_scan_expr(root, (Node*)splan->final_topk, rtoffset);
            splan->text_candidates = (Expr*)fix_scan_expr(root, (Node*)splan->text_candidates, rtoffset);
            splan->vector_candidates = (Expr*)fix_scan_expr(root, (Node*)splan->vector_candidates, rtoffset);
            splan->recheck_quals = (List*)fix_scan_list(root, splan->recheck_quals, rtoffset);
        } break;
        case T_Limit:
        case T_VecLimit: {
            Limit* splan = (Limit*)plan;

            /*
             * Like the plan types above, Limit doesn't evaluate its tlist
             * or quals.  It does have live expressions for limit/offset,
             * however; and those cannot contain subplan variable refs, so
             * fix_scan_expr works for them.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");

            splan->limitOffset = fix_scan_expr(root, splan->limitOffset, rtoffset);
            splan->limitCount = fix_scan_expr(root, splan->limitCount, rtoffset);
        } break;
        case T_Agg:
        case T_VecAgg:
#ifdef PGXC
            /* If the lower plan is RemoteQuery plan, adjust the aggregates */
            if (IS_STREAM_PLAN) {
                set_upper_references(root, plan, rtoffset);
                break;
            } else
                pgxc_set_agg_references(root, (Agg*)plan);
                /* Fall through */
#endif /* PGXC */
        case T_Group:
        case T_VecGroup:
            set_upper_references(root, plan, rtoffset);
            break;
        case T_VecWindowAgg:
        case T_WindowAgg: {
            WindowAgg* wplan = (WindowAgg*)plan;

            set_upper_references(root, plan, rtoffset);

            /*
             * Like Limit node limit/offset expressions, WindowAgg has
             * frame offset expressions, which cannot contain subplan
             * variable refs, so fix_scan_expr works for them.
             */
            wplan->startOffset = fix_scan_expr(root, wplan->startOffset, rtoffset);
            wplan->endOffset = fix_scan_expr(root, wplan->endOffset, rtoffset);
        } break;
        case T_BaseResult: {
            BaseResult* splan = (BaseResult*)plan;

            /*
             * Result may or may not have a subplan; if not, it's more
             * like a scan node than an upper node.
             */
            if (splan->plan.lefttree != NULL)
                set_upper_references(root, plan, rtoffset);
            else {
                splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            }
            /* resconstantqual can't contain any subplan variable refs */
            splan->resconstantqual = fix_scan_expr(root, splan->resconstantqual, rtoffset);
        } break;
#ifdef USE_SPQ
        case T_Result: {
            Result* splan = (Result*)plan;

            if (splan->plan.lefttree != NULL)
                set_upper_references(root, plan, rtoffset);
            else {
                splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            }
            /* resconstantqual can't contain any subplan variable refs */
            splan->resconstantqual = fix_scan_expr(root, splan->resconstantqual, rtoffset);
        }
#endif
        case T_ProjectSet:
            set_upper_references(root, plan, rtoffset);
            break;
        case T_VecResult: {
            VecResult* splan = (VecResult*)plan;

            /*
             * Result may or may not have a subplan; if not, it's more
             * like a scan node than an upper node.
             */
            if (splan->plan.lefttree != NULL)
                set_upper_references(root, plan, rtoffset);
            else {
                splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            }
            /* resconstantqual can't contain any subplan variable refs */
            splan->resconstantqual = fix_scan_expr(root, splan->resconstantqual, rtoffset);
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* splan = (ModifyTable*)plan;
#ifdef PGXC
            int n = 0;
            List* firstRetList = NULL; /* First returning list required for
                                        * setting up visible plan target list
                                        */
#endif

            AssertEreport(
                splan->plan.targetlist == NIL && splan->plan.qual == NIL, MOD_OPT, "targelist and qual should be null");

            if (splan->returningLists) {
                List* newRL = NIL;
                ListCell* lcrl = NULL;
                ListCell* lcrr = NULL;
                ListCell* lcp = NULL;

                /*
                 * Pass each per-subplan returningList
                 * through set_returning_clause_references().
                 */
                AssertEreport(list_length(splan->returningLists) == list_length(splan->resultRelations) &&
                                  list_length(splan->returningLists) == list_length(splan->plans),
                    MOD_OPT,
                    "inconsistent state in subplan returningList");
                forthree(lcrl, splan->returningLists, lcrr, splan->resultRelations, lcp, splan->plans)
                {
                    List* rlist = (List*)lfirst(lcrl);
                    Index resultrel = (Index)linitial_int((List*)lfirst(lcrr));
                    Plan* subplan = (Plan*)lfirst(lcp);
#ifdef PGXC
                    RemoteQuery* rq = NULL;

                    if (n == 0) {
                        /*
                         * Set up first returning list before we change
                         * var references to point to RTE_REMOTE_DUMMY
                         */
                        firstRetList = set_returning_clause_references(root, rlist, subplan, resultrel, rtoffset);
                        /* Restore the returning list changed by the above call */
                        rlist = (List*)lfirst(lcrl);
                    }

                    if (splan->remote_plans)
                        rq = (RemoteQuery*)list_nth(splan->remote_plans, n);
                    n++;

                    if (rq != NULL && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        /*
                         * Set references of returning clause by adjusting
                         * varno/varattno according to target list in
                         * remote query node
                         */
                        rlist = set_remote_returning_refs(root, rlist, (Plan*)rq, rq->scan.scanrelid, rtoffset);
                        /*
                         * The next call to set_returning_clause_references
                         * should skip the vars already taken care of by
                         * the above call to set_remote_returning_refs
                         */
                        resultrel = rq->scan.scanrelid;
                    }
#endif
                    rlist = set_returning_clause_references(root, rlist, subplan, resultrel, rtoffset);
                    newRL = lappend(newRL, rlist);
                }
                splan->returningLists = newRL;

#ifdef PGXC
                /*
                 * In XC we do not need to set the target list as the
                 * first RETURNING list from the finalized list because
                 * it can contain vars referring to RTE_REMOTE_DUMMY.
                 * We therefore create a list before fixing
                 * remote returning references and use that here.
                 */
                splan->plan.targetlist = (List*)copyObject(firstRetList);
#else
                /*
                 * Set up the visible plan targetlist as being the same as
                 * the first RETURNING list. This is for the use of
                 * EXPLAIN; the executor won't pay any attention to the
                 * targetlist.	We postpone this step until here so that
                 * we don't have to do set_returning_clause_references()
                 * twice on identical targetlists.
                 */
                splan->plan.targetlist = copyObject(linitial(newRL));
#endif
            }

            /*
             * The MERGE statement produces the target rows by performing a
             * right join between the target relation and the source
             * relation (which could be a plain relation or a subquery).
             * The INSERT and UPDATE actions of the MERGE statement
             * requires access to the columns from the source relation. We
             * arrange things so that the source relation attributes are
             * available as INNER_VAR and the target relation attributes
             * are available from the scan tuple.
             */
            Index resultRelation = (Index)linitial_int((List*)linitial(splan->resultRelations));
            if (splan->mergeActionList != NIL && (IS_STREAM_PLAN || IS_SINGLE_NODE || IS_PGXC_DATANODE)) {
                /*
                 * mergeSourceTargetList is already setup correctly to
                 * include all Vars coming from the source relation. So we
                 * fix the targetList of individual action nodes by
                 * ensuring that the source relation Vars are referenced
                 * as INNER_VAR. Note that for this to work correctly,
                 * during execution, the ecxt_innertuple must be set to
                 * the tuple obtained from the source relation.
                 *
                 * We leave the Vars from the result relation (i.e. the
                 * target relation) unchanged i.e. those Vars would be
                 * picked from the scan slot. So during execution, we must
                 * ensure that ecxt_scantuple is setup correctly to refer
                 * to the tuple from the target relation.
                 */
                indexed_tlist* itlist = NULL;

                itlist = build_tlist_index(splan->mergeSourceTargetList);

                splan->mergeTargetRelation += rtoffset;

                foreach (l, splan->mergeActionList) {
                    MergeAction* action = (MergeAction*)lfirst(l);

                    /* Fix targetList of each action. */
                    action->targetList = fix_join_expr(
                        root, action->targetList, NULL, itlist, resultRelation, rtoffset);

                    /* Fix quals too. */
                    action->qual = (Node*)fix_join_expr(
                        root, (List*)action->qual, NULL, itlist, resultRelation, rtoffset);
                }
            }

            if (splan->updateTlist != NIL) {
                indexed_tlist* itlist;
                itlist = build_tlist_index(splan->exclRelTlist);
                splan->updateTlist = fix_join_expr(root, splan->updateTlist, NULL,
                    itlist, resultRelation, rtoffset);
                splan->upsertWhere = (Node*)fix_join_expr(root, (List*)splan->upsertWhere, NULL,
                    itlist, resultRelation, rtoffset);
                splan->exclRelTlist =
                        fix_scan_list(root, splan->exclRelTlist, rtoffset);
            }

            splan->exclRelRTIndex += rtoffset;

            foreach (l, splan->resultRelations) {
                List* list = (List*)lfirst(l);
                foreach_cell (lc, list) {
                    lfirst_int(lc) += rtoffset;
                }
            }
            foreach (l, splan->rowMarks) {
                PlanRowMark* rc = (PlanRowMark*)lfirst(l);

                rc->rti += rtoffset;
                rc->prti += rtoffset;
            }
            ListCell* lc = NULL;
            forboth (l, splan->plans, lc, splan->resultRelations) {
                List* resultRelations = (List*)lfirst(lc);
                Plan* subplan = (Plan*)lfirst(l);

                /*
                 * If the DML has mutil-relation to modify, The targetlist of each target table is arranged on the
                 * subplan->targetlist. transform and split the original subplan->targetlist to splan->targetlists.
                 */
                if (list_length(resultRelations) > 1) {
                    Assert(subplan->type == T_LockRows);
                    Plan* subplan_left = subplan->lefttree;
                    Assert(subplan_left->type == T_NestLoop || subplan_left->type == T_HashJoin ||
                           subplan_left->type == T_MergeJoin || subplan_left->type == T_BaseResult);
                    List* tmp_result_relation = list_copy(resultRelations);
                    make_sorted_list(tmp_result_relation, list_sorted_int_cmp);
                    assign_lockrows_targetlist(subplan, subplan_left, tmp_result_relation);
                    lfirst(l) = set_plan_refs(root, subplan, rtoffset);

                    fix_assign_targetlists(splan, subplan_left, tmp_result_relation);
                    list_free_ext(tmp_result_relation);
                } else {
                    lfirst(l) = set_plan_refs(root, subplan, rtoffset);
                }
            }

            /*
             * Append this ModifyTable node's final result relation RT
             * index(es) to the global list for the plan, and set its
             * resultRelIndex to reflect their starting position in the
             * global list.
             */
            splan->resultRelIndex = 0;
            foreach (l, root->glob->resultRelations) {
                splan->resultRelIndex += list_length((List*)lfirst(l));
            }
            root->glob->resultRelations = list_concat(root->glob->resultRelations, list_copy(splan->resultRelations));

#ifdef PGXC
            /* Adjust references of remote query nodes in ModifyTable node */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                ListCell* elt = NULL;
                RemoteQuery* rq = NULL;

                foreach (elt, splan->remote_plans) {
                    rq = (RemoteQuery*)lfirst(elt);

                    if (rq != NULL) {
                        /*
                         * If base_tlist is set, it means that we have a reduced remote
                         * query plan. So need to set the var references accordingly.
                         */
                        if (rq->base_tlist)
                            set_remote_references(root, rq, rtoffset);
                        rq->scan.plan.targetlist = fix_scan_list(root, rq->scan.plan.targetlist, rtoffset);
                        rq->scan.plan.qual = fix_scan_list(root, rq->scan.plan.qual, rtoffset);
                        rq->base_tlist = fix_scan_list(root, rq->base_tlist, rtoffset);
                        rq->scan.scanrelid += rtoffset;
                    }
                }
            }
#endif
        } break;
        case T_Append:
        case T_VecAppend: {
            Append* splan = (Append*)plan;

            /*
             * Append, like Sort et al, doesn't actually evaluate its
             * targetlist or check quals.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");
            foreach (l, splan->appendplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
            delete_redundant_streams_of_append_plan(splan);
        } break;
        case T_MergeAppend: {
            MergeAppend* splan = (MergeAppend*)plan;

            /*
             * MergeAppend, like Sort et al, doesn't actually evaluate its
             * targetlist or check quals.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");
            foreach (l, splan->mergeplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_RecursiveUnion:
            /* This doesn't evaluate targetlist or check quals either */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(plan->qual == NIL, MOD_OPT, "qual should be null");
            break;
        case T_BitmapAnd: {
            BitmapAnd* splan = (BitmapAnd*)plan;

            /* BitmapAnd works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_BitmapOr: {
            BitmapOr* splan = (BitmapOr*)plan;

            /* BitmapOr works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_CStoreIndexAnd: {
            CStoreIndexAnd* splan = (CStoreIndexAnd*)plan;

            /* BitmapAnd works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_CStoreIndexOr: {
            CStoreIndexOr* splan = (CStoreIndexOr*)plan;

            /* BitmapOr works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL, MOD_OPT, "It should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
#ifdef STREAMPLAN
        case T_Stream:
        case T_VecStream: {

            /*
             * These plan types don't actually bother to evaluate their
             * targetlists, because they just return their unmodified input
             * tuples.	Even though the targetlist won't be used by the
             * executor, we fix it up for possible use by EXPLAIN (not to
             * mention ease of debugging --- wrong varnos are very confusing).
             */
            Stream* stream = (Stream*)plan;
            Plan* subplan = plan->lefttree;
            indexed_tlist* subplan_itlist = NULL;

            /* Remove LOCAL GATHER for Recursive, which is just lefttree of another normal Stream */
            if (IsA(plan->lefttree, Stream) && ((Stream*)plan->lefttree)->is_recursive_local) {
                plan->lefttree = plan->lefttree->lefttree;
            }

#ifdef USE_ASSERT_CHECKING
            if (plan->plan_node_id > 0) {
                ListCell* cell1 = NULL;
                ListCell* cell2 = NULL;

                forboth(cell1, plan->targetlist, cell2, plan->lefttree->targetlist)
                {
                    TargetEntry* te1 = (TargetEntry*)lfirst(cell1);
                    TargetEntry* te2 = (TargetEntry*)lfirst(cell2);

                    if (!equal(te1->expr, te2->expr)) {
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                                errmsg("targetlist of stream node with plan_node_id %d should "
                                       "be equal to its child's targetlist",
                                    plan->plan_node_id)));
                    }
                }
            }
#endif

            set_dummy_tlist_references(plan, rtoffset);
            subplan_itlist = build_tlist_index(subplan->targetlist);

            /* Not only redistribute stream, but also hybrid stream needs distribute keys. */
            if (stream->distribute_keys != NIL) {
                stream->distribute_keys =
                    (List*)fix_upper_expr(root, (Node*)stream->distribute_keys, subplan_itlist, OUTER_VAR, rtoffset);
            }

            if (stream->skew_list != NIL)
                fix_skew_quals(root, plan, subplan_itlist, rtoffset);
        } break;
#endif
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type in set plan refs: %d", (int)nodeTag(plan))));
        } break;
    }

    /*
     * Now recurse into child plans, if any
     *
     * NOTE: it is essential that we recurse into child plans AFTER we set
     * subplan references in this plan's tlist and quals.  If we did the
     * reference-adjustments bottom-up, then we would fail to match this
     * plan's var nodes against the already-modified nodes of the children.
     */
    plan->lefttree = set_plan_refs(root, plan->lefttree, rtoffset);
    plan->righttree = set_plan_refs(root, plan->righttree, rtoffset);

    return plan;
}
```

<a id="source-025"></a>

#### set_subqueryscan_references()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1267-1353`；范围：**完整函数**。

内容 SHA256：`48938f76b37761ba3b435967c95566e02fc56ab41ef9326af689447367035d39`。

```cpp
static Plan* set_subqueryscan_references(PlannerInfo* root, SubqueryScan* plan, int rtoffset)
{
    RelOptInfo* rel = NULL;
    Plan* result = NULL;
    PlannerInfo* subroot = NULL;

    /* Need to look up the subquery's RelOptInfo, since we need its subroot */
    rel = find_base_rel(root, plan->scan.scanrelid);

    /*
     * Fallback/vectorize plan may cause the difference between rel->subplan
     * and plan->subplan. If the plan->subplan is a VecToRow or RowToVec node,
     * check the lefttree of plan->subplan instead.
     *
     * We can skip the check when the plan is under recursive cte, since we may
     * copy the plan and use the original planner info, in this case the plans are
     * definitely different
     */
    if (!IsA(plan->subplan, VecToRow) && !IsA(plan->subplan, RowToVec)) {
        /*
         * We can skip the check when the plan is under recursive cte, since we may
         * copy the plan and use the original planner info, in this case the plans are
         * definitely different
         */
        AssertEreport(root->is_under_recursive_cte || rel->subplan == plan->subplan ||
                    (((RelOptInfo*)linitial(rel->alternatives))->subplan == plan->subplan),
                    MOD_OPT,
                    "inconsistent state after fallback/vectorize plan");
    } else {
        AssertEreport(root->is_under_recursive_cte || rel->subplan == plan->subplan->lefttree ||
                    (((RelOptInfo*)linitial(rel->alternatives))->subplan == plan->subplan->lefttree),
            MOD_OPT,
            "inconsistent state after fallback/vectorize plan");
    }

    /* Recursively process the subplan */
    if (rel->alternatives != NIL && ((RelOptInfo*)linitial(rel->alternatives))->subplan == plan->subplan) {
        subroot = ((RelOptInfo*)linitial(rel->alternatives))->subroot;
    } else {
        subroot = rel->subroot;
    }
    plan->subplan = set_plan_references(subroot, plan->subplan);

    if (trivial_subqueryscan(plan)) {
        /*
         * We can omit the SubqueryScan node and just pull up the subplan.
         */
        ListCell* lp = NULL;
        ListCell* lc = NULL;

        result = plan->subplan;

        /* We have to be sure we don't lose any initplans */
        result->initPlan = list_concat(plan->scan.plan.initPlan, result->initPlan);

        /*
         * We also have to transfer the SubqueryScan's result-column names
         * into the subplan, else columns sent to client will be improperly
         * labeled if this is the topmost plan level.  Copy the "source
         * column" information too.
         */
        forboth(lp, plan->scan.plan.targetlist, lc, result->targetlist)
        {
            TargetEntry* ptle = (TargetEntry*)lfirst(lp);
            TargetEntry* ctle = (TargetEntry*)lfirst(lc);

            ctle->resname = ptle->resname;
            ctle->resorigtbl = ptle->resorigtbl;
            ctle->resorigcol = ptle->resorigcol;
        }
    } else {
        /*
         * Keep the SubqueryScan node.	We have to do the processing that
         * set_plan_references would otherwise have done on it.  Notice we do
         * not do set_upper_references() here, because a SubqueryScan will
         * always have been created with correct references to its subplan's
         * outputs to begin with.
         */
        plan->scan.scanrelid += rtoffset;
        plan->scan.plan.targetlist = fix_scan_list(root, plan->scan.plan.targetlist, rtoffset);
        plan->scan.plan.qual = fix_scan_list(root, plan->scan.plan.qual, rtoffset);

        result = (Plan*)plan;
    }

    return result;
}
```

<a id="source-026"></a>

#### trivial_subqueryscan()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1430-1481`；范围：**完整函数**。

内容 SHA256：`3c24c41ca0309a3415ffe8f0811342f01b7ab94603327ba8f35afe208bf24cc0`。

```cpp
bool trivial_subqueryscan(SubqueryScan* plan)
{
    int attrno;
    ListCell* lp = NULL;
    ListCell* lc = NULL;

    if (plan->scan.plan.qual != NIL)
        return false;

    /*
     * remove the subquery upon broadcast stream, while later add a Gather
     * would produce a Gather->Broadcast plan which is illegal
     */
    if (is_plan_a_broadcast_stream(plan->subplan))
        return false;

    if (list_length(plan->scan.plan.targetlist) != list_length(plan->subplan->targetlist))
        return false; /* tlists not same length */

    attrno = 1;
    forboth(lp, plan->scan.plan.targetlist, lc, plan->subplan->targetlist)
    {
        TargetEntry* ptle = (TargetEntry*)lfirst(lp);
        TargetEntry* ctle = (TargetEntry*)lfirst(lc);

        if (ptle->resjunk != ctle->resjunk)
            return false; /* tlist doesn't match junk status */

        /*
         * We accept either a Var referencing the corresponding element of the
         * subplan tlist, or a Const equaling the subplan element. See
         * generate_setop_tlist() for motivation.
         */
        if (ptle->expr && IsA(ptle->expr, Var)) {
            Var* var = (Var*)ptle->expr;

            AssertEreport(var->varno == plan->scan.scanrelid && var->varlevelsup == 0,
                MOD_OPT,
                "var should be at current level and varno should be equal plan's scanrelid");
            if (var->varattno != attrno)
                return false; /* out of order */
        } else if (ptle->expr && IsA(ptle->expr, Const)) {
            if (!equal(ptle->expr, ctle->expr))
                return false;
        } else
            return false;

        attrno++;
    }

    return true;
}
```

<a id="source-027"></a>

#### fix_expr_common()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1510-1569`；范围：**完整函数**。

内容 SHA256：`4b90ca5689aca939a813d6c3a89dd648bcf7307df646c504ccc866e44b370206`。

```cpp
static void fix_expr_common(PlannerInfo* root, Node* node)
{
    /* We assume callers won't call us on a NULL pointer */
    if (IsA(node, Aggref)) {
        record_plan_function_dependency(root, ((Aggref*)node)->aggfnoid);
    } else if (IsA(node, WindowFunc)) {
        record_plan_function_dependency(root, ((WindowFunc*)node)->winfnoid);
    } else if (IsA(node, FuncExpr)) {
        record_plan_function_dependency(root, ((FuncExpr*)node)->funcid);
    } else if (IsA(node, OpExpr)) {
        set_opfuncid((OpExpr*)node);
        record_plan_function_dependency(root, ((OpExpr*)node)->opfuncid);
    } else if (IsA(node, DistinctExpr)) {
        set_opfuncid((OpExpr*)node); /* rely on struct equivalence */
        record_plan_function_dependency(root, ((DistinctExpr*)node)->opfuncid);
    } else if (IsA(node, NullIfExpr)) {
        set_opfuncid((OpExpr*)node); /* rely on struct equivalence */
        record_plan_function_dependency(root, ((NullIfExpr*)node)->opfuncid);
    } else if (IsA(node, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *)node;

        set_sa_opfuncid(saop);
        record_plan_function_dependency(root, saop->opfuncid);

        if (OidIsValid(saop->hashfuncid))
            record_plan_function_dependency(root, saop->hashfuncid);

        if (OidIsValid(saop->negfuncid))
            record_plan_function_dependency(root, saop->negfuncid);
    } else if (IsA(node, ArrayCoerceExpr)) {
        if (OidIsValid(((ArrayCoerceExpr*)node)->elemfuncid))
            record_plan_function_dependency(root, ((ArrayCoerceExpr*)node)->elemfuncid);
    } else if (IsA(node, Const)) {
        Const* con = (Const*)node;

        /* Check for regclass reference */
        if (ISREGCLASSCONST(con))
            root->glob->relationOids = lappend_oid(root->glob->relationOids, DatumGetObjectId(con->constvalue));
    } else if (IsA(node, GroupingFunc)) {
        GroupingFunc* g = (GroupingFunc*)node;
        AttrNumber* grouping_map = root->grouping_map;

        /* If there are no grouping sets, we don't need this. */
        AssertEreport(grouping_map || g->cols == NIL, MOD_OPT, "If there are no grouping sets, we don't need this.");

        if (grouping_map != NULL) {
            ListCell* lc = NULL;
            List* cols = NIL;

            foreach (lc, g->refs) {
                cols = lappend_int(cols, grouping_map[lfirst_int(lc)]);
            }

            AssertEreport(g->cols == NIL || equal(cols, g->cols), MOD_OPT, "inconsistent g->cols state.");

            if (!g->cols)
                g->cols = cols;
        }
    }
}
```

<a id="source-028"></a>

#### fix_scan_expr()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1579-1600`；范围：**完整函数**。

内容 SHA256：`606046c35b44657070345f1045a8199d00efd66f605a24979dbad816482595c9`。

```cpp
static Node* fix_scan_expr(PlannerInfo* root, Node* node, int rtoffset)
{
    fix_scan_expr_context context;

    context.root = root;
    context.rtoffset = rtoffset;

    if (rtoffset != 0 || root->glob->lastPHId != 0) {
        return fix_scan_expr_mutator(node, &context);
    } else {
        /*
         * If rtoffset == 0, we don't need to change any Vars, and if there
         * are no placeholders anywhere we won't need to remove them.  Then
         * it's OK to just scribble on the input node tree instead of copying
         * (since the only change, filling in any unset opfuncid fields, is
         * harmless).  This saves just enough cycles to be noticeable on
         * trivial queries.
         */
        (void)fix_scan_expr_walker(node, &context);
        return node;
    }
}
```

<a id="source-029"></a>

#### fix_scan_expr_mutator()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1619-1659`；范围：**完整函数**。

内容 SHA256：`b2a17f2feaa6e38b1502e30a0d31ec991b875913d1e40d9ee7915c2d2302e9c4`。

```cpp
static Node* fix_scan_expr_mutator(Node* node, fix_scan_expr_context* context)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = copyVar((Var*)node);

        AssertEreport(var->varlevelsup == 0, MOD_OPT, "unexpected var's varlevelsup value");

        /*
         * We should not see any Vars marked INNER_VAR or OUTER_VAR.  But an
         * indexqual expression could contain INDEX_VAR Vars.
         */
        AssertEreport(var->varno != INNER_VAR && var->varno != OUTER_VAR,
            MOD_OPT,
            "should not any Vars marked INNER_VAR or OUTER_VAR");
        if (!IS_SPECIAL_VARNO(var->varno))
            var->varno += context->rtoffset;
        if (var->varnoold > 0)
            var->varnoold += context->rtoffset;
        return (Node*)var;
    }
    if (IsA(node, CurrentOfExpr)) {
        CurrentOfExpr* cexpr = (CurrentOfExpr*)copyObject(node);

        AssertEreport(cexpr->cvarno != INNER_VAR && cexpr->cvarno != OUTER_VAR,
            MOD_OPT,
            "should not any Vars marked INNER_VAR or OUTER_VAR");
        if (!IS_SPECIAL_VARNO(cexpr->cvarno))
            cexpr->cvarno += context->rtoffset;
        return (Node*)cexpr;
    }
    if (IsA(node, PlaceHolderVar)) {
        /* At scan level, we should always just evaluate the contained expr */
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        return fix_scan_expr_mutator((Node*)phv->phexpr, context);
    }
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_scan_expr_mutator, (void*)context);
}
```

<a id="source-030"></a>

#### fix_scan_expr_walker()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1661-1668`；范围：**完整函数**。

内容 SHA256：`96a72bd2108801f81d362203ea26f6d08b8245c0e1e9c80dfdf7707f3c712d54`。

```cpp
static bool fix_scan_expr_walker(Node* node, fix_scan_expr_context* context)
{
    if (node == NULL)
        return false;
    AssertEreport(!IsA(node, PlaceHolderVar), MOD_OPT, "unexpected node type.");
    fix_expr_common(context->root, node);
    return expression_tree_walker(node, (bool (*)())fix_scan_expr_walker, (void*)context);
}
```

<a id="source-031"></a>

#### set_join_references()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1678-1765`；范围：**完整函数**。

内容 SHA256：`22de278aa3d07d8760f9cf902d89c5c334b5ec5d8520a662a4520629d06dc54f`。

```cpp
static void set_join_references(PlannerInfo* root, Join* join, int rtoffset)
{
    Plan* outer_plan = join->plan.lefttree;
    Plan* inner_plan = join->plan.righttree;
    indexed_tlist* outer_itlist = NULL;
    indexed_tlist* inner_itlist = NULL;

    outer_itlist = build_tlist_index(outer_plan->targetlist);
    inner_itlist = build_tlist_index(inner_plan->targetlist);

    /*
     * First process the joinquals (including merge or hash clauses).  These
     * are logically below the join so they can always use all values
     * available from the input tlists.  It's okay to also handle
     * NestLoopParams now, because those couldn't refer to nullable
     * subexpressions.
     */
    join->joinqual = fix_join_expr(root, join->joinqual, outer_itlist, inner_itlist, (Index)0, rtoffset);
    join->nulleqqual = fix_join_expr(root, join->nulleqqual, outer_itlist, inner_itlist, (Index)0, rtoffset);
    /*
     * Now we need to fix up the targetlist and qpqual, which are logically
     * above the join.  This means they should not re-use any input expression
     * that was computed in the nullable side of an outer join.  Vars and
     * PlaceHolderVars are fine, so we can implement this restriction just by
     * clearing has_non_vars in the indexed_tlist structs.
     *
     * XXX This is a grotty workaround for the fact that we don't clearly
     * distinguish between a Var appearing below an outer join and the "same"
     * Var appearing above it.  If we did, we'd not need to hack the matching
     * rules this way.
     */
    switch (join->jointype) {
        case JOIN_LEFT:
        case JOIN_SEMI:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            inner_itlist->has_non_vars = false;
            break;

        case JOIN_RIGHT:
        case JOIN_RIGHT_SEMI:
        case JOIN_RIGHT_ANTI:
        case JOIN_RIGHT_ANTI_FULL:
            outer_itlist->has_non_vars = false;
            break;

        case JOIN_FULL:
            outer_itlist->has_non_vars = false;
            inner_itlist->has_non_vars = false;
            break;
        default:
            break;
    }

    /* All join plans have tlist, qual, and joinqual */
    join->plan.targetlist = fix_join_expr(root, join->plan.targetlist, outer_itlist, inner_itlist, (Index)0, rtoffset);
    join->plan.var_list = fix_join_expr(root, join->plan.var_list, outer_itlist, inner_itlist, (Index)0, rtoffset);
    join->plan.qual = fix_join_expr(root, join->plan.qual, outer_itlist, inner_itlist, (Index)0, rtoffset);

    /* Now do join-type-specific stuff */
    if (IsA(join, NestLoop) || IsA(join, VecNestLoop)) {
        NestLoop* nl = (NestLoop*)join;
        ListCell* lc = NULL;

        foreach (lc, nl->nestParams) {
            NestLoopParam* nlp = (NestLoopParam*)lfirst(lc);

            nlp->paramval = (Var*)fix_upper_expr(root, (Node*)nlp->paramval, outer_itlist, OUTER_VAR, rtoffset);
            /* Check we replaced any PlaceHolderVar with simple Var */
            if (!(IsA(nlp->paramval, Var) && nlp->paramval->varno == OUTER_VAR))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("NestLoopParam was not reduced to a simple Var"))));
        }
    } else if (IsA(join, MergeJoin) || IsA(join, VecMergeJoin)) {
        MergeJoin* mj = (MergeJoin*)join;

        mj->mergeclauses = fix_join_expr(root, mj->mergeclauses, outer_itlist, inner_itlist, (Index)0, rtoffset);
    } else if (IsA(join, HashJoin) || IsA(join, VecHashJoin) || IsA(join, AsofJoin) || IsA(join, VecAsofJoin)) {
        HashJoin* hj = (HashJoin*)join;

        hj->hashclauses = fix_join_expr(root, hj->hashclauses, outer_itlist, inner_itlist, (Index)0, rtoffset);
    }

    pfree_ext(outer_itlist);
    pfree_ext(inner_itlist);
}
```

<a id="source-032"></a>

#### has_same_groupby()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1768-1785`；范围：**完整函数**。

内容 SHA256：`4912e0601ff6da0d5181fad447b8fc470c01c153b03d874d6fe2dd7c69e2ae8b`。

```cpp
static bool has_same_groupby(Plan* plan, Plan* subplan)
{
    if (!IsA(plan, Agg) || !IsA(subplan, Agg)) {
        return true;
    }

    Agg* agg = (Agg*)plan;
    Agg* subagg = (Agg*)subplan;

    if (agg->numCols != subagg->numCols)
        return false;

    for (int16 i = 0; i < agg->numCols; i++) {
        if (agg->grpColIdx[i] != subagg->grpColIdx[i])
            return false;
    }
    return true;
}
```

<a id="source-033"></a>

#### set_upper_references()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1806-1852`；范围：**完整函数**。

内容 SHA256：`44ae7eab83ca77a5fc64e64d0cec9558154adb870ef2d22d2b33dc4315f08abb`。

```cpp
static void set_upper_references(PlannerInfo* root, Plan* plan, int rtoffset)
{
    Plan* subplan = plan->lefttree;
    indexed_tlist* subplan_itlist = NULL;
    List* output_targetlist = NIL;
    ListCell* l = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    bool groupby_same = has_same_groupby(plan, subplan);
#endif

    subplan_itlist = build_tlist_index(subplan->targetlist);

    output_targetlist = NIL;
    foreach (l, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        Node* newexpr = NULL;

        /* If it's a non-Var sort/group item, first try to match by sortref */
#ifdef ENABLE_MULTIPLE_NODES
        if (tle->ressortgroupref != 0 && !IsA(tle->expr, Var)) {
#else
        if (tle->ressortgroupref != 0 && groupby_same) {
#endif
            newexpr = (Node*)search_indexed_tlist_for_sortgroupref(
                (Node*)tle->expr, tle->ressortgroupref, subplan_itlist, OUTER_VAR);
            if (newexpr == NULL)
                newexpr = fix_upper_expr(root, (Node*)tle->expr, subplan_itlist, OUTER_VAR, rtoffset);
        } else
            newexpr = fix_upper_expr(root, (Node*)tle->expr, subplan_itlist, OUTER_VAR, rtoffset);
        tle = flatCopyTargetEntry(tle);
        tle->expr = (Expr*)newexpr;
        output_targetlist = lappend(output_targetlist, tle);
    }
    plan->targetlist = output_targetlist;

    /*
     * For Agg, qual's const can not replace with Var, because const can be set to NULL
     * in Ap function.
     */
    if (IsA(plan, Agg) || IsA(plan, VecAgg)) {
        subplan_itlist->return_const = true;
    }

    plan->qual = (List*)fix_upper_expr(root, (Node*)plan->qual, subplan_itlist, OUTER_VAR, rtoffset);

    pfree_ext(subplan_itlist);
}
```

<a id="source-034"></a>

#### set_dummy_tlist_references()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1867-1895`；范围：**完整函数**。

内容 SHA256：`7d64e183127a0e77ecdb1b03e0279177750641f77924eef3c6aa8a6526e0c4d2`。

```cpp
static void set_dummy_tlist_references(Plan* plan, int rtoffset)
{
    List* output_targetlist = NIL;
    ListCell* l = NULL;

    output_targetlist = NIL;
    foreach (l, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        Var* oldvar = (Var*)tle->expr;
        Var* newvar = NULL;

        newvar = makeVar(
            OUTER_VAR, tle->resno, exprType((Node*)oldvar), exprTypmod((Node*)oldvar), exprCollation((Node*)oldvar), 0);
        if (IsA(oldvar, Var)) {
            newvar->varnoold = oldvar->varno + rtoffset;
            newvar->varoattno = oldvar->varattno;
        } else {
            newvar->varnoold = 0; /* wasn't ever a plain Var */
            newvar->varoattno = 0;
        }

        tle = flatCopyTargetEntry(tle);
        tle->expr = (Expr*)newvar;
        output_targetlist = lappend(output_targetlist, tle);
    }
    plan->targetlist = output_targetlist;

    /* We don't touch plan->qual here */
}
```

<a id="source-035"></a>

#### build_tlist_index()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1910-1947`；范围：**完整函数**。

内容 SHA256：`79e5480bb4e430d04ba169bfe11c3b5e2137e63477ed2628b5cbf6c544aa8d32`。

```cpp
static indexed_tlist* build_tlist_index(List* tlist, bool returnConstForConst)
{
    indexed_tlist* itlist = NULL;
    tlist_vinfo* vinfo = NULL;
    ListCell* l = NULL;

    /* Create data structure with enough slots for all tlist entries */
    itlist = (indexed_tlist*)palloc(offsetof(indexed_tlist, vars) + list_length(tlist) * sizeof(tlist_vinfo));

    itlist->tlist = tlist;
    itlist->has_ph_vars = false;
    itlist->has_non_vars = false;
    itlist->return_const = false;

    /* Find the Vars and fill in the index array */
    vinfo = itlist->vars;
    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;

            vinfo->varno = var->varno;
            vinfo->varattno = var->varattno;
            vinfo->resno = tle->resno;
            vinfo++;
        } else if (tle->expr && IsA(tle->expr, PlaceHolderVar))
            itlist->has_ph_vars = true;
        else if (tle->expr && IsA(tle->expr, Const) && returnConstForConst)
            itlist->return_const = true;
        else
            itlist->has_non_vars = true;
    }

    itlist->num_vars = (vinfo - itlist->vars);

    return itlist;
}
```

<a id="source-036"></a>

#### build_tlist_index_other_vars()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:1957-1992`；范围：**完整函数**。

内容 SHA256：`83ea390e6e9fc76025f1086ea432e93eb79ec4bcc46ed55486a169e526cdc427`。

```cpp
static indexed_tlist* build_tlist_index_other_vars(List* tlist, Index ignore_rel)
{
    indexed_tlist* itlist = NULL;
    tlist_vinfo* vinfo = NULL;
    ListCell* l = NULL;

    /* Create data structure with enough slots for all tlist entries */
    itlist = (indexed_tlist*)palloc(offsetof(indexed_tlist, vars) + list_length(tlist) * sizeof(tlist_vinfo));

    itlist->tlist = tlist;
    itlist->has_ph_vars = false;
    itlist->has_non_vars = false;
    itlist->return_const = false;

    /* Find the desired Vars and fill in the index array */
    vinfo = itlist->vars;
    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;

            if (var->varno != ignore_rel) {
                vinfo->varno = var->varno;
                vinfo->varattno = var->varattno;
                vinfo->resno = tle->resno;
                vinfo++;
            }
        } else if (tle->expr && IsA(tle->expr, PlaceHolderVar))
            itlist->has_ph_vars = true;
    }

    itlist->num_vars = (vinfo - itlist->vars);

    return itlist;
}
```

<a id="source-037"></a>

#### search_indexed_tlist_for_var()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2002-2025`；范围：**完整函数**。

内容 SHA256：`1363e5bb0b29edce0256ba9e3b0e5bc6ee42488b8ced453b5e02217177e7e275`。

```cpp
static Var* search_indexed_tlist_for_var(Var* var, indexed_tlist* itlist, Index newvarno, int rtoffset)
{
    Index varno = var->varno;
    AttrNumber varattno = var->varattno;
    tlist_vinfo* vinfo = NULL;
    int i;

    vinfo = itlist->vars;
    i = itlist->num_vars;
    while (i-- > 0) {
        if (vinfo->varno == varno && vinfo->varattno == varattno) {
            /* Found a match */
            Var* newvar = copyVar(var);

            newvar->varno = newvarno;
            newvar->varattno = vinfo->resno;
            if (newvar->varnoold > 0)
                newvar->varnoold += rtoffset;
            return newvar;
        }
        vinfo++;
    }
    return NULL; /* no match */
}
```

<a id="source-038"></a>

#### search_indexed_tlist_for_non_var()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2038-2106`；范围：**完整函数**。

内容 SHA256：`2389f7e57ff26d38cfc51a43b13e23e0412ad0088258d395213a9c4c5c443a49`。

```cpp
static Var* search_indexed_tlist_for_non_var(Node* node, indexed_tlist* itlist, Index newvarno)
{
    TargetEntry* tle = NULL;
    bool nested_agg = false;
    bool nested_relabeltype = false;

    /* Only used this const rather than Var. */
    if (itlist->return_const && IsA(node, Const)) {
        return NULL;
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && IS_STREAM) {
#else
    if (IS_STREAM) {
#endif
        tle = tlist_member_except_aggref(node, itlist->tlist, &nested_agg, &nested_relabeltype);
    } else {
        tle = tlist_member(node, itlist->tlist);
    }
    if (tle != NULL) {
        /* Found a matching subplan output expression */
        Var* newvar = NULL;

        newvar = makeVarFromTargetEntry(newvarno, tle);
        newvar->varnoold = 0; /* wasn't ever a plain Var */
        newvar->varoattno = 0;
#ifdef STREAMPLAN
        if (nested_agg) {
            Aggref* agg_node = (Aggref*)node;
            TargetEntry* new_tle = NULL;
            TargetEntry* sec_tle = NULL;
            char* resname = tle->resname != NULL ? pstrdup(tle->resname) : NULL;

            AssertEreport(IsA(node, Aggref), MOD_OPT, "node type aggref is required.");
            if (agg_node->args != NULL) {
                /*
                 * Supplement the second argument for string_agg at top level in
                 * nested agg scene.
                 * e.g. string_agg((string_agg((id1)::text, ','::text)), ','::text)
                 */
                if (agg_node->aggfnoid == STRINGAGGFUNCOID && IsA(lsecond(agg_node->args), TargetEntry))
                    sec_tle = flatCopyTargetEntry((TargetEntry*)lsecond(agg_node->args));

                list_free_deep(agg_node->args);
            }

            new_tle = makeTargetEntry((Expr*)newvar, 1, resname, tle->resjunk);

            if (sec_tle == NULL)
                agg_node->args = list_make1(new_tle);
            else
                agg_node->args = list_make2(new_tle, sec_tle);
            return (Var*)agg_node;
        } else if (nested_relabeltype) {
            /*
             * We should make sure the the return type of the node unchanging, so wrap the Var node
             * with the origin relabeltype node.
             */
            RelabelType* newnode = (RelabelType*)node;
            newnode->arg = (Expr*)newvar;
            return (Var*)newnode;
        } else
#endif
            return newvar;
    }

    return NULL; /* no match */
}
```

<a id="source-039"></a>

#### search_indexed_tlist_for_sortgroupref()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2118-2137`；范围：**完整函数**。

内容 SHA256：`f043778053a09de4b8f749211fce66395232d25c1bf962e8370e925cf15d87d2`。

```cpp
static Var* search_indexed_tlist_for_sortgroupref(Node* node, Index sortgroupref, indexed_tlist* itlist, Index newvarno)
{
    ListCell* lc = NULL;

    foreach (lc, itlist->tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);

        /* The equal() check should be redundant, but let's be paranoid */
        if (tle->ressortgroupref == sortgroupref && equal(node, tle->expr)) {
            /* Found a matching subplan output expression */
            Var* newvar = NULL;

            newvar = makeVarFromTargetEntry(newvarno, tle);
            newvar->varnoold = 0; /* wasn't ever a plain Var */
            newvar->varoattno = 0;
            return newvar;
        }
    }
    return NULL; /* no match */
}
```

<a id="source-040"></a>

#### fix_join_expr()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2169-2180`；范围：**完整函数**。

内容 SHA256：`a3f38549090938255119a47635f6ded8720ac7fd9177e900a56f176cd0bd9be3`。

```cpp
static List* fix_join_expr(PlannerInfo* root, List* clauses, indexed_tlist* outer_itlist, indexed_tlist* inner_itlist,
    Index acceptable_rel, int rtoffset)
{
    fix_join_expr_context context;

    context.root = root;
    context.outer_itlist = outer_itlist;
    context.inner_itlist = inner_itlist;
    context.acceptable_rel = acceptable_rel;
    context.rtoffset = rtoffset;
    return (List*)fix_join_expr_mutator((Node*)clauses, &context);
}
```

<a id="source-041"></a>

#### fix_join_expr_mutator()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2182-2249`；范围：**完整函数**。

内容 SHA256：`05be4123ee3dbf58c24775c7925dbe316b20539b31698547a92524861a8e8b4d`。

```cpp
static Node* fix_join_expr_mutator(Node* node, fix_join_expr_context* context)
{
    Var* newvar = NULL;

    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /* First look for the var in the input tlists */
        if (context->outer_itlist != NULL) {
            newvar = search_indexed_tlist_for_var(var, context->outer_itlist, OUTER_VAR, context->rtoffset);
            if (newvar != NULL)
                return (Node*)newvar;
        }
        if (context->inner_itlist != NULL) {
            newvar = search_indexed_tlist_for_var(var, context->inner_itlist, INNER_VAR, context->rtoffset);
            if (newvar != NULL)
                return (Node*)newvar;
        }

        /* If it's for acceptable_rel, adjust and return it */
        if (var->varno == context->acceptable_rel) {
            var = copyVar(var);
            var->varno += context->rtoffset;
            if (var->varnoold > 0)
                var->varnoold += context->rtoffset;
            return (Node*)var;
        }

        /* No referent found for Var */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("variable not found in subplan target lists"))));
    }
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        /* See if the PlaceHolderVar has bubbled up from a lower plan node */
        if (context->outer_itlist != NULL && context->outer_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_non_var((Node*)phv, context->outer_itlist, OUTER_VAR);
            if (newvar != NULL)
                return (Node*)newvar;
        }
        if (context->inner_itlist && context->inner_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_non_var((Node*)phv, context->inner_itlist, INNER_VAR);
            if (newvar != NULL)
                return (Node*)newvar;
        }

        /* If not supplied by input plans, evaluate the contained expr */
        return fix_join_expr_mutator((Node*)phv->phexpr, context);
    }
    /* Try matching more complex expressions too, if tlists have any */
    if (context->outer_itlist && context->outer_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->outer_itlist, OUTER_VAR);
        if (newvar != NULL)
            return (Node*)newvar;
    }
    if (context->inner_itlist && context->inner_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->inner_itlist, INNER_VAR);
        if (newvar != NULL)
            return (Node*)newvar;
    }
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_join_expr_mutator, (void*)context);
}
```

<a id="source-042"></a>

#### fix_upper_expr()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2279-2288`；范围：**完整函数**。

内容 SHA256：`980a02b48fa0ca13e13b2e66639d6474a1401ca31b02c40ac07e93d22e1fd6ef`。

```cpp
static Node* fix_upper_expr(PlannerInfo* root, Node* node, indexed_tlist* subplan_itlist, Index newvarno, int rtoffset)
{
    fix_upper_expr_context context;

    context.root = root;
    context.subplan_itlist = subplan_itlist;
    context.newvarno = newvarno;
    context.rtoffset = rtoffset;
    return fix_upper_expr_mutator(node, &context);
}
```

<a id="source-043"></a>

#### fix_upper_expr_mutator()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2290-2327`；范围：**完整函数**。

内容 SHA256：`2a4a56173f7e0c8073801ab1f86376e343ca296226eeb673d234c602b6162584`。

```cpp
static Node* fix_upper_expr_mutator(Node* node, fix_upper_expr_context* context)
{
    Var* newvar = NULL;

    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        newvar = search_indexed_tlist_for_var(var, context->subplan_itlist, context->newvarno, context->rtoffset);
        if (newvar == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    (errmsg("variable not found in subplan target list"))));
        return (Node*)newvar;
    }
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        /* See if the PlaceHolderVar has bubbled up from a lower plan node */
        if (context->subplan_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_non_var((Node*)phv, context->subplan_itlist, context->newvarno);
            if (newvar != NULL)
                return (Node*)newvar;
        }
        /* If not supplied by input plan, evaluate the contained expr */
        return fix_upper_expr_mutator((Node*)phv->phexpr, context);
    }
    /* Try matching more complex expressions too, if tlist has any */
    if (context->subplan_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->subplan_itlist, context->newvarno);
        if (newvar != NULL)
            return (Node*)newvar;
    }
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_upper_expr_mutator, (void*)context);
}
```

<a id="source-044"></a>

#### set_foreignscan_references()

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:2861-2907`；范围：**完整函数**。

内容 SHA256：`d9e9967718b30baf77fd1187fdbd47f58456262a5fb18d80457d67204a6837bd`。

```cpp
static void set_foreignscan_references(PlannerInfo *root, ForeignScan *fscan, int rtoffset)
{
    /* Adjust scanrelid if it's valid */
    if (fscan->scan.scanrelid > 0) {
        fscan->scan.scanrelid += rtoffset;
    }

    if (fscan->fdw_scan_tlist != NIL || fscan->scan.scanrelid == 0) {
        /*
         * Adjust tlist, qual, fdw_exprs, fdw_recheck_quals to reference
         * foreign scan tuple
         */
        indexed_tlist *itlist = build_tlist_index(fscan->fdw_scan_tlist);

        fscan->scan.plan.targetlist =
            (List *)fix_upper_expr(root, (Node *)fscan->scan.plan.targetlist, itlist, INDEX_VAR, rtoffset);
        fscan->scan.plan.qual =
            (List *)fix_upper_expr(root, (Node *)fscan->scan.plan.qual, itlist, INDEX_VAR, rtoffset);
        fscan->fdw_exprs = (List *)fix_upper_expr(root, (Node *)fscan->fdw_exprs, itlist, INDEX_VAR, rtoffset);
        fscan->fdw_recheck_quals =
            (List *)fix_upper_expr(root, (Node *)fscan->fdw_recheck_quals, itlist, INDEX_VAR, rtoffset);
        pfree(itlist);
        /* fdw_scan_tlist itself just needs fix_scan_list() adjustments */
        fscan->fdw_scan_tlist = fix_scan_list(root, fscan->fdw_scan_tlist, rtoffset);
    } else {
        /*
         * Adjust tlist, qual, fdw_exprs, fdw_recheck_quals in the standard
         * way
         */
        fscan->scan.plan.targetlist = fix_scan_list(root, fscan->scan.plan.targetlist, rtoffset);
        fscan->scan.plan.qual = fix_scan_list(root, fscan->scan.plan.qual, rtoffset);
        fscan->fdw_exprs = fix_scan_list(root, fscan->fdw_exprs, rtoffset);
        fscan->fdw_recheck_quals = fix_scan_list(root, fscan->fdw_recheck_quals, rtoffset);
    }

    if (fscan->scan.plan.distributed_keys != NIL) {
        fscan->scan.plan.distributed_keys = fix_scan_list(root, fscan->scan.plan.distributed_keys, rtoffset);
    }

    fscan->fs_relids = offset_relid_set(fscan->fs_relids, rtoffset);

    if (fscan->scan_relid > 0 && isObsOrHdfsTableFormTblOid(fscan->scan_relid)) {
        DefElem* private_data = (DefElem*)linitial(fscan->fdw_private);
        DfsPrivateItem* item = (DfsPrivateItem*)private_data->arg;
        fix_dfs_private_item(root, rtoffset, item);
    }
}
```

<a id="source-045"></a>

#### indexed_tlist 与引用改写上下文结构定义

来源：`src/gausskernel/optimizer/plan/setrefs.cpp:47-82`；范围：**明确标注的源码片段**。

内容 SHA256：`57cdd685215e46e3cab83af4716ec6d63c82f3a64e82e3d3783938bde1b66e57`。

非函数定义上下文；has_non_vars 决定是否尝试完整距离表达式匹配。

```cpp

typedef struct {
    Index varno;         /* RT index of Var */
    AttrNumber varattno; /* attr number of Var */
    AttrNumber resno;    /* TLE position of Var */
} tlist_vinfo;

typedef struct {
    List* tlist;       /* underlying target list */
    int num_vars;      /* number of plain Var tlist entries */
    bool has_ph_vars;  /* are there PlaceHolderVar entries? */
    bool has_non_vars; /* are there other entries? */
    bool return_const; /* return const or Var to const. */
    /* array of num_vars entries: */
    tlist_vinfo vars[1]; /* VARIABLE LENGTH ARRAY */
} indexed_tlist;         /* VARIABLE LENGTH STRUCT */

typedef struct {
    PlannerInfo* root;
    int rtoffset;
} fix_scan_expr_context;

typedef struct {
    PlannerInfo* root;
    indexed_tlist* outer_itlist;
    indexed_tlist* inner_itlist;
    Index acceptable_rel;
    int rtoffset;
} fix_join_expr_context;

typedef struct {
    PlannerInfo* root;
    indexed_tlist* subplan_itlist;
    Index newvarno;
    int rtoffset;
} fix_upper_expr_context;
```

### E. 物理计划构造与 UNION 输出接口

<a id="source-046"></a>

#### create_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:528-602`；范围：**完整函数**。

内容 SHA256：`c15b78c42587af174d99d0276985704c31533caf1938da2052e8504145c97c5b`。

```cpp
Plan* create_plan(PlannerInfo* root, Path* best_path)
{
    Plan* plan = NULL;

    /* plan_params should not be in use in current query level */
    Assert(root->plan_params == NIL);

    /* Initialize this module's private workspace in PlannerInfo */
    root->curOuterRels = NULL;
    root->curOuterParams = NIL;
    u_sess->opt_cxt.is_under_append_plan = false;

    /*
     * Find all called functions from the best_path and save their properties.
     * strictly limit it to read-only SELECT.
     */
    if (ENABLE_FUNCTION_RESULT_CACHE() && root->parse) {
        FuncRetCacheWalkerContext wcxt;
        HASHCTL wcxt_hash_ctl;
        PlannerInfo* parent_root = root->parent_root;

        wcxt.root = root;
        wcxt.isSupportedCmd = root->parse->commandType == CMD_SELECT && !root->parse->hasForUpdate;
        while (wcxt.isSupportedCmd && parent_root != NULL) {
            wcxt.isSupportedCmd &= parent_root->parse->commandType == CMD_SELECT && !parent_root->parse->hasForUpdate;
            if (parent_root->parent_root == NULL) {
                break;
            }
            parent_root = parent_root->parent_root;
        }

        wcxt_hash_ctl.keysize = sizeof(FuncRetcacheKey);
        wcxt_hash_ctl.entrysize = sizeof(FuncRetcacheEntry);
        wcxt_hash_ctl.hash = tag_hash;
        wcxt_hash_ctl.hcxt = AllocSetContextCreate(root->planner_cxt,
                                    "FuncRetCacheWalkerMemoryContext",
                                    ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_MAXSIZE);

        wcxt.hashp = hash_create("func retcache flag hash", 32,
                                              &wcxt_hash_ctl,
                                              HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        scan_func_rescache_recurse(best_path, &wcxt);

        MemoryContextDelete(wcxt_hash_ctl.hcxt);
    }

    /* Recursively process the path tree */
    plan = create_plan_recurse(root, best_path);

    /* Check we successfully assigned all NestLoopParams to plan nodes */
    if (root->curOuterParams != NIL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("failed to assign all NestLoopParams to plan nodes"))));
    /*
     * Reset plan_params to ensure param IDs used for nestloop params are not
     * re-used later
     */
    root->plan_params = NIL;

    /* Try to find the changed vars in chosed inlist2join path */
    if (u_sess->opt_cxt.qrw_inlist2join_optmode == QRW_INLIST2JOIN_CBO && root->var_mappings != NIL) {
        find_inlist2join_path(root, best_path);
    }

    if (u_sess->hook_cxt.forTsdbHook && DB_IS_CMPT(PG_FORMAT) && root->minmax_aggs == NIL) {
        plan->exec_nodes = ng_get_default_computing_group_exec_node();
    }

    return plan;
}
```

<a id="source-047"></a>

#### create_plan_recurse()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:608-732`；范围：**完整函数**。

内容 SHA256：`4399268206c460092fdfed6e89a04765712ce0dd83226e177dbabf2be2c1bd14`。

```cpp
static Plan* create_plan_recurse(PlannerInfo* root, Path* best_path, bool *may_change)
{
    Plan* plan = NULL;
    bool change = false;
    /* Guard against stack overflow due to overly complex plans */
    check_stack_depth();

    switch (best_path->pathtype) {
        case T_HybridRecall:
            plan = create_hybridrecall_plan(root, (HybridRecallPath*)best_path,
                                            root->parse->targetList);
            break;
        case T_CStoreScan:
#ifdef ENABLE_HTAP
        case T_IMCStoreScan:
#endif
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_AnnIndexScan:
        case T_SeqScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_TidRangeScan:
        case T_SubqueryScan:
        case T_FunctionScan:
        case T_TableFuncScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_ForeignScan:
        case T_ExtensiblePlan:
            plan = create_scan_plan(root, best_path);
            break;
        case T_HashJoin:
        case T_MergeJoin:
        case T_NestLoop:
        case T_AsofJoin:
            plan = create_join_plan(root, (JoinPath*)best_path);
            break;
        case T_Append:
            plan = create_append_plan(root, (AppendPath*)best_path);
            break;
        case T_MergeAppend:
            plan = create_merge_append_plan(root, (MergeAppendPath*)best_path);
            break;
        case T_BaseResult:
            /* In new expression framework, the BaseResult node need do projection. */
            if (root->parse->is_flt_frame) {
                if (IsA(best_path, ProjectionPath)) {
                    plan = create_projection_plan(root, (ProjectionPath *)best_path);
                } else {
                    Assert(IsA(best_path, ResultPath));
                    plan = (Plan*)create_result_plan(root, (ResultPath*)best_path);
                }
            } else if (IsA(best_path, ResultPath)) {
                plan = (Plan*)create_result_plan(root, (ResultPath*)best_path);
            } else {
                Assert(IsA(best_path, Path));
                plan = create_scan_plan(root, best_path);
            }
            break;
        case T_ProjectSet:
            plan = (Plan *) create_project_set_plan(root, (ProjectSetPath *) best_path);
            break;
        case T_Memoize:
            plan = (Plan*)create_memoize_plan(root, (MemoizePath*)best_path);
            break;
        case T_Material:
            plan = (Plan*)create_material_plan(root, (MaterialPath*)best_path);
            break;
        case T_Unique:
            plan = create_unique_plan(root, (UniquePath*)best_path);
            break;
        case T_PartIterator:
            if (may_change == NULL) {
                plan = (Plan*)create_globalpartInterator_plan(root, (PartIteratorPath*)best_path, &change);
            } else {
                plan = (Plan*)create_globalpartInterator_plan(root, (PartIteratorPath*)best_path, may_change);
                change = *may_change;
            }
            
            break;
#ifdef PGXC
        case T_RemoteQuery:
            plan = create_remotequery_plan(root, (RemoteQueryPath*)best_path);
            break;
#endif
#ifdef STREAMPLAN
        case T_Stream:
            plan = create_stream_plan(root, (StreamPath*)best_path);
            break;
#endif
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("create_plan_recurse: unrecognized node type: %d", (int)best_path->pathtype)));

            plan = NULL; /* keep compiler quiet */
        } break;
    }
    AssertEreport(PointerIsValid(plan), MOD_OPT, "plan value is null");

    if (root->isPartIteratorPlanning) {
        plan->ispwj = true;
        plan->paramno = root->curIteratorParamIndex;
        plan->subparamno = root->curSubPartIteratorParamIndex;
    } else {
        plan->ispwj = false;
        plan->paramno = -1;
    }

    /*
     * Set smp info for Plan.
     * If the plan is on CN, we should not parallelize.
     * For append path, we have manage it before, do not bother here.
     */
    if (!change) {
        plan->dop = is_execute_on_datanodes(plan) ? SET_DOP(best_path->dop) : 1;
    }

    return plan;
}
```

<a id="source-048"></a>

#### create_scan_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:1232-1425`；范围：**完整函数**。

内容 SHA256：`0a4f6b0c26a66e49930d7cbdfa63f4d0484ef0b9bc2eb1ea016382d123cb076c`。

```cpp
static Plan* create_scan_plan(PlannerInfo* root, Path* best_path)
{
    RelOptInfo* rel = best_path->parent;
    List* tlist = NIL;
    List* scan_clauses = NIL;
    Plan* plan = NULL;

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * If planning is uner recursive CTE, we need check if we are going to generate
     * path that not supported with current Recursive-Execution mode.
     */
    if (STREAM_RECURSIVECTE_SUPPORTED && root->is_under_recursive_cte) {
        if (best_path->pathtype == T_ForeignScan) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "RecursiveUnion contains ForeignScan is not shippable");
            securec_check_ss_c(sprintf_rc, "\0", "\0");

            mark_stream_unsupport();
        }
    }
#endif

    /*
     * For table scans, rather than using the relation targetlist (which is
     * only those Vars actually needed by the query), we prefer to generate a
     * tlist containing all Vars in order.	This will allow the executor to
     * optimize away projection of the table tuples, if possible.  (Note that
     * planner.c may replace the tlist we generate here, forcing projection to
     * occur.)
     */
    if (use_physical_tlist(root, rel) && best_path->pathtype != T_ForeignScan) {
        if (best_path->pathtype == T_IndexOnlyScan) {
            /* For index-only scan, the preferred tlist is the index's */
            tlist = (List*)copyObject(((IndexPath*)best_path)->indexinfo->indextlist);
        } else {
            tlist = build_physical_tlist(root, rel);
            /* if fail because of dropped cols, use regular method */
            if (tlist == NIL) {
                tlist = build_path_tlist(root, best_path);
            }
        }
    } else {
        tlist = build_path_tlist(root, best_path);
    }

    if (u_sess->opt_cxt.is_under_append_plan && tlist == NIL) {
        Const *c = makeConst(INT4OID, -1, InvalidOid, -2, (Datum)0, true, false);
        tlist = lappend(tlist, makeTargetEntry((Expr*)c, 1, NULL, true));
    }

    /*
     * Extract the relevant restriction clauses from the parent relation. The
     * executor must apply all these restrictions during the scan, except for
     * pseudoconstants which we'll take care of below.
     */
    scan_clauses = rel->baserestrictinfo;

    /*
     * If this is a parameterized scan, we also need to enforce all the join
     * clauses available from the outer relation(s).
     *
     * For paranoia's sake, don't modify the stored baserestrictinfo list.
     */
    if (best_path->param_info) {
        scan_clauses = list_concat(list_copy(scan_clauses), best_path->param_info->ppi_clauses);
    }

    switch (best_path->pathtype) {
        case T_HybridRecall:
            plan = (Plan*)create_hybridrecall_plan(root, (HybridRecallPath*)best_path, tlist);
            break;
        case T_SeqScan:
            plan = (Plan*)create_seqscan_plan(root, best_path, tlist, scan_clauses);
            break;
        case T_CStoreScan:
            plan = (Plan*)create_cstorescan_plan(root, best_path, tlist, scan_clauses);
            break;

#ifdef ENABLE_HTAP
        case T_IMCStoreScan:
            plan = (Plan*)create_imcstorescan_plan(root, best_path, tlist, scan_clauses);
            break;
#endif
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            plan = (Plan*)create_tsstorescan_plan(root, best_path, tlist, scan_clauses);
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
        case T_AnnIndexScan:
            if (SUBQUERY_IS_PARAM(root) && PATH_REQ_UPPER(best_path) != NULL) {
                scan_clauses = list_concat(scan_clauses, rel->subplanrestrictinfo);
            }

            plan = (Plan*)create_indexscan_plan(root, (IndexPath*)best_path, tlist, scan_clauses, false);
            break;

        case T_IndexOnlyScan:
            if (SUBQUERY_IS_PARAM(root) && PATH_REQ_UPPER(best_path) != NULL) {
                scan_clauses = list_concat(scan_clauses, rel->subplanrestrictinfo);
            }

            plan = (Plan*)create_indexscan_plan(root, (IndexPath*)best_path, tlist, scan_clauses, true);
            break;

        case T_BitmapHeapScan:
            if (SUBQUERY_IS_PARAM(root) && PATH_REQ_UPPER(best_path) != NULL) {
                scan_clauses = list_concat(scan_clauses, rel->subplanrestrictinfo);
            }

            plan = (Plan*)create_bitmap_scan_plan(root, (BitmapHeapPath*)best_path, tlist, scan_clauses);
            break;

        case T_TidScan:
            plan = (Plan*)create_tidscan_plan(root, (TidPath*)best_path, tlist, scan_clauses);
            break;

        case T_TidRangeScan:
            plan = (Plan*)create_tidrangescan_plan(root, (TidRangePath*)best_path, tlist, scan_clauses);
            break;

        case T_SubqueryScan:
            plan = (Plan*)create_subqueryscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_FunctionScan:
            plan = (Plan*)create_functionscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_TableFuncScan:
            plan = (Plan *) create_tablefuncscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_ValuesScan:
            plan = (Plan*)create_valuesscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_CteScan:
            plan = (Plan*)create_ctescan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_BaseResult:
            plan = (Plan *) create_resultscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_WorkTableScan:
            plan = (Plan*)create_worktablescan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_ExtensiblePlan:
            plan = (Plan*)create_extensible_plan(root, (ExtensiblePath*)best_path, tlist, scan_clauses);
            break;

        case T_ForeignScan:
            plan = (Plan*)create_foreignscan_plan(root, (ForeignPath*)best_path, tlist, scan_clauses);
            break;

        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("create_scan_plan: unrecognized node type: %d", (int)best_path->pathtype)));
            plan = NULL; /* keep compiler quiet */
        } break;
    }

    /* update IO cost */
    if (ENABLE_WORKLOAD_CONTROL && plan != NULL)
        WLMmonitor_check_and_update_IOCost(root, best_path->pathtype, plan->total_cost);

    if (!CheckPathUseGlobalPartIndex(best_path)) {
        (void*)setPartitionParam(root, plan, best_path->parent);
    }
#ifdef ENABLE_MULTIPLE_NODES
    (void*)setBucketInfoParam(root, plan, best_path->parent);
#endif
    /*
     * If there are any pseudoconstant clauses attached to this node, insert a
     * gating Result node that evaluates the pseudoconstants as one-time
     * quals.
     */
    if (root->hasPseudoConstantQuals) {
        plan = create_gating_plan(root, plan, scan_clauses);
    } else if (ScanQualsViolateNotNullConstr(root, rel, best_path)) {
        /*
        * If there is IS NULL qual on a known NOT-NULL attribute, insert a Result node to prevent needless execution.
        */
        plan = (Plan*)make_result(root, plan->targetlist, (Node*)list_make1(makeBoolConst(false, false)), plan);
    }

    return plan;
}
```

<a id="source-049"></a>

#### build_relation_tlist()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:1462-1490`；范围：**完整函数**。

内容 SHA256：`3a3c052ead83a3654b168e9fe0979d969297f487ac8e688ef3998938f9be54ab`。

```cpp
static List* build_relation_tlist(RelOptInfo* rel)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* v = NULL;

    /* For alternative rels, we use its base rel to generate targetlist in Path stage */
    if (rel->base_rel != NULL) {
        Assert(u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_DISABLE);
        Assert((rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
                rel->reloptkind == RELOPT_BASEREL) &&
               rel->rtekind == RTE_SUBQUERY);
        rel = rel->base_rel;
    }

    foreach (v, rel->reltarget->exprs) {
        /* Do we really need to copy here?	Not sure */
        Node* node = (Node*)copyObject(lfirst(v));

        /* Don't lose the resjunk info */
        bool resjunk = false;
        if (((Var *)node)->varattno == 0 && ((Var *)node)->varno > 0) {
            resjunk = true;
        }
        tlist = lappend(tlist, makeTargetEntry((Expr*)node, resno, NULL, resjunk));
        resno++;
    }
    return tlist;
}
```

<a id="source-050"></a>

#### build_path_tlist()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:1499-1528`；范围：**完整函数**。

内容 SHA256：`a0602556b4f7043e14bc7f1591d6dd139bbbf0866be9c298e03c7c99e8876b57`。

```cpp
static List *
build_path_tlist(PlannerInfo *root, Path *path)
{
    List *tlist = NIL;
    Index *sortgrouprefs = path->pathtarget->sortgrouprefs;
    int resno = 1;
    ListCell *v;

    foreach (v, path->pathtarget->exprs) {
        Node *node = (Node *)lfirst(v);
        TargetEntry *tle;

        /*
         * If it's a parameterized path, there might be lateral references in
         * the tlist, which need to be replaced with Params.  There's no need
         * to remake the TargetEntry nodes, so apply this to each list item
         * separately.
         */
        if (path->param_info)
            node = replace_nestloop_params(root, node);

        tle = makeTargetEntry((Expr *)node, resno, NULL, false);
        if (sortgrouprefs)
            tle->ressortgroupref = sortgrouprefs[resno - 1];

        tlist = lappend(tlist, tle);
        resno++;
    }
    return tlist;
}
```

<a id="source-051"></a>

#### use_physical_tlist()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:1570-1629`；范围：**完整函数**。

内容 SHA256：`1f89f6a7f362e91210adefbbda1b11b306377e2b7ca5bdefe071a01af656c9c6`。

```cpp
static bool use_physical_tlist(PlannerInfo* root, RelOptInfo* rel)
{
    int i;
    ListCell* lc = NULL;

    /*
     * Required-column UNION ALL rewriting deliberately narrows every source
     * boundary.  Re-expanding a descendant base scan to the table's physical
     * rowtype would put dead attributes back into EXPLAIN Output and into
     * join/stream slots.
     */
    if (root->union_all_required_columns_pruned && rel->rtekind == RTE_RELATION)
        return false;

    /*
     * We can do this for real relation scans, subquery scans, function scans,
     * values scans, and CTE scans (but not for, eg, joins).
     */
    if (rel->rtekind != RTE_RELATION && rel->rtekind != RTE_SUBQUERY && rel->rtekind != RTE_FUNCTION &&
        rel->rtekind != RTE_TABLEFUNC &&
        rel->rtekind != RTE_VALUES && rel->rtekind != RTE_CTE)
        return false;

    /* In case of cost-based rewrite we can not use physical scan */
    if (rel->base_rel != NULL) {
        return false;
    }

    /*
     * Can't do it with inheritance cases either (mainly because Append
     * doesn't project).
     */
    if (rel->reloptkind != RELOPT_BASEREL)
        return false;

    /*
     * Can't do it if any system columns or whole-row Vars are requested.
     * (This could possibly be fixed but would take some fragile assumptions
     * in setrefs.c, I think.)
     */
    for (i = rel->min_attr; i <= 0; i++) {
        if (!bms_is_empty(rel->attr_needed[i - rel->min_attr])) {
            return false;
        }
    }

    /*
     * Can't do it if the rel is required to emit any placeholder expressions,
     * either.
     */
    foreach (lc, root->placeholder_list) {
        PlaceHolderInfo* phinfo = (PlaceHolderInfo*)lfirst(lc);

        if (bms_nonempty_difference(phinfo->ph_needed, rel->relids) && bms_is_subset(phinfo->ph_eval_at, rel->relids)) {
            return false;
        }
    }

    return true;
}
```

<a id="source-052"></a>

#### disuse_physical_tlist()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:1668-1750`；范围：**完整函数**。

内容 SHA256：`f47ea409639535accb10f5b7110465f41acce7cd5804ac96e581b573468a0c48`。

```cpp
void disuse_physical_tlist(PlannerInfo *root, Plan* plan, Path* path)
{
#ifndef ENABLE_MULTIPLE_NODES
    /* StreamPath may not create stream plan, but it already do this step, so just return. */
    if (IsA(path, StreamPath)) {
        return;
    }
#endif

    /* Only need to undo it for path types handled by create_scan_plan() */
    switch (nodeTag(plan)) {
        case T_SubqueryScan: {
            /* For converted SubqueryScan path we keep the tlist not changed */
            if (path->parent != NULL && path->parent->base_rel != NULL) {
                /* Assert the func-invocation context */
                Assert(u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_DISABLE);
                Assert((path->parent->reloptkind == RELOPT_OTHER_MEMBER_REL ||
                        path->parent->reloptkind == RELOPT_BASEREL) &&
                       path->parent->rtekind == RTE_SUBQUERY);
                break;
            }
            /* Stream plan would be reduced when it's executed on coordinator, while path wouldn't. */
            if (is_execute_on_coordinator(plan) && IsA(path, StreamPath)) {
                path = ((StreamPath*)path)->subpath;
            }
        }
        /* fall throuth */
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_AnnIndexScan:
        case T_BitmapHeapScan:
        case T_CStoreIndexHeapScan:
        case T_TidScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_WorkTableScan:
        case T_CteScan:
        case T_ForeignScan:
        case T_ExtensiblePlan:
        case T_BaseResult: {
            if (IsA(plan, WorkTableScan) && (((WorkTableScan *)plan)->forStartWith)) {
                /* In StartWith-ConnectBy cases we keep the whole worktable scan results
                 * for the upcoming processing steps. */
                break;
            }
            List* tarList = NULL;
            if (path->parent != NULL)
                tarList = build_path_tlist(root, path);
            if (tarList != NULL)
                plan->targetlist = tarList;
            break;
        }
        case T_PartIterator: {
            path = wipe_dummy_path(plan, path);
            PartIteratorPath* piPath = (PartIteratorPath*)path;
            Plan* subPlan = plan->lefttree;
            Plan* scanPlan = IsA(subPlan, Limit) ? subPlan->lefttree : subPlan;
            switch (piPath->subPath->pathtype) {
                case T_SeqScan:
                case T_IndexScan:
                case T_IndexOnlyScan:
                case T_AnnIndexScan:
                case T_BitmapHeapScan:
                case T_CStoreIndexHeapScan:
                case T_TidScan: {
                    List* sub_parList = build_path_tlist(root, path);
                    if (sub_parList != NULL) {
                        subPlan->targetlist = sub_parList;
                        if (scanPlan != subPlan)
                            scanPlan->targetlist = sub_parList;
                        plan->targetlist = sub_parList;
                    }
                    break;
                }
                default:
                    break;
            }
        } break;
        default:
            break;
    }
}
```

<a id="source-053"></a>

#### create_append_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:1925-1992`；范围：**完整函数**。

内容 SHA256：`ded9c37d55b0b7b794e3114839ab6413434b67119d96a321904adc5424f70a6d`。

```cpp
static Plan* create_append_plan(PlannerInfo* root, AppendPath* best_path)
{
    Append* plan = NULL;
    List* tlist = build_path_tlist(root, &best_path->path);
    List* subplans = NIL;
    ListCell* subpaths = NULL;

    if (tlist == NIL) {
        Const* c = makeConst(INT4OID, -1, InvalidOid, -2, (Datum)0, true, false);
        tlist = lappend(tlist, makeTargetEntry((Expr*)c, 1, NULL, true));
    }

    /*
     * It is possible for the subplans list to contain only one entry, or even
     * no entries.	Handle these cases specially.
     *
     * XXX ideally, if there's just one entry, we'd not bother to generate an
     * Append node but just return the single child.  At the moment this does
     * not work because the varno of the child scan plan won't match the
     * parent-rel Vars it'll be asked to emit.
     */
    if (best_path->subpaths == NIL) {
        /* Generate a Result plan with constant-FALSE gating qual */
        return (Plan*)make_result(root, tlist, (Node*)list_make1(makeBoolConst(false, false)), NULL);
    }

    /* Normal case with multiple subpaths */
    foreach (subpaths, best_path->subpaths) {
        Path* subpath = (Path*)lfirst(subpaths);
        u_sess->opt_cxt.is_under_append_plan = true;
        subplans = lappend(subplans, create_plan_recurse(root, subpath));
        u_sess->opt_cxt.is_under_append_plan = false;
    }

    plan = make_append(subplans, tlist);

    /* For hdfs append rel, we set plan rows according to hint value previous applied */
    if (best_path->path.parent->rtekind == RTE_RELATION) {
        plan->plan.plan_rows = best_path->path.rows;
    }

    if (IS_STREAM_PLAN) {
        mark_distribute_setop(root, (Node*)plan, true, best_path->path.distribute_keys == NIL);
#ifdef ENABLE_MULTIPLE_NODES
        /*
         * If we have got distribute keys from best_path, we can not change it any more.
         * But if the diskeys in best_path is not in targetlist, that means the diskeys
         * is redundant, so skip the check of this scene.
         */
        if (best_path->path.distribute_keys != NIL &&
            distributeKeyIndex(root, best_path->path.distribute_keys, tlist) != NIL)
            AssertEreport(equal_distributekey(root, best_path->path.distribute_keys, plan->plan.distributed_keys),
                MOD_OPT_SETOP,
                "Distribute keys error when create append plan.");
#endif
    }

    /*
     * If there are any pseudoconstant clauses attached to this node, insert a
     * gating Result node that evaluates the pseudoconstants as one-time
     * quals.
     */
    if (root->hasPseudoConstantQuals) {
        return create_gating_plan(root, (Plan*)plan, best_path->path.parent->baserestrictinfo);
    }

    return (Plan*)plan;
}
```

<a id="source-054"></a>

#### create_merge_append_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:2001-2127`；范围：**完整函数**。

内容 SHA256：`c9cd14fda00eede98b7ec59101495e12f23ea1cd7f469182b4586d8e1e339ca6`。

```cpp
static Plan* create_merge_append_plan(PlannerInfo* root, MergeAppendPath* best_path)
{
    MergeAppend* node = makeNode(MergeAppend);
    Plan* plan = &node->plan;
    List* tlist = build_path_tlist(root, &best_path->path);
    List* pathkeys = best_path->path.pathkeys;
    List* subplans = NIL;
    ListCell* subpaths = NULL;

    /*
     * We don't have the actual creation of the MergeAppend node split out
     * into a separate make_xxx function.  This is because we want to run
     * prepare_sort_from_pathkeys on it before we do so on the individual
     * child plans, to make cross-checking the sort info easier.
     */
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;

    /* Compute sort column info, and adjust MergeAppend's tlist as needed */
    (void)prepare_sort_from_pathkeys(root,
        plan,
        pathkeys,
        best_path->path.parent->relids,
        NULL,
        true,
        &node->numCols,
        &node->sortColIdx,
        &node->sortOperators,
        &node->collations,
        &node->nullsFirst);

    plan->exec_nodes = NULL;

    /*
     * Now prepare the child plans.  We must apply prepare_sort_from_pathkeys
     * even to subplans that don't need an explicit sort, to make sure they
     * are returning the same sort key columns the MergeAppend expects.
     */
    int i = 0;
    foreach (subpaths, best_path->subpaths) {
        Path* subpath = (Path*)lfirst(subpaths);
        Plan* subplan = NULL;
        int numsortkeys = 0;
        AttrNumber* sortColIdx = NULL;
        Oid* sortOperators = NULL;
        Oid* collations = NULL;
        bool* nullsFirst = NULL;
        u_sess->opt_cxt.is_under_append_plan = true;
        /* Build the child plan */
        subplan = create_plan_recurse(root, subpath);
        u_sess->opt_cxt.is_under_append_plan = false;
        /* Compute sort column info, and adjust subplan's tlist as needed */
        subplan = prepare_sort_from_pathkeys(root,
            subplan,
            pathkeys,
            subpath->parent->relids,
            node->sortColIdx,
            false,
            &numsortkeys,
            &sortColIdx,
            &sortOperators,
            &collations,
            &nullsFirst);

        /*
         * Check that we got the same sort key information.  We just Assert
         * that the sortops match, since those depend only on the pathkeys;
         * but it seems like a good idea to check the sort column numbers
         * explicitly, to ensure the tlists really do match up.
         */
        Assert(numsortkeys == node->numCols);
        if (memcmp(sortColIdx, node->sortColIdx, numsortkeys * sizeof(AttrNumber)) != 0)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("MergeAppend child's targetlist doesn't match MergeAppend"))));
        Assert(memcmp(sortOperators, node->sortOperators, numsortkeys * sizeof(Oid)) == 0);
        Assert(memcmp(collations, node->collations, numsortkeys * sizeof(Oid)) == 0);
        Assert(memcmp(nullsFirst, node->nullsFirst, numsortkeys * sizeof(bool)) == 0);

        /* Now, insert a Sort node if subplan isn't sufficiently ordered */
        if (!pathkeys_contained_in(pathkeys, subpath->pathkeys)) {
            subplan = (Plan*)make_sort(
                root, subplan, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst, best_path->limit_tuples);
            copy_mem_info(&((Sort*)subplan)->mem_info, &best_path->mem_info[i]);
        }

        subplans = lappend(subplans, subplan);

        ExecNodes* subplan_exec_nodes = ng_get_dest_execnodes(subplan);
        if (plan->exec_nodes == NULL) {
            plan->exec_nodes = (ExecNodes*)copyObject(subplan_exec_nodes);
        } else if (!ng_is_same_group(&plan->exec_nodes->distribution, &subplan_exec_nodes->distribution)) {
            plan->exec_nodes->distribution.group_oid = InvalidOid;
        }
        i++;
    }

    node->mergeplans = subplans;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        mark_distribute_setop(root, (Node*)node, true, best_path->path.distribute_keys == NIL);

        /*
         * If we have got distribute keys from best_path, we can not change it any more.
         * But if the diskeys in best_path is not in targetlist, that means the diskeys
         * is redundant, so skip the check of this scene.
         */
        if (best_path->path.distribute_keys != NIL &&
            distributeKeyIndex(root, best_path->path.distribute_keys, tlist) != NIL)
            AssertEreport(equal_distributekey(root, best_path->path.distribute_keys, node->plan.distributed_keys),
                MOD_OPT_SETOP,
                "Distribute keys error when create merge append plan.");
    } else {
        plan->distributed_keys = NIL;
        plan->exec_type = EXEC_ON_COORDS;
        plan->exec_nodes = ng_get_single_node_group_exec_node();
    }
#endif

    copy_path_costsize(plan, (Path*)best_path);

    return (Plan*)node;
}
```

<a id="source-055"></a>

#### create_result_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:2187-2224`；范围：**完整函数**。

内容 SHA256：`171c6871745bf3bb374011870a3e927ea6e0c04978c20d8b6b89a2b8347c1b59`。

```cpp
static BaseResult* create_result_plan(PlannerInfo* root, ResultPath* best_path)
{
    List* tlist = NIL;
    List* quals = NIL;
    List* subquals = NIL;
    Plan* subplan = NULL;

    if (best_path->subpath != NULL) {
        subplan = create_plan_recurse(root, best_path->subpath);
        tlist = subplan->targetlist;

        /*
         * If the path is added because the lower pathnode cannot process vector
         * engine unsupport expression, We need add extra targetlist to lower plannode
         * to process the result->pathqual pulled up from Scan qual
         */
        if (best_path->ispulledupqual) {
            tlist = list_copy(subplan->targetlist);
            adjust_scan_targetlist(best_path, subplan);
        }
    } else {
        if (root->parse->is_flt_frame) {
            /* This is a bit useless currently, because rel will have empty tlist */
            tlist = build_path_tlist(root, &best_path->path);
        } else {
            /* The tlist will be installed later, since we have no RelOptInfo */
            Assert(best_path->path.parent == NULL);
            tlist = NIL;
        }
        
    }

    /* best_path->quals is just bare clauses */
    quals = order_qual_clauses(root, best_path->quals);
    subquals = order_qual_clauses(root, best_path->pathqual);

    return make_result(root, tlist, (Node*)quals, subplan, subquals);
}
```

<a id="source-056"></a>

#### create_projection_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:2233-2282`；范围：**完整函数**。

内容 SHA256：`d6c8e932b6e665883aed5f5644338420b95aba8c2cfe2060b0fdeacfeb60e922`。

```cpp
static Plan *
create_projection_plan(PlannerInfo *root, ProjectionPath *best_path)
{
    Plan       *plan;
    Plan       *subplan;
    List       *tlist;

    /* Since we intend to project, we don't need to constrain child tlist */
    subplan = create_plan_recurse(root, best_path->subpath);

    tlist = build_path_tlist(root, &best_path->path);

    /*
     * We might not really need a Result node here, either because the subplan
     * can project or because it's returning the right list of expressions
     * anyway.  Usually create_projection_path will have detected that and set
     * dummypp if we don't need a Result; but its decision can't be final,
     * because some createplan.c routines change the tlists of their nodes.
     * (An example is that create_merge_append_plan might add resjunk sort
     * columns to a MergeAppend.)  So we have to recheck here.  If we do
     * arrive at a different answer than create_projection_path did, we'll
     * have made slightly wrong cost estimates; but label the plan with the
     * cost estimates we actually used, not "corrected" ones.  (XXX this could
     * be cleaned up if we moved more of the sortcolumn setup logic into Path
     * creation, but that would add expense to creating Paths we might end up
     * not using.)
     */
    if (is_projection_capable_path(best_path->subpath) ||
        tlist_same_exprs(tlist, subplan->targetlist))
    {
        /* Don't need a separate Result, just assign tlist to subplan */
        plan = subplan;
        plan->targetlist = tlist;

        /* Label plan with the estimated costs we actually used */
        plan->startup_cost = best_path->path.startup_cost;
        plan->total_cost = best_path->path.total_cost;
        plan->plan_rows = best_path->path.rows;
        plan->plan_width = best_path->path.pathtarget->width;
        /* ... but be careful not to munge subplan's parallel-aware flag */
    }
    else {
        /* We need a Result node */
        plan = (Plan *) make_result(root, tlist, NULL, subplan, NULL);

        copy_generic_path_info(plan, (Path *) best_path);
    }

    return plan;
}
```

<a id="source-057"></a>

#### create_indexscan_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:3275-3528`；范围：**完整函数**。

内容 SHA256：`767ca1320b5acae0a9783e16e06484bb48aef3d16a112332d2e0e070c38d3b8b`。

```cpp
static Scan* create_indexscan_plan(
    PlannerInfo* root, IndexPath* best_path, List* tlist, List* scan_clauses, bool indexonly, Bitmapset** out_prefixkeys)
{
    Scan* scan_plan = NULL;
    List* indexquals = best_path->indexquals;
    List* indexorderbys = best_path->indexorderbys;
    Index baserelid = best_path->path.parent->relid;
    Oid indexoid = best_path->indexinfo->indexoid;
    List* qpqual = NIL;
    List* stripped_indexquals = NIL;
    List* fixed_indexquals = NIL;
    List* fixed_indexorderbys = NIL;
    List* opquals = NIL;
    double indexselectivity = best_path->indexselectivity;
    ListCell* l = NULL;
    Bitmapset *prefixkeys = NULL;
    Oid* indexorderbyops = NULL;

    /* it should be a base rel... */
    Assert(baserelid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);

    /*
     * Build "stripped" indexquals structure (no RestrictInfos) to pass to
     * executor as indexqualorig
     */
    stripped_indexquals = get_actual_clauses(indexquals);

    /*
     * The executor needs a copy with the indexkey on the left of each clause
     * and with index Vars substituted for table ones.
     */
    fixed_indexquals = fix_indexqual_references(root, best_path, &prefixkeys);

    /*
     * Likewise fix up index attr references in the ORDER BY expressions.
     */
    fixed_indexorderbys = fix_indexorderby_references(root, best_path);

    /*
     * The qpqual list must contain all restrictions not automatically handled
     * by the index, other than pseudoconstant clauses which will be handled
     * by a separate gating plan node.	All the predicates in the indexquals
     * will be checked (either by the index itself, or by nodeIndexscan.c),
     * but if there are any "special" operators involved then they must be
     * included in qpqual.	The upshot is that qpqual must contain
     * scan_clauses minus whatever appears in indexquals.
     *
     * In normal cases simple pointer equality checks will be enough to spot
     * duplicate RestrictInfos, so we try that first.
     *
     * Another common case is that a scan_clauses entry is generated from the
     * same EquivalenceClass as some indexqual, and is therefore redundant
     * with it, though not equal.  (This happens when indxpath.c prefers a
     * different derived equality than what generate_join_implied_equalities
     * picked for a parameterized scan's ppi_clauses.)
     *
     * In some situations (particularly with OR'd index conditions) we may
     * have scan_clauses that are not equal to, but are logically implied by,
     * the index quals; so we also try a predicate_implied_by() check to see
     * if we can discard quals that way.  (predicate_implied_by assumes its
     * first input contains only immutable functions, so we have to check
     * that.)
     *
     * We can also discard quals that are implied by a partial index's
     * predicate, but only in a plain SELECT; when scanning a target relation
     * of UPDATE/DELETE/SELECT FOR UPDATE, we must leave such quals in the
     * plan so that they'll be properly rechecked by EvalPlanQual testing.
     */
    qpqual = NIL;
    foreach (l, scan_clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

        Assert(IsA(rinfo, RestrictInfo));
        if (rinfo->pseudoconstant)
            continue; /* we may drop pseudoconstants here */
        opquals = lappend(opquals, rinfo->clause);

        if (clause_relate_to_prefixkey(rinfo, baserelid, prefixkeys)) {
            qpqual = lappend(qpqual, rinfo);
            continue;
        }
        if (list_member_ptr(indexquals, rinfo))
            continue; /* simple duplicate */
        if (is_redundant_derived_clause(rinfo, indexquals))
            continue; /* derived from same EquivalenceClass */
        if (!contain_mutable_functions((Node*)rinfo->clause)) {
            List* clausel = list_make1(rinfo->clause);

            if (predicate_implied_by(clausel, indexquals))
                continue; /* provably implied by indexquals */
            if (best_path->indexinfo->indpred) {
                if (baserelid != (unsigned int)linitial2_int(root->parse->resultRelations) &&
                    get_parse_rowmark(root->parse, baserelid) == NULL)
                    if (predicate_implied_by(clausel, best_path->indexinfo->indpred))
                        continue; /* implied by index predicate */
            }
        }
        qpqual = lappend(qpqual, rinfo);
    }

    if (u_sess->opt_cxt.is_under_append_plan && tlist == NIL) {
        Const *c = makeConst(INT4OID, -1, InvalidOid, -2, (Datum)0, true, false);
        tlist = lappend(tlist, makeTargetEntry((Expr*)c, 1, NULL, true));
    }

    /* Sort clauses into best execution order */
    qpqual = order_qual_clauses(root, qpqual);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    qpqual = extract_actual_clauses(qpqual, false);

    /*
     * We have to replace any outer-relation variables with nestloop params in
     * the indexqualorig, qpqual, and indexorderbyorig expressions.  A bit
     * annoying to have to do this separately from the processing in
     * fix_indexqual_references --- rethink this when generalizing the inner
     * indexscan support.  But note we can't really do this earlier because
     * it'd break the comparisons to predicates above ... (or would it?  Those
     * wouldn't have outer refs)
     */
    if (best_path->path.param_info) {
        stripped_indexquals = (List*)replace_nestloop_params(root, (Node*)stripped_indexquals);
        qpqual = (List*)replace_nestloop_params(root, (Node*)qpqual);
        indexorderbys = (List*)replace_nestloop_params(root, (Node*)indexorderbys);
    }

    /*
     * If there are ORDER BY expressions, look up the sort operators for
     * their datatypes.
     */
    if (best_path->path.pathkeys && indexorderbys) {
        int       numOrderBys = list_length(indexorderbys);
        int       i;
        ListCell* pathkeyCell;
        ListCell* exprCell;
        PathKey*  pathkey;
        Expr*     expr;

        indexorderbyops = (Oid *) palloc(numOrderBys * sizeof(Oid));

        /*
         * For each ORDER BY expression, get its datatype directly and look up
         * the sort operator in the pathkey's operator family.
         *
         * This approach avoids depending on EquivalenceMember lookup, which can
         * return NULL for child relations in UNION ALL scenarios (e.g., when
         * find_ec_member_for_expr is called with relids=NULL but the member is
         * a child member with non-NULL em_relids).
         */
        Assert(list_length(best_path->path.pathkeys) == list_length(indexorderbys));

        i = 0;
        forboth (pathkeyCell, best_path->path.pathkeys, exprCell, indexorderbys) {
            pathkey = (PathKey *) lfirst(pathkeyCell);
            expr = (Expr *) lfirst(exprCell);
            Oid exprtype = exprType((Node *)expr);
            Oid sortop;

            /* Get sort operator from opfamily */
            sortop = get_opfamily_member(pathkey->pk_opfamily,
                                         exprtype,
                                         exprtype,
                                         pathkey->pk_strategy);
            if (!OidIsValid(sortop))
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                     errmsg("missing sort operator for index ORDER BY")));

            indexorderbyops[i++] = sortop;
        }
    }

    /* Finally ready to build the plan node */
    if (best_path->path.parent->orientation == REL_COL_ORIENTED) {
        scan_plan = (Scan*)make_cstoreindexscan(root,
            &best_path->path,
            tlist,
            qpqual,
            baserelid,
            indexoid,
            fixed_indexquals,
            stripped_indexquals,
            fixed_indexorderbys,
            indexorderbys,
            best_path->indexinfo->indextlist,
            best_path->indexscandir,
            indexonly);
#ifdef ENABLE_MULTIPLE_NODES
    } else if (best_path->path.parent->orientation == REL_TIMESERIES_ORIENTED) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupported Index Scan FOR TIMESERIES.")));
#endif
    } else {
        if (indexonly) {
            scan_plan = (Scan*)make_indexonlyscan(tlist,
                qpqual,
                baserelid,
                indexoid,
                fixed_indexquals,
                stripped_indexquals,
                fixed_indexorderbys,
                best_path->indexinfo->indextlist,
                best_path->indexscandir,
                indexselectivity,
                (best_path->indexinfo->indpred != NIL));
        } else {
            if (best_path->isAnnIndex) {
                scan_plan = (Scan*)make_annindexscan(tlist,
                    qpqual,
                    baserelid,
                    indexoid,
                    fixed_indexquals,
                    stripped_indexquals,
                    fixed_indexorderbys,
                    indexorderbys,
                    best_path->indexscandir,
                    indexselectivity,
                    (best_path->indexinfo->indpred != NIL),
                    best_path->annCount);
                ((AnnIndexScan*)scan_plan)->is_ustore = best_path->is_ustore;
            } else {
                scan_plan = (Scan*)make_indexscan(tlist,
                    qpqual,
                    baserelid,
                    indexoid,
                    fixed_indexquals,
                    stripped_indexquals,
                    fixed_indexorderbys,
                    indexorderbys,
                    indexorderbyops,
                    best_path->indexscandir,
                    indexselectivity,
                    (best_path->indexinfo->indpred != NIL));
                ((IndexScan*)scan_plan)->is_ustore = best_path->is_ustore;
            }
        }
    }

#ifdef STREAMPLAN
    add_distribute_info(root, &(scan_plan->plan), baserelid, &(best_path->path), opquals);
#endif

    if (out_prefixkeys != NULL) {
        *out_prefixkeys = prefixkeys;
    } else {
        bms_free(prefixkeys);
    }

    copy_path_costsize(&scan_plan->plan, &best_path->path);

    return scan_plan;
}
```

<a id="source-058"></a>

#### make_annindexscan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:7093-7115`；范围：**完整函数**。

内容 SHA256：`8a37542560fc447adb8f682bdb2e7181ee8f87f82169605c106df32e88f0e6c7`。

```cpp
static AnnIndexScan* make_annindexscan(List* qptlist, List* qpqual, Index scanrelid, Oid indexid, List* indexqual,
    List* indexqualorig, List* indexorderby, List* indexorderbyorig, ScanDirection indexscandir, double indexselectivity, bool is_partial, double annCount)
{
    AnnIndexScan* node = makeNode(AnnIndexScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->annCount = annCount;
    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;
    node->indexorderby = indexorderby;
    node->indexorderbyorig = indexorderbyorig;
    node->indexorderdir = indexscandir;
    node->selectivity = indexselectivity;
    node->is_partial = is_partial;
    return node;
}
```

<a id="source-059"></a>

#### make_limit()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:9693-9738`；范围：**完整函数**。

内容 SHA256：`2d8f4cc975e02d6035823319ac9bd22f65bd06a6c7cb32c0a2058dcd93de47fc`。

```cpp
Limit* make_limit(PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est,
    int64 count_est, bool enable_parallel)
{
    Limit* node = makeNode(Limit);
    Plan* plan = &node->plan;

    /*
     * If we want to parallel sort + limit, we need to create a plan like:
     * sort -> limit -> local gather -> sort -> limit.
     */
    if (enable_parallel && lefttree->dop > 1)
        lefttree = parallel_limit_sort(root, lefttree, limitOffset, limitCount, offset_est, count_est);

    /*
     * In case of Limit + star with CteScan, we need add Result node to elimit additional internal entry
     * not match in some case, e.g. limit it top-plan node in current subquery
     */
    if (IsA(lefttree, CteScan) && IsCteScanProcessForStartWith((CteScan *)lefttree)) {
        List *tlist = lefttree->targetlist;
        lefttree = (Plan *)make_result(root, tlist, NULL, lefttree, NULL);
    }

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree);
    if (ENABLE_CACHEDPLAN_MGR && root->glob->boundParams != NULL && root->glob->boundParams->uParamInfo != DEFUALT_INFO) {
        if (limitOffset != NULL && IsA(limitOffset, Param)) {
            Param *offset = (Param *)limitOffset;
            Assert(root->glob->boundParams->params[offset->paramid - 1].ptype == INT8OID);
            offset_est = DatumGetInt64(root->glob->boundParams->params[offset->paramid - 1].value);
        }
    }
    cost_limit(plan, lefttree, offset_est, count_est);

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    node->limitOffset = limitOffset;
    node->limitCount = limitCount;

    return node;
}
```

<a id="source-060"></a>

#### make_sort()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:8405-8448`；范围：**完整函数**。

内容 SHA256：`e9f81f98d0c88b185169182e33cccfff7918ed8168deb8a47c03d717ea7f77ac`。

```cpp
Sort* make_sort(PlannerInfo* root, Plan* lefttree, int numCols, AttrNumber* sortColIdx, Oid* sortOperators,
    Oid* collations, bool* nullsFirst, double limit_tuples)
{
    Sort* node = makeNode(Sort);
    Plan* plan = &node->plan;
    Path sort_path; /* dummy for result of cost_sort */
    int width = get_plan_actual_total_width(lefttree, root->glob->vectorized, OP_SORT);

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree); /* only care about copying size */
    cost_sort(&sort_path,
        NIL,
        lefttree->total_cost,
        PLAN_LOCAL_ROWS(lefttree),
        width,
        0.0,
        u_sess->opt_cxt.op_work_mem,
        limit_tuples,
        root->glob->vectorized,
        SET_DOP(lefttree->dop),
        &node->mem_info);
    plan->startup_cost = sort_path.startup_cost;
    plan->total_cost = sort_path.total_cost;
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->hasUniqueResults = lefttree->hasUniqueResults;
    plan->dop = lefttree->dop;
    node->numCols = numCols;
    node->sortColIdx = sortColIdx;
    node->sortOperators = sortOperators;
    node->collations = collations;
    node->nullsFirst = nullsFirst;

    if (root->isPartIteratorPlanning) {
        node->plan.ispwj = true;
    }

    return node;
}
```

<a id="source-061"></a>

#### make_result()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:10224-10278`；范围：**完整函数**。

内容 SHA256：`a2690b6cbc3374c01556e57e492d8cd05e38facf5d12abdf3b6f9b7dee73404f`。

```cpp
BaseResult* make_result(PlannerInfo* root, List* tlist, Node* resconstantqual, Plan* subplan, List* qual)
{
    BaseResult* node = makeNode(BaseResult);
    Plan* plan = &node->plan;

    plan->targetlist = tlist;
    plan->qual = qual;
    plan->righttree = NULL;
    node->resconstantqual = resconstantqual;

    /* 
     * We currently apply this optimization in multiple nodes mode only
     * while sticking to the original plans in the single node mode, because
     * dummy path implementation for single node mode has not been completed yet.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (is_dummy_plan(plan)) {
        subplan = NULL;
    }
#endif
    plan->lefttree = subplan;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN && subplan) {
        inherit_plan_locator_info(plan, subplan);
    } else {
        plan->exec_type = EXEC_ON_ALL_NODES;
        plan->exec_nodes = ng_get_default_computing_group_exec_node();
    }
#endif

    if (subplan != NULL) {
        copy_plan_costsize(plan, subplan);
    } else {
        /*
         * origin comments:
         * 1. wrong if we have a set-valued function? (for multiple and plan_rows)
         * 2. XXX is it worth being smarter? (for plan_width)
         */
        init_plan_cost(plan);
        plan->plan_rows = 1.0;
        plan->total_cost = u_sess->attr.attr_sql.cpu_tuple_cost;

        if (resconstantqual != NULL) {
            QualCost qual_cost;

            cost_qual_eval(&qual_cost, (List*)resconstantqual, root);
            /* resconstantqual is evaluated once at startup */
            plan->startup_cost += qual_cost.startup + qual_cost.per_tuple;
            plan->total_cost += qual_cost.startup + qual_cost.per_tuple;
        }
    }

    return node;
}
```

<a id="source-062"></a>

#### is_projection_capable_plan()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:10884-10949`；范围：**完整函数**。

内容 SHA256：`ff30b76d69424c690ee22c0bc8586ee357064623e47898b7d2b320b061ff236b`。

```cpp
bool is_projection_capable_plan(Plan* plan)
{
    /* Most plan types can project, so just list the ones that can't */
    switch (nodeTag(plan)) {
        case T_Hash:
        case T_Material:
        case T_Sort:
        case T_SortGroup:
        case T_Unique:
        case T_SetOp:
        case T_LockRows:
        case T_Limit:
        case T_ModifyTable:
        case T_Append:
        case T_MergeAppend:
        case T_RecursiveUnion:
        case T_Stream:
#ifdef USE_SPQ
        case T_Motion:
        case T_ShareInputScan:
        case T_Sequence:
        case T_PartitionSelector:
#endif
            return false;

        case T_PartIterator:
            /* target: Data Partition */
            /*
             * If subplan of the PartIterator is a scan, we will do projection
             * on scan node, so no need to add a result node above PartIterator.
             */
            switch (nodeTag(plan->lefttree)) {
                case T_SeqScan:
                case T_CStoreScan:
#ifdef ENABLE_HTAP
                case T_IMCStoreScan:
#endif
#ifdef ENABLE_MULTIPLE_NODES
                case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
                case T_IndexScan:
                case T_IndexOnlyScan:
                case T_AnnIndexScan:
                case T_BitmapHeapScan:
                case T_TidScan:
                case T_CStoreIndexScan:
                case T_CStoreIndexHeapScan:
                    return true;
                default:
                    return false;
            }
        case T_ExtensiblePlan:
            return ((ExtensiblePlan *)plan)->flags & EXTENSIBLEPATH_SUPPORT_PROJECTION;
        case T_ProjectSet:
            /*
             * Although ProjectSet certainly projects, say "no" because we
             * don't want the planner to randomly replace its tlist with
             * something else; the SRFs have to stay at top level.  This might
             * get relaxed later.
             */
            return false;
        default:
            break;
    }
    return true;
}
```

<a id="source-063"></a>

#### is_projection_capable_path()

来源：`src/gausskernel/optimizer/plan/createplan.cpp:12327-12365`；范围：**完整函数**。

内容 SHA256：`29699f1a65e10bd38d61c3957e4c0b78b7c6cbf51cd5e5f9a94d5ede5e7c279f`。

```cpp
bool is_projection_capable_path(Path *path)
{
    /* Most plan types can project, so just list the ones that can't */
    switch (path->pathtype) {
        case T_Hash:
        case T_Material:
        case T_Sort:
        case T_SortGroup:
        case T_Unique:
        case T_SetOp:
        case T_LockRows:
        case T_Limit:
        case T_ModifyTable:
        case T_MergeAppend:
        case T_RecursiveUnion:
            return false;
        case T_Append:

            /*
             * Append can't project, but if it's being used to represent a
             * dummy path, claim that it can project.  This prevents us from
             * converting a rel from dummy to non-dummy status by applying a
             * projection to its dummy path.
             */
            return IS_DUMMY_PATH(path);
        case T_ProjectSet:

            /*
             * Although ProjectSet certainly projects, say "no" because we
             * don't want the planner to randomly replace its tlist with
             * something else; the SRFs have to stay at top level.  This might
             * get relaxed later.
             */
            return false;
        default:
            break;
    }
    return true;
}
```

<a id="source-064"></a>

#### build_base_rel_tlists()

来源：`src/gausskernel/optimizer/plan/initsplan.cpp:122-130`；范围：**完整函数**。

内容 SHA256：`f76ed5909bd03d380e15f28320fa298c1e618f79bb36ab2932a1cba1b5685258`。

核对原始列需求如何进入基础关系；表达式放在 Join 层并不等于本特性把它主动上拉。

```cpp
void build_base_rel_tlists(PlannerInfo* root, List* final_tlist)
{
    List* tlist_vars = pull_var_clause((Node*)final_tlist, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

    if (tlist_vars != NIL) {
        add_vars_to_targetlist(root, tlist_vars, bms_make_singleton(0), true);
        list_free_ext(tlist_vars);
    }
}
```

<a id="source-065"></a>

#### add_vars_to_targetlist()

来源：`src/gausskernel/optimizer/plan/initsplan.cpp:147-183`；范围：**完整函数**。

内容 SHA256：`523dcb32ef36348f83d1109f82728f0b85778eb53823e82933cd0cd696baf579`。

核对原始列需求如何进入基础关系；表达式放在 Join 层并不等于本特性把它主动上拉。

```cpp
void add_vars_to_targetlist(PlannerInfo* root, List* vars, Relids where_needed, bool create_new_ph)
{
    ListCell* temp = NULL;

    AssertEreport(!bms_is_empty(where_needed), MOD_OPT, "bms should not be null");

    foreach (temp, vars) {
        Node* node = (Node*)lfirst(temp);

        if (IsA(node, Var)) {
            Var* var = (Var*)node;
            RelOptInfo* rel = find_base_rel(root, var->varno);
            int attno = var->varattno;
            if (bms_is_subset(where_needed, rel->relids))
                continue;

            AssertEreport(attno >= rel->min_attr && attno <= rel->max_attr, MOD_OPT, "attno is out of range");
            attno -= rel->min_attr;
            if (rel->attr_needed[attno] == NULL) {
                /* Variable not yet requested, so add to rel's targetlist */
                /* XXX is copyObject necessary here? */
                rel->reltarget->exprs = lappend(rel->reltarget->exprs, copyObject(var));
            }
            rel->attr_needed[attno] = bms_add_members(rel->attr_needed[attno], where_needed);
        } else if (IsA(node, PlaceHolderVar)) {
            PlaceHolderVar* phv = (PlaceHolderVar*)node;
            PlaceHolderInfo* phinfo = find_placeholder_info(root, phv, create_new_ph);

            phinfo->ph_needed = bms_add_members(phinfo->ph_needed, where_needed);
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type when add vars to targetlist: %d", (int)nodeTag(node))));
        }
    }
}
```

<a id="source-066"></a>

#### recurse_set_operations()

来源：`src/gausskernel/optimizer/prep/prepunion.cpp:210-394`；范围：**完整函数**。

内容 SHA256：`85a579a86b24a783b6582ee124cb4382ed41cc9305db107769399249aa2ec49e`。

用于检查 visible/resjunk 接口；flattened UNION 与 setop 路径可能不同，要以 GV 实际调用栈为准。

```cpp
static Plan* recurse_set_operations(Node* setOp, PlannerInfo* root, double tuple_fraction, List* colTypes,
    List* colCollations, bool junkOK, int flag, List* refnames_tlist, List** sortClauses, double* pNumGroups)
{
    /* Guard against stack overflow due to overly complex setop nests */
    check_stack_depth();

    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        RangeTblEntry* rte = root->simple_rte_array[rtr->rtindex];
        Query* subquery = rte->subquery;
        RelOptInfo* rel = NULL;
        PlannerInfo* subroot = NULL;
        List* targetlist = NIL;
        Plan* subplan = NULL;
        Plan* plan = NULL;

        bool hack_constants = true;

        AssertEreport(subquery != NULL, MOD_OPT, "subquery should not be NULL in recurse_set_operations");

        /*
         * We need to build a RelOptInfo for each leaf subquery.  This isn't
         * used for anything here, but it carries the subroot data structures
         * forward to setrefs.c processing.
         */
        rel = build_simple_rel(root, rtr->rtindex, RELOPT_BASEREL);
        if (rel == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), (errmsg("Valid rel not found. "))));
            return plan; /* suppress the static check warmings */
        }

        /* plan_params should not be in use in current query level */
        AssertEreport(root->plan_params == NIL,
            MOD_OPT,
            "plan_params should not be in use in current query level in recurse_set_operations");

        /*
         * Generate plan for primitive subquery
         */
        subplan = subquery_planner(root->glob, subquery, root, false, tuple_fraction, &subroot, root->subquery_type, &root->dis_keys);

        /* Save subroot and subplan in RelOptInfo for setrefs.c */
        rel->subplan = subplan;
        rel->subroot = subroot;

        /*
         * It should not be possible for the primitive query to contain any
         * cross-references to other primitive queries in the setop tree.
         */
        if (root->plan_params)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("unexpected outer reference in set operation subquery"))));

        /*
         * Estimate number of groups if caller wants it.  If the subquery used
         * grouping or aggregation, its output is probably mostly unique
         * anyway; otherwise do statistical estimation.
         */
        if (pNumGroups != NULL) {
            if (subquery->groupClause || subquery->distinctClause || subquery->groupingSets || subroot->hasHavingQual ||
                subquery->hasAggs) {
                pNumGroups[0] = PLAN_LOCAL_ROWS(subplan);
                pNumGroups[1] = subplan->plan_rows;
            } else {
                double numdistinct[DISTINCT_GROUPS_LEN] = {1, 1};
                get_num_distinct(subroot,
                    get_tlist_exprs(subquery->targetList, false),
                    PLAN_LOCAL_ROWS(subplan),
                    subplan->plan_rows,
                    ng_get_dest_num_data_nodes(subplan),
                    numdistinct);
                pNumGroups[0] = numdistinct[0];
                pNumGroups[1] = numdistinct[1];
            }
        }

        /*
         * If agg include groupingSets, can not hack const else can lead to result error,
         * because this const can be set to NULL after groupingSet.
         */
        if (subquery->groupingSets) {
            hack_constants = false;
        }

        /*
         * Add a SubqueryScan with the caller-requested targetlist
         */
        targetlist = generate_setop_tlist(
            colTypes, colCollations, flag, rtr->rtindex, hack_constants, subplan->targetlist, refnames_tlist);
#ifdef STREAMPLAN
        if (IS_STREAM_PLAN && is_execute_on_datanodes(subplan) && is_hashed_plan(subplan)) {
            /* Get distribute key index from subplan */
            List* distribute_index = distributeKeyIndex(rel->subroot, subplan->distributed_keys, subplan->targetlist);
            if (distribute_index == NIL) {
                rel->distribute_keys = NIL;
                rel->rangelistOid = InvalidOid;
            } else {
                ListCell* lc = NULL;
                /* Find each distribute key for new subquery */
                foreach (lc, distribute_index) {
                    int resno = lfirst_int(lc);
                    ListCell* lc2 = NULL;
                    Var* var = NULL;

                    /*
                     * Find from subquery targetlist for distribute key. We should traverse
                     * the targetlist and get the real var, because targetlist of subquery can
                     * be a subset of subplan's targetlist, and there can be type cast on base
                     * vars
                     */
                    foreach (lc2, targetlist) {
                        TargetEntry* tle = (TargetEntry*)lfirst(lc2);
                        var = locate_distribute_var(tle->expr);
                        if (var != NULL && var->varattno == resno)
                            break;
                    }
                    if (lc2 != NULL) {
                        rel->distribute_keys = lappend(rel->distribute_keys, var);
                    } else {
                        list_free_ext(rel->distribute_keys);
                        rel->distribute_keys = NIL;
                        /* If this distribute keys can not found, rel's distribute_keys should be null. */
                        break;
                    }
                }
            }
            rel->locator_type = get_locator_type(subplan);
        }
#endif
        plan = (Plan*)make_subqueryscan(targetlist, NIL, rtr->rtindex, rel->subplan);
#ifdef STREAMPLAN
        plan->distributed_keys = rel->distribute_keys;
#endif

        root->param_upper = bms_union(root->param_upper, subroot->param_upper);

        /*
         * We don't bother to determine the subquery's output ordering since
         * it won't be reflected in the set-op result anyhow.
         */
        *sortClauses = NIL;

        return plan;
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;
        Plan* plan = NULL;

        /* UNIONs are much different from INTERSECT/EXCEPT */
        if (op->op == SETOP_UNION)
            plan = generate_union_plan(op, root, tuple_fraction, refnames_tlist, sortClauses, pNumGroups);
        else
            plan = generate_nonunion_plan(op, root, tuple_fraction, refnames_tlist, sortClauses, pNumGroups);

        /*
         * If necessary, add a Result node to project the caller-requested
         * output columns.
         *
         * XXX you don't really want to know about this: setrefs.c will apply
         * fix_upper_expr() to the Result node's tlist. This would fail if the
         * Vars generated by generate_setop_tlist() were not exactly equal()
         * to the corresponding tlist entries of the subplan. However, since
         * the subplan was generated by generate_union_plan() or
         * generate_nonunion_plan(), and hence its tlist was generated by
         * generate_append_tlist(), this will work.  We just tell
         * generate_setop_tlist() to use varno 0.
         */
        if (flag >= 0 || !tlist_same_datatypes(plan->targetlist, colTypes, junkOK) ||
            !tlist_same_collations(plan->targetlist, colCollations, junkOK)) {
            plan = (Plan*)make_result(root,
                generate_setop_tlist(colTypes, colCollations, flag, 0, false, plan->targetlist, refnames_tlist),
                NULL,
                plan);
        }
        return plan;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(setOp))));
        return NULL; /* keep compiler quiet */
    }
}
```

<a id="source-067"></a>

#### generate_union_plan()

来源：`src/gausskernel/optimizer/prep/prepunion.cpp:629-701`；范围：**完整函数**。

内容 SHA256：`60d069fe2a4b0bea1a4950fa20b95dc9d34ac594818b450547b795514fd3748b`。

用于检查 visible/resjunk 接口；flattened UNION 与 setop 路径可能不同，要以 GV 实际调用栈为准。

```cpp
static Plan* generate_union_plan(SetOperationStmt* op, PlannerInfo* root, double tuple_fraction, List* refnames_tlist,
    List** sortClauses, double* pNumGroups)
{
    List* planlist = NIL;
    List* tlist = NIL;
    Plan* plan = NULL;

    /*
     * If plain UNION, tell children to fetch all tuples.
     *
     * Note: in UNION ALL, we pass the top-level tuple_fraction unmodified to
     * each arm of the UNION ALL.  One could make a case for reducing the
     * tuple fraction for later arms (discounting by the expected size of the
     * earlier arms' results) but it seems not worth the trouble. The normal
     * case where tuple_fraction isn't already zero is a LIMIT at top level,
     * and passing it down as-is is usually enough to get the desired result
     * of preferring fast-start plans.
     */
    if (!op->all)
        tuple_fraction = 0.0;

    /*
     * If any of my children are identical UNION nodes (same op, all-flag, and
     * colTypes) then they can be merged into this node so that we generate
     * only one Append and unique-ification for the lot.  Recurse to find such
     * nodes and compute their children's plans.
     */
    planlist = list_concat(recurse_union_children(op->larg, root, tuple_fraction, op, refnames_tlist),
        recurse_union_children(op->rarg, root, tuple_fraction, op, refnames_tlist));

    /*
     * Generate tlist for Append plan node.
     *
     * The tlist for an Append plan isn't important as far as the Append is
     * concerned, but we must make it look real anyway for the benefit of the
     * next plan level up.
     */
    tlist = generate_append_tlist(op->colTypes, op->colCollations, false, planlist, refnames_tlist);

    /*
     * Append the child results together.
     */
    plan = (Plan*)make_append(planlist, tlist);

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        bool isunionall = (op->all ? true : false);
        mark_distribute_setop(root, (Node*)plan, isunionall, true);
        parallel_setop(root, plan, isunionall);
    }
#endif

    /*
     * For UNION ALL, we just need the Append plan.  For UNION, need to add
     * node(s) to remove duplicates.
     */
    if (op->all)
        *sortClauses = NIL; /* result of UNION ALL is always unsorted */
    else
        plan = make_union_unique(op, plan, root, tuple_fraction, sortClauses);

    /*
     * Estimate number of groups if caller wants it.  For now we just assume
     * the output is unique --- this is certainly true for the UNION case, and
     * we want worst-case estimates anyway.
     */
    if (pNumGroups != NULL) {
        pNumGroups[0] = PLAN_LOCAL_ROWS(plan);
        pNumGroups[1] = plan->plan_rows;
    }

    return plan;
}
```

<a id="source-068"></a>

#### generate_setop_tlist()

来源：`src/gausskernel/optimizer/prep/prepunion.cpp:1140-1234`；范围：**完整函数**。

内容 SHA256：`b9bd68baa89d9ccdae17823c8ef9c80fe704ad180b1e37df5717eb7745e58d06`。

用于检查 visible/resjunk 接口；flattened UNION 与 setop 路径可能不同，要以 GV 实际调用栈为准。

```cpp
static List* generate_setop_tlist(List* colTypes, List* colCollations, int flag, Index varno, bool hack_constants,
    List* input_tlist, List* refnames_tlist)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* ctlc = NULL;
    ListCell* cclc = NULL;
    ListCell* itlc = NULL;
    ListCell* rtlc = NULL;
    TargetEntry* tle = NULL;
    Node* expr = NULL;

    /* there's no forfour() so we must chase one list manually */
    rtlc = list_head(refnames_tlist);
    forthree(ctlc, colTypes, cclc, colCollations, itlc, input_tlist)
    {
        Oid colType = lfirst_oid(ctlc);
        Oid colColl = lfirst_oid(cclc);
        TargetEntry* inputtle = (TargetEntry*)lfirst(itlc);
        TargetEntry* reftle = (TargetEntry*)lfirst(rtlc);

        rtlc = lnext(rtlc);

        AssertEreport(inputtle->resno == resno, MOD_OPT, "inputtle's resno mismatch in generate_setop_tlist");
        AssertEreport(reftle->resno == resno, MOD_OPT, "reftle's resno mismatch in generate_setop_tlist");
        AssertEreport(!inputtle->resjunk, MOD_OPT, "inputtle's resjunk should be false in generate_setop_tlist");
        AssertEreport(!reftle->resjunk, MOD_OPT, "reftle's resjunk should be false in generate_setop_tlist");

        /*
         * Generate columns referencing input columns and having appropriate
         * data types and column names.  Insert datatype coercions where
         * necessary.
         *
         * HACK: constants in the input's targetlist are copied up as-is
         * rather than being referenced as subquery outputs.  This is mainly
         * to ensure that when we try to coerce them to the output column's
         * datatype, the right things happen for UNKNOWN constants.  But do
         * this only at the first level of subquery-scan plans; we don't want
         * phony constants appearing in the output tlists of upper-level
         * nodes!
         */
        if (hack_constants && inputtle->expr && IsA(inputtle->expr, Const))
            expr = (Node*)inputtle->expr;
        else
            expr = (Node*)makeVar(varno,
                inputtle->resno,
                exprType((Node*)inputtle->expr),
                exprTypmod((Node*)inputtle->expr),
                exprCollation((Node*)inputtle->expr),
                0);

        if (exprType(expr) != colType) {
            /*
             * Note: it's not really cool to be applying coerce_to_common_type
             * here; one notable point is that assign_expr_collations never
             * gets run on any generated nodes.  For the moment that's not a
             * problem because we force the correct exposed collation below.
             * It would likely be best to make the parser generate the correct
             * output tlist for every set-op to begin with, though.
             */
            expr = coerce_to_common_type(NULL, /* no UNKNOWNs here */
                expr,
                colType,
                "UNION/INTERSECT/EXCEPT");
        }

        /*
         * Ensure the tlist entry's exposed collation matches the set-op. This
         * is necessary because plan_set_operations() reports the result
         * ordering as a list of SortGroupClauses, which don't carry collation
         * themselves but just refer to tlist entries.	If we don't show the
         * right collation then planner.c might do the wrong thing in
         * higher-level queries.
         *
         * Note we use RelabelType, not CollateExpr, since this expression
         * will reach the executor without any further processing.
         */
        if (exprCollation(expr) != colColl) {
            expr = (Node*)makeRelabelType((Expr*)expr, exprType(expr), exprTypmod(expr), colColl, COERCE_DONTCARE);
        }

        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup(reftle->resname), false);
        tlist = lappend(tlist, tle);
    }

    if (flag >= 0) {
        /* Add a resjunk flag column */
        /* flag value is the given constant */
        expr = (Node*)makeConst(INT4OID, -1, InvalidOid, sizeof(int4), Int32GetDatum(flag), false, true);
        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup("flag"), true);
        tlist = lappend(tlist, tle);
    }

    return tlist;
}
```

<a id="source-069"></a>

#### generate_append_tlist()

来源：`src/gausskernel/optimizer/prep/prepunion.cpp:1249-1338`；范围：**完整函数**。

内容 SHA256：`593ef2fb328425cd36b9d3eee88d3a3bd26ae74b61706458536e4a95acd3cc6d`。

用于检查 visible/resjunk 接口；flattened UNION 与 setop 路径可能不同，要以 GV 实际调用栈为准。

```cpp
static List* generate_append_tlist(
    List* colTypes, List* colCollations, bool flag, List* input_plans, List* refnames_tlist)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* curColType = NULL;
    ListCell* curColCollation = NULL;
    ListCell* ref_tl_item = NULL;
    int colindex;
    TargetEntry* tle = NULL;
    Node* expr = NULL;
    ListCell* planl = NULL;
    int32* colTypmods = NULL;

    /*
     * First extract typmods to use.
     *
     * If the inputs all agree on type and typmod of a particular column, use
     * that typmod; else use -1.
     */
    colTypmods = (int32*)palloc(list_length(colTypes) * sizeof(int32));

    foreach (planl, input_plans) {
        Plan* subplan = (Plan*)lfirst(planl);
        ListCell* subtlist = NULL;

        curColType = list_head(colTypes);
        colindex = 0;
        foreach (subtlist, subplan->targetlist) {
            TargetEntry* subtle = (TargetEntry*)lfirst(subtlist);

            if (subtle->resjunk)
                continue;
            AssertEreport(curColType != NULL,
                MOD_OPT,
                "set-op's result column datatype should not be NULL in processing in generate_append_tlist");
            if (exprType((Node*)subtle->expr) == lfirst_oid(curColType)) {
                /* If first subplan, copy the typmod; else compare */
                int32 subtypmod = exprTypmod((Node*)subtle->expr);

                if (planl == list_head(input_plans))
                    colTypmods[colindex] = subtypmod;
                else if (subtypmod != colTypmods[colindex])
                    colTypmods[colindex] = -1;
            } else {
                /* types disagree, so force typmod to -1 */
                colTypmods[colindex] = -1;
            }
            curColType = lnext(curColType);
            colindex++;
        }
        AssertEreport(curColType == NULL,
            MOD_OPT,
            "set-op's result column datatype should be NULL after processing in generate_append_tlist");
    }

    /*
     * Now we can build the tlist for the Append.
     */
    colindex = 0;
    forthree(curColType, colTypes, curColCollation, colCollations, ref_tl_item, refnames_tlist)
    {
        Oid colType = lfirst_oid(curColType);
        int32 colTypmod = colTypmods[colindex++];
        Oid colColl = lfirst_oid(curColCollation);
        TargetEntry* reftle = (TargetEntry*)lfirst(ref_tl_item);

        AssertEreport(reftle->resno == resno,
            MOD_OPT,
            "resno mismatch when building the tlist for the Append in generate_append_tlist");
        AssertEreport(!reftle->resjunk,
            MOD_OPT,
            "resjunk should be fase when building the tlist for the Append in generate_append_tlist");
        expr = (Node*)makeVar(0, resno, colType, colTypmod, colColl, 0);
        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup(reftle->resname), false);
        tlist = lappend(tlist, tle);
    }

    if (flag) {
        /* Add a resjunk flag column */
        /* flag value is shown as copied up from subplan */
        expr = (Node*)makeVar(0, resno, INT4OID, -1, InvalidOid, 0);
        tle = makeTargetEntry((Expr*)expr, (AttrNumber)resno++, pstrdup("flag"), true);
        tlist = lappend(tlist, tle);
    }

    pfree_ext(colTypmods);

    return tlist;
}
```

### F. 计划遍历与兄弟分支隔离

<a id="source-070"></a>

#### walk_plan_node_fields()

来源：`src/gausskernel/optimizer/util/planmem_walker.cpp:97-128`；范围：**完整函数**。

内容 SHA256：`312b568d1f6aa09687b55ee73b1da7f58334e784af0299039ee0b3d5f6899f31`。

```cpp
bool walk_plan_node_fields(Plan* plan, MethodWalker walker, void* context)
{
    MethodP2Walker p2walker = (MethodP2Walker)walker;
    MethodPlanWalkerContext* mcontext = ((MethodPlanWalkerContext*)context);

    /* Init Plan nodes (uncorrelated expr subselects */
    mcontext->base.init_plans = list_concat(mcontext->base.init_plans, list_copy(plan->initPlan));

    /* If current node is join, then mark the flag about its children node */
    if (IsBlockedJoinNode(plan))
        mcontext->status |= UNDER_MULTI_GROUP_OP;

    /* input plan tree(s) */
    if (p2walker((Node*)(plan->lefttree), context))
        return true;

    /* target list to be computed at this node */
    if (p2walker((Node*)(plan->righttree), context))
        return true;

    mcontext->status &= ~UNDER_MULTI_GROUP_OP;

    /* target list to be computed at this node */
    if (p2walker((Node*)(plan->targetlist), context))
        return true;

    /* implicitly ANDed qual conditions */
    if (p2walker((Node*)(plan->qual), context))
        return true;

    return false;
}
```

<a id="source-071"></a>

#### walk_scan_node_fields()

来源：`src/gausskernel/optimizer/util/planmem_walker.cpp:145-153`；范围：**完整函数**。

内容 SHA256：`e32676060f576f4165a31c6a0aa1a7704dd7fa00b2cbce98f9d018d3f03be780`。

```cpp
bool walk_scan_node_fields(Scan* scan, MethodWalker walker, void* context)
{
    /* A Scan node is a kind of Plan node. */
    if (walk_plan_node_fields((Plan*)scan, walker, context))
        return true;

    /* The only additional field is an Index so no extra walking. */
    return false;
}
```

<a id="source-072"></a>

#### walk_join_node_fields()

来源：`src/gausskernel/optimizer/util/planmem_walker.cpp:169-178`；范围：**完整函数**。

内容 SHA256：`f2299682ba9d0829099f77174a3927bc27724cf70519e2368b3cc9308114ea07`。

```cpp
bool walk_join_node_fields(Join* join, MethodWalker walker, void* context)
{
    /* A Join node is a kind of Plan node. */
    if (walk_plan_node_fields((Plan*)join, walker, context))
        return true;

    MethodP2Walker p2walker = (MethodP2Walker)walker;

    return p2walker((Node*)(join->joinqual), context);
}
```

<a id="source-073"></a>

#### plan_tree_walker()

来源：`src/gausskernel/optimizer/util/planmem_walker.cpp:190-663`；范围：**完整函数**。

内容 SHA256：`7546e6cf7f536c0b393bc2359663dfd19afd9aed9e9e3362f0e70294bdbe417e`。

```cpp
bool plan_tree_walker(Node* node, MethodWalker walker, void* context)
{
    MethodP2Walker p2walker = (MethodP2Walker)walker;

    if (node == NULL)
        return false;

    check_stack_depth();

    switch (nodeTag(node)) {
        case T_Plan:
        case T_ProjectSet:
            return walk_plan_node_fields((Plan*)node, walker, context);
#ifdef USE_SPQ
        case T_Result:
#endif
        case T_BaseResult:
        case T_VecResult:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BaseResult*)node)->resconstantqual, context))
                return true;
            break;

        case T_Append:
        case T_VecAppend: {
            MethodPlanWalkerContext* mcontext = (MethodPlanWalkerContext*)context;

            /* Mark the status to let children node know it's under multi group node */
            mcontext->status |= UNDER_MULTI_GROUP_OP;
            if (p2walker((Node*)((Append*)node)->appendplans, context)) {
                mcontext->status &= ~UNDER_MULTI_GROUP_OP;
                return true;
            }
            mcontext->status &= ~UNDER_MULTI_GROUP_OP;

            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
        } break;

        case T_MergeAppend: {
            MethodPlanWalkerContext* mcontext = (MethodPlanWalkerContext*)context;

            /* Mark the status to let children node know it's under multi group node */
            mcontext->status |= UNDER_MULTI_GROUP_OP;
            if (p2walker((Node*)((MergeAppend*)node)->mergeplans, context)) {
                mcontext->status &= ~UNDER_MULTI_GROUP_OP;
                return true;
            }
            mcontext->status &= ~UNDER_MULTI_GROUP_OP;

            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
        } break;

        case T_CStoreIndexAnd:
        case T_BitmapAnd:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BitmapAnd*)node)->bitmapplans, context))
                return true;
            break;

        case T_Scan:
            return walk_scan_node_fields((Scan*)node, walker, context);

        case T_CStoreIndexOr:
        case T_BitmapOr:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BitmapOr*)node)->bitmapplans, context))
                return true;
            break;

        case T_SeqScan:
#ifdef USE_SPQ
        case T_SpqSeqScan:
#endif
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (IsA(node, ValuesScan)) {
                ValuesScan* splan = (ValuesScan*)node;
                if (p2walker((Node*)splan->values_lists, context))
                    return true;
            }
            break;

        case T_VecForeignScan:
        case T_ForeignScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((ForeignScan*)node)->fdw_exprs, context))
                return true;
            if (p2walker((Node*)((ForeignScan*)node)->fdw_private, context))
                return true;
            break;
        case T_ExtensiblePlan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((ExtensiblePlan*)node)->extensible_plans, context))
                return true;
            if (p2walker((Node*)((ExtensiblePlan*)node)->extensible_exprs, context))
                return true;
            break;
#ifdef USE_SPQ
        case T_SpqIndexScan:
#endif
        case T_IndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((IndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;
#ifdef USE_SPQ
        case T_SpqIndexOnlyScan:
#endif
        case T_IndexOnlyScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((IndexOnlyScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_AnnIndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((AnnIndexScan*)node)->indexqual, context)) {
                return true;
            }
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_CStoreIndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreIndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_BitmapIndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BitmapIndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_CStoreIndexCtidScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreIndexCtidScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_CStoreIndexHeapScan:
#ifdef USE_SPQ
        case T_SpqBitmapHeapScan:
#endif
        case T_BitmapHeapScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            break;

        case T_TidScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((TidScan*)node)->tidquals, context))
                return true;
            break;
        case T_TidRangeScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((TidRangeScan*)node)->tidrangequals, context))
                return true;
            break;
        case T_VecSubqueryScan:
        case T_SubqueryScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((SubqueryScan*)node)->subplan, context))
                return true;
            break;

        case T_CStoreScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreScan*)node)->cstorequal, context))
                return true;
            break;

#ifdef ENABLE_HTAP
        case T_IMCStoreScan:
#ifdef USE_SPQ
        case T_SpqCStoreScan:
#endif
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreScan*)node)->cstorequal, context))
                return true;
            break;
#endif

#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((TsStoreScan*)node)->tsstorequal, context))
                return true;
            break;
#endif   /* ENABLE_MULTIPLE_NODES */

        case T_VecStream:
        case T_Stream:
            /* Under stream node should be marked */
            ((MethodPlanWalkerContext*)context)->status |= UNDER_STREAM;
            return walk_scan_node_fields((Scan*)node, walker, context);

        case T_Join:
            /* Abstract: really should see only subclasses. */
            return walk_join_node_fields((Join*)node, walker, context);

        case T_VecNestLoop:
        case T_NestLoop:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            break;

        case T_VecMergeJoin:
        case T_MergeJoin:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            if (p2walker((Node*)((MergeJoin*)node)->mergeclauses, context))
                return true;
            break;

        case T_VecHashJoin:
        case T_HashJoin:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            if (p2walker((Node*)((HashJoin*)node)->hashclauses, context))
                return true;
#ifdef USE_SPQ
            if (p2walker((Node *)((HashJoin *)node)->hashqualclauses, context))
                return true;
#endif
            break;
        case T_AsofJoin:
        case T_VecAsofJoin:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            if (p2walker((Node*)((VecAsofJoin*)node)->hashclauses, context))
                return true;
            if (p2walker((Node*)((VecAsofJoin*)node)->mergeclauses, context))
                return true;
            break;
        case T_VecToRow:
        case T_RowToVec:
        case T_VecMaterial:
        case T_Material:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            break;

        case T_VecSort:
        case T_Sort:
        case T_SortGroup:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple counts and lists of indexes and oids. */
            break;

        case T_VecAgg:
        case T_Agg:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple items and lists of simple items. */
            break;

        case T_WindowAgg:
        case T_VecWindowAgg:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple items and lists of simple items. */
            break;

        case T_Unique:
        case T_VecUnique:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple items and lists of simple items. */
            break;

        case T_Hash:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other info is in parent HashJoin node. */
            break;

        case T_VecLimit:
        case T_Limit:
            if (walk_plan_node_fields((Plan*)node, walker, context)) {
                return true;
            }

            if (p2walker((Node*)(((Limit*)node)->limitCount), context)) {
                return true;
            }

            if (p2walker((Node*)(((Limit*)node)->limitOffset), context)) {
                return true;
            }

            break;

#ifdef USE_SPQ
        case T_Motion:
            if (walk_plan_node_fields((Plan *) node, walker, context))
                return true;
 
            if (p2walker((Node *) ((Motion *)node)->hashExprs, context))
                return true;
 
            break;
 
        case T_SplitUpdate:
        case T_AssertOp:
            if (walk_plan_node_fields((Plan *) node, walker, context))
                return true;
            break;
 
        case T_ShareInputScan:
            if (walk_plan_node_fields((Plan *) node, walker, context))
                return true;
            break;
 
        case T_Sequence:
            if (walk_plan_node_fields((Plan *) node, walker, context))
                return true;
            if (p2walker((Node *) ((Sequence *) node)->subplans, context))
                return true;
            break;
#endif
        case T_VecModifyTable:
        case T_ModifyTable: {
            ModifyTable* modifytable = (ModifyTable*)node;

            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;

            /* recursive to list of plans */
            if (p2walker((Node*)(((ModifyTable*)modifytable)->plans), context))
                return true;
        } break;

        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            Plan* subplan_plan = plan_tree_base_subplan_get_plan((plan_tree_base_prefix*)context, subplan);

            if (subplan_plan == NULL) {
                return false;
            }

            if (expression_tree_walker((Node*)subplan->testexpr, walker, context)) {
                return true;
            }

            if (p2walker((Node*)subplan_plan, context)) {
                return true;
            }

            if (expression_tree_walker((Node*)subplan->args, walker, context)) {
                return true;
            }
        } break;

        /* handle InitPlan */
        case T_Param: {
            Param* p = (Param*)node;
            plan_tree_base_prefix* pcontext = (plan_tree_base_prefix*)context;
            ListCell* lc = NULL;

            /* Traverse the subplan from the init plan entry */
            if (p->paramkind == PARAM_EXEC) {
                foreach (lc, pcontext->init_plans) {
                    SubPlan* subplan = (SubPlan*)lfirst(lc);
                    if (list_member_int(subplan->setParam, p->paramid)) {
                        if (plan_tree_walker((Node*)subplan, walker, context))
                            return true;
                        break;
                    }
                }
            }
        } break;

        case T_Query:
            return query_tree_walker((Query*)node, walker, context, 0);

        case T_SetOp:
        case T_VecSetOp:
        case T_PartIterator:
        case T_VecPartIterator:
        case T_Group:
        case T_VecGroup:
        case T_LockRows:
        case T_RecursiveUnion:
        case T_VecRemoteQuery:
        case T_RemoteQuery: {
            MethodPlanWalkerContext* mcontext = (MethodPlanWalkerContext*)context;

            /*
             * Since query can contain multiple remote query, we should
             * change to a new group for each remote query
             */
            if (IsA(node, RemoteQuery) || IsA(node, VecRemoteQuery)) {
                /* Query on obs: If we find dn gather, then treat it as scan and return */
                if (mcontext->dnExec && ((RemoteQuery*)node)->position != GATHER)
                    return false;

                if (mcontext->phase == ASSIGN_MEM)
                    mcontext->groupTree = NULL;
                else
                    mcontext->groupTree = (OperatorGroupNode*)list_nth(mcontext->groupTreeList, mcontext->groupTreeIdx);

                /* With new group tree, reset traverse variable */
                mcontext->nextGroupId = 0;
                for (int i = 0; i < mcontext->ng_num; i++) {
                    mcontext->ng_queryMemKBArray[i].currQueryMemKB = 0;
                    mcontext->ng_queryMemKBArray[i].minCurrQueryMemKB = 0;
                }
                mcontext->dnExec = true;
            }
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (IsA(node, RemoteQuery) || IsA(node, VecRemoteQuery)) {
                /* Finishing scan the query, add it to the list for re-traverse */
                if (mcontext->phase == ASSIGN_MEM)
                    mcontext->groupTreeList = lappend(mcontext->groupTreeList, mcontext->groupTree);
                else
                    mcontext->groupTreeIdx++;

                /* Every remotequery is serialized, so get the max */
                for (int i = 0; i < mcontext->ng_num; i++) {
                    mcontext->ng_queryMemKBArray[i].queryMemKB =
                        Max(mcontext->ng_queryMemKBArray[i].queryMemKB, mcontext->ng_queryMemKBArray[i].currQueryMemKB);
                    mcontext->ng_queryMemKBArray[i].minQueryMemKB = Max(mcontext->ng_queryMemKBArray[i].minQueryMemKB,
                        mcontext->ng_queryMemKBArray[i].minCurrQueryMemKB);
                }
                ((MethodPlanWalkerContext*)context)->dnExec = false;
            }
        } break;

        case T_IntList:
        case T_OidList:
        case T_DefElem:
        case T_Var:
        case T_Const:
        case T_CoerceToDomainValue:
        case T_CaseTestExpr:
        case T_SetToDefault:
        case T_RangeTblRef:
            break;
        default:
            return expression_tree_walker(node, walker, context);
    }
    return false;
}
```

<a id="source-074"></a>

#### plan_tree_base_subplan_get_plan()

来源：`src/gausskernel/optimizer/util/planmem_walker.cpp:672-691`；范围：**完整函数**。

内容 SHA256：`e798927453b59db7bffebd1901714a12fffec0e9d457831d8e60826484de1e3a`。

```cpp
Plan* plan_tree_base_subplan_get_plan(plan_tree_base_prefix* base, SubPlan* subplan)
{
    if (base == NULL)
        return NULL;
    else if (IsA(base->node, PlannedStmt)) {
        /* If the subplan has already traversed, just return */
        if (base->traverse_flag[subplan->plan_id - 1])
            return NULL;
        base->traverse_flag[subplan->plan_id - 1] = true;
        return exec_subplan_get_plan((PlannedStmt*)base->node, subplan);
    } else if (IsA(base->node, PlannerInfo))
        return planner_subplan_get_plan((PlannerInfo*)base->node, subplan);
    else if (IsA(base->node, PlannerGlobal)) {
        PlannerInfo rootdata;
        rootdata.glob = (PlannerGlobal*)base->node;
        return planner_subplan_get_plan(&rootdata, subplan);
    }

    return NULL;
}
```

### G. GUC、状态与节点定义

<a id="source-075"></a>

#### PlannerInfo 的列裁剪标志

来源：`src/include/nodes/relation.h:524-534`；范围：**明确标注的源码片段**。

内容 SHA256：`d676f679cd9b48c99dc78eb8b5d9c896d659e2907be2a03095c5dbbc0af24ca2`。

```cpp
    bool ru_is_under_start_with;
    /*
     * A parent UNION ALL query has physically compacted its private Query
     * tree. Descendant base scans must retain the compact PathTarget.
     */
    bool union_all_required_columns_pruned;
} PlannerInfo;

/*
 * In places where it's known that simple_rte_array[] must have been prepared
 * already, we just index into it to fetch RTEs.  In code that might be
```

<a id="source-076"></a>

#### AnnOrderByRef 定义

来源：`src/include/nodes/primnodes.h:982-999`；范围：**明确标注的源码片段**。

内容 SHA256：`88cd99e05f442234f7e85071e570849436eb18093a27e10493fdfcf6db152ed8`。

```cpp

/*
 * AnnOrderByRef
 *
 * A post-setrefs executor reference to the current DiskANN candidate's
 * already-computed ORDER BY distance.  original_expr is retained both for
 * EXPLAIN deparsing and as the fail-closed executor fallback when the
 * runtime policy or AM channel cannot satisfy the planned contract.
 *
 * This node is deliberately introduced only in an AnnIndexScan targetlist;
 * it is not parser-visible and must never be treated as a Var.
 */
typedef struct AnnOrderByRef {
    Expr xpr;
    int orderby_index; /* zero-based index into the AM ORDER BY channel */
    int quality;       /* AnnOrderByDistanceQuality, kept as int to avoid an access-header dependency */
    Expr* original_expr;
} AnnOrderByRef;
```

<a id="source-077"></a>

#### AM 距离质量与发布通道

来源：`src/include/access/relscan.h:30-59`；范围：**明确标注的源码片段**。

内容 SHA256：`090bd63ca7354eee649c70772cc5ef541cd05a0b11a65be8c0427797fa0e07f4`。

源仓 L2 标记 exact、Cosine 标记 approximate；KGN 两者均近似，不得机械沿用 exact-L2 分支和回退权限。

```cpp
#define IsValidScanDesc(sd)  (sd != NULL)

/* Quality of a distance published by an ANN access method. */
typedef enum AnnOrderByDistanceQuality {
    ANN_ORDERBY_DISTANCE_UNAVAILABLE = 0,
    ANN_ORDERBY_DISTANCE_EXACT_L2,
    ANN_ORDERBY_DISTANCE_APPROX_COSINE
} AnnOrderByDistanceQuality;

/*
 * Dedicated current-row channel for ANN ORDER BY result reuse.  It is
 * separate from xs_orderbyvals because approximate values must not be
 * exposed as generic exact ordering-operator results.
 */
typedef struct AnnOrderByDistanceChannel {
    bool requested;
    int requested_mode;
    bool valid;
    bool isnull;
    /* L2 only: a merged TID need not own the node's stored vector. */
    bool heap_recheck;
    Datum value;
    AnnOrderByDistanceQuality quality;
    uint64 scan_epoch;
    uint64 row_token;
    ItemPointerData heap_tid;
    uint64 full_distance_evaluations;
    uint64 output_conversions;
    uint64 publications;
} AnnOrderByDistanceChannel;
```

<a id="source-078"></a>

#### 距离复用枚举 GUC 取值

来源：`src/common/backend/utils/misc/guc/guc_sql.cpp:369-376`；范围：**明确标注的源码片段**。

内容 SHA256：`58dd6b4133921f58930ac15060f43fa4551c59504736424e90c6c713fc05f766`。

```cpp
static const struct config_enum_entry ann_orderby_distance_reuse_options[] = {
    {"off", ANN_ORDERBY_REUSE_OFF, false},
    {"l2", ANN_ORDERBY_REUSE_L2, false},
    {"cosine", ANN_ORDERBY_REUSE_COSINE, false},
    {"all", ANN_ORDERBY_REUSE_ALL, false},
    {NULL, 0, false}
};

```

<a id="source-079"></a>

#### 两个 UNION ALL 布尔 GUC 注册

来源：`src/common/backend/utils/misc/guc/guc_sql.cpp:853-874`；范围：**明确标注的源码片段**。

内容 SHA256：`f34e04d71ed86f4570365eba14345e090bedaf43ccb76b09d8fd7c2c2ee9ea7c`。

```cpp
        {{"enable_union_all_diskann_topk_pushdown",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables pushing ORDER BY + LIMIT to UNION ALL leaves for DiskANN vector TopK."),
            NULL},
            &u_sess->attr.attr_sql.enable_union_all_diskann_topk_pushdown,
            false,
            NULL,
            NULL,
            NULL},
        {{"enable_union_all_required_columns_pushdown",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Enables pruning unused UNION ALL output columns at every leaf."),
            NULL},
            &u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown,
            false,
            NULL,
            NULL,
            NULL},
```

<a id="source-080"></a>

#### 距离复用 GUC 注册

来源：`src/common/backend/utils/misc/guc/guc_sql.cpp:3855-3868`；范围：**明确标注的源码片段**。

内容 SHA256：`a20014a83deb703bc72ef30db0874c8dcf747c8bb0907318f2162cd55e48cc11`。

```cpp
        {{"ann_orderby_distance_reuse",
            PGC_USERSET,
            NODE_ALL,
            QUERY_TUNING_METHOD,
            gettext_noop("Controls reuse of DiskANN's computed ORDER BY distance."),
            gettext_noop("Off recomputes the SQL expression; l2 reuses exact L2 values; "
                         "cosine permits DiskANN's approximate normalized cosine value; "
                         "all enables both.")},
            &u_sess->attr.attr_sql.ann_orderby_distance_reuse,
            ANN_ORDERBY_REUSE_OFF,
            ann_orderby_distance_reuse_options,
            NULL,
            NULL,
            NULL},
```

<a id="source-081"></a>

#### 距离复用工作版本门槛

来源：`src/include/miscadmin.h:876-880`；范围：**明确标注的源码片段**。

内容 SHA256：`f5eb995b28e18aa17434497ecdd6160bfaafd85598b7bdb6c19a8bf905af01c7`。

```cpp
 * server working version as its local compatibility floor; do not borrow
 * the unrelated DiskANN INCLUDE gate. */
#define ANN_ORDERBY_DISTANCE_REUSE_VERSION_NUM 93098
#ifdef ENABLE_NEON
extern void InitStandaloneProcess(const char* argv0);
```

<a id="source-082"></a>

#### 距离缓存、绑定状态与表达式运行时上下文

来源：`src/include/nodes/execnodes.h:2188-2218`；范围：**明确标注的源码片段**。

内容 SHA256：`b570374a36b3d7d7bd063afd74faa2824137be28958bd9a9c2ab68838189014c`。

```cpp
    int64 limitCount;
    /* Current heap row's value copied from IndexScanDesc.xs_ann_orderby. */
    Datum iss_AnnOrderByValue;
    bool iss_AnnOrderByIsNull;
    bool iss_AnnOrderByValid;
    bool iss_AnnOrderByHeapRecheck;
    AnnOrderByDistanceQuality iss_AnnOrderByQuality;
    AnnOrderByDistanceQuality iss_AnnOrderByExpectedQuality;
    uint64 iss_AnnOrderByEpoch;
    uint64 iss_AnnOrderByRowToken;
    uint64 iss_AnnOrderByReads;
    uint64 iss_AnnOrderByFallbacks;
    /* DiskANN materializes qualified candidates before first output. The
     * sorter spills under work_mem; its private tuples preserve heap system
     * attributes as well as the exact/approximate key used for this scan. */
    bool iss_DiskannOrdered;
    bool iss_OrderedReady;
    bool iss_OrderedHasSRF;
    struct Tuplesortstate* iss_OrderedSort;
    TupleTableSlot* iss_OrderedSlot;
    ExprState* iss_OrderedExpr;
    List* iss_OrderedQual;
} AnnIndexScanState;

/* Shared private payload used by recursive and flattened evaluation of an
 * AnnOrderByRef. */
typedef struct AnnOrderByRefExprRuntime {
    AnnIndexScanState* scanstate;
    ExprState* fallback;
    AnnOrderByDistanceQuality quality;
} AnnOrderByRefExprRuntime;
```

<a id="source-083"></a>

#### ANN_ORDERBY_REUSE_OFF 定义上下文

来源：`src/include/utils/guc.h:458-466`；范围：**明确标注的源码片段**。

内容 SHA256：`251ada7c5d1fee43385db2650bbff717bad80ee02d3c59a55305f8508a324cfa`。

```cpp
} HnswIterativeScanOptions;

typedef enum {
    ANN_ORDERBY_REUSE_OFF = 0,
    ANN_ORDERBY_REUSE_L2 = 1 << 0,
    ANN_ORDERBY_REUSE_COSINE = 1 << 1,
    ANN_ORDERBY_REUSE_ALL = ANN_ORDERBY_REUSE_L2 | ANN_ORDERBY_REUSE_COSINE
} AnnOrderByDistanceReuseMode;

```

<a id="source-084"></a>

#### preprocess_union_all_required_columns 定义上下文

来源：`src/include/optimizer/prep.h:117-125`；范围：**明确标注的源码片段**。

内容 SHA256：`25a3aec1b1a8db674d52eedd9c993558bd7971668470b80742ce2208a49921f2`。

```cpp
/*
 * prototypes for preptopk.cpp
 */
extern bool preprocess_union_all_required_columns(PlannerInfo* root);
extern void preprocess_union_all_diskann_topk(PlannerInfo* root);
extern void preprocess_union_all_order_key_projection(PlannerInfo* root);


#endif /* PREP_H */
```

<a id="source-085"></a>

#### ann_orderby_distance_reuse 定义上下文

来源：`src/include/knl/knl_guc/knl_session_attr_sql.h:293-301`；范围：**明确标注的源码片段**。

内容 SHA256：`17f3868890b69a352ca6a9314a9e3dfc87ea2c81f7877327ab7a2f94a651c20e`。

```cpp
    bool enable_hybrid_recall;
    bool hybrid_recall_log_summary;
    int vectorEngineStrategy;
    int ann_orderby_distance_reuse;
    int hybrid_recall_parallel_mode;
    int gms_stats_history_retention;
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
    bool enable_custom_parser;
    bool dolphin;
```

<a id="source-086"></a>

#### enable_union_all_required_columns_pushdown 定义上下文

来源：`src/include/knl/knl_guc/knl_session_attr_sql.h:68-76`；范围：**明确标注的源码片段**。

内容 SHA256：`056f98ec0ed794f41cef252f0fc3720636301bfd220af729e009d28bad4bb222`。

```cpp
    bool enable_indexonlyscan_prefetch;
#endif
    bool enable_bitmapscan;
    bool enable_union_all_required_columns_pushdown;
    bool enable_union_all_diskann_topk_pushdown;
    bool force_bitmapand;
    #ifdef ENABLE_HTAP
    bool enable_imcsscan;
    #endif
```

### H. EXPLAIN 与执行器取值验证

<a id="source-087"></a>

#### show_plan_tlist()

来源：`src/gausskernel/optimizer/commands/explain.cpp:3690-3804`；范围：**完整函数**。

内容 SHA256：`491dcd1af7d14db78ca01b4a7b4b7ea3b305f9a8998ef27bc4a664f705bd7fe8`。

源仓扫描层双括号要求 AnnIndexScan + AnnOrderByRef；GV 普通 IndexScan 要核查对应分支，不能仅凭括号判断真实节点。

```cpp
static void show_plan_tlist(PlanState* planstate, List* ancestors, ExplainState* es)
{
    Plan* plan = planstate->plan;
    List* context = NIL;
    List* result = NIL;
    bool useprefix = false;
    ListCell* lc = NULL;

    /* No work if empty tlist (this occurs eg in bitmap indexscans) */
    if (plan->targetlist == NIL)
        return;
    /* The tlist of an Append isn't real helpful, so suppress it */
    if (IsA(plan, Append) || IsA(plan, VecAppend))
        return;
    /* Likewise for MergeAppend and RecursiveUnion */
    if (IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend))
        return;
    if (IsA(plan, RecursiveUnion) || IsA(plan, VecRecursiveUnion))
        return;

    /* Set up deparsing context */
    context = deparse_context_for_planstate((Node*)planstate, ancestors, es->rtable);
    useprefix = list_length(es->rtable) > 1;

    /* Deparse each result column (we now include resjunk ones) */
    foreach (lc, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        char* output = NULL;

        /* skip pseudo columns once startwith exist */
        if (IsPseudoReturnTargetEntry(tle) || IsPseudoInternalTargetEntry(tle)) {
            continue;
        }

        output = deparse_expression((Node*)tle->expr, context, useprefix, false);
        /*
         * AnnOrderByRef is executor-only, so ruleutils deliberately prints
         * its original SQL expression.  Add one display-only wrapper at the
         * producing AnnIndexScan boundary: together with the OpExpr's own
         * parentheses this mirrors the familiar "referenced result" form.
         * Keep the generic deparser transparent so upper OUTER_VAR expansion
         * does not grow a third pair of parentheses.
         */
        if (IsA(plan, AnnIndexScan) && IsA(tle->expr, AnnOrderByRef)) {
            char* referencedOutput = psprintf("(%s)", output);
            pfree_ext(output);
            output = referencedOutput;
        }
        result = lappend(result, output);
    }

    if (IsA(plan, ForeignScan) || IsA(plan, VecForeignScan)) {
        ForeignScan* fscan = (ForeignScan*)plan;
        if (OidIsValid(fscan->scan_relid) && IsSpecifiedFDWFromRelid(fscan->scan_relid, GC_FDW)) {
            List* str_targetlist = get_str_targetlist(fscan->fdw_private);
            if (str_targetlist != NULL)
                result = str_targetlist;
        }
    }

    /* Print results */
    ExplainPropertyList("Output", result, es);

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
        es->planinfo->m_verboseInfo->set_plan_name<false, true>();
        appendStringInfo(es->planinfo->m_verboseInfo->info_str, "Output: ");
        ExplainPrettyList(result, es);
    }

    /* For explain plan */
    if (es->plan)
        es->planinfo->m_planTableData->set_plan_table_projection(plan->plan_node_id, result);
#ifdef ENABLE_MULTIPLE_NODES
    /* show distributeKey of scan */
    if (plan->distributed_keys != NIL && show_scan_distributekey(plan)) {
        List* distKey = NIL;
        List* keyList = NIL;
        keyList = plan->distributed_keys;
        foreach (lc, keyList) {
            Var* distriVar = (Var*)lfirst(lc);
            distKey = lappend(distKey, deparse_expression((Node*)distriVar, context, useprefix, false));
        }

        ExplainPropertyList("Distribute Key", distKey, es);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            es->planinfo->m_verboseInfo->set_plan_name<false, true>();
            appendStringInfoString(es->planinfo->m_verboseInfo->info_str, "Distribute Key: ");
            ExplainPrettyList(distKey, es);
        }
    }
#endif
#ifdef STREAMPLAN
    if ((IsA(plan, Stream) || IsA(plan, VecStream)) &&
        (is_redistribute_stream((Stream*)plan) || is_hybid_stream((Stream*)plan)) &&
        ((Stream*)plan)->distribute_keys != NIL) {
        List* distKey = NIL;
        List* keyList = NIL;
        keyList = ((Stream*)plan)->distribute_keys;
        foreach (lc, keyList) {
            Var* distriVar = (Var*)lfirst(lc);
            distKey = lappend(distKey, deparse_expression((Node*)distriVar, context, useprefix, false));
        }

        ExplainPropertyList("Distribute Key", distKey, es);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            es->planinfo->m_verboseInfo->set_plan_name<false, true>();
            appendStringInfoString(es->planinfo->m_verboseInfo->info_str, "Distribute Key: ");
            ExplainPrettyList(distKey, es);
        }
        list_free_deep(distKey);
    }
#endif
}
```

<a id="source-088"></a>

#### get_rule_expr()

来源：`src/common/backend/utils/adt/ruleutils.cpp:10069-11056`；范围：**完整函数**。

内容 SHA256：`9abc9553d438365c274847417aa526bb782f7a1d9a0b9a19c1b07fe544b1a7a6`。

完整反解析函数，包含 AnnOrderByRef 分支；原始表达式的显示不代表执行器重新计算。

```cpp
static void get_rule_expr(Node* node, deparse_context* context, bool showimplicit, bool no_alias)
{
    StringInfo buf = context->buf;
    char* tmp = NULL;

    if (node == NULL)
        return;

    /*
     * Each level of get_rule_expr must emit an indivisible term
     * (parenthesized if necessary) to ensure result is reparsed into the same
     * expression tree.  The only exception is that when the input is a List,
     * we emit the component items comma-separated with no surrounding
     * decoration; this is convenient for most callers.
     */
    switch (nodeTag(node)) {
        case T_Var:
            tmp = get_variable((Var*)node, 0, false, context, false, no_alias);
            if (tmp != NULL) {
                pfree_ext(tmp);
            }
            break;
            
        case T_Rownum:
            appendStringInfo(buf, "ROWNUM");
            break;

        case T_Const:
            get_const_expr((Const*)node, context, 0);
            break;

        case T_UserVar:
            appendStringInfo(buf, "@");
            appendStringInfo(buf, "%s", ((UserVar*)node)->name);
            break;

        case T_UserSetElem:{
            UserSetElem* userSet = (UserSetElem*)node;
            appendStringInfoString(buf, "@var := ");
            get_rule_expr((Node*)userSet->val, context, true, no_alias);
            break;
        }

        case T_SetVariableExpr:
            get_const_expr((Const*)(((SetVariableExpr*)node)->value), context, 0);
            break;

        case T_Param:
            get_parameter((Param*)node, context);
            break;

        case T_Aggref:
            get_agg_expr((Aggref*)node, context);
            break;

        case T_GroupingFunc: {
            GroupingFunc* gexpr = (GroupingFunc*)node;

            appendStringInfoString(buf, "GROUPING(");
            get_rule_expr((Node*)gexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_GroupingId: {
            appendStringInfoString(buf, "GROUPINGID()");
        } break;

        case T_WindowFunc:
            get_windowfunc_expr((WindowFunc*)node, context);
            break;

        case T_ArrayRef: {
            ArrayRef* aref = (ArrayRef*)node;
            bool need_parens = false;

            /*
             * If the argument is a CaseTestExpr, we must be inside a
             * FieldStore, ie, we are assigning to an element of an array
             * within a composite column.  Since we already punted on
             * displaying the FieldStore's target information, just punt
             * here too, and display only the assignment source
             * expression.
             */
            if (IsA(aref->refexpr, CaseTestExpr)) {
                Assert(aref->refassgnexpr);
                get_rule_expr((Node*)aref->refassgnexpr, context, showimplicit, no_alias);
                break;
            }

            /*
             * Parenthesize the argument unless it's a simple Var or a
             * FieldSelect.  (In particular, if it's another ArrayRef, we
             * *must* parenthesize to avoid confusion.)
             */
            need_parens = !IsA(aref->refexpr, Var) && !IsA(aref->refexpr, FieldSelect);
            if (need_parens)
                appendStringInfoChar(buf, '(');
            get_rule_expr((Node*)aref->refexpr, context, showimplicit, no_alias);
            if (need_parens)
                appendStringInfoChar(buf, ')');

            /*
             * If there's a refassgnexpr, we want to print the node in the
             * format "array[subscripts] := refassgnexpr".	This is not
             * legal SQL, so decompilation of INSERT or UPDATE statements
             * should always use processIndirection as part of the
             * statement-level syntax.	We should only see this when
             * EXPLAIN tries to print the targetlist of a plan resulting
             * from such a statement.
             */
            if (aref->refassgnexpr) {
                Node* refassgnexpr = NULL;

                /*
                 * Use processIndirection to print this node's subscripts
                 * as well as any additional field selections or
                 * subscripting in immediate descendants.  It returns the
                 * RHS expr that is actually being "assigned".
                 */
                refassgnexpr = processIndirection(node, context, true);
                appendStringInfoString(buf, " := ");
                get_rule_expr(refassgnexpr, context, showimplicit, no_alias);
            } else {
                /* Just an ordinary array fetch, so print subscripts */
                printSubscripts(aref, context);
            }
        } break;

        case T_FuncExpr:
            get_func_expr((FuncExpr*)node, context, showimplicit);
            break;

        case T_NamedArgExpr: {
            NamedArgExpr* na = (NamedArgExpr*)node;

            appendStringInfo(buf, "%s := ", quote_identifier(na->name));
            get_rule_expr((Node*)na->arg, context, showimplicit, no_alias);
        } break;

        case T_OpExpr:
            get_oper_expr((OpExpr*)node, context, no_alias);
            break;

        case T_DistinctExpr: {
            DistinctExpr* expr = (DistinctExpr*)node;
            List* args = expr->args;
            Node* arg1 = (Node*)linitial(args);
            Node* arg2 = (Node*)lsecond(args);

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren(arg1, context, true, node, no_alias);
            appendStringInfo(buf, " IS DISTINCT FROM ");
            get_rule_expr_paren(arg2, context, true, node, no_alias);
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_NullIfExpr: {
            NullIfExpr* nullifexpr = (NullIfExpr*)node;

            appendStringInfo(buf, "NULLIF(");
            get_rule_expr((Node*)nullifexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;
            List* args = expr->args;
            Node* arg1 = (Node*)linitial(args);
            Node* arg2 = (Node*)lsecond(args);

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren(arg1, context, true, node, no_alias);
            appendStringInfo(buf,
                " %s %s (",
                generate_operator_name(expr->opno, exprType(arg1), get_base_element_type(exprType(arg2))),
                expr->useOr ? "ANY" : "ALL");
            get_rule_expr_paren(arg2, context, true, node, no_alias);

            /*
             * There's inherent ambiguity in "x op ANY/ALL (y)" when y is
             * a bare sub-SELECT.  Since we're here, the sub-SELECT must
             * be meant as a scalar sub-SELECT yielding an array value to
             * be used in ScalarArrayOpExpr; but the grammar will
             * preferentially interpret such a construct as an ANY/ALL
             * SubLink.  To prevent misparsing the output that way, insert
             * a dummy coercion (which will be stripped by parse analysis,
             * so no inefficiency is added in dump and reload).  This is
             * indeed most likely what the user wrote to get the construct
             * accepted in the first place.
             */
            if (IsA(arg2, SubLink) && ((SubLink*)arg2)->subLinkType == EXPR_SUBLINK)
                appendStringInfo(buf, "::%s", format_type_with_typemod(exprType(arg2), exprTypmod(arg2)));
            appendStringInfoChar(buf, ')');
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            Node* first_arg = (Node*)linitial(expr->args);
            ListCell* arg = lnext(list_head(expr->args));

            switch (expr->boolop) {
                case AND_EXPR:
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, '(');
                    get_rule_expr_paren(first_arg, context, false, node, no_alias);
                    while (arg != NULL) {
                        appendStringInfo(buf, " AND ");
                        get_rule_expr_paren((Node*)lfirst(arg), context, false, node, no_alias);
                        arg = lnext(arg);
                    }
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, ')');
                    break;

                case OR_EXPR:
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, '(');
                    get_rule_expr_paren(first_arg, context, false, node, no_alias);
                    while (arg != NULL) {
                        appendStringInfo(buf, " OR ");
                        get_rule_expr_paren((Node*)lfirst(arg), context, false, node, no_alias);
                        arg = lnext(arg);
                    }
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, ')');
                    break;

                case NOT_EXPR:
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, '(');
                    appendStringInfo(buf, "NOT ");
                    get_rule_expr_paren(first_arg, context, false, node, no_alias);
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, ')');
                    break;

                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized boolop: %d", (int)expr->boolop)));
            }
        } break;

        case T_SubLink:
            get_sublink_expr((SubLink*)node, context);
            break;

        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;

            /*
             * We cannot see an already-planned subplan in rule deparsing,
             * only while EXPLAINing a query plan.	We don't try to
             * reconstruct the original SQL, just reference the subplan
             * that appears elsewhere in EXPLAIN's result.
             */
            if (subplan->useHashTable)
                appendStringInfo(buf, "(hashed %s)", subplan->plan_name);
            else
                appendStringInfo(buf, "(%s)", subplan->plan_name);
        } break;

        case T_AlternativeSubPlan: {
            AlternativeSubPlan* asplan = (AlternativeSubPlan*)node;
            ListCell* lc = NULL;

            /* As above, this can only happen during EXPLAIN */
            appendStringInfo(buf, "(alternatives: ");
            foreach (lc, asplan->subplans) {
                SubPlan* splan = (SubPlan*)lfirst(lc);

                Assert(IsA(splan, SubPlan));
                if (splan->useHashTable)
                    appendStringInfo(buf, "hashed %s", splan->plan_name);
                else
                    appendStringInfo(buf, "%s", splan->plan_name);
                if (lnext(lc))
                    appendStringInfo(buf, " or ");
            }
            appendStringInfo(buf, ")");
        } break;

        case T_FieldSelect: {
            FieldSelect* fselect = (FieldSelect*)node;
            Node* arg = (Node*)fselect->arg;
            int fno = fselect->fieldnum;
            const char* fieldname = NULL;
            bool need_parens = false;

            /*
             * Parenthesize the argument unless it's an ArrayRef or
             * another FieldSelect.  Note in particular that it would be
             * WRONG to not parenthesize a Var argument; simplicity is not
             * the issue here, having the right number of names is.
             */
            need_parens = !IsA(arg, ArrayRef) && !IsA(arg, FieldSelect);
            if (need_parens)
                appendStringInfoChar(buf, '(');
            get_rule_expr(arg, context, true, no_alias);
            if (need_parens)
                appendStringInfoChar(buf, ')');

            /*
             * Get and print the field name.
             */
            fieldname = get_name_for_var_field((Var*)arg, fno, 0, context);
            appendStringInfo(buf, ".%s", quote_identifier(fieldname));
        } break;

        case T_FieldStore: {
            FieldStore* fstore = (FieldStore*)node;
            bool need_parens = false;

            /*
             * There is no good way to represent a FieldStore as real SQL,
             * so decompilation of INSERT or UPDATE statements should
             * always use processIndirection as part of the
             * statement-level syntax.	We should only get here when
             * EXPLAIN tries to print the targetlist of a plan resulting
             * from such a statement.  The plan case is even harder than
             * ordinary rules would be, because the planner tries to
             * collapse multiple assignments to the same field or subfield
             * into one FieldStore; so we can see a list of target fields
             * not just one, and the arguments could be FieldStores
             * themselves.	We don't bother to try to print the target
             * field names; we just print the source arguments, with a
             * ROW() around them if there's more than one.  This isn't
             * terribly complete, but it's probably good enough for
             * EXPLAIN's purposes; especially since anything more would be
             * either hopelessly confusing or an even poorer
             * representation of what the plan is actually doing.
             */
            need_parens = (list_length(fstore->newvals) != 1);
            if (need_parens)
                appendStringInfoString(buf, "ROW(");
            get_rule_expr((Node*)fstore->newvals, context, showimplicit, no_alias);
            if (need_parens)
                appendStringInfoChar(buf, ')');
        } break;

        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            Node* arg = (Node*)relabel->arg;

            if (relabel->relabelformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, relabel->resulttype, relabel->resulttypmod, node);
            }
        } break;

        case T_CoerceViaIO: {
            CoerceViaIO* iocoerce = (CoerceViaIO*)node;
            Node* arg = (Node*)iocoerce->arg;

            if (iocoerce->coerceformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, iocoerce->resulttype, -1, node);
            }
        } break;

        case T_ArrayCoerceExpr: {
            ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
            Node* arg = (Node*)acoerce->arg;

            if (acoerce->coerceformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, acoerce->resulttype, acoerce->resulttypmod, node);
            }
        } break;

        case T_ConvertRowtypeExpr: {
            ConvertRowtypeExpr* convert = (ConvertRowtypeExpr*)node;
            Node* arg = (Node*)convert->arg;

            if (convert->convertformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, convert->resulttype, -1, node);
            }
        } break;

        case T_CollateExpr: {
            CollateExpr* collate = (CollateExpr*)node;
            Node* arg = (Node*)collate->arg;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren(arg, context, showimplicit, node, no_alias);
            appendStringInfo(buf, " COLLATE %s", generate_collation_name(collate->collOid));
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            ListCell* temp = NULL;

            appendContextKeyword(context, "CASE", 0, PRETTYINDENT_VAR, 0);
            if (caseexpr->arg) {
                appendStringInfoChar(buf, ' ');
                get_rule_expr((Node*)caseexpr->arg, context, true);
            }
            foreach (temp, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(temp);
                Node* w = (Node*)when->expr;

                if (caseexpr->arg) {
                    /*
                     * The parser should have produced WHEN clauses of the
                     * form "CaseTestExpr = RHS", possibly with an
                     * implicit coercion inserted above the CaseTestExpr.
                     * For accurate decompilation of rules it's essential
                     * that we show just the RHS.  However in an
                     * expression that's been through the optimizer, the
                     * WHEN clause could be almost anything (since the
                     * equality operator could have been expanded into an
                     * inline function).  If we don't recognize the form
                     * of the WHEN clause, just punt and display it as-is.
                     */
                    if (IsA(w, OpExpr)) {
                        List* args = ((OpExpr*)w)->args;

                        if (list_length(args) == 2 &&
                            IsA(strip_implicit_coercions((Node*)linitial(args)), CaseTestExpr))
                            w = (Node*)lsecond(args);
                    }
                }

                if (!PRETTY_INDENT(context))
                    appendStringInfoChar(buf, ' ');
                appendContextKeyword(context, "WHEN ", 0, 0, 0);
                get_rule_expr(w, context, false, no_alias);
                appendStringInfo(buf, " THEN ");
                get_rule_expr((Node*)when->result, context, true, no_alias);
            }
            if (!PRETTY_INDENT(context))
                appendStringInfoChar(buf, ' ');
            appendContextKeyword(context, "ELSE ", 0, 0, 0);
            get_rule_expr((Node*)caseexpr->defresult, context, true, no_alias);
            if (!PRETTY_INDENT(context))
                appendStringInfoChar(buf, ' ');
            appendContextKeyword(context, "END", -PRETTYINDENT_VAR, 0, 0);
        } break;

        case T_CaseTestExpr: {
            /*
             * Normally we should never get here, since for expressions
             * that can contain this node type we attempt to avoid
             * recursing to it.  But in an optimized expression we might
             * be unable to avoid that (see comments for CaseExpr).  If we
             * do see one, print it as CASE_TEST_EXPR.
             */
            appendStringInfo(buf, "CASE_TEST_EXPR");
        } break;

        case T_AnnOrderByRef: {
            /* Keep EXPLAIN stable: show the original SQL expression, not
             * the executor-only reference node. */
            AnnOrderByRef* ref = (AnnOrderByRef*)node;
            get_rule_expr((Node*)ref->original_expr, context, showimplicit, no_alias);
        } break;

        case T_ArrayExpr: {
            ArrayExpr* arrayexpr = (ArrayExpr*)node;

            appendStringInfo(buf, "ARRAY[");
            get_rule_expr((Node*)arrayexpr->elements, context, true, no_alias);
            appendStringInfoChar(buf, ']');

            /*
             * If the array isn't empty, we assume its elements are
             * coerced to the desired type.  If it's empty, though, we
             * need an explicit coercion to the array type.
             */
            if (arrayexpr->elements == NIL)
                appendStringInfo(buf, "::%s", format_type_with_typemod(arrayexpr->array_typeid, -1));
        } break;

        case T_RowExpr: {
            RowExpr* rowexpr = (RowExpr*)node;
            TupleDesc tupdesc = NULL;
            ListCell* arg = NULL;
            int i;
            char* sep = NULL;

            /*
             * If it's a named type and not RECORD, we may have to skip
             * dropped columns and/or claim there are NULLs for added
             * columns.
             */
            if (rowexpr->row_typeid != RECORDOID) {
                tupdesc = lookup_rowtype_tupdesc(rowexpr->row_typeid, -1);
                Assert(list_length(rowexpr->args) <= tupdesc->natts);
            }

            /*
             * SQL99 allows "ROW" to be omitted when there is more than
             * one column, but for simplicity we always print it.
             */
            appendStringInfo(buf, "ROW(");
            sep = "";
            i = 0;
            foreach (arg, rowexpr->args) {
                Node* e = (Node*)lfirst(arg);

                if (tupdesc == NULL || !tupdesc->attrs[i].attisdropped) {
                    appendStringInfoString(buf, sep);
                    get_rule_expr(e, context, true, no_alias);
                    sep = ", ";
                }
                i++;
            }
            if (tupdesc != NULL) {
                while (i < tupdesc->natts) {
                    if (!tupdesc->attrs[i].attisdropped) {
                        appendStringInfoString(buf, sep);
                        appendStringInfo(buf, "NULL");
                        sep = ", ";
                    }
                    i++;
                }

                ReleaseTupleDesc(tupdesc);
            }
            appendStringInfo(buf, ")");
            if (rowexpr->row_format == COERCE_EXPLICIT_CAST)
                appendStringInfo(buf, "::%s", format_type_with_typemod(rowexpr->row_typeid, -1));
        } break;

        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            ListCell* arg = NULL;
            char* sep = NULL;

            /*
             * SQL99 allows "ROW" to be omitted when there is more than
             * one column, but for simplicity we always print it.
             */
            appendStringInfo(buf, "(ROW(");
            sep = "";
            foreach (arg, rcexpr->largs) {
                Node* e = (Node*)lfirst(arg);

                appendStringInfoString(buf, sep);
                get_rule_expr(e, context, true, no_alias);
                sep = ", ";
            }

            /*
             * We assume that the name of the first-column operator will
             * do for all the rest too.  This is definitely open to
             * failure, eg if some but not all operators were renamed
             * since the construct was parsed, but there seems no way to
             * be perfect.
             */
            appendStringInfo(buf,
                ") %s ROW(",
                generate_operator_name(linitial_oid(rcexpr->opnos),
                    exprType((Node*)linitial(rcexpr->largs)),
                    exprType((Node*)linitial(rcexpr->rargs))));
            sep = "";
            foreach (arg, rcexpr->rargs) {
                Node* e = (Node*)lfirst(arg);

                appendStringInfoString(buf, sep);
                get_rule_expr(e, context, true, no_alias);
                sep = ", ";
            }
            appendStringInfo(buf, "))");
        } break;

        case T_CoalesceExpr: {
            CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;

            appendStringInfo(buf, "COALESCE(");
            get_rule_expr((Node*)coalesceexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_MinMaxExpr: {
            MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;

            switch (minmaxexpr->op) {
                case IS_GREATEST:
                    appendStringInfo(buf, "GREATEST(");
                    break;
                case IS_LEAST:
                    appendStringInfo(buf, "LEAST(");
                    break;
                default:
                    break;
            }
            get_rule_expr((Node*)minmaxexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;
            bool needcomma = false;
            ListCell* arg = NULL;
            ListCell* narg = NULL;
            Const* con = NULL;

            switch (xexpr->op) {
                case IS_XMLCONCAT:
                    appendStringInfoString(buf, "XMLCONCAT(");
                    break;
                case IS_XMLELEMENT:
                    appendStringInfoString(buf, "XMLELEMENT(");
                    break;
                case IS_XMLFOREST:
                    appendStringInfoString(buf, "XMLFOREST(");
                    break;
                case IS_XMLPARSE:
                    appendStringInfoString(buf, "XMLPARSE(");
                    break;
                case IS_XMLPI:
                    appendStringInfoString(buf, "XMLPI(");
                    break;
                case IS_XMLROOT:
                    appendStringInfoString(buf, "XMLROOT(");
                    break;
                case IS_XMLSERIALIZE:
                    appendStringInfoString(buf, "XMLSERIALIZE(");
                    break;
                case IS_DOCUMENT:
                    break;
                default:
                    break;
            }
            if (xexpr->op == IS_XMLPARSE || xexpr->op == IS_XMLSERIALIZE) {
                if (xexpr->xmloption == XMLOPTION_DOCUMENT)
                    appendStringInfoString(buf, "DOCUMENT ");
                else
                    appendStringInfoString(buf, "CONTENT ");
            }
            if (xexpr->name) {
                appendStringInfo(buf, "NAME %s", quote_identifier(map_xml_name_to_sql_identifier(xexpr->name)));
                needcomma = true;
            }
            if (xexpr->named_args) {
                if (xexpr->op != IS_XMLFOREST) {
                    if (needcomma)
                        appendStringInfoString(buf, ", ");
                    appendStringInfoString(buf, "XMLATTRIBUTES(");
                    needcomma = false;
                }
                forboth(arg, xexpr->named_args, narg, xexpr->arg_names)
                {
                    Node* e = (Node*)lfirst(arg);
                    char* argname = strVal(lfirst(narg));

                    if (needcomma)
                        appendStringInfoString(buf, ", ");
                    get_rule_expr((Node*)e, context, true, no_alias);
                    appendStringInfo(buf, " AS %s", quote_identifier(map_xml_name_to_sql_identifier(argname)));
                    needcomma = true;
                }
                if (xexpr->op != IS_XMLFOREST)
                    appendStringInfoChar(buf, ')');
            }
            if (xexpr->args) {
                if (needcomma)
                    appendStringInfoString(buf, ", ");
                switch (xexpr->op) {
                    case IS_XMLCONCAT:
                    case IS_XMLELEMENT:
                    case IS_XMLFOREST:
                    case IS_XMLPI:
                    case IS_XMLSERIALIZE:
                        /* no extra decoration needed */
                        get_rule_expr((Node*)xexpr->args, context, true, no_alias);
                        break;
                    case IS_XMLPARSE:
                        Assert(list_length(xexpr->args) == 2);

                        get_rule_expr((Node*)linitial(xexpr->args), context, true, no_alias);

                        con = (Const*)lsecond(xexpr->args);
                        Assert(IsA(con, Const));
                        Assert(!con->constisnull);
                        if (DatumGetBool(con->constvalue))
                            appendStringInfoString(buf, " PRESERVE WHITESPACE");
                        else
                            appendStringInfoString(buf, " STRIP WHITESPACE");
                        break;
                    case IS_XMLROOT:
                        Assert(list_length(xexpr->args) == 3);

                        get_rule_expr((Node*)linitial(xexpr->args), context, true, no_alias);

                        appendStringInfoString(buf, ", VERSION ");
                        con = (Const*)lsecond(xexpr->args);
                        if (IsA(con, Const) && con->constisnull)
                            appendStringInfoString(buf, "NO VALUE");
                        else
                            get_rule_expr((Node*)con, context, false, no_alias);

                        con = (Const*)lthird(xexpr->args);
                        Assert(IsA(con, Const));
                        if (con->constisnull)
                            /* suppress STANDALONE NO VALUE */;
                        else {
                            switch (DatumGetInt32(con->constvalue)) {
                                case XML_STANDALONE_YES:
                                    appendStringInfoString(buf, ", STANDALONE YES");
                                    break;
                                case XML_STANDALONE_NO:
                                    appendStringInfoString(buf, ", STANDALONE NO");
                                    break;
                                case XML_STANDALONE_NO_VALUE:
                                    appendStringInfoString(buf, ", STANDALONE NO VALUE");
                                    break;
                                default:
                                    break;
                            }
                        }
                        break;
                    case IS_DOCUMENT:
                        get_rule_expr_paren((Node*)xexpr->args, context, false, node, no_alias);
                        break;
                    default:
                        break;
                }
            }
            if (xexpr->op == IS_XMLSERIALIZE)
                appendStringInfo(buf, " AS %s", format_type_with_typemod(xexpr->type, xexpr->typmod));
            if (xexpr->op == IS_DOCUMENT)
                appendStringInfoString(buf, " IS DOCUMENT");
            else
                appendStringInfoChar(buf, ')');
        } break;

        case T_PriorExpr: {
            appendContextKeyword(context, "Prior ", 0, 0, 0);
            PriorExpr* prior_expr = (PriorExpr*)node;
            get_rule_expr(prior_expr->node, context, showimplicit, no_alias);
        } break;

        case T_TableFunc:
            get_tablefunc((TableFunc *) node, context, showimplicit);
            break;

        case T_NullTest: {
            NullTest* ntest = (NullTest*)node;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren((Node*)ntest->arg, context, true, node, no_alias);
            /*
             * For scalar inputs, we prefer to print as IS [NOT] NULL,
             * which is shorter and traditional.  If it's a rowtype input
             * but we're applying a scalar test, must print IS [NOT]
             * DISTINCT FROM NULL to be semantically correct.
             */
            if (ntest->argisrow || !type_is_rowtype(exprType((Node *) ntest->arg))) {
                switch (ntest->nulltesttype)
                {
                    case IS_NULL:
                        appendStringInfoString(buf, " IS NULL");
                        break;
                    case IS_NOT_NULL:
                        appendStringInfoString(buf, " IS NOT NULL");
                        break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
                }
            } else {
                switch (ntest->nulltesttype)
                {
                    case IS_NULL:
                        appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL");
                        break;
                    case IS_NOT_NULL:
                        appendStringInfoString(buf, " IS DISTINCT FROM NULL");
                        break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
                }
            }   
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_NanTest: {
            NanTest* ntest = (NanTest*)node;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren((Node*)ntest->arg, context, true, node, no_alias);

            switch (ntest->nantesttype)
            {
                case IS_NAN:
                    appendStringInfoString(buf, " IS NAN");
                    break;
                case IS_NOT_NAN:
                    appendStringInfoString(buf, " IS NOT NAN");
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized nantesttype: %d", (int)ntest->nantesttype)));
            }
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_InfiniteTest: {
            InfiniteTest* ntest = (InfiniteTest*)node;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren((Node*)ntest->arg, context, true, node, no_alias);

            switch (ntest->infinitetesttype)
            {
                case IS_INFINITE:
                    appendStringInfoString(buf, " IS INFINITE");
                    break;
                case IS_NOT_INFINITE:
                    appendStringInfoString(buf, " IS NOT INFINITE");
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized infinitetesttype: %d", (int)ntest->infinitetesttype)));
            }
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_HashFilter: {
            HashFilter* htest = (HashFilter*)node;
            ListCell* distVar = NULL;
            char* sep = "";

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');

            appendStringInfo(buf, "Hash By ");
            foreach (distVar, htest->arg) {
                appendStringInfoString(buf, sep);
                Node* distriVar = (Node*)lfirst(distVar);
                get_rule_expr_paren(distriVar, context, true, node, no_alias);
                sep = ", ";
            }

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_BooleanTest: {
            BooleanTest* btest = (BooleanTest*)node;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren((Node*)btest->arg, context, false, node, no_alias);
            switch (btest->booltesttype) {
                case IS_TRUE:
                    appendStringInfo(buf, " IS TRUE");
                    break;
                case IS_NOT_TRUE:
                    appendStringInfo(buf, " IS NOT TRUE");
                    break;
                case IS_FALSE:
                    appendStringInfo(buf, " IS FALSE");
                    break;
                case IS_NOT_FALSE:
                    appendStringInfo(buf, " IS NOT FALSE");
                    break;
                case IS_UNKNOWN:
                    appendStringInfo(buf, " IS UNKNOWN");
                    break;
                case IS_NOT_UNKNOWN:
                    appendStringInfo(buf, " IS NOT UNKNOWN");
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized booltesttype: %d", (int)btest->booltesttype)));
            }
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_CoerceToDomain: {
            CoerceToDomain* ctest = (CoerceToDomain*)node;
            Node* arg = (Node*)ctest->arg;

            if (ctest->coercionformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr(arg, context, false, no_alias);
            } else {
                get_coercion_expr(arg, context, ctest->resulttype, ctest->resulttypmod, node);
            }
        } break;

        case T_CoerceToDomainValue:
            appendStringInfo(buf, "VALUE");
            break;

        case T_SetToDefault:
            appendStringInfo(buf, "DEFAULT");
            break;

        case T_CurrentOfExpr: {
            CurrentOfExpr* cexpr = (CurrentOfExpr*)node;

            if (cexpr->cursor_name)
                appendStringInfo(buf, "CURRENT OF %s", quote_identifier(cexpr->cursor_name));
            else
                appendStringInfo(buf, "CURRENT OF $%d", cexpr->cursor_param);
        } break;

        case T_List: {
            char* sep = NULL;
            ListCell* l = NULL;

            sep = "";
            foreach (l, (List*)node) {
                appendStringInfoString(buf, sep);
                get_rule_expr((Node*)lfirst(l), context, showimplicit, no_alias);
                sep = ", ";
            }
        } break;

        case T_AutoIncrement:
            appendStringInfo(buf, "AUTO_INCREMENT");
            break;

        case T_PrefixKey: {
            PrefixKey* pkey = (PrefixKey*)node;
            get_rule_expr((Node*)pkey->arg, context, showimplicit, no_alias);
            appendStringInfo(buf, "(%d)", pkey->length);
        } break;

        case T_CursorExpression: {
            CursorExpression* stmt = (CursorExpression*) node;
            appendStringInfo(buf, "CURSOR(%s)", stmt->raw_query_str);
        } break;

        case T_TypeCast: {
            TypeCast* tc = (TypeCast*) node;
             if (showimplicit) {
                get_coercion_expr(tc->arg, context, tc->typname->typeOid, -1, node);
            } else {
                /* don't show the implicit cast */
                get_rule_expr_paren(tc->arg, context, false, node, no_alias);
            }
        } break;

        case T_TSQL_HexString: {
            Value* value = (Value*)node;
            appendStringInfo(buf, "%s", value->val.str);
        } break;

#ifdef USE_SPQ
        case T_DMLActionExpr:
            appendStringInfo(buf, "DMLAction");
            break;
#endif
        default:
            if (context->qrw_phase)
                appendStringInfo(buf, "<unknown %d>", (int)nodeTag(node));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized node type: %d", (int)nodeTag(node))));
            break;
    }
}
```

<a id="source-089"></a>

#### AnnIndexFindOrderByRef()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:54-65`；范围：**完整函数**。

内容 SHA256：`46c0c62c165f027006cd55b0325faefacc4e9f585766bbe25b9181f846b7f891`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static AnnOrderByRef* AnnIndexFindOrderByRef(AnnIndexScan* plan)
{
    ListCell* lc = NULL;

    foreach (lc, plan->scan.plan.targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (IsA(tle->expr, AnnOrderByRef)) {
            return (AnnOrderByRef*)tle->expr;
        }
    }
    return NULL;
}
```

<a id="source-090"></a>

#### AnnIndexReuseModeAllows()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:67-76`；范围：**完整函数**。

内容 SHA256：`f0c293709ba24cfa12896553a18ea84027e436274fa8a91ee0a94519fb9ed808`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static bool AnnIndexReuseModeAllows(int mode, AnnOrderByDistanceQuality quality)
{
    if (quality == ANN_ORDERBY_DISTANCE_EXACT_L2) {
        return (mode & ANN_ORDERBY_REUSE_L2) != 0;
    }
    if (quality == ANN_ORDERBY_DISTANCE_APPROX_COSINE) {
        return (mode & ANN_ORDERBY_REUSE_COSINE) != 0;
    }
    return false;
}
```

<a id="source-091"></a>

#### AnnIndexRuntimeDistanceQuality()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:81-105`；范围：**完整函数**。

内容 SHA256：`cd70ae375ce8fefea081c72264550de105e1ecaad0f319be11a203535e963816`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static AnnOrderByDistanceQuality AnnIndexRuntimeDistanceQuality(IndexScanDesc scan)
{
    if (scan == NULL || scan->indexRelation == NULL ||
        scan->indexRelation->rd_rel->relam != DISKANN_AM_OID ||
        TupleDescAttr(scan->indexRelation->rd_att, 0)->atttypid != VECTOROID ||
        scan->indexRelation->rd_index->indkey.values[0] <= 0 ||
        RELATION_HAS_BUCKET(scan->indexRelation)) {
        return ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    }

    DiskAnnOptions* opts = (DiskAnnOptions*)scan->indexRelation->rd_options;
    if (opts != NULL && opts->enablePQ) {
        return ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    }

    Oid distanceProc = index_getprocid(scan->indexRelation, 1, DISKANN_DISTANCE_PROC);
    Oid normProc = index_getprocid(scan->indexRelation, 1, DISKANN_NORM_PROC);
    if (distanceProc == L2_FUNC_OID && !OidIsValid(normProc)) {
        return ANN_ORDERBY_DISTANCE_EXACT_L2;
    }
    if (distanceProc == IP_FUNC_OID && OidIsValid(normProc)) {
        return ANN_ORDERBY_DISTANCE_APPROX_COSINE;
    }
    return ANN_ORDERBY_DISTANCE_UNAVAILABLE;
}
```

<a id="source-092"></a>

#### AnnIndexInvalidateOrderByValue()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:107-114`；范围：**完整函数**。

内容 SHA256：`2a376770127c51de4c0fbaa57b4367a67c4fcafc75f687baf5b8e4844e3f4b57`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static void AnnIndexInvalidateOrderByValue(AnnIndexScanState* node)
{
    node->iss_AnnOrderByValid = false;
    node->iss_AnnOrderByHeapRecheck = false;
    node->iss_AnnOrderByIsNull = true;
    node->iss_AnnOrderByValue = (Datum)0;
    node->iss_AnnOrderByQuality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;
}
```

<a id="source-093"></a>

#### AnnIndexBindOrderByValue()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:119-172`；范围：**完整函数**。

内容 SHA256：`44f7fb8266c4502c48802f8cbfb377d1be3a85c3509e2eaf19f1fbc28d0dc90c`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static void AnnIndexBindOrderByValue(AnnIndexScanState* node, IndexScanDesc scan)
{
    AnnOrderByDistanceChannel* channel = &scan->xs_ann_orderby;
    uint64 previousEpoch = node->iss_AnnOrderByEpoch;
    uint64 previousToken = node->iss_AnnOrderByRowToken;

    AnnIndexInvalidateOrderByValue(node);
    /* A cached plan may be executed after the GUC has been lowered.  That
     * choice is made once in ExecInitAnnIndexScan; every row then uses the
     * fallback expression and the AM channel stays unrequested. */
    if (node->iss_AnnOrderByExpectedQuality == ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
        return;
    }

    /* Once reuse is selected for this scan, a missing or stale publication
     * is an AM/executor contract violation. The sole row-local exception
     * is an explicit L2 heap recheck: both sources return exact SQL L2.
     * Never silently fall back from an approximate cosine publication. */
    if (!channel->requested || !channel->valid || channel->isnull ||
        channel->quality != node->iss_AnnOrderByExpectedQuality ||
        (channel->heap_recheck && channel->quality != ANN_ORDERBY_DISTANCE_EXACT_L2) ||
        channel->scan_epoch == 0 || channel->row_token == 0 ||
        !ItemPointerIsValid(&channel->heap_tid) ||
        !ItemPointerIsValid(&scan->xs_ctup.t_self) ||
        ItemPointerGetBlockNumber(&channel->heap_tid) !=
            ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) ||
        (channel->scan_epoch == previousEpoch && channel->row_token <= previousToken)) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("DiskANN did not publish a valid ORDER BY distance for the current row")));
    }

    node->iss_AnnOrderByValue = channel->value;
    node->iss_AnnOrderByIsNull = false;
    node->iss_AnnOrderByValid = true;
    node->iss_AnnOrderByHeapRecheck = channel->heap_recheck;
    node->iss_AnnOrderByQuality = channel->quality;
    node->iss_AnnOrderByEpoch = channel->scan_epoch;
    node->iss_AnnOrderByRowToken = channel->row_token;

    if (!node->iss_AnnOrderByHeapRecheck) {
        AttrNumber attno = scan->indexRelation->rd_index->indkey.values[0];
        bool isnull = false;
        Datum vector = tableam_tslot_getattr(node->ss.ss_ScanTupleSlot, attno, &isnull);
        if (isnull || !diskann_orderby_vector_matches(scan, vector)) {
            if (channel->quality != ANN_ORDERBY_DISTANCE_EXACT_L2) {
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("DiskANN distance belongs to a different heap vector"),
                    errhint("REINDEX this legacy index before reusing approximate cosine distances.")));
            }
            node->iss_AnnOrderByHeapRecheck = true;
        }
    }
}
```

<a id="source-094"></a>

#### AnnIndexNext()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:181-271`；范围：**完整函数**。

内容 SHA256：`d54c8486ca7eda00406077d5b915fbd4d2066587a534c34c14feed2f5b5814dd`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static TupleTableSlot* AnnIndexNext(AnnIndexScanState* node)
{
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    ScanDirection direction;
    IndexScanDesc scandesc;
    HeapTuple tuple;
    TupleTableSlot* slot = NULL;
    bool isUstore = false;

    /*
     * extract necessary information from index scan node
     */
    estate = node->ss.ps.state;
    direction = estate->es_direction;
    /* flip direction if this is an overall backward scan */
    if (ScanDirectionIsBackward(((AnnIndexScan*)node->ss.ps.plan)->indexorderdir)) {
        if (ScanDirectionIsForward(direction))
            direction = BackwardScanDirection;
        else if (ScanDirectionIsBackward(direction))
            direction = ForwardScanDirection;
    }
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    scandesc = node->iss_ScanDesc;
    /*
     * annCount is a planner-side costing estimate. Do not push it down as the
     * initial HNSW search size; iterative scan expands candidates on demand.
     */
    scandesc->count = 0;
    scandesc->limitk = (int64_t)node->limitCount;
    isUstore = RelationIsUstoreFormat(node->ss.ss_currentRelation);
    /*
     * ok, now that we have what we need, fetch the next tuple.
     */
    // we should change abs_idx_getnext to call IdxScanAm(scan)->idx_getnext and channge .idx_getnext in g_HeapIdxAm to
    // IndexGetnextSlot
    while (true) {
        CHECK_FOR_INTERRUPTS();

        AnnIndexInvalidateOrderByValue(node);

        IndexScanDesc indexScan = GetIndexScanDesc(scandesc);
        if (isUstore) {
            if (!IndexGetnextSlot(scandesc, direction, slot, &node->ss.ps.state->have_current_xact_date)) {
                break;
            }
        } else {
            if ((tuple = scan_handler_idx_getnext(scandesc, direction, InvalidOid, InvalidBktId,
                                                  &node->ss.ps.state->have_current_xact_date)) == NULL) {
                break;
            }
            /* Update indexScan, because hashbucket may switch current index in scan_handler_idx_getnext */
            indexScan = GetIndexScanDesc(scandesc);
            /*
             * Store the scanned tuple in the scan tuple slot of the scan state.
             * Note: we pass 'false' because tuples returned by amgetnext are
             * pointers onto disk pages and must not be pfree_ext()'d.
             */
            (void)ExecStoreTuple(tuple, /* tuple to store */
                slot,                   /* slot to store in */
                indexScan->xs_cbuf,     /* buffer containing tuple */
                false);                 /* don't pfree */
        }

        AnnIndexBindOrderByValue(node, indexScan);

        /*
         * If the index was lossy, we have to recheck the index quals using
         * the fetched tuple.
         */
        if (indexScan->xs_recheck) {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);
            if (!ExecQual(node->indexqualorig, econtext, false)) {
                /* Fails recheck, so drop it and loop back for another */
                AnnIndexInvalidateOrderByValue(node);
                InstrCountFiltered2(node, 1);
                continue;
            }
        }

        return slot;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    return ExecClearTuple(slot);
}
```

<a id="source-095"></a>

#### AnnIndexSortPut()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:276-309`；范围：**完整函数**。

内容 SHA256：`f19b83b2cb25164d030c878188173270c9ffe37c7aa3f701c66cdf5fd90f9b1f`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static void AnnIndexSortPut(AnnIndexScanState* node, TupleTableSlot* slot, Datum distance, bool isnull)
{
    HeapTuple tuple = ExecCopySlotTuple(slot);
    Size bytes = HEAPTUPLESIZE + tuple->t_len;
    bytea* payload = (bytea*)palloc0(VARHDRSZ + bytes);
    SET_VARSIZE(payload, VARHDRSZ + bytes);
    HeapTupleData header;
    errno_t rc = memset_s(&header, sizeof(header), 0, sizeof(header));
    securec_check(rc, "\0", "\0");
    header.tupTableType = HEAP_TUPLE;
    header.t_len = tuple->t_len;
    header.t_self = tuple->t_self;
    header.t_tableOid = tuple->t_tableOid;
    header.t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(&header, tuple);
#ifdef ENABLE_MULTIPLE_NODES
    header.t_xc_node_id = tuple->t_xc_node_id;
#endif
    rc = memcpy_s(VARDATA(payload), bytes, &header, sizeof(header));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(VARDATA(payload) + HEAPTUPLESIZE, tuple->t_len, tuple->t_data, tuple->t_len);
    securec_check(rc, "\0", "\0");
    TupleTableSlot* saved = node->iss_OrderedSlot;
    ExecClearTuple(saved);
    saved->tts_values[0] = distance;
    saved->tts_isnull[0] = isnull;
    saved->tts_values[1] = PointerGetDatum(payload);
    saved->tts_isnull[1] = false;
    ExecStoreVirtualTuple(saved);
    tuplesort_puttupleslot(node->iss_OrderedSort, saved);
    ExecClearTuple(saved);
    pfree(payload);
    heap_freetuple(tuple);
}
```

<a id="source-096"></a>

#### AnnIndexOrderedNext()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:311-407`；范围：**完整函数**。

内容 SHA256：`d002c6ea960d77aa2e3495efb954387d451a45066e9da80b5cc1edc21acad147`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static TupleTableSlot* AnnIndexOrderedNext(AnnIndexScanState* node)
{
    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    IndexScanDesc scan = GetIndexScanDesc(node->iss_ScanDesc);
    if (!node->iss_OrderedReady) {
        MemoryContext oldCtx = MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
        AttrNumber key = 1;
        Oid sortop = FLOAT8LTOID;
        Oid collation = InvalidOid;
        bool nullsFirst = false;
        node->iss_OrderedSort = tuplesort_begin_heap(node->iss_OrderedSlot->tts_tupleDescriptor,
            1, &key, &sortop, &collation, &nullsFirst, u_sess->attr.attr_memory.work_mem, false);
        MemoryContextSwitchTo(oldCtx);
        ((DiskAnnScanOpaque)scan->opaque)->orderedBatch = true;
        uint64 qualified = 0;
        /* A target SRF can emit zero rows, so a base-row bound is unsafe. */
        int64 bound = node->iss_OrderedHasSRF ? 0 : node->limitCount;
        while (diskann_advance_ordered_batch(scan)) {
            for (;;) {
                CHECK_FOR_INTERRUPTS();
                ResetExprContext(econtext);
                TupleTableSlot* next = AnnIndexNext(node);
                if (TupIsNull(next)) {
                    break;
                }
                econtext->ecxt_scantuple = next;
                if (node->iss_OrderedQual != NIL && !ExecQual(node->iss_OrderedQual, econtext)) {
                    InstrCountFiltered1(node, 1);
                    continue;
                }
                bool isnull = false;
                Datum distance;
                if (node->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE &&
                    !node->iss_AnnOrderByHeapRecheck) {
                    distance = node->iss_AnnOrderByValue;
                    isnull = node->iss_AnnOrderByIsNull;
                } else {
                    distance = ExecEvalExprSwitchContext(node->iss_OrderedExpr, econtext, &isnull, NULL);
                    if (node->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
                        node->iss_AnnOrderByFallbacks++;
                    }
                }
                AnnIndexSortPut(node, next, distance, isnull);
                qualified++;
            }
            /* Complete the entire last batch, including merged TIDs, before
             * deciding to stop. No later expansion may follow publication. */
            if (bound > 0 && qualified >= (uint64)bound) {
                break;
            }
        }
        tuplesort_performsort(node->iss_OrderedSort);
        node->iss_OrderedReady = true;
    }

    ExecClearTuple(slot);
    if (!tuplesort_gettupleslot(node->iss_OrderedSort, true, node->iss_OrderedSlot, NULL)) {
        AnnIndexInvalidateOrderByValue(node);
        return slot;
    }
    bool isnull = false;
    Datum distance = tableam_tslot_getattr(node->iss_OrderedSlot, 1, &isnull);
    bool payloadNull = false;
    Datum payloadDatum = tableam_tslot_getattr(node->iss_OrderedSlot, 2, &payloadNull);
    if (payloadNull) {
        elog(ERROR, "missing DiskANN sort tuple");
    }
    bytea* payload = DatumGetByteaP(payloadDatum);
    Size bytes = VARSIZE(payload) - VARHDRSZ;
    if (bytes < HEAPTUPLESIZE) {
        elog(ERROR, "invalid DiskANN sort tuple");
    }
    HeapTupleData header;
    errno_t rc = memcpy_s(&header, sizeof(header), VARDATA(payload), sizeof(header));
    securec_check(rc, "\0", "\0");
    if (bytes != HEAPTUPLESIZE + header.t_len) {
        elog(ERROR, "invalid DiskANN sort tuple length");
    }
    HeapTuple tuple = heaptup_alloc(bytes);
    *tuple = header;
    tuple->t_data = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);
    rc = memcpy_s(tuple->t_data, tuple->t_len, VARDATA(payload) + HEAPTUPLESIZE, tuple->t_len);
    securec_check(rc, "\0", "\0");
    if ((Pointer)payload != DatumGetPointer(payloadDatum)) {
        pfree(payload);
    }
    ExecStoreTuple(tuple, slot, InvalidBuffer, true);
    if (node->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
        node->iss_AnnOrderByValue = distance;
        node->iss_AnnOrderByIsNull = isnull;
        node->iss_AnnOrderByValid = true;
        node->iss_AnnOrderByHeapRecheck = false;
        node->iss_AnnOrderByQuality = node->iss_AnnOrderByExpectedQuality;
    }
    return slot;
}
```

<a id="source-097"></a>

#### ExecAnnIndexScan()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:433-479`；范围：**完整函数**。

内容 SHA256：`44b0339eca3a95c119ffe1b2013e474da79422eb253beb3878c0572f0d77b353`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
static TupleTableSlot* ExecAnnIndexScan(PlanState* state)
{
    AnnIndexScanState* node = castNode(AnnIndexScanState, state);
    /*
     * If we have runtime keys and they've not already been set up, do it now.
     */
    if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady) {
        /*
         * set a flag for partitioned table, so we can deal with it specially
         * when we rescan the partitioned table
         */
        if (node->ss.isPartTbl) {
            if (PointerIsValid(node->ss.partitions)) {
                node->ss.ss_ReScan = true;
                ExecReScan((PlanState*)node);
            }
        } else {
            ExecReScan((PlanState*)node);
        }
    } else if (DB_IS_CMPT(B_FORMAT) && node->iss_NumRuntimeKeys != 0 && u_sess->parser_cxt.has_set_uservar) {
        ExprContext* econtext = node->iss_RuntimeContext;
        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }

    if (node->iss_DiskannOrdered && node->ss.ps.state->es_epqTuple == NULL) {
        /* OrderedNext checks quals once while collecting candidates. Keep
         * projection here so volatile targets and SRFs run on emitted rows.
         * EPQ substitute tuples must still go through normal qual checking. */
        TupleTableSlot* result = NULL;
        node->ss.ps.qual = NIL;
        PG_TRY();
        {
            result = ExecScan(&node->ss, (ExecScanAccessMtd)AnnIndexOrderedNext,
                (ExecScanRecheckMtd)AnnIndexRecheck);
        }
        PG_CATCH();
        {
            node->ss.ps.qual = node->iss_OrderedQual;
            PG_RE_THROW();
        }
        PG_END_TRY();
        node->ss.ps.qual = node->iss_OrderedQual;
        return result;
    }
    return ExecScan(&node->ss, (ExecScanAccessMtd)AnnIndexNext, (ExecScanRecheckMtd)AnnIndexRecheck);
}
```

<a id="source-098"></a>

#### ExecReScanAnnIndexScan()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:492-564`；范围：**完整函数**。

内容 SHA256：`cdf3166fde487438fb5630e02e67f92a03c787c2991d4bd2227bca8a11120565`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
void ExecReScanAnnIndexScan(AnnIndexScanState* node)
{
    if (node->iss_OrderedSort != NULL) {
        ExecClearTuple(node->iss_OrderedSlot);
        tuplesort_end(node->iss_OrderedSort);
        node->iss_OrderedSort = NULL;
    }
    node->iss_OrderedReady = false;
    AnnIndexInvalidateOrderByValue(node);
    /*
     * For recursive-stream rescan, if number of RuntimeKeys not euqal zero,
     * just return without rescan.
     *
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.  But first,
     * reset the context so we don't leak memory as each outer tuple is
     * scanned.  Note this assumes that we will recalculate *all* runtime keys
     * on each call.
     */
    if (node->iss_NumRuntimeKeys != 0) {
        if (node->ss.ps.state->es_recursive_next_iteration) {
            node->iss_RuntimeKeysReady = false;
            return;
        }

        ExprContext* econtext = node->iss_RuntimeContext;

        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }
    node->iss_RuntimeKeysReady = true;

    /*
     * deal with partitioned table
     */
    bool partpruning = !RelationIsSubPartitioned(node->ss.ss_currentRelation) &&
        ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && list_length(node->ss.partitions) == 1;
    /* if only one partition is scaned in indexscan, we don't need do rescan for partition */
    if (node->ss.isPartTbl && !partpruning) {
        /*
         * if node->ss.ss_ReScan = true, just do rescaning as non-partitioned
         * table; else switch to next partition for scaning.
         */
        if (node->ss.ss_ReScan ||
            (((Scan *)node->ss.ps.plan)->partition_iterator_elimination)) {
            /* reset the rescan falg */
            node->ss.ss_ReScan = false;
        } else {
            if (!PointerIsValid(node->ss.partitions)) {
                /*
                 * give up rescaning the index if there is no partition to scan
                 */
                return;
            }

            /* switch to next partition for scaning */
            Assert(PointerIsValid(node->iss_ScanDesc));
            scan_handler_idx_endscan(node->iss_ScanDesc);
            /*  initialize Scan for the next partition */
            ExecInitNextPartitionForAnnIndexScan(node);
            ExecScanReScan(&node->ss);
            return;
        }
    }

    /* reset index scan */
    scan_handler_idx_rescan(
        node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys, node->iss_OrderByKeys, node->iss_NumOrderByKeys);

    scan_handler_idx_rescan_parallel(node->iss_ScanDesc);

    ExecScanReScan(&node->ss);
}
```

<a id="source-099"></a>

#### ExecEndAnnIndexScan()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:570-652`；范围：**完整函数**。

内容 SHA256：`e652df0ba94d53581763cfba16515ad7bc33c99f0467b330a0fb5d1b893e327f`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
void ExecEndAnnIndexScan(AnnIndexScanState* node)
{
    if (node->iss_OrderedSort != NULL) {
        ExecClearTuple(node->iss_OrderedSlot);
        tuplesort_end(node->iss_OrderedSort);
        node->iss_OrderedSort = NULL;
    }
    Relation index_relation_desc;
    IndexScanDesc index_scan_desc;
    Relation relation;

    /*
     * extract information from the node
     */
    index_relation_desc = node->iss_RelationDesc;
    index_scan_desc = node->iss_ScanDesc;
    relation = node->ss.ss_currentRelation;

    /*
     * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
     */
#ifdef NOT_USED
    ExecFreeExprContext(&node->ss.ps);
    if (node->iss_RuntimeContext)
        FreeExprContext(node->iss_RuntimeContext, true);
#endif

    /*
     * clear out tuple table slots
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if (index_scan_desc) {
        scan_handler_idx_endscan(index_scan_desc);
        if (WorkerThreadAmI() && node->ss.ps.plan->dop > 1) {
            u_sess->stream_cxt.global_obj->DestroyStreamDesc(
                node->ss.ps.state->es_plannedstmt->queryId, node->ss.ps.plan);
        }
    }

    /*
     * close the index relation (no-op if we didn't open it)
     * close the index relation if the relation is non-partitioned table
     * close the index partitions and table partitions if the relation is
     * non-partitioned table
     */
    if (node->ss.isPartTbl) {
        if (PointerIsValid(node->iss_IndexPartitionList)) {
            Assert(PointerIsValid(index_relation_desc));
            Assert(PointerIsValid(node->ss.partitions));
            Assert(node->ss.partitions->length == node->iss_IndexPartitionList->length);

            Assert(PointerIsValid(node->iss_CurrentIndexPartition));
            releaseDummyRelation(&(node->iss_CurrentIndexPartition));

            Assert(PointerIsValid(node->ss.ss_currentPartition));
            releaseDummyRelation(&(node->ss.ss_currentPartition));

            if (RelationIsSubPartitioned(relation)) {
                releaseSubPartitionList(index_relation_desc, &(node->iss_IndexPartitionList), NoLock);
                releaseSubPartitionList(node->ss.ss_currentRelation, &(node->ss.subpartitions), NoLock);
            } else {
                /* close index partition */
                releasePartitionList(node->iss_RelationDesc, &(node->iss_IndexPartitionList), NoLock);
            }

            /* close table partition */
            releasePartitionList(node->ss.ss_currentRelation, &(node->ss.partitions), NoLock);
        }
    }

    if (index_relation_desc)
        index_close(index_relation_desc, NoLock);

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);
}
```

<a id="source-100"></a>

#### ExecInitAnnIndexScan()

来源：`src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp:802-1023`；范围：**完整函数**。

内容 SHA256：`ac385ba16ed4641bbafad911176e369941cbdde9afb3560a565fd35472d89676`。

只有已生成 AnnOrderByRef 后才进入本组运行期排查；KGN 应比对其普通 IndexScan 执行器中的对应实现。

```cpp
AnnIndexScanState* ExecInitAnnIndexScan(AnnIndexScan* node, EState* estate, int eflags)
{
    AnnIndexScanState* index_state = NULL;
    Relation current_relation;
    bool relisTarget = false;

    gstrace_entry(GS_TRC_ID_ExecInitIndexScan);
    /*
     * create state structure
     */
    index_state = makeNode(AnnIndexScanState);
    index_state->ss.ps.plan = (Plan*)node;
    index_state->ss.ps.state = estate;
    index_state->ss.isPartTbl = node->scan.isPartTbl;
    index_state->ss.currentSlot = 0;
    index_state->ss.partScanDirection = node->indexorderdir;
    index_state->ss.ps.ExecProcNode = ExecAnnIndexScan;
    index_state->annCount = node->annCount;
    index_state->iss_AnnOrderByExpectedQuality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    index_state->iss_AnnOrderByEpoch = 0;
    index_state->iss_AnnOrderByRowToken = 0;
    AnnIndexInvalidateOrderByValue(index_state);
    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &index_state->ss.ps);

    index_state->ss.ps.ps_vec_TupFromTlist = false;

    /*
     * initialize child expressions
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).  Likewise for
     * indexorderby, if any.  But the indexqualorig expression is always
     * initialized even though it will only be used in some uncommon cases ---
     * would be nice to improve that.  (Problem is that any SubPlans present
     * in the expression must be found now...)
     */
    if (estate->es_is_flt_frame) {
        index_state->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig = (List*)ExecInitQualByFlatten(node->indexqualorig, (PlanState*)index_state);
    } else {
        index_state->ss.ps.targetlist = (List*)ExecInitExprByRecursion(
            (Expr*)node->scan.plan.targetlist, (PlanState*)index_state);
        index_state->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig =
            (List*)ExecInitExprByRecursion((Expr*)node->indexqualorig, (PlanState*)index_state);
    }

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    index_state->ss.ss_currentRelation = current_relation;
    index_state->ss.ss_currentScanDesc = NULL; /* no heap scan here */
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &index_state->ss.ps, current_relation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, &index_state->ss, current_relation->rd_tam_ops);

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&index_state->ss, CreateTupleDescCopy(RelationGetDescr(current_relation)));
    index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops = current_relation->rd_tam_ops;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&index_state->ss.ps);

    index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops =
            index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops;

    ExecAssignScanProjectionInfo(&index_state->ss);

    Assert(index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
        return index_state;
    }

    /*
     * Open the index relation.
     *
     * If the parent table is one of the target relations of the query, then
     * InitPlan already opened and write-locked the index, so we can avoid
     * taking another lock here.  Otherwise we need a normal reader's lock.
     */
    relisTarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    index_state->iss_RelationDesc = index_open(node->indexid, relisTarget ? NoLock : AccessShareLock);
    if (!IndexIsUsable(index_state->iss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("can't initialize index scans using unusable index \"%s\"",
                    RelationGetRelationName(index_state->iss_RelationDesc))));
    }
    /*
     * Initialize index-specific scan state
     */
    index_state->iss_RuntimeKeysReady = false;
    index_state->iss_RuntimeKeys = NULL;
    index_state->iss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexqual,
        false,
        &index_state->iss_ScanKeys,
        &index_state->iss_NumScanKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexorderby,
        true,
        &index_state->iss_OrderByKeys,
        &index_state->iss_NumOrderByKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (index_state->iss_NumRuntimeKeys != 0) {
        ExprContext* stdecontext = index_state->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &index_state->ss.ps);
        index_state->iss_RuntimeContext = index_state->ss.ps.ps_ExprContext;
        index_state->ss.ps.ps_ExprContext = stdecontext;
    } else {
        index_state->iss_RuntimeContext = NULL;
    }

    /* deal with partition info */
    ExecInitAnnIndexRelation(index_state, estate, eflags);

    /* Only DiskANN heap scans use this reorder protocol. HNSW, index-only,
     * bucket and Ustore execution retain their own access contracts. */
    if (index_state->iss_RelationDesc->rd_rel->relam == DISKANN_AM_OID &&
        !node->index_only_scan && !node->is_ustore &&
        !RELATION_HAS_BUCKET(current_relation) && list_length(node->indexorderbyorig) == 1 &&
        exprType((Node*)linitial(node->indexorderbyorig)) == FLOAT8OID) {
        index_state->iss_DiskannOrdered = true;
        index_state->iss_OrderedQual = index_state->ss.ps.qual;
        index_state->iss_OrderedHasSRF = expression_returns_set((Node*)node->scan.plan.targetlist);
        index_state->iss_OrderedExpr = ExecInitExprByRecursion(
            (Expr*)linitial(node->indexorderbyorig), (PlanState*)index_state);
        TupleDesc desc = CreateTemplateTupleDesc(2, false);
        TupleDescInitEntry(desc, 1, "distance", FLOAT8OID, -1, 0);
        TupleDescInitEntry(desc, 2, "heap_tuple", BYTEAOID, -1, 0);
        index_state->iss_OrderedSlot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(index_state->iss_OrderedSlot, desc);
    }

    /* Request the dedicated AM channel only when this execution's GUC still
     * satisfies the quality encoded by the post-setrefs reference. */
    AnnOrderByRef* orderByRef = AnnIndexFindOrderByRef(node);
    if (orderByRef != NULL && index_state->iss_ScanDesc != NULL) {
        AnnOrderByDistanceQuality quality = (AnnOrderByDistanceQuality)orderByRef->quality;
        IndexScanDesc scanDesc = GetIndexScanDesc(index_state->iss_ScanDesc);
        AnnOrderByDistanceQuality runtimeQuality = AnnIndexRuntimeDistanceQuality(scanDesc);
        bool runtimeShapeSupported = !node->is_ustore &&
            !node->scan.isPartTbl && node->scan.plan.dop <= 1 &&
            !ScanDirectionIsBackward(node->indexorderdir) &&
            index_state->ss.ss_currentRelation != NULL &&
            !RELATION_HAS_BUCKET(index_state->ss.ss_currentRelation);

        if (runtimeShapeSupported && runtimeQuality == quality &&
            AnnIndexReuseModeAllows(u_sess->attr.attr_sql.ann_orderby_distance_reuse, quality)) {
            scanDesc->xs_ann_orderby.requested = true;
            scanDesc->xs_ann_orderby.requested_mode =
                u_sess->attr.attr_sql.ann_orderby_distance_reuse;
            index_state->iss_AnnOrderByExpectedQuality = quality;
        }
    }

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if (index_state->iss_ScanDesc == NULL) {
        index_state->ss.ps.stubType = PST_Scan;
    } else if (index_state->iss_NumRuntimeKeys == 0) {
        scan_handler_idx_rescan_local(index_state->iss_ScanDesc,
            index_state->iss_ScanKeys,
            index_state->iss_NumScanKeys,
            index_state->iss_OrderByKeys,
            index_state->iss_NumOrderByKeys);
    }

    /*
     * all done.
     */
    gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
    return index_state;
}
```

<a id="source-101"></a>

#### ExecEvalAnnOrderByRef()

来源：`src/gausskernel/runtime/executor/execQual.cpp:4308-4350`；范围：**完整函数**。

内容 SHA256：`147fd657da6b4cb31ae89f52e20f305d69ed4830ad43ab13a10a1c8d31a2670a`。

```cpp
static Datum ExecEvalAnnOrderByRef(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
    AnnOrderByRefExprRuntime* runtime = (AnnOrderByRefExprRuntime*)exprstate->evalfunc_private;
    AnnIndexScanState* scanstate = runtime->scanstate;

    if (scanstate == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("ANN ORDER BY reference is not owned by an AnnIndexScan")));
    }

    if (scanstate->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
        if (!scanstate->iss_AnnOrderByValid ||
            scanstate->iss_AnnOrderByQuality != runtime->quality ||
            scanstate->iss_AnnOrderByExpectedQuality != runtime->quality) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("ANN ORDER BY distance publication does not match the planned reference")));
        }
        if (scanstate->iss_AnnOrderByHeapRecheck) {
            /* Old DiskANN indexes may merge unequal vectors by a distance
             * threshold. Re-evaluate only the unproven L2 TID, then cache
             * its exact result for other references in the same row. */
            if (runtime->quality != ANN_ORDERBY_DISTANCE_EXACT_L2) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("only exact L2 supports an ANN heap distance recheck")));
            }
            scanstate->iss_AnnOrderByValue = ExecEvalExpr(runtime->fallback, econtext,
                &scanstate->iss_AnnOrderByIsNull, NULL);
            scanstate->iss_AnnOrderByHeapRecheck = false;
            scanstate->iss_AnnOrderByFallbacks++;
        }
        if (isDone != NULL) {
            *isDone = ExprSingleResult;
        }
        scanstate->iss_AnnOrderByReads++;
        *isNull = scanstate->iss_AnnOrderByIsNull;
        return scanstate->iss_AnnOrderByValue;
    }

    scanstate->iss_AnnOrderByFallbacks++;
    return ExecEvalExpr(runtime->fallback, econtext, isNull, isDone);
}
```

<a id="source-102"></a>

#### ExecInitExprByRecursion()

来源：`src/gausskernel/runtime/executor/execQual.cpp:6042-6051`；范围：**完整函数**。

内容 SHA256：`632d396e5875de67e2e42d47a791640cfd9fface6950c21ef250bf9bf9b6d465`。

完整传统表达式初始化函数，保留 AnnOrderByRef 的 owner/state/fallback 处理。

```cpp
ExprState* ExecInitExprByRecursion(Expr* node, PlanState* parent)
{
    if (u_sess->hook_cxt.execInitExprHook != NULL) {
        ExprState* expr = ((execInitExprFunc)(u_sess->hook_cxt.execInitExprHook))(node, parent);
        if (expr != NULL) {
            return expr;
        }
    }
    return ExecInitExprByRecursionInternal(node, parent);
}
```

<a id="source-103"></a>

#### ExecInitExprByRecursionInternal()

来源：`src/gausskernel/runtime/executor/execQual.cpp:6053-6859`；范围：**完整函数**。

内容 SHA256：`6b0bdcafb93cd4b5672dbaaf6e96bbb300009fd1d2e6d01bde28e7af2519f313`。

完整传统表达式初始化函数，保留 AnnOrderByRef 的 owner/state/fallback 处理。

```cpp
ExprState* ExecInitExprByRecursionInternal(Expr* node, PlanState* parent)
{
   ExprState* state = NULL;

   gstrace_entry(GS_TRC_ID_ExecInitExpr);
   if (node == NULL) {
       gstrace_exit(GS_TRC_ID_ExecInitExpr);
       return NULL;
   }

   /* Guard against stack overflow due to overly complex expressions */
   check_stack_depth();

   switch (nodeTag(node)) {
       case T_Var:
           /* varattno == InvalidAttrNumber means it's a whole-row Var */
           if (((Var*)node)->varattno == InvalidAttrNumber) {
               WholeRowVarExprState* wstate = makeNode(WholeRowVarExprState);

                wstate->parent = parent;
                wstate->wrv_junkFilter = NULL;
                state = (ExprState*)wstate;
                state->evalfunc = (ExprStateEvalFunc)ExecEvalWholeRowVar;
            } else {
                state = (ExprState*)makeNode(ExprState);
                state->evalfunc = ExecEvalScalarVar;
            }
            break;
        case T_Const:
        case T_UserVar:
        case T_SetVariableExpr:
            state = (ExprState*)makeNode(ExprState);
            state->is_flt_frame = false;
            state->evalfunc = ExecEvalConst;
            break;
       case T_Param:
           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           switch (((Param*)node)->paramkind) {
               case PARAM_EXEC:
                   state->evalfunc = ExecEvalParamExec;
                   break;
               case PARAM_EXTERN:
                   state->evalfunc = ExecEvalParamExtern;
                   break;
               default:
                   ereport(ERROR,
                           (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_EXECUTOR),
                            errmsg("unrecognized paramkind: %d", (int)((Param*)node)->paramkind)));
                   break;
           }
           break;
       case T_CoerceToDomainValue:
           state = (ExprState*)makeNode(ExprState);
           state->evalfunc = ExecEvalCoerceToDomainValue;
           break;
       case T_CaseTestExpr:
           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           state->evalfunc = ExecEvalCaseTestExpr;
           break;
       case T_AnnOrderByRef: {
           AnnOrderByRef* ref = (AnnOrderByRef*)node;
           AnnOrderByRefExprRuntime* runtime =
               (AnnOrderByRefExprRuntime*)palloc0(sizeof(AnnOrderByRefExprRuntime));

           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           state->evalfunc = ExecEvalAnnOrderByRef;
           runtime->scanstate = (parent != NULL && IsA(parent, AnnIndexScanState))
               ? (AnnIndexScanState*)parent
               : NULL;
           runtime->fallback = ExecInitExprByRecursion(ref->original_expr, parent);
           runtime->quality = (AnnOrderByDistanceQuality)ref->quality;
           state->evalfunc_private = runtime;
           break;
       }
       case T_Aggref: {
           Aggref* aggref = (Aggref*)node;
           AggrefExprState* astate = makeNode(AggrefExprState);

           astate->xprstate.is_flt_frame = false;
           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalAggref;
           if (parent && (IsA(parent, AggState) || IsA(parent, VecAggState))) {
               AggState* aggstate = (AggState*)parent;
               int naggs;

               aggstate->aggs = lcons(astate, aggstate->aggs);
               naggs = ++aggstate->numaggs;

               astate->aggdirectargs = (List*)ExecInitExprByRecursion((Expr*)aggref->aggdirectargs, parent);

               astate->args = (List*)ExecInitExprByRecursion((Expr*)aggref->args, parent);
               astate->aggfilter = ExecInitExprByRecursion((Expr *)aggref->aggfilter, parent);

               /*
                * Complain if the aggregate's arguments contain any
                * aggregates; nested agg functions are semantically
                * nonsensical.  (This should have been caught earlier,
                * but we defend against it here anyway.)
                */
               if (naggs != aggstate->numaggs)
                   ereport(ERROR,
                           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("aggregate function calls cannot be nested")));
           } else {
               /* planner messed up */
               ereport(ERROR,
                       (errcode(ERRCODE_INVALID_AGG), errmodule(MOD_OPT), errmsg("Aggref found in non-Agg plan node")));
           }
           state = (ExprState*)astate;
       } break;
       case T_GroupingFunc: {
           GroupingFunc* grp_node = (GroupingFunc*)node;
           GroupingFuncExprState* grp_state = makeNode(GroupingFuncExprState);
           grp_state->xprstate.is_flt_frame = false;
           Agg* agg = NULL;

           if (parent && (IsA(parent, AggState) || IsA(parent, VecAggState))) {
               grp_state->aggstate = (AggState*)parent;

               agg = (Agg*)(parent->plan);

               if (agg->groupingSets)
                   grp_state->clauses = grp_node->cols;
               else
                   grp_state->clauses = NIL;

               state = (ExprState*)grp_state;
               state->evalfunc = (ExprStateEvalFunc)ExecEvalGroupingFuncExpr;
           } else
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("parent of GROUPING is not Agg node")));
       } break;
       case T_GroupingId: {
           GroupingIdExprState* grp_id_state = makeNode(GroupingIdExprState);
           grp_id_state->xprstate.is_flt_frame = false;
           if (parent == NULL || !IsA(parent, AggState) || !IsA(parent->plan, Agg)) {
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("parent of GROUPINGID is not Agg node")));
           }
           grp_id_state->aggstate = (AggState*)parent;
           state = (ExprState*)grp_id_state;
           state->evalfunc = (ExprStateEvalFunc)ExecEvalGroupingIdExpr;
       } break;
       case T_WindowFunc: {
           WindowFunc* wfunc = (WindowFunc*)node;
           WindowFuncExprState* wfstate = makeNode(WindowFuncExprState);
           wfstate->xprstate.is_flt_frame = false;

           wfstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalWindowFunc;
           if (parent && (IsA(parent, WindowAggState) || IsA(parent, VecWindowAggState))) {
               WindowAggState* winstate = (WindowAggState*)parent;
               int nfuncs;

               winstate->funcs = lcons(wfstate, winstate->funcs);
               nfuncs = ++winstate->numfuncs;
               if (wfunc->winagg)
                   winstate->numaggs++;

                wfstate->keep_args = (List*)ExecInitExprByRecursion((Expr*)wfunc->keep_args, parent);
                wfstate->keep_first = wfunc->winkpfirst;
                wfstate->args = (List*)ExecInitExprByRecursion((Expr*)wfunc->args, parent);

               /*
                * Complain if the windowfunc's arguments contain any
                * windowfuncs; nested window functions are semantically
                * nonsensical.  (This should have been caught earlier,
                * but we defend against it here anyway.)
                */
               if (nfuncs != winstate->numfuncs)
                   ereport(
                       ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("window function calls cannot be nested")));
           } else {
               /* planner messed up */
               ereport(
                   ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("WindowFunc found in non-WindowAgg plan node")));
           }
           state = (ExprState*)wfstate;
       } break;
       case T_ArrayRef: {
           ArrayRef* aref = (ArrayRef*)node;
           ArrayRefExprState* astate = makeNode(ArrayRefExprState);
           astate->xprstate.is_flt_frame = false;

           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalArrayRef;
           astate->refupperindexpr = (List*)ExecInitExprByRecursion((Expr*)aref->refupperindexpr, parent);
           astate->reflowerindexpr = (List*)ExecInitExprByRecursion((Expr*)aref->reflowerindexpr, parent);
           astate->refexpr = ExecInitExprByRecursion(aref->refexpr, parent);
           astate->refassgnexpr = ExecInitExprByRecursion(aref->refassgnexpr, parent);
           /* do one-time catalog lookups for type info */
           astate->refattrlength = get_typlen(aref->refarraytype);
           get_typlenbyvalalign(
               aref->refelemtype, &astate->refelemlength, &astate->refelembyval, &astate->refelemalign);
           state = (ExprState*)astate;
       } break;
       case T_FuncExpr: {
           FuncExpr* funcexpr = (FuncExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;
           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalFunc;


           fstate->args = (List*)ExecInitExprByRecursion((Expr*)funcexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           fstate->is_plpgsql_func_with_outparam = is_function_with_plpgsql_language_and_outparam(funcexpr->funcid);
           fstate->funcReturnsSet = false;
           fstate->fmtstr = funcexpr->fmtstr;
           fstate->nlsfmtstr = funcexpr->nlsfmtstr;
           state = (ExprState*)fstate;
       } break;
       case T_OpExpr: {
           OpExpr* opexpr = (OpExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalOper;
           fstate->args = (List*)ExecInitExprByRecursion((Expr*)opexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           fstate->funcReturnsSet = false;
           state = (ExprState*)fstate;
       } break;
       case T_DistinctExpr: {
           DistinctExpr* distinctexpr = (DistinctExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalDistinct;
           fstate->args = (List*)ExecInitExprByRecursion((Expr*)distinctexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           state = (ExprState*)fstate;
       } break;
       case T_NullIfExpr: {
           NullIfExpr* nullifexpr = (NullIfExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNullIf;
           fstate->args = (List*)ExecInitExprByRecursion((Expr*)nullifexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           state = (ExprState*)fstate;
       } break;
       case T_ScalarArrayOpExpr: {
           ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
           ScalarArrayOpExprState* sstate = makeNode(ScalarArrayOpExprState);
           sstate->fxprstate.xprstate.is_flt_frame = false;

           sstate->fxprstate.xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalScalarArrayOp;
           sstate->fxprstate.args = (List*)ExecInitExprByRecursion((Expr*)opexpr->args, parent);
           sstate->fxprstate.func.fn_oid = InvalidOid; /* not initialized */
           sstate->element_type = InvalidOid;          /* ditto */
           state = (ExprState*)sstate;
       } break;
       case T_BoolExpr: {
           BoolExpr* boolexpr = (BoolExpr*)node;
           BoolExprState* bstate = makeNode(BoolExprState);
           bstate->xprstate.is_flt_frame = false;

           switch (boolexpr->boolop) {
               case AND_EXPR:
                   bstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalAnd;
                   break;
               case OR_EXPR:
                   bstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalOr;
                   break;
               case NOT_EXPR:
                   bstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNot;
                   break;
               default:
                   ereport(ERROR,
                           (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_OPT),
                            errmsg("unrecognized boolop: %d", (int)boolexpr->boolop)));
                   break;
           }
           bstate->args = (List*)ExecInitExprByRecursion((Expr*)boolexpr->args, parent);
           state = (ExprState*)bstate;
       } break;
       case T_SubPlan: {
           SubPlan* subplan = (SubPlan*)node;
           SubPlanState* sstate = NULL;

           if (parent == NULL)
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("SubPlan found with no parent plan")));

           sstate = ExecInitSubPlan(subplan, parent);

           /* Add SubPlanState nodes to parent->subPlan */
           parent->subPlan = lappend(parent->subPlan, sstate);

           state = (ExprState*)sstate;
       } break;
       case T_AlternativeSubPlan: {
           AlternativeSubPlan* asplan = (AlternativeSubPlan*)node;
           AlternativeSubPlanState* asstate = NULL;

           if (parent == NULL)
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("AlternativeSubPlan found with no parent plan")));

           asstate = ExecInitAlternativeSubPlan(asplan, parent);

           state = (ExprState*)asstate;
       } break;
       case T_FieldSelect: {
           FieldSelect* fselect = (FieldSelect*)node;
           FieldSelectState* fstate = makeNode(FieldSelectState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalFieldSelect;
           fstate->arg = ExecInitExprByRecursion(fselect->arg, parent);
           fstate->argdesc = NULL;
           state = (ExprState*)fstate;
       } break;
       case T_FieldStore: {
           FieldStore* fstore = (FieldStore*)node;
           FieldStoreState* fstate = makeNode(FieldStoreState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalFieldStore;
           fstate->arg = ExecInitExprByRecursion(fstore->arg, parent);
           fstate->newvals = (List*)ExecInitExprByRecursion((Expr*)fstore->newvals, parent);
           fstate->argdesc = NULL;
           state = (ExprState*)fstate;
       } break;
       case T_RelabelType: {
           RelabelType* relabel = (RelabelType*)node;
           GenericExprState* gstate = makeNode(GenericExprState);
           gstate->xprstate.is_flt_frame = false;

           gstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalRelabelType;
           gstate->arg = ExecInitExprByRecursion(relabel->arg, parent);
           state = (ExprState*)gstate;
       } break;
       case T_CoerceViaIO: {
           CoerceViaIO* iocoerce = (CoerceViaIO*)node;
           CoerceViaIOState* iostate = makeNode(CoerceViaIOState);
           iostate->xprstate.is_flt_frame = false;
           Oid iofunc;
           bool typisvarlena = false;

           iostate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCoerceViaIO;
           iostate->arg = ExecInitExprByRecursion(iocoerce->arg, parent);
           iostate->fmtstr = iocoerce->fmtstr;
           iostate->nlsfmtstr = iocoerce->nlsfmtstr;
           /* lookup the result type's input function */
           getTypeInputInfo(iocoerce->resulttype, &iofunc, &iostate->intypioparam);
           fmgr_info(iofunc, &iostate->infunc);
           /* lookup the input type's output function */
           getTypeOutputInfo(exprType((Node*)iocoerce->arg), &iofunc, &typisvarlena);
           fmgr_info(iofunc, &iostate->outfunc);
           state = (ExprState*)iostate;
       } break;
       case T_ArrayCoerceExpr: {
           ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
           ArrayCoerceExprState* astate = makeNode(ArrayCoerceExprState);
           astate->xprstate.is_flt_frame = false;

           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalArrayCoerceExpr;
           astate->arg = ExecInitExprByRecursion(acoerce->arg, parent);
           astate->fmtstr = acoerce->fmtstr;
           astate->nlsfmtstr = acoerce->nlsfmtstr;
           astate->resultelemtype = get_element_type(acoerce->resulttype);
           if (astate->resultelemtype == InvalidOid)
               ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("target type is not an array")));
           /* Arrays over domains aren't supported yet */
           Assert(getBaseType(astate->resultelemtype) == astate->resultelemtype);
           astate->elemfunc.fn_oid = InvalidOid; /* not initialized */
           astate->amstate = (ArrayMapState*)palloc0(sizeof(ArrayMapState));
           state = (ExprState*)astate;
       } break;
       case T_ConvertRowtypeExpr: {
           ConvertRowtypeExpr* convert = (ConvertRowtypeExpr*)node;
           ConvertRowtypeExprState* cstate = makeNode(ConvertRowtypeExprState);
           cstate->xprstate.is_flt_frame = false;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalConvertRowtype;
           cstate->arg = ExecInitExprByRecursion(convert->arg, parent);
           state = (ExprState*)cstate;
       } break;
       case T_CaseExpr: {
           CaseExpr* caseexpr = (CaseExpr*)node;
           CaseExprState* cstate = makeNode(CaseExprState);
           cstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCase;
           cstate->arg = ExecInitExprByRecursion(caseexpr->arg, parent);
           foreach (l, caseexpr->args) {
               CaseWhen* when = (CaseWhen*)lfirst(l);
               CaseWhenState* wstate = makeNode(CaseWhenState);

               Assert(IsA(when, CaseWhen));
               wstate->xprstate.evalfunc = NULL; /* not used */
               wstate->xprstate.expr = (Expr*)when;
               wstate->xprstate.is_flt_frame = false;
               wstate->expr = ExecInitExprByRecursion(when->expr, parent);
               wstate->result = ExecInitExprByRecursion(when->result, parent);
               outlist = lappend(outlist, wstate);
           }
           cstate->args = outlist;
           cstate->defresult = ExecInitExprByRecursion(caseexpr->defresult, parent);
           state = (ExprState*)cstate;
       } break;
       case T_ArrayExpr: {
           ArrayExpr* arrayexpr = (ArrayExpr*)node;
           ArrayExprState* astate = makeNode(ArrayExprState);
           astate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;

           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalArray;
           foreach (l, arrayexpr->elements) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           astate->elements = outlist;
           /* do one-time catalog lookup for type info */
           get_typlenbyvalalign(
               arrayexpr->element_typeid, &astate->elemlength, &astate->elembyval, &astate->elemalign);
           state = (ExprState*)astate;
       } break;
       case T_RowExpr: {
           RowExpr* rowexpr = (RowExpr*)node;
           RowExprState* rstate = makeNode(RowExprState);
           rstate->xprstate.is_flt_frame = false;
           FormData_pg_attribute* attrs = NULL;
           List* outlist = NIL;
           ListCell* l = NULL;
           int i;

           rstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalRow;
           /* Build tupdesc to describe result tuples */
           if (rowexpr->row_typeid == RECORDOID) {
               /* generic record, use runtime type assignment */
               rstate->tupdesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
               BlessTupleDesc(rstate->tupdesc);
               /* we won't need to redo this at runtime */
           } else {
               /* it's been cast to a named type, use that */
               rstate->tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
           }
           /* Set up evaluation, skipping any deleted columns */
           Assert(list_length(rowexpr->args) <= rstate->tupdesc->natts);
           attrs = rstate->tupdesc->attrs;
           i = 0;
           foreach (l, rowexpr->args) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               if (!attrs[i].attisdropped) {
                   /*
                    * Guard against ALTER COLUMN TYPE on rowtype since
                    * the RowExpr was created.  XXX should we check
                    * typmod too?	Not sure we can be sure it'll be the
                    * same.
                    */
                   if (exprType((Node*)e) != attrs[i].atttypid)
                       ereport(ERROR,
                               (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("ROW() column has type %s instead of type %s",
                                       format_type_be(exprType((Node*)e)),
                                       format_type_be(attrs[i].atttypid))));
               } else {
                   /*
                    * Ignore original expression and insert a NULL. We
                    * don't really care what type of NULL it is, so
                    * always make an int4 NULL.
                    */
                   e = (Expr*)makeNullConst(INT4OID, -1, InvalidOid);
               }
               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
               i++;
           }
           rstate->args = outlist;
           state = (ExprState*)rstate;
       } break;
       case T_RowCompareExpr: {
           RowCompareExpr* rcexpr = (RowCompareExpr*)node;
           RowCompareExprState* rstate = makeNode(RowCompareExprState);
           rstate->xprstate.is_flt_frame = false;
           int nopers = list_length(rcexpr->opnos);
           List* outlist = NIL;
           ListCell* l = NULL;
           ListCell* l2 = NULL;
           ListCell* l3 = NULL;
           int i;

           rstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalRowCompare;
           Assert(list_length(rcexpr->largs) == nopers);
           outlist = NIL;
           foreach (l, rcexpr->largs) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           rstate->largs = outlist;
           Assert(list_length(rcexpr->rargs) == nopers);
           outlist = NIL;
           foreach (l, rcexpr->rargs) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           rstate->rargs = outlist;
           Assert(list_length(rcexpr->opfamilies) == nopers);
           rstate->funcs = (FmgrInfo*)palloc(nopers * sizeof(FmgrInfo));
           rstate->collations = (Oid*)palloc(nopers * sizeof(Oid));
           i = 0;
           forthree(l, rcexpr->opnos, l2, rcexpr->opfamilies, l3, rcexpr->inputcollids)
           {
               Oid opno = lfirst_oid(l);
               Oid opfamily = lfirst_oid(l2);
               Oid inputcollid = lfirst_oid(l3);
               int strategy;
               Oid lefttype;
               Oid righttype;
               Oid proc;

               get_op_opfamily_properties(opno, opfamily, false, &strategy, &lefttype, &righttype);
               proc = get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);

               /*
                * If we enforced permissions checks on index support
                * functions, we'd need to make a check here.  But the
                * index support machinery doesn't do that, and neither
                * does this code.
                */
               fmgr_info(proc, &(rstate->funcs[i]));
               rstate->collations[i] = inputcollid;
               i++;
           }
           state = (ExprState*)rstate;
       } break;
       case T_CoalesceExpr: {
           CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;
           CoalesceExprState* cstate = makeNode(CoalesceExprState);
           cstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCoalesce;
           foreach (l, coalesceexpr->args) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           cstate->args = outlist;
           state = (ExprState*)cstate;
       } break;
       case T_MinMaxExpr: {
           MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;
           MinMaxExprState* mstate = makeNode(MinMaxExprState);
           mstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;
           TypeCacheEntry* typentry = NULL;

           mstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalMinMax;
           foreach (l, minmaxexpr->args) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           mstate->args = outlist;
           /* Look up the btree comparison function for the datatype */
           typentry = lookup_type_cache(minmaxexpr->minmaxtype, TYPECACHE_CMP_PROC);
           if (!OidIsValid(typentry->cmp_proc))
               ereport(ERROR,
                       (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("could not identify a comparison function for type %s",
                               format_type_be(minmaxexpr->minmaxtype))));

           /*
            * If we enforced permissions checks on index support
            * functions, we'd need to make a check here.  But the index
            * support machinery doesn't do that, and neither does this
            * code.
            */
           fmgr_info(typentry->cmp_proc, &(mstate->cfunc));
           state = (ExprState*)mstate;
       } break;
       case T_XmlExpr: {
           XmlExpr* xexpr = (XmlExpr*)node;
           XmlExprState* xstate = makeNode(XmlExprState);
           xstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* arg = NULL;

           xstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalXml;
           outlist = NIL;
           foreach (arg, xexpr->named_args) {
               Expr* e = (Expr*)lfirst(arg);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           xstate->named_args = outlist;

           outlist = NIL;
           foreach (arg, xexpr->args) {
               Expr* e = (Expr*)lfirst(arg);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           xstate->args = outlist;

           state = (ExprState*)xstate;
       } break;
       case T_NullTest: {
           NullTest* ntest = (NullTest*)node;
           NullTestState* nstate = makeNode(NullTestState);
           nstate->xprstate.is_flt_frame = false;

           nstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNullTest;
           nstate->arg = ExecInitExprByRecursion(ntest->arg, parent);
           nstate->argdesc = NULL;
           state = (ExprState*)nstate;
       } break;
        case T_NanTest: {
            NanTest* ntest = (NanTest*)node;
            NanTestState* nstate = makeNode(NanTestState);
            nstate->xprstate.is_flt_frame = false;

            nstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNanTest;
            nstate->arg = ExecInitExprByRecursion(ntest->arg, parent);
            state = (ExprState*)nstate;
       } break;
        case T_InfiniteTest: {
           InfiniteTest* itest = (InfiniteTest*)node;
           InfiniteTestState* nstate = makeNode(InfiniteTestState);
           nstate->xprstate.is_flt_frame = false;

           nstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalInfiniteTest;
           nstate->arg = ExecInitExprByRecursion(itest->arg, parent);
           state = (ExprState*)nstate;
       } break;
       case T_HashFilter: {
           HashFilter* htest = (HashFilter*)node;
           HashFilterState* hstate = makeNode(HashFilterState);
           hstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;
           int idx = 0;

           hstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalHashFilter;

           foreach (l, htest->arg) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }

           hstate->arg = outlist;
           hstate->bucketMap = get_bucketmap_by_execnode(parent->plan->exec_nodes,
                                                         parent->state->es_plannedstmt,
                                                         &hstate->bucketCnt);
           hstate->nodelist = (uint2*)palloc(list_length(htest->nodeList) * sizeof(uint2));
           foreach (l, htest->nodeList)
               hstate->nodelist[idx++] = lfirst_int(l);

           state = (ExprState*)hstate;
       } break;
       case T_BooleanTest: {
           BooleanTest* btest = (BooleanTest*)node;
           GenericExprState* gstate = makeNode(GenericExprState);
           gstate->xprstate.is_flt_frame = false;

           gstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalBooleanTest;
           gstate->arg = ExecInitExprByRecursion(btest->arg, parent);
           state = (ExprState*)gstate;
       } break;
       case T_CoerceToDomain: {
           CoerceToDomain* ctest = (CoerceToDomain*)node;
           CoerceToDomainState* cstate = makeNode(CoerceToDomainState);
           cstate->xprstate.is_flt_frame = false;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCoerceToDomain;
           cstate->arg = ExecInitExprByRecursion(ctest->arg, parent);
           cstate->constraints = GetDomainConstraints(ctest->resulttype);
           state = (ExprState*)cstate;
       } break;
       case T_CurrentOfExpr:
           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           state->evalfunc = ExecEvalCurrentOfExpr;
           break;
       case T_TargetEntry: {
           TargetEntry* tle = (TargetEntry*)node;
           GenericExprState* gstate = makeNode(GenericExprState);
           gstate->xprstate.is_flt_frame = false;

           gstate->xprstate.evalfunc = NULL; /* not used */
           gstate->arg = ExecInitExprByRecursion(tle->expr, parent);
           state = (ExprState*)gstate;
       } break;
       case T_List: {
           List* outlist = NIL;
           ListCell* l = NULL;

           foreach (l, (List*)node) {
               outlist = lappend(outlist, ExecInitExprByRecursion((Expr*)lfirst(l), parent));
           }
           /* Don't fall through to the "common" code below */
           gstrace_exit(GS_TRC_ID_ExecInitExpr);
           return (ExprState*)outlist;
       }
       case T_Rownum: {
           RownumState* rnstate = (RownumState*)makeNode(RownumState);
           rnstate->xprstate.is_flt_frame = false;
           rnstate->ps = parent;
           state = (ExprState*)rnstate;
           state->evalfunc = (ExprStateEvalFunc)ExecEvalRownum;
       } break;
       case T_PrefixKey: {
            PrefixKey* pkey = (PrefixKey*)node;
            PrefixKeyState* pstate = makeNode(PrefixKeyState);
            Oid argtype = exprType((Node*)pkey->arg);

            if (argtype == BYTEAOID || argtype == RAWOID || argtype == BLOBOID) {
                pstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalPrefixBytea;
                pstate->encoding = PG_INVALID_ENCODING;
            } else {
                Oid argcollation = exprCollation((Node*)pkey->arg);
                pstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalPrefixText;
                pstate->encoding = get_valid_charset_by_collation(argcollation);
            }
            pstate->arg = ExecInitExpr(pkey->arg, parent);
            
            state = (ExprState*)pstate;
        } break;
        case T_UserSetElem: {
            UserSetElem* useexpr = (UserSetElem*)node;
            UserSetElemState* usestate = (UserSetElemState*)makeNode(UserSetElemState);
            usestate->use = useexpr;
            state = (ExprState*)usestate;
            state->evalfunc = (ExprStateEvalFunc)ExecEvalUserSetElm;
            usestate->instate = ExecInitExpr((Expr *)useexpr->val, parent);
            u_sess->exec_cxt.has_equal_uservar = true;
        } break;
       case T_PriorExpr:
            state = (ExprState*)makeNode(ExprState);
            state->evalfunc = ExecEvalPriorExpr;
            break;

        case T_CursorExpression: {
            ListCell* l = NULL;
            CursorExpression* cursor_expression = (CursorExpression*)node;
            CursorExpressionState* dstate = makeNode(CursorExpressionState);
            dstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCursorExpression;
            dstate->cursor_expression = cursor_expression;
            List* outlist = NIL;
            foreach (l, cursor_expression->param) {
                Expr* e = (Expr*)lfirst(l);
                ExprState* estate = NULL;
                estate = ExecInitExprByRecursion(e, parent);
                outlist = lappend(outlist, estate);
            }
            dstate->param = outlist;
            state = (ExprState*)dstate;
         } break;

        
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when initializing expression.", (int)nodeTag(node))));
           state = NULL; /* keep compiler quiet */
           break;
   }

   /* Common code for all state-node types */
   state->expr = node;

   if (nodeTag(node) != T_TargetEntry)
       state->resultType = exprType((Node*)node);

   gstrace_exit(GS_TRC_ID_ExecInitExpr);
   return state;
}
```

<a id="source-104"></a>

#### ExecInitExprRec()

来源：`src/gausskernel/runtime/executor/execExpr.cpp:532-2192`；范围：**完整函数**。

内容 SHA256：`9d28b54598b8bccca8ccf9e1b2b9c88a61f3c2001fc2abd07688424c1ba43dca`。

完整展平表达式初始化函数，需与对应解释执行 opcode 配对迁移。

```cpp
static void
ExecInitExprRec(Expr *node, ExprState *state,
				Datum *resv, bool *resnull, Expr *parent_node)
{
    ExprEvalStep scratch = {0};

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/* Step's output location is always what the caller gave us */
	Assert(resv != NULL && resnull != NULL);
	scratch.resvalue = resv;
	scratch.resnull = resnull;

	/* cases should be ordered as they are in enum NodeTag */
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var *variable = (Var *) node;

				if (variable->varattno == InvalidAttrNumber)
				{
					/* whole-row Var */
					ExecInitWholeRowVar(&scratch, variable, state);
				}
				else if (variable->varattno <= 0)
				{
					/* system column */
					scratch.d.var.attnum = variable->varattno;
					scratch.d.var.vartype = variable->vartype;
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_SYSVAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_SYSVAR;
							break;

							/* INDEX_VAR is handled by default case */

						default:
							scratch.opcode = EEOP_SCAN_SYSVAR;
							break;
					}
				}
				else
				{
					/* regular user column */
					scratch.d.var.attnum = variable->varattno - 1;
					scratch.d.var.vartype = variable->vartype;
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_VAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_VAR;
							break;

							/* INDEX_VAR is handled by default case */

						default:
							scratch.opcode = EEOP_SCAN_VAR;
							break;
					}
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}
        case T_Const:
			{
				Const *con = (Const *) node;

				scratch.opcode = EEOP_CONST;
				scratch.d.constval.value = con->constvalue;
				scratch.d.constval.isnull = con->constisnull;
				scratch.d.constval.con = (Const *) node;

				if (parent_node && IsA(parent_node, FuncExpr) && 
					expr_func_has_refcursor_args(((FuncExpr*)parent_node)->funcid))
					scratch.d.constval.is_cursor = true;
				else
					scratch.d.constval.is_cursor = false;

				ExprEvalPushStep(state, &scratch);
				break;			
			}
		case T_UserVar:
		case T_SetVariableExpr:
			{
				scratch.opcode = EEOP_USERVAR_OR_SETVARIABLE;

				Const *con = NULL;
				if (IsA((Expr*)node, UserVar)) {
					bool found = false;
					UserVar *uservar = (UserVar *)node;
					GucUserParamsEntry *entry = (GucUserParamsEntry *)hash_search(KNL_UTILS_GUC_FIELD(&u_sess->utils_cxt, set_user_params_htab),
																				uservar->name, HASH_FIND, &found);

					if (found) {
						if (entry->isParse) {
                            con = (Const *)uservar->value;
                            entry->isParse = false;
						} else {
                            Node *node_tmp = coerce_type(NULL, (Node *)entry->value, entry->value->consttype,
                                ((Const *)uservar->value)->consttype, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
                                    NULL, NULL, -1);
                            node_tmp = eval_const_expression_value(NULL, node_tmp, NULL);
							if (!IsA(node_tmp, Const)) {
								ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                        			errmsg("The value of a user_defined variable must be convertible to a constant.")));
							}
							con = (Const *)node_tmp;
						}
					} else {
						constexpr int32 consttypmod = -1;
						constexpr int constlen = -2;
						con = makeConst(UNKNOWNOID, consttypmod, InvalidOid, constlen, (Datum)0, true, false);
					}
				} else {
					SetVariableExpr* setvar = (SetVariableExpr*)transformSetVariableExpr((SetVariableExpr*)node);
					con = (Const*)setvar->value;
				}
				
				scratch.d.uservar.con = con;
				ExprEvalPushStep(state, &scratch);

				break;
			}
		case T_UserSetElem: 
			{
            	UserSetElem* useexpr = (UserSetElem*)node;
            	// UserSetElemState* usestate = (UserSetElemState*)makeNode(UserSetElemState);
            	// usestate->use = useexpr;
				scratch.d.userset.useexpr = useexpr;
            	// state = (ExprState*)usestate;
            	// state->evalfunc = (ExprStateEvalFunc)ExecEvalUserSetElm;
				scratch.opcode = EEOP_USERSET_ELEM;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_Param:
			{
				Param *param = (Param *) node;			

				switch (param->paramkind)
				{
					case PARAM_EXEC:
						scratch.opcode = EEOP_PARAM_EXEC;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;
						ExprEvalPushStep(state, &scratch);
						break;
					case PARAM_EXTERN:
						scratch.opcode = EEOP_PARAM_EXTERN;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;

						if (parent_node && IsA(parent_node, FuncExpr) && 
							expr_func_has_refcursor_args(((FuncExpr*)parent_node)->funcid))
							scratch.d.param.is_cursor = true;
						else
							scratch.d.param.is_cursor = false;

						ExprEvalPushStep(state, &scratch);
						break;
					default:
						elog(ERROR, "unrecognized paramkind: %d",
							 (int) param->paramkind);
						break;
				}
				break;
			}
		case T_Aggref:
			{
				Aggref* aggref = (Aggref*)node;
				AggrefExprState* astate = makeNode(AggrefExprState);
				scratch.opcode = EEOP_AGGREF;
				scratch.d.aggref.astate = astate;
				astate->aggref = aggref;
				astate->xprstate.expr = node;

				if (state->parent && (IsA(state->parent, AggState) || IsA(state->parent, VecAggState))) {
					AggState* aggstate = (AggState*)state->parent;
					int naggs;

					aggstate->aggs = lcons(astate, aggstate->aggs);
					naggs = ++aggstate->numaggs;

					astate->aggdirectargs = ExecInitExprList(aggref->aggdirectargs, state->parent);

					/*
					* Complain if the aggregate's arguments contain any
					* aggregates; nested agg functions are semantically
					* nonsensical.  (This should have been caught earlier,
					* but we defend against it here anyway.)
					*/
					if (naggs != aggstate->numaggs)
						ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("aggregate function calls cannot be nested")));
				} else {
					/* planner messed up */
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_AGG), errmodule(MOD_OPT), errmsg("Aggref found in non-Agg plan node")));
				}
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_GroupingFunc:
			{
				GroupingFunc *grp_node = (GroupingFunc *) node;
				Agg		   *agg;

				if (!state->parent 
					|| (!IsA(state->parent, AggState) && !IsA(state->parent, VecAggState)) 
					|| (!IsA(state->parent->plan, Agg) && !IsA(state->parent->plan, VecAgg)))
					elog(ERROR, "GroupingFunc found in non-Agg plan node");

				scratch.opcode = EEOP_GROUPING_FUNC;

				agg = (Agg *) (state->parent->plan);

				if (agg->groupingSets)
					scratch.d.grouping_func.clauses = grp_node->cols;
				else
					scratch.d.grouping_func.clauses = NIL;

				ExprEvalPushStep(state, &scratch);
				break;				
			}
		case T_WindowFunc:
			{
				WindowFunc* wfunc = (WindowFunc*)node;
				WindowFuncExprState* wfstate = makeNode(WindowFuncExprState);
				wfstate->wfunc = wfunc;
				wfstate->xprstate.expr = node;
				
				if (state->parent && (IsA(state->parent, WindowAggState) || IsA(state->parent, VecWindowAggState))) {
					WindowAggState* winstate = (WindowAggState*)state->parent;
					int nfuncs;

					winstate->funcs = lappend(winstate->funcs, wfstate);
					nfuncs = ++winstate->numfuncs;
					if (wfunc->winagg)
						winstate->numaggs++;

					wfstate->args = ExecInitExprList(wfunc->args, state->parent);

					/*
					* Complain if the windowfunc's arguments contain any
					* windowfuncs; nested window functions are semantically
					* nonsensical.  (This should have been caught earlier,
					* but we defend against it here anyway.)
					*/
					if (nfuncs != winstate->numfuncs)
						ereport(
							ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("window function calls cannot be nested")));
				} else {
					/* planner messed up */
					ereport(
						ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("WindowFunc found in non-WindowAgg plan node")));
				}

				scratch.opcode = EEOP_WINDOW_FUNC;
				scratch.d.window_func.wfstate = wfstate;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_ArrayRef:
			{
				ArrayRef *aref = (ArrayRef *) node;

				ExecInitArrayRef(&scratch, aref, state, resv, resnull, node);
				break;		
			}
		case T_FuncExpr:
			{
				FuncExpr *func = (FuncExpr *) node;
				ExecInitFunc(&scratch, node,
							 func->args, func->funcid, func->inputcollid,
							 state);
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_OpExpr:
			{
				OpExpr *op = (OpExpr *) node;

				ExecInitFunc(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 state);
				ExprEvalPushStep(state, &scratch);
				break;
			}		
		case T_DistinctExpr:
			{
				DistinctExpr *op = (DistinctExpr *) node;

				ExecInitFunc(&scratch, node,
								op->args, op->opfuncid, op->inputcollid,
								state);

				/*
				* Change opcode of call instruction to EEOP_DISTINCT.
				*
				* XXX: historically we've not called the function usage
				* pgstat infrastructure - that seems inconsistent given that
				* we do so for normal function *and* operator evaluation.  If
				* we decided to do that here, we'd probably want separate
				* opcodes for FUSAGE or not.
				*/
				scratch.opcode = EEOP_DISTINCT;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_NullIfExpr:
			{	
				NullIfExpr *op = (NullIfExpr *) node;

				ExecInitFunc(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 state);

				/*
				 * Change opcode of call instruction to EEOP_NULLIF.
				 *
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation.  If
				 * we decided to do that here, we'd probably want separate
				 * opcodes for FUSAGE or not.
				 */
				scratch.opcode = EEOP_NULLIF;
				ExprEvalPushStep(state, &scratch);
				break;	
			}
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) node;
				Expr	   *scalararg;
				Expr	   *arrayarg;
				FmgrInfo   *finfo;
				FunctionCallInfo fcinfo;
				AclResult	aclresult;
                Oid cmpfuncid;

                /*
                 * Select the correct comparison function.  When we do hashed
                 * NOT IN clauses, the opfuncid will be the inequality
                 * comparison function and negfuncid will be set to equality.
                 * We need to use the equality function for hash probes.
                 */
                if (OidIsValid(opexpr->negfuncid)) {
                    Assert(OidIsValid(opexpr->hashfuncid));
                    cmpfuncid = opexpr->negfuncid;
                } else {
                    cmpfuncid = opexpr->opfuncid;
                }

				Assert(list_length(opexpr->args) == 2);
				scalararg = (Expr *) linitial(opexpr->args);
				arrayarg = (Expr *) lsecond(opexpr->args);

				/* Check permission to call function */
                aclresult = pg_proc_aclcheck(cmpfuncid,
											 GetUserId(),
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
                    aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(cmpfuncid));
                InvokeFunctionExecuteHook(cmpfuncid);

                if (OidIsValid(opexpr->hashfuncid)) {
                    aclresult = pg_proc_aclcheck(opexpr->hashfuncid, GetUserId(), ACL_EXECUTE);
                    if (aclresult != ACLCHECK_OK)
                        aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(opexpr->hashfuncid));
                    InvokeFunctionExecuteHook(opexpr->hashfuncid);
                }

				/* Set up the primary fmgr lookup information */
				finfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
				fcinfo = (FunctionCallInfo)palloc0(sizeof(FunctionCallInfoData));
                fmgr_info(cmpfuncid, finfo);
				fmgr_info_set_expr((Node *) node, finfo);
				InitFunctionCallInfoData(*fcinfo, finfo, 2,
										 opexpr->inputcollid, NULL, NULL);

                /*
                 * If hashfuncid is set, we create a EEOP_HASHED_SCALARARRAYOP
                 * step instead of a EEOP_SCALARARRAYOP.  This provides much
                 * faster lookup performance than the normal linear search
                 * when the number of items in the array is anything but very
                 * small.
                 */
                if (OidIsValid(opexpr->hashfuncid)) {
                    /* Evaluate scalar directly into left function argument */
                    ExecInitExprRec(scalararg, state, &fcinfo->arg[0], &fcinfo->argnull[0], node);

                    /*
                     * Evaluate array argument into our return value.  There's
                     * no danger in that, because the return value is
                     * guaranteed to be overwritten by
                     * EEOP_HASHED_SCALARARRAYOP, and will not be passed to
                     * any other expression.
                     */
                    ExecInitExprRec(arrayarg, state, resv, resnull, node);

                    /* And perform the operation */
                    scratch.opcode = EEOP_HASHED_SCALARARRAYOP;
                    scratch.d.hashedscalararrayop.inclause = opexpr->useOr;
                    scratch.d.hashedscalararrayop.finfo = finfo;
                    scratch.d.hashedscalararrayop.fcinfo_data = fcinfo;
                    scratch.d.hashedscalararrayop.saop = opexpr;
                    scratch.d.hashedscalararrayop.elements_tab = NULL;

                    ExprEvalPushStep(state, &scratch);
                } else {
                    /* Evaluate scalar directly into left function argument */
                    ExecInitExprRec(scalararg, state, &fcinfo->arg[0], &fcinfo->argnull[0], node);

                    /*
                     * Evaluate array argument into our return value.  There's no
                     * danger in that, because the return value is guaranteed to
                     * be overwritten by EEOP_SCALARARRAYOP, and will not be
                     * passed to any other expression.
                     */
                    ExecInitExprRec(arrayarg, state, resv, resnull, node);

                    /* And perform the operation */
                    scratch.opcode = EEOP_SCALARARRAYOP;
                    scratch.d.scalararrayop.element_type = InvalidOid;
                    scratch.d.scalararrayop.useOr = opexpr->useOr;
                    scratch.d.scalararrayop.finfo = finfo;
                    scratch.d.scalararrayop.fcinfo_data = fcinfo;
                    scratch.d.scalararrayop.fn_addr = finfo->fn_addr;
                    ExprEvalPushStep(state, &scratch);
                }
				break;
			}
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				int			nargs = list_length(boolexpr->args);
				List	   *adjust_jumps = NIL;
				int			off;
				ListCell   *lc;

				/* allocate scratch memory used by all steps of AND/OR */
				if (boolexpr->boolop != NOT_EXPR)
					scratch.d.boolexpr.anynull = (bool *) palloc(sizeof(bool));

				/*
				 * For each argument evaluate the argument itself, then
				 * perform the bool operation's appropriate handling.
				 *
				 * We can evaluate each argument into our result area, since
				 * the short-circuiting logic means we only need to remember
				 * previous NULL values.
				 *
				 * AND/OR is split into separate STEP_FIRST (one) / STEP (zero
				 * or more) / STEP_LAST (one) steps, as each of those has to
				 * perform different work.  The FIRST/LAST split is valid
				 * because AND/OR have at least two arguments.
				 */
				off = 0;
				foreach(lc, boolexpr->args)
				{
					Expr	   *arg = (Expr *) lfirst(lc);

					/* Evaluate argument into our output variable */
					ExecInitExprRec(arg, state, resv, resnull, node);

					/* Perform the appropriate step type */
					switch (boolexpr->boolop)
					{
						case AND_EXPR:
							Assert(nargs >= 2);

							if (off == 0)
								scratch.opcode = EEOP_BOOL_AND_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_BOOL_AND_STEP_LAST;
							else
								scratch.opcode = EEOP_BOOL_AND_STEP;
							break;
						case OR_EXPR:
							Assert(nargs >= 2);

							if (off == 0)
								scratch.opcode = EEOP_BOOL_OR_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_BOOL_OR_STEP_LAST;
							else
								scratch.opcode = EEOP_BOOL_OR_STEP;
							break;
						case NOT_EXPR:
							Assert(nargs == 1);

							scratch.opcode = EEOP_BOOL_NOT_STEP;
							break;
						default:
							elog(ERROR, "unrecognized boolop: %d",
								 (int) boolexpr->boolop);
							break;
					}

					scratch.d.boolexpr.jumpdone = -1;
					ExprEvalPushStep(state, &scratch);
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
					off++;
				}

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->d.boolexpr.jumpdone == -1);
					as->d.boolexpr.jumpdone = state->steps_len;
				}

				break;
			}
		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;
				SubPlanState *sstate;

				if (!state->parent)
					elog(ERROR, "SubPlan found with no parent plan");

				sstate = ExecInitSubPlan(subplan, state->parent);

				/* add SubPlanState nodes to state->parent->subPlan */
				state->parent->subPlan = lappend(state->parent->subPlan,
												 sstate);

				scratch.opcode = EEOP_SUBPLAN;
				scratch.d.subplan.sstate = sstate;

				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_AlternativeSubPlan:
			{
				AlternativeSubPlan *asplan = (AlternativeSubPlan *) node;
				AlternativeSubPlanState *asstate;

				if (!state->parent)
					elog(ERROR, "AlternativeSubPlan found with no parent plan");

				asstate = ExecInitAlternativeSubPlan(asplan, state->parent);

				scratch.opcode = EEOP_ALTERNATIVE_SUBPLAN;
				scratch.d.alternative_subplan.asstate = asstate;

				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_FieldSelect:
			{
				FieldSelect *fselect = (FieldSelect *) node;

				/* evaluate row/record argument into result area */
				ExecInitExprRec(fselect->arg, state, resv, resnull, node);

				/* and extract field */
				scratch.opcode = EEOP_FIELDSELECT;
				scratch.d.fieldselect.fieldnum = fselect->fieldnum;
				scratch.d.fieldselect.resulttype = fselect->resulttype;
				scratch.d.fieldselect.argdesc = NULL;

				ExprEvalPushStep(state, &scratch);
				break;
			}	
		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;
				TupleDesc	tupDesc;
				TupleDesc  *descp;
				Datum	   *values;
				bool	   *nulls;
				int			ncolumns;
				ListCell   *l1,
						   *l2;

				/* find out the number of columns in the composite type */
				tupDesc = lookup_rowtype_tupdesc(fstore->resulttype, -1);
				ncolumns = tupDesc->natts;
				DecrTupleDescRefCount(tupDesc);

				/* create workspace for column values */
				values = (Datum *) palloc(sizeof(Datum) * ncolumns);
				nulls = (bool *) palloc(sizeof(bool) * ncolumns);

				/* create workspace for runtime tupdesc cache */
				descp = (TupleDesc *) palloc(sizeof(TupleDesc));
				*descp = NULL;

				/* emit code to evaluate the composite input value */
				ExecInitExprRec(fstore->arg, state, resv, resnull, node);

				/* next, deform the input tuple into our workspace */
				scratch.opcode = EEOP_FIELDSTORE_DEFORM;
				scratch.d.fieldstore.fstore = fstore;
				scratch.d.fieldstore.argdesc = descp;
				scratch.d.fieldstore.values = values;
				scratch.d.fieldstore.nulls = nulls;
				scratch.d.fieldstore.ncolumns = ncolumns;
				ExprEvalPushStep(state, &scratch);

				/* evaluate new field values, store in workspace columns */
				forboth(l1, fstore->newvals, l2, fstore->fieldnums)
				{
					Expr	   *e = (Expr *) lfirst(l1);
					AttrNumber	fieldnum = lfirst_int(l2);
					Datum	   *save_innermost_caseval;
					bool	   *save_innermost_casenull;

					if (fieldnum <= 0 || fieldnum > ncolumns)
						elog(ERROR, "field number %d is out of range in FieldStore",
							 fieldnum);

					/*
					 * Use the CaseTestExpr mechanism to pass down the old
					 * value of the field being replaced; this is needed in
					 * case the newval is itself a FieldStore or ArrayRef that
					 * has to obtain and modify the old value.  It's safe to
					 * reuse the CASE mechanism because there cannot be a CASE
					 * between here and where the value would be needed, and a
					 * field assignment can't be within a CASE either.  (So
					 * saving and restoring innermost_caseval is just
					 * paranoia, but let's do it anyway.)
					 */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = &values[fieldnum - 1];
					state->innermost_casenull = &nulls[fieldnum - 1];

					ExecInitExprRec(e, state,
									&values[fieldnum - 1],
									&nulls[fieldnum - 1], node);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;
				}

				/* finally, form result tuple */
				scratch.opcode = EEOP_FIELDSTORE_FORM;
				scratch.d.fieldstore.fstore = fstore;
				scratch.d.fieldstore.argdesc = descp;
				scratch.d.fieldstore.values = values;
				scratch.d.fieldstore.nulls = nulls;
				scratch.d.fieldstore.ncolumns = ncolumns;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_RelabelType:
			{
				/* relabel doesn't need to do anything at runtime */
				RelabelType *relabel = (RelabelType *) node;

				ExecInitExprRec(relabel->arg, state, resv, resnull, node);
				break;
			}
		case T_CoerceViaIO:
			{
				CoerceViaIO *iocoerce = (CoerceViaIO *) node;
				Oid			iofunc;
				bool		typisvarlena;
				Oid			typioparam;
				FunctionCallInfo fcinfo_in;

				/* evaluate argument into step's result area */
				ExecInitExprRec(iocoerce->arg, state, resv, resnull, node);

				/*
				 * Prepare both output and input function calls, to be
				 * evaluated inside a single evaluation step for speed - this
				 * can be a very common operation.
				 *
				 * We don't check permissions here as a type's input/output
				 * function are assumed to be executable by everyone.
				 */
				scratch.opcode = EEOP_IOCOERCE;

				/* lookup the source type's output function */
				scratch.d.iocoerce.finfo_out = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
				scratch.d.iocoerce.fcinfo_data_out = (FunctionCallInfo)palloc0(sizeof(FunctionCallInfoData));

				getTypeOutputInfo(exprType((Node *) iocoerce->arg),
								  &iofunc, &typisvarlena);
				fmgr_info(iofunc, scratch.d.iocoerce.finfo_out);
				fmgr_info_set_expr((Node *) node, scratch.d.iocoerce.finfo_out);
				InitFunctionCallInfoData(*scratch.d.iocoerce.fcinfo_data_out,
										 scratch.d.iocoerce.finfo_out,
										 1, InvalidOid, NULL, NULL);

				/* lookup the result type's input function */
				scratch.d.iocoerce.finfo_in = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
				scratch.d.iocoerce.fcinfo_data_in = (FunctionCallInfo)palloc0(sizeof(FunctionCallInfoData));

				getTypeInputInfo(iocoerce->resulttype,
								 &iofunc, &typioparam);
				fmgr_info(iofunc, scratch.d.iocoerce.finfo_in);
				fmgr_info_set_expr((Node *) node, scratch.d.iocoerce.finfo_in);
				InitFunctionCallInfoData(*scratch.d.iocoerce.fcinfo_data_in,
										 scratch.d.iocoerce.finfo_in,
										 3, InvalidOid, NULL, NULL);

				/*
				 * We can preload the second and third arguments for the input
				 * function, since they're constants.
				 */
				fcinfo_in = scratch.d.iocoerce.fcinfo_data_in;
				fcinfo_in->arg[1] = ObjectIdGetDatum(typioparam);
				fcinfo_in->argnull[1] = false;
				fcinfo_in->arg[2] = Int32GetDatum(-1);
				fcinfo_in->argnull[2] = false;

				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acoerce = (ArrayCoerceExpr *) node;
				Oid			resultelemtype;

				/* evaluate argument into step's result area */
				ExecInitExprRec(acoerce->arg, state, resv, resnull, node);

				resultelemtype = get_element_type(acoerce->resulttype);
				if (!OidIsValid(resultelemtype))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("target type is not an array")));
				/* Arrays over domains aren't supported yet */
				Assert(getBaseType(resultelemtype) == resultelemtype);

				scratch.opcode = EEOP_ARRAYCOERCE;
				scratch.d.arraycoerce.coerceexpr = acoerce;
				scratch.d.arraycoerce.resultelemtype = resultelemtype;

				if (OidIsValid(acoerce->elemfuncid))
				{
					AclResult	aclresult;

					/* Check permission to call function */
					aclresult = pg_proc_aclcheck(acoerce->elemfuncid,
												 GetUserId(),
												 ACL_EXECUTE);
					if (aclresult != ACLCHECK_OK)
						aclcheck_error(aclresult, ACL_KIND_PROC,
									   get_func_name(acoerce->elemfuncid));
					InvokeFunctionExecuteHook(acoerce->elemfuncid);

					/* Set up the primary fmgr lookup information */
					scratch.d.arraycoerce.elemfunc =
						(FmgrInfo *) palloc0(sizeof(FmgrInfo));
					fmgr_info(acoerce->elemfuncid,
							  scratch.d.arraycoerce.elemfunc);
					fmgr_info_set_expr((Node *) acoerce,
									   scratch.d.arraycoerce.elemfunc);

					/* Set up workspace for array_map */
					scratch.d.arraycoerce.amstate =
						(ArrayMapState *) palloc0(sizeof(ArrayMapState));
				}
				else
				{
					/* Don't need workspace if there's no conversion func */
					scratch.d.arraycoerce.elemfunc = NULL;
					scratch.d.arraycoerce.amstate = NULL;
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) node;

				/* evaluate argument into step's result area */
				ExecInitExprRec(convert->arg, state, resv, resnull, node);

				/* and push conversion step */
				scratch.opcode = EEOP_CONVERT_ROWTYPE;
				scratch.d.convert_rowtype.convert = convert;
				scratch.d.convert_rowtype.indesc = NULL;
				scratch.d.convert_rowtype.outdesc = NULL;
				scratch.d.convert_rowtype.map = NULL;
				scratch.d.convert_rowtype.initialized = false;

				ExprEvalPushStep(state, &scratch);
				break;
			}

			/* note that CaseWhen expressions are handled within this block */	
		case T_CaseExpr:
			{
				CaseExpr   *caseExpr = (CaseExpr *) node;
				List	   *adjust_jumps = NIL;
				Datum	   *caseval = NULL;
				bool	   *casenull = NULL;
				ListCell   *lc;

				/*
				 * If there's a test expression, we have to evaluate it and
				 * save the value where the CaseTestExpr placeholders can find
				 * it.
				 */
				if (caseExpr->arg != NULL)
				{
					/* Evaluate testexpr into caseval/casenull workspace */
					caseval = (Datum*)palloc(sizeof(Datum));
					casenull = (bool*)palloc(sizeof(bool));

					ExecInitExprRec(caseExpr->arg, state,
									caseval, casenull, node);

					/*
					 * Since value might be read multiple times, force to R/O
					 * - but only if it could be an expanded datum.
					 */
					if (get_typlen(exprType((Node *) caseExpr->arg)) == -1)
					{
						/* change caseval in-place */
						scratch.opcode = EEOP_MAKE_READONLY;
						scratch.resvalue = caseval;
						scratch.resnull = casenull;
						scratch.d.make_readonly.value = caseval;
						scratch.d.make_readonly.isnull = casenull;
						ExprEvalPushStep(state, &scratch);
						/* restore normal settings of scratch fields */
						scratch.resvalue = resv;
						scratch.resnull = resnull;
					}
				}

				/*
				 * Prepare to evaluate each of the WHEN clauses in turn; as
				 * soon as one is true we return the value of the
				 * corresponding THEN clause.  If none are true then we return
				 * the value of the ELSE clause, or NULL if there is none.
				 */
				foreach(lc, caseExpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(lc);
					Datum	   *save_innermost_caseval;
					bool	   *save_innermost_casenull;
					int			whenstep;

					/*
					 * Make testexpr result available to CaseTestExpr nodes
					 * within the condition.  We must save and restore prior
					 * setting of innermost_caseval fields, in case this node
					 * is itself within a larger CASE.
					 *
					 * If there's no test expression, we don't actually need
					 * to save and restore these fields; but it's less code to
					 * just do so unconditionally.
					 */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = caseval;
					state->innermost_casenull = casenull;

					/* evaluate condition into CASE's result variables */
					ExecInitExprRec(when->expr, state, resv, resnull, node);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;

					/* If WHEN result isn't true, jump to next CASE arm */
					scratch.opcode = EEOP_JUMP_IF_NOT_TRUE;
					scratch.d.jump.jumpdone = -1;		/* computed later */
					ExprEvalPushStep(state, &scratch);
					whenstep = state->steps_len - 1;

					/*
					 * If WHEN result is true, evaluate THEN result, storing
					 * it into the CASE's result variables.
					 */
					ExecInitExprRec(when->result, state, resv, resnull, node);

					/* Emit JUMP step to jump to end of CASE's code */
					scratch.opcode = EEOP_JUMP;
					scratch.d.jump.jumpdone = -1;		/* computed later */
					ExprEvalPushStep(state, &scratch);

					/*
					 * Don't know address for that jump yet, compute once the
					 * whole CASE expression is built.
					 */
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);

					/*
					 * But we can set WHEN test's jump target now, to make it
					 * jump to the next WHEN subexpression or the ELSE.
					 */
					state->steps[whenstep].d.jump.jumpdone = state->steps_len;
				}

				if (caseExpr->defresult)
				{
					/* evaluate ELSE expr into CASE's result variables */
					ExecInitExprRec(caseExpr->defresult, state,
									resv, resnull, node);
				}
				else
				{
					/* default ELSE is to return NULL */
					scratch.opcode = EEOP_CONST;
					scratch.d.constval.value = (Datum) 0;
					scratch.d.constval.isnull = true;
					scratch.d.constval.con = NULL;

					ExprEvalPushStep(state, &scratch);
				}

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_JUMP);
					Assert(as->d.jump.jumpdone == -1);
					as->d.jump.jumpdone = state->steps_len;
				}

				break;
			}
		case T_CaseTestExpr:
			{
				/*
				 * Read from location identified by innermost_caseval.  Note
				 * that innermost_caseval could be NULL, if this node isn't
				 * actually within a CaseExpr, ArrayCoerceExpr, etc structure.
				 * That can happen because some parts of the system abuse
				 * CaseTestExpr to cause a read of a value externally supplied
				 * in econtext->caseValue_datum.  We'll take care of that
				 * scenario at runtime.
				 */
				scratch.opcode = EEOP_CASE_TESTVAL;
				scratch.d.casetest.value = state->innermost_caseval;
				scratch.d.casetest.isnull = state->innermost_casenull;

				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_AnnOrderByRef:
			{
				AnnOrderByRef *ref = (AnnOrderByRef *) node;
				AnnOrderByRefExprRuntime *runtime =
					(AnnOrderByRefExprRuntime *) palloc0(sizeof(AnnOrderByRefExprRuntime));

				runtime->scanstate = (state->parent != NULL && IsA(state->parent, AnnIndexScanState))
					? (AnnIndexScanState *) state->parent
					: NULL;
				runtime->fallback = ExecInitExprByRecursion(ref->original_expr, state->parent);
				runtime->quality = (AnnOrderByDistanceQuality) ref->quality;
				scratch.opcode = EEOP_ANN_ORDERBY_REF;
				scratch.d.annorderby.runtime = runtime;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				int			nelems = list_length(arrayexpr->elements);
				ListCell   *lc;
				int			elemoff;

				/*
				 * Evaluate by computing each element, and then forming the
				 * array.  Elements are computed into scratch arrays
				 * associated with the ARRAYEXPR step.
				 */
				scratch.opcode = EEOP_ARRAYEXPR;
				scratch.d.arrayexpr.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.arrayexpr.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);
				scratch.d.arrayexpr.nelems = nelems;

				/* fill remaining fields of step */
				scratch.d.arrayexpr.multidims = arrayexpr->multidims;
				scratch.d.arrayexpr.elemtype = arrayexpr->element_typeid;

				/* do one-time catalog lookup for type info */
				get_typlenbyvalalign(arrayexpr->element_typeid,
									 &scratch.d.arrayexpr.elemlength,
									 &scratch.d.arrayexpr.elembyval,
									 &scratch.d.arrayexpr.elemalign);

				/* prepare to evaluate all arguments */
				elemoff = 0;
				foreach(lc, arrayexpr->elements)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					ExecInitExprRec(e, state,
									&scratch.d.arrayexpr.elemvalues[elemoff],
									&scratch.d.arrayexpr.elemnulls[elemoff], node);
					elemoff++;
				}

				/* and then collect all into an array */
				ExprEvalPushStep(state, &scratch);
				break;			
			}
		case T_RowExpr:
			{
				RowExpr    *rowexpr = (RowExpr *) node;
				int			nelems = list_length(rowexpr->args);
				TupleDesc	tupdesc;
				FormData_pg_attribute* attrs = NULL;
				int			i;
				ListCell   *l;

				/* Build tupdesc to describe result tuples */
				if (rowexpr->row_typeid == RECORDOID)
				{
					/* generic record, use types of given expressions */
					tupdesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
					BlessTupleDesc(tupdesc);
				}
				else
				{
					/* it's been cast to a named type, use that */
					tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
				}

				/*
				 * In the named-type case, the tupdesc could have more columns
				 * than are in the args list, since the type might have had
				 * columns added since the ROW() was parsed.  We want those
				 * extra columns to go to nulls, so we make sure that the
				 * workspace arrays are large enough and then initialize any
				 * extra columns to read as NULLs.
				 */
				Assert(nelems <= tupdesc->natts);
				nelems = Max(nelems, tupdesc->natts);

				/*
				 * Evaluate by first building datums for each field, and then
				 * a final step forming the composite datum.
				 */
				scratch.opcode = EEOP_ROW;
				scratch.d.row.tupdesc = tupdesc;

				/* space for the individual field datums */
				scratch.d.row.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.row.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);
				/* as explained above, make sure any extra columns are null */
				memset(scratch.d.row.elemnulls, true, sizeof(bool) * nelems);

				/* Set up evaluation, skipping any deleted columns */
				if (tupdesc->attrs){
					attrs = tupdesc->attrs;
				}
				i = 0;
				foreach(l, rowexpr->args)
				{
					Expr *e = (Expr *) lfirst(l);

					if (!attrs[i].attisdropped)
					{
						/*
						 * Guard against ALTER COLUMN TYPE on rowtype since
						 * the RowExpr was created.  XXX should we check
						 * typmod too?	Not sure we can be sure it'll be the
						 * same.
						 */
						if (exprType((Node *) e) != attrs[i].atttypid)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("ROW() column has type %s instead of type %s",
										format_type_be(exprType((Node *) e)),
									   format_type_be(attrs[i].atttypid))));
					}
					else
					{
						/*
						 * Ignore original expression and insert a NULL. We
						 * don't really care what type of NULL it is, so
						 * always make an int4 NULL.
						 */
						e = (Expr *) makeNullConst(INT4OID, -1, InvalidOid);
					}

					/* Evaluate column expr into appropriate workspace slot */
					ExecInitExprRec(e, state,
									&scratch.d.row.elemvalues[i],
									&scratch.d.row.elemnulls[i], node);
					i++;
				}

				/* And finally build the row value */
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				int			nopers = list_length(rcexpr->opnos);
				List	   *adjust_jumps = NIL;
				ListCell   *l_left_expr,
						   *l_right_expr,
						   *l_opno,
						   *l_opfamily,
						   *l_inputcollid;
				ListCell   *lc;
				int			off;

				/*
				 * Iterate over each field, prepare comparisons.  To handle
				 * NULL results, prepare jumps to after the expression.  If a
				 * comparison yields a != 0 result, jump to the final step.
				 */
				Assert(list_length(rcexpr->largs) == nopers);
				Assert(list_length(rcexpr->rargs) == nopers);
				Assert(list_length(rcexpr->opfamilies) == nopers);
				Assert(list_length(rcexpr->inputcollids) == nopers);

				off = 0;
				for (off = 0,
					 l_left_expr = list_head(rcexpr->largs),
					 l_right_expr = list_head(rcexpr->rargs),
					 l_opno = list_head(rcexpr->opnos),
					 l_opfamily = list_head(rcexpr->opfamilies),
					 l_inputcollid = list_head(rcexpr->inputcollids);
					 off < nopers;
					 off++,
					 l_left_expr = lnext(l_left_expr),
					 l_right_expr = lnext(l_right_expr),
					 l_opno = lnext(l_opno),
					 l_opfamily = lnext(l_opfamily),
					 l_inputcollid = lnext(l_inputcollid))
				{
					Expr	   *left_expr = (Expr *) lfirst(l_left_expr);
					Expr	   *right_expr = (Expr *) lfirst(l_right_expr);
					Oid			opno = lfirst_oid(l_opno);
					Oid			opfamily = lfirst_oid(l_opfamily);
					Oid			inputcollid = lfirst_oid(l_inputcollid);
					int			strategy;
					Oid			lefttype;
					Oid			righttype;
					Oid			proc;
					FmgrInfo   *finfo;
					FunctionCallInfo fcinfo;

					get_op_opfamily_properties(opno, opfamily, false,
											   &strategy,
											   &lefttype,
											   &righttype);
					proc = get_opfamily_proc(opfamily,
											 lefttype,
											 righttype,
											 BTORDER_PROC);

					/* Set up the primary fmgr lookup information */
					finfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo));
					fcinfo = (FunctionCallInfo)palloc0(sizeof(FunctionCallInfoData));
					fmgr_info(proc, finfo);
					fmgr_info_set_expr((Node *) node, finfo);
					InitFunctionCallInfoData(*fcinfo, finfo, 2,
											 inputcollid, NULL, NULL);

					/*
					 * If we enforced permissions checks on index support
					 * functions, we'd need to make a check here.  But the
					 * index support machinery doesn't do that, and thus
					 * neither does this code.
					 */

					/* evaluate left and right args directly into fcinfo */
					ExecInitExprRec(left_expr, state,
									&fcinfo->arg[0], &fcinfo->argnull[0], node);
					ExecInitExprRec(right_expr, state,
									&fcinfo->arg[1], &fcinfo->argnull[1], node);

					scratch.opcode = EEOP_ROWCOMPARE_STEP;
					scratch.d.rowcompare_step.finfo = finfo;
					scratch.d.rowcompare_step.fcinfo_data = fcinfo;
					scratch.d.rowcompare_step.fn_addr = finfo->fn_addr;
					/* jump targets filled below */
					scratch.d.rowcompare_step.jumpnull = -1;
					scratch.d.rowcompare_step.jumpdone = -1;

					ExprEvalPushStep(state, &scratch);
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
				}

				/*
				 * We could have a zero-column rowtype, in which case the rows
				 * necessarily compare equal.
				 */
				if (nopers == 0)
				{
					scratch.opcode = EEOP_CONST;
					scratch.d.constval.value = Int32GetDatum(0);
					scratch.d.constval.isnull = false;
					scratch.d.constval.con = NULL;

					ExprEvalPushStep(state, &scratch);
				}

				/* Finally, examine the last comparison result */
				scratch.opcode = EEOP_ROWCOMPARE_FINAL;
				scratch.d.rowcompare_final.rctype = rcexpr->rctype;
				ExprEvalPushStep(state, &scratch);

				/* adjust jump targetss */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_ROWCOMPARE_STEP);
					Assert(as->d.rowcompare_step.jumpdone == -1);
					Assert(as->d.rowcompare_step.jumpnull == -1);

					/* jump to comparison evaluation */
					as->d.rowcompare_step.jumpdone = state->steps_len - 1;
					/* jump to the following expression */
					as->d.rowcompare_step.jumpnull = state->steps_len;
				}

				break;
			}
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;
				List	   *adjust_jumps = NIL;
				ListCell   *lc;

				/* We assume there's at least one arg */
				Assert(coalesce->args != NIL);

				/*
				 * Prepare evaluation of all coalesced arguments, after each
				 * one push a step that short-circuits if not null.
				 */
				foreach(lc, coalesce->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					/* evaluate argument, directly into result datum */
					ExecInitExprRec(e, state, resv, resnull, node);

					/* if it's not null, skip to end of COALESCE expr */
					scratch.opcode = EEOP_JUMP_IF_NOT_NULL;
					scratch.d.jump.jumpdone = -1;		/* adjust later */
					ExprEvalPushStep(state, &scratch);

					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
				}

				/*
				 * No need to add a constant NULL return - we only can get to
				 * the end of the expression if a NULL already is being
				 * returned.
				 */

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_JUMP_IF_NOT_NULL);
					Assert(as->d.jump.jumpdone == -1);
					as->d.jump.jumpdone = state->steps_len;
				}

				break;
			}
		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
				int			nelems = list_length(minmaxexpr->args);
				TypeCacheEntry *typentry;
				FmgrInfo   *finfo;
				FunctionCallInfo fcinfo;
				ListCell   *lc;
				int			off;

				/* Look up the btree comparison function for the datatype */
				typentry = lookup_type_cache(minmaxexpr->minmaxtype,
											 TYPECACHE_CMP_PROC);
				if (!OidIsValid(typentry->cmp_proc))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("could not identify a comparison function for type %s",
									format_type_be(minmaxexpr->minmaxtype))));

				/*
				 * If we enforced permissions checks on index support
				 * functions, we'd need to make a check here.  But the index
				 * support machinery doesn't do that, and thus neither does
				 * this code.
				 */

				/* Perform function lookup */
				finfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo));
				fcinfo = (FunctionCallInfo)palloc0(sizeof(FunctionCallInfoData));
				fmgr_info(typentry->cmp_proc, finfo);
				fmgr_info_set_expr((Node *) node, finfo);
				InitFunctionCallInfoData(*fcinfo, finfo, 2,
										 minmaxexpr->inputcollid, NULL, NULL);

				scratch.opcode = EEOP_MINMAX;
				/* allocate space to store arguments */
				scratch.d.minmax.values =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.minmax.nulls =
					(bool *) palloc(sizeof(bool) * nelems);
				scratch.d.minmax.nelems = nelems;

				scratch.d.minmax.op = minmaxexpr->op;
				scratch.d.minmax.finfo = finfo;
				scratch.d.minmax.fcinfo_data = fcinfo;

				/* evaluate expressions into minmax->values/nulls */
				off = 0;
				foreach(lc, minmaxexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					ExecInitExprRec(e, state,
									&scratch.d.minmax.values[off],
									&scratch.d.minmax.nulls[off], node);
					off++;
				}

				/* and push the final comparison */
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;
				int			nnamed = list_length(xexpr->named_args);
				int			nargs = list_length(xexpr->args);
				int			off;
				ListCell   *arg;

				scratch.opcode = EEOP_XMLEXPR;
				scratch.d.xmlexpr.xexpr = xexpr;

				/* allocate space for storing all the arguments */
				if (nnamed)
				{
					scratch.d.xmlexpr.named_argvalue =
						(Datum *) palloc(sizeof(Datum) * nnamed);
					scratch.d.xmlexpr.named_argnull =
						(bool *) palloc(sizeof(bool) * nnamed);
				}
				else
				{
					scratch.d.xmlexpr.named_argvalue = NULL;
					scratch.d.xmlexpr.named_argnull = NULL;
				}

				if (nargs)
				{
					scratch.d.xmlexpr.argvalue =
						(Datum *) palloc(sizeof(Datum) * nargs);
					scratch.d.xmlexpr.argnull =
						(bool *) palloc(sizeof(bool) * nargs);
				}
				else
				{
					scratch.d.xmlexpr.argvalue = NULL;
					scratch.d.xmlexpr.argnull = NULL;
				}

				/* prepare argument execution */
				off = 0;
				foreach(arg, xexpr->named_args)
				{
					Expr *e = (Expr *) lfirst(arg);

					ExecInitExprRec(e, state,
									&scratch.d.xmlexpr.named_argvalue[off],
									&scratch.d.xmlexpr.named_argnull[off], node);
					off++;
				}

				off = 0;
				foreach(arg, xexpr->args)
				{
					Expr *e = (Expr *) lfirst(arg);

					ExecInitExprRec(e, state,
									&scratch.d.xmlexpr.argvalue[off],
									&scratch.d.xmlexpr.argnull[off], node);
					off++;
				}

				/* and evaluate the actual XML expression */
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;

				if (ntest->nulltesttype == IS_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNULL;
				}
				else if (ntest->nulltesttype == IS_NOT_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNOTNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
				}
				else
				{
					elog(ERROR, "unrecognized nulltesttype: %d",
						 (int) ntest->nulltesttype);
				}
				/* initialize cache in case it's a row test */
				scratch.d.nulltest_row.argdesc = NULL;

				/* first evaluate argument into result variable */
				ExecInitExprRec(ntest->arg, state,
								resv, resnull, node);

				/* then push the test of that argument */
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;

				/*
				 * Evaluate argument, directly into result datum.  That's ok,
				 * because resv/resnull is definitely not used anywhere else,
				 * and will get overwritten by the below EEOP_BOOLTEST_IS_*
				 * step.
				 */
				ExecInitExprRec(btest->arg, state, resv, resnull, node);

				switch (btest->booltesttype)
				{
					case IS_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_TRUE;
						break;
					case IS_NOT_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_TRUE;
						break;
					case IS_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_FALSE;
						break;
					case IS_NOT_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_FALSE;
						break;
					case IS_UNKNOWN:
						/* Same as scalar IS NULL test */
						scratch.opcode = EEOP_NULLTEST_ISNULL;
						break;
					case IS_NOT_UNKNOWN:
						/* Same as scalar IS NOT NULL test */
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
						break;
					default:
						elog(ERROR, "unrecognized booltesttype: %d",
							 (int) btest->booltesttype);
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_CoerceToDomain:
			{
				CoerceToDomain *ctest = (CoerceToDomain *) node;

				ExecInitCoerceToDomain(&scratch, ctest, state,
									   resv, resnull, node);
				break;
			}
		case T_CoerceToDomainValue:	
			{
				/*
				 * Read from location identified by innermost_domainval.  Note
				 * that innermost_domainval could be NULL, if we're compiling
				 * a standalone domain check rather than one embedded in a
				 * larger expression.  In that case we must read from
				 * econtext->domainValue_datum.  We'll take care of that
				 * scenario at runtime.
				 */
				scratch.opcode = EEOP_DOMAIN_TESTVAL;
				/* we share instruction union variant with case testval */
				scratch.d.casetest.value = state->innermost_domainval;
				scratch.d.casetest.isnull = state->innermost_domainnull;

				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_CurrentOfExpr:
			{
				scratch.opcode = EEOP_CURRENTOFEXPR;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_Rownum:
			{
				scratch.d.rownum.RownumState = state->parent;
                scratch.d.rownum.typeCompat = (u_sess->utils_cxt.behavior_compat_flags & OPT_ROWNUM_TYPE_COMPAT) > 0;
				scratch.opcode = EEOP_ROWNUM;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_PrefixKey:
			{
				PrefixKey *pkey = (PrefixKey*)node;
				scratch.d.prefix_key.pkey = pkey;
				Oid argtype = exprType((Node*)pkey->arg);
				if (argtype == BYTEAOID || argtype == RAWOID || argtype == BLOBOID) {
                	scratch.opcode = EEOP_PREFIX_BTYEA;
					scratch.d.prefix_key.encoding = PG_INVALID_ENCODING;
            	} else {
					Oid collation = exprCollation((Node*)pkey->arg);
                	scratch.d.prefix_key.encoding = get_valid_charset_by_collation(collation);
                	scratch.opcode = EEOP_PREFIX_TEXT;
            	}
				ExecInitExprRec(pkey->arg, state, resv, resnull, node);
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_GroupingId:
			{
				scratch.d.grouping_id.GroupingIdState = (AggState*)state->parent;
				scratch.opcode = EEOP_GROUPING_ID;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_HashFilter:
			{
				HashFilter* htest = (HashFilter*)node;
				int nargs = list_length(htest->arg);
				List* outlist = NIL;
            	ListCell* l = NULL;
            	int idx = 0;

				scratch.opcode = EEOP_HASH_FILTER;

				if (nargs) {
					scratch.d.hash_filter.argvalue =
						(Datum *) palloc(sizeof(Datum) * nargs);
					scratch.d.hash_filter.argnull =
						(bool *) palloc(sizeof(bool) * nargs);
				}
				else {
					scratch.d.hash_filter.argvalue = NULL;
					scratch.d.hash_filter.argnull = NULL;
				}
				
				foreach (l, htest->arg) {
					Expr *e = (Expr *) lfirst(l);

					ExecInitExprRec(e, state,
									&scratch.d.hash_filter.argvalue[idx],
									&scratch.d.hash_filter.argnull[idx], node);
					idx++;
            	}
				
				scratch.d.hash_filter.arg = outlist;
            	scratch.d.hash_filter.bucketMap = get_bucketmap_by_execnode(state->parent->plan->exec_nodes,
                                                          state->parent->state->es_plannedstmt,
                                                          &scratch.d.hash_filter.bucketCnt);
            	scratch.d.hash_filter.nodelist = (uint2*)palloc(list_length(htest->nodeList) * sizeof(uint2));
				idx = 0;
            	foreach (l, htest->nodeList)
                	scratch.d.hash_filter.nodelist[idx++] = lfirst_int(l);

				scratch.d.hash_filter.typeOids = htest->typeOids;
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_NanTest:
			{
				NanTest *ntest = (NanTest *) node;

				if (ntest ->nantesttype == IS_NAN) 
				{
					scratch.opcode = EEOP_NANTEST_ISNAN;
				}
				else if (ntest->nantesttype == IS_NOT_NAN)
				{
					scratch.opcode = EEOP_NANTEST_ISNOTNAN;
				}
				else
				{
					elog(ERROR, "unrecognized nantesttype: %d",
							(int) ntest->nantesttype);
				}

				scratch.d.decspecexpr.value = (Datum *) palloc(sizeof(Datum));
				scratch.d.decspecexpr.isnull = (bool *) palloc(sizeof(bool));
				/* first evaluate argument into result variable */
				ExecInitExprRec(ntest->arg, state, 
							    scratch.d.decspecexpr.value, scratch.d.decspecexpr.isnull, 
								node);
				scratch.d.decspecexpr.expr = node;

				/* then push the test of that argument */
				ExprEvalPushStep(state, &scratch);
				break;
			}
		case T_InfiniteTest:
			{
				InfiniteTest *itest = (InfiniteTest *) node;

				if (itest->infinitetesttype == IS_INFINITE)
				{
					scratch.opcode = EEOP_INFINITETEST_ISINFINITE;
				}
				else if (itest->infinitetesttype == IS_NOT_INFINITE)
				{
					scratch.opcode = EEOP_INFINITETEST_ISNOTINFINITE;
				}
				else 
				{
					elog(ERROR, "unrecognized infinitetesttype: %d",
							(int) itest->infinitetesttype);
				}

				scratch.d.decspecexpr.value = (Datum *) palloc(sizeof(Datum));
				scratch.d.decspecexpr.isnull = (bool *) palloc(sizeof(bool));
				/* first evaluate argument into result variable */
				ExecInitExprRec(itest->arg, state,
								scratch.d.decspecexpr.value, scratch.d.decspecexpr.isnull, 
								node);
				scratch.d.decspecexpr.expr = node;
								
				/* then push the test of that argument */
				ExprEvalPushStep(state, &scratch);
				break;
			}
		default:
			elog(ERROR, "unrecognized node type: %d, line=%d, func:%s",
				 (int) nodeTag(node), __LINE__, __func__);	
			break;
	}
}
```

<a id="source-105"></a>

#### ExecInterpExpr()

来源：`src/gausskernel/runtime/executor/execExprInterp.cpp:565-2314`；范围：**完整函数**。

内容 SHA256：`80ccee6c9a998e7a47c691f068f13da24c288b70e132234c1eea6296a481572c`。

完整展平解释执行函数；本地此前两档 fusion 设置实际都命中传统取值入口，展平分支不能声称已覆盖。

```cpp
static Datum
ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull, ExprDoneCond* isDone)
{
	ExprEvalStep *op;
	TupleTableSlot *resultslot;
	TupleTableSlot *innerslot;
	TupleTableSlot *outerslot;
	TupleTableSlot *scanslot;

	/*
	 * This array has to be in the same order as enum ExprEvalOp.
	 */
#if defined(EEO_USE_COMPUTED_GOTO)
	static const void *const dispatch_table[] = {
		&&CASE_EEOP_DONE,
		&&CASE_EEOP_INNER_FETCHSOME,
		&&CASE_EEOP_OUTER_FETCHSOME,
		&&CASE_EEOP_SCAN_FETCHSOME,
		&&CASE_EEOP_INNER_VAR,
		&&CASE_EEOP_OUTER_VAR,
		&&CASE_EEOP_SCAN_VAR,
		&&CASE_EEOP_INNER_SYSVAR,
		&&CASE_EEOP_OUTER_SYSVAR,
		&&CASE_EEOP_SCAN_SYSVAR,
		&&CASE_EEOP_WHOLEROW,
		&&CASE_EEOP_ASSIGN_INNER_VAR,
		&&CASE_EEOP_ASSIGN_OUTER_VAR,
		&&CASE_EEOP_ASSIGN_SCAN_VAR,
		&&CASE_EEOP_ASSIGN_TMP,
		&&CASE_EEOP_ASSIGN_TMP_MAKE_RO,
		&&CASE_EEOP_CONST,
		&&CASE_EEOP_FUNCEXPR,
        &&CASE_EEOP_FUNCEXPR_STRICT,
        &&CASE_EEOP_FUNCEXPR_FUSAGE,
        &&CASE_EEOP_FUNCEXPR_STRICT_FUSAGE,
        &&CASE_EEOP_FUNCEXPR_MAKE_FUNCTION_RESULT,
		&&CASE_EEOP_BOOL_AND_STEP_FIRST,
		&&CASE_EEOP_BOOL_AND_STEP,
		&&CASE_EEOP_BOOL_AND_STEP_LAST,
		&&CASE_EEOP_BOOL_OR_STEP_FIRST,
		&&CASE_EEOP_BOOL_OR_STEP,
		&&CASE_EEOP_BOOL_OR_STEP_LAST,
		&&CASE_EEOP_BOOL_NOT_STEP,
		&&CASE_EEOP_QUAL,
		&&CASE_EEOP_JUMP,
		&&CASE_EEOP_JUMP_IF_NULL,
		&&CASE_EEOP_JUMP_IF_NOT_NULL,
		&&CASE_EEOP_JUMP_IF_NOT_TRUE,
		&&CASE_EEOP_NULLTEST_ISNULL,
		&&CASE_EEOP_NULLTEST_ISNOTNULL,
		&&CASE_EEOP_NULLTEST_ROWISNULL,
		&&CASE_EEOP_NULLTEST_ROWISNOTNULL,
		&&CASE_EEOP_BOOLTEST_IS_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_FALSE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_FALSE,
		&&CASE_EEOP_PARAM_EXEC,
		&&CASE_EEOP_PARAM_EXTERN,
		&&CASE_EEOP_CASE_TESTVAL,
		&&CASE_EEOP_ANN_ORDERBY_REF,
		&&CASE_EEOP_MAKE_READONLY,
		&&CASE_EEOP_IOCOERCE,
		&&CASE_EEOP_DISTINCT,
		&&CASE_EEOP_NULLIF,
		&&CASE_EEOP_CURRENTOFEXPR,
		&&CASE_EEOP_ARRAYEXPR,
		&&CASE_EEOP_ARRAYCOERCE,
		&&CASE_EEOP_ROW,
		&&CASE_EEOP_ROWCOMPARE_STEP,
		&&CASE_EEOP_ROWCOMPARE_FINAL,
		&&CASE_EEOP_MINMAX,
		&&CASE_EEOP_FIELDSELECT,
		&&CASE_EEOP_FIELDSTORE_DEFORM,
		&&CASE_EEOP_FIELDSTORE_FORM,
		&&CASE_EEOP_ARRAYREF_SUBSCRIPT,
		&&CASE_EEOP_ARRAYREF_OLD,
		&&CASE_EEOP_ARRAYREF_ASSIGN,
		&&CASE_EEOP_ARRAYREF_FETCH,
		&&CASE_EEOP_DOMAIN_TESTVAL,
		&&CASE_EEOP_DOMAIN_NOTNULL,
		&&CASE_EEOP_DOMAIN_CHECK,
		&&CASE_EEOP_CONVERT_ROWTYPE,
		&&CASE_EEOP_SCALARARRAYOP,
		&&CASE_EEOP_HASHED_SCALARARRAYOP,
		&&CASE_EEOP_XMLEXPR,
		&&CASE_EEOP_AGGREF,
		&&CASE_EEOP_GROUPING_FUNC,
		&&CASE_EEOP_WINDOW_FUNC,
		&&CASE_EEOP_SUBPLAN,
		&&CASE_EEOP_ALTERNATIVE_SUBPLAN,
		&&CASE_EEOP_ROWNUM,
		&&CASE_EEOP_GROUPING_ID,
		&&CASE_EEOP_HASH_FILTER,
		&&CASE_EEOP_USERVAR_OR_SETVARIABLE,
		&&CASE_EEOP_USERSET_ELEM,
        &&CASE_EEOP_PREFIX_BTYEA,
        &&CASE_EEOP_PREFIX_TEXT,
        &&CASE_EEOP_AGG_STRICT_DESERIALIZE,
        &&CASE_EEOP_AGG_DESERIALIZE,
        &&CASE_EEOP_AGG_STRICT_INPUT_CHECK,
        &&CASE_EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYVAL,
        &&CASE_EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYVAL,
        &&CASE_EEOP_AGG_PLAIN_TRANS_BYVAL,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_BYVAL,
        &&CASE_EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYREF,
        &&CASE_EEOP_AGG_PLAIN_TRANS_STRICT_BYREF,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYREF,
        &&CASE_EEOP_AGG_PLAIN_TRANS_BYREF,
        &&CASE_EEOP_AGG_COLLECT_PLAIN_TRANS_BYREF,
        &&CASE_EEOP_AGG_ORDERED_TRANS_DATUM,
        &&CASE_EEOP_AGG_ORDERED_TRANS_TUPLE,
		&&CASE_EEOP_NANTEST_ISNAN,
		&&CASE_EEOP_NANTEST_ISNOTNAN,
		&&CASE_EEOP_INFINITETEST_ISINFINITE,
		&&CASE_EEOP_INFINITETEST_ISNOTINFINITE,
		&&CASE_EEOP_LAST
	};

	StaticAssertStmt(EEOP_LAST + 1 == lengthof(dispatch_table),
					 "dispatch_table out of whack with ExprEvalOp");

	if (unlikely(state == NULL))
		return PointerGetDatum(dispatch_table);
#else
	Assert(state != NULL);
#endif							/* EEO_USE_COMPUTED_GOTO */

	/* setup state */
	op = state->steps;
	resultslot = state->resultslot;
	innerslot = econtext->ecxt_innertuple;
	outerslot = econtext->ecxt_outertuple;
	scanslot = econtext->ecxt_scantuple;

	if (isDone != NULL)
        *isDone = ExprSingleResult;

#if defined(EEO_USE_COMPUTED_GOTO)
	EEO_DISPATCH();
#endif

	EEO_SWITCH()
	{
		EEO_CASE(EEOP_DONE)
		{
			goto out;
		}

		EEO_CASE(EEOP_INNER_FETCHSOME)
		{
			tableam_tslot_getsomeattrs(innerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_FETCHSOME)
		{
			tableam_tslot_getsomeattrs(outerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_FETCHSOME)
		{
			tableam_tslot_getsomeattrs(scanslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_VAR)
		{
			int			attnum = op->d.var.attnum;

			/*
			 * Since we already extracted all referenced columns from the
			 * tuple with a FETCHSOME step, we can just grab the value
			 * directly out of the slot's decomposed-data arrays.  But let's
			 * have an Assert to check that that did happen.
			 */
			*op->resvalue = innerslot->tts_values[attnum];
			*op->resnull = innerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_VAR)
		{
			int			attnum = op->d.var.attnum;

			/* See EEOP_INNER_VAR comments */
			*op->resvalue = outerslot->tts_values[attnum];
			*op->resnull = outerslot->tts_isnull[attnum];

			EEO_NEXT();
		}
		
		EEO_CASE(EEOP_SCAN_VAR)
		{
			int			attnum = op->d.var.attnum;

			/* See EEOP_INNER_VAR comments */
			*op->resvalue = scanslot->tts_values[attnum];
			*op->resnull = scanslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_SYSVAR)
		{
			int			attnum = op->d.var.attnum;
			Datum		d;

			/* these asserts must match defenses in slot_getattr */
			Assert(innerslot->tts_tuple != NULL);
			Assert(innerslot->tts_tuple != &(innerslot->tts_minhdr));

#ifdef USE_SPQ
            if (attnum == RootSelfItemPointerAttributeNumber) {
                Assert(innerslot->tts_tuple);
                *op->resnull = false;
                d = spq_get_root_ctid((HeapTuple)innerslot->tts_tuple, innerslot->tts_buffer, econtext);
                *op->resvalue = d;
            } else
#endif
            {
			/* heap_getsysattr has sufficient defenses against bad attnums */
			d = tableam_tslot_getattr(innerslot, attnum, op->resnull);
			*op->resvalue = d;
            }

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_SYSVAR)
		{
			int			attnum = op->d.var.attnum;
			Datum		d;

			/* these asserts must match defenses in slot_getattr */
			Assert(outerslot->tts_tuple != NULL);
			Assert(outerslot->tts_tuple != &(outerslot->tts_minhdr));
#ifdef USE_SPQ
            if (attnum == RootSelfItemPointerAttributeNumber) {
                Assert(outerslot->tts_tuple);
                *op->resnull = false;
                d = spq_get_root_ctid((HeapTuple)outerslot->tts_tuple, outerslot->tts_buffer, econtext);
                *op->resvalue = d;
            } else
#endif
            {
            /* heap_getsysattr has sufficient defenses against bad attnums */
            d = tableam_tslot_getattr(outerslot, attnum, op->resnull);
            *op->resvalue = d;
            }

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_SYSVAR)
		{
			int			attnum = op->d.var.attnum;
			Datum		d;

			/* these asserts must match defenses in slot_getattr */
			Assert(scanslot->tts_tuple != NULL);
			Assert(scanslot->tts_tuple != &(scanslot->tts_minhdr));
#ifdef USE_SPQ
            if (attnum == RootSelfItemPointerAttributeNumber) {
                Assert(scanslot->tts_tuple);
                *op->resnull = false;
                d = spq_get_root_ctid((HeapTuple)scanslot->tts_tuple, scanslot->tts_buffer, econtext);
                *op->resvalue = d;
            } else
#endif
            {
            /* heap_getsysattr has sufficient defenses against bad attnums */
            d = tableam_tslot_getattr(scanslot, attnum, op->resnull);
            *op->resvalue = d;
            }

			EEO_NEXT();

		}

		EEO_CASE(EEOP_WHOLEROW)
		{
			/* too complex for an inline implementation */
			ExecEvalWholeRowVar(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_INNER_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = innerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = innerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_OUTER_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = outerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = outerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_SCAN_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = scanslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = scanslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP)
		{
			int			resultnum = op->d.assign_tmp.resultnum;

			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = state->resvalue;
			resultslot->tts_isnull[resultnum] = state->resnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP_MAKE_RO)
		{
			int			resultnum = op->d.assign_tmp.resultnum;

			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_isnull[resultnum] = state->resnull;
			if (!resultslot->tts_isnull[resultnum])
				resultslot->tts_values[resultnum] =
					MakeExpandedObjectReadOnlyInternal(state->resvalue);
			else
				resultslot->tts_values[resultnum] = state->resvalue;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONST)
		{
			*op->resnull = op->d.constval.isnull;
			*op->resvalue = op->d.constval.value;

			/* if a const cursor, copy cursor option data to econtext */
    		if ((econtext->is_cursor || op->d.constval.is_cursor) && op->d.constval.con && 
				op->d.constval.con->consttype == REFCURSOROID) {
    		    CopyCursorInfoData(&econtext->cursor_data, &op->d.constval.con->cursor_data);
    		    econtext->dno = op->d.constval.con->cursor_data.cur_dno;
    		}

			EEO_NEXT();
		}

		/*
		 * Function-call implementations. Arguments have previously been
		 * evaluated directly into fcinfo->args.
		 *
		 * As both STRICT checks and function-usage are noticeable performance
		 * wise, and function calls are a very hot-path (they also back
		 * operators!), it's worth having so many separate opcodes.
		 *
		 * Note: the reason for using a temporary variable "d", here and in
		 * other places, is that some compilers think "*op->resvalue = f();"
		 * requires them to evaluate op->resvalue into a register before
		 * calling f(), just in case f() is able to modify op->resvalue
		 * somehow.  The extra line of code can save a useless register spill
		 * and reload across the function call.
		 */
        EEO_CASE(EEOP_FUNCEXPR)
        {
            // ExecMakeFunctionResultNoSets(state, op, econtext);
            FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
            Datum d;

            int func_encoding = PG_INVALID_ENCODING;
            int db_encoding = PG_INVALID_ENCODING;

            if (DB_IS_CMPT_BD) {
                func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
                db_encoding = GetDatabaseEncoding();
            }

            if (econtext) {
                fcinfo->can_ignore = econtext->can_ignore;
            }

            fcinfo->isnull = false;
            if (func_encoding != db_encoding) {
                DB_ENCODING_SWITCH_TO(func_encoding);
                d = op->d.func.fn_addr(fcinfo);
                DB_ENCODING_SWITCH_BACK(db_encoding);
            } else {
                d = op->d.func.fn_addr(fcinfo);
            }
            *op->resvalue = d;
            *op->resnull = fcinfo->isnull;

            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_STRICT)
        {
            FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
            int nargs = op->d.func.nargs;
            Datum d;

            int func_encoding = PG_INVALID_ENCODING;
            int db_encoding = PG_INVALID_ENCODING;

            if (DB_IS_CMPT_BD) {
                func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
                db_encoding = GetDatabaseEncoding();
            }

            if (econtext) {
                fcinfo->can_ignore = econtext->can_ignore;
            }

            /* strict function, so check for NULL args */
            for (int argno = 0; argno < nargs; argno++) {
                if (fcinfo->argnull[argno]) {
                    *op->resnull = true;
                    goto strictfail;
                }
            }
            fcinfo->isnull = false;
            if (func_encoding != db_encoding) {
                DB_ENCODING_SWITCH_TO(func_encoding);
                d = op->d.func.fn_addr(fcinfo);
                DB_ENCODING_SWITCH_BACK(db_encoding);
            } else {
                d = op->d.func.fn_addr(fcinfo);
            }
            *op->resvalue = d;
            *op->resnull = fcinfo->isnull;

        strictfail:
            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_FUSAGE)
        {
            /* not common enough to inline */
            ExecEvalFuncExprFusage(op, econtext);

            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_STRICT_FUSAGE)
        {
            /* not common enough to inline */
            ExecEvalFuncExprStrictFusage(op, econtext);

            EEO_NEXT();
        }

        EEO_CASE(EEOP_FUNCEXPR_MAKE_FUNCTION_RESULT)
        {
            ExecMakeFunctionResultNoSets(state, op, econtext);
            EEO_NEXT();
        }

		/*
		 * If any of its clauses is FALSE, an AND's result is FALSE regardless
		 * of the states of the rest of the clauses, so we can stop evaluating
		 * and return FALSE immediately.  If none are FALSE and one or more is
		 * NULL, we return NULL; otherwise we return TRUE.  This makes sense
		 * when you interpret NULL as "don't know": perhaps one of the "don't
		 * knows" would have been FALSE if we'd known its value.  Only when
		 * all the inputs are known to be TRUE can we state confidently that
		 * the AND's result is TRUE.
		 */
		EEO_CASE(EEOP_BOOL_AND_STEP_FIRST)
		{
			*op->d.boolexpr.anynull = false;

			/*
			 * EEOP_BOOL_AND_STEP_FIRST resets anynull, otherwise it's the
			 * same as EEOP_BOOL_AND_STEP - so fall through to that.
			 */

			/* FALL THROUGH */
		}

		EEO_CASE(EEOP_BOOL_AND_STEP)
		{
			if (*op->resnull)
			{
				*op->d.boolexpr.anynull = true;
			}
			else if (!DatumGetBool(*op->resvalue))
			{
				/* result is already set to FALSE, need not change it */
				/* bail out early */
				EEO_JUMP(op->d.boolexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_AND_STEP_LAST)
		{
			if (*op->resnull)
			{
				/* result is already set to NULL, need not change it */
			}
			else if (!DatumGetBool(*op->resvalue))
			{
				/* result is already set to FALSE, need not change it */

				/*
				 * No point jumping early to jumpdone - would be same target
				 * (as this is the last argument to the AND expression),
				 * except more expensive.
				 */
			}
			else if (*op->d.boolexpr.anynull)
			{
				*op->resvalue = (Datum) 0;
				*op->resnull = true;
			}
			else
			{
				/* result is already set to TRUE, need not change it */
			}

			EEO_NEXT();
		}

		/*
		 * If any of its clauses is TRUE, an OR's result is TRUE regardless of
		 * the states of the rest of the clauses, so we can stop evaluating
		 * and return TRUE immediately.  If none are TRUE and one or more is
		 * NULL, we return NULL; otherwise we return FALSE.  This makes sense
		 * when you interpret NULL as "don't know": perhaps one of the "don't
		 * knows" would have been TRUE if we'd known its value.  Only when all
		 * the inputs are known to be FALSE can we state confidently that the
		 * OR's result is FALSE.
		 */
		EEO_CASE(EEOP_BOOL_OR_STEP_FIRST)
		{
			*op->d.boolexpr.anynull = false;

			/*
			 * EEOP_BOOL_OR_STEP_FIRST resets anynull, otherwise it's the same
			 * as EEOP_BOOL_OR_STEP - so fall through to that.
			 */

			/* FALL THROUGH */
		}

		EEO_CASE(EEOP_BOOL_OR_STEP)
		{
			if (*op->resnull)
			{
				*op->d.boolexpr.anynull = true;
			}
			else if (DatumGetBool(*op->resvalue))
			{
				/* result is already set to TRUE, need not change it */
				/* bail out early */
				EEO_JUMP(op->d.boolexpr.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_OR_STEP_LAST)
		{
			if (*op->resnull)
			{
				/* result is already set to NULL, need not change it */
			}
			else if (DatumGetBool(*op->resvalue))
			{
				/* result is already set to TRUE, need not change it */

				/*
				 * No point jumping to jumpdone - would be same target (as
				 * this is the last argument to the AND expression), except
				 * more expensive.
				 */
			}
			else if (*op->d.boolexpr.anynull)
			{
				*op->resvalue = (Datum) 0;
				*op->resnull = true;
			}
			else
			{
				/* result is already set to FALSE, need not change it */
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_NOT_STEP)
		{
			/*
			 * Evaluation of 'not' is simple... if expr is false, then return
			 * 'true' and vice versa.  It's safe to do this even on a
			 * nominally null value, so we ignore resnull; that means that
			 * NULL in produces NULL out, which is what we want.
			 */
			*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));

			EEO_NEXT();
		}

		EEO_CASE(EEOP_QUAL)
		{
			/* simplified version of BOOL_AND_STEP for use by ExecQual() */

			/* If argument (also result) is false or null ... */
			if (*op->resnull ||
				!DatumGetBool(*op->resvalue))
			{
				/* ... bail out early, returning FALSE */
				*op->resnull = false;
				*op->resvalue = BoolGetDatum(false);
				EEO_JUMP(op->d.qualexpr.jumpdone);
			}

			/*
			 * Otherwise, leave the TRUE value in place, in case this is the
			 * last qual.  Then, TRUE is the correct answer.
			 */

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP)
		{
			/* Unconditionally jump to target step */
			EEO_JUMP(op->d.jump.jumpdone);
		}

		EEO_CASE(EEOP_JUMP_IF_NULL)
		{
			/* Transfer control if current result is null */
			if (*op->resnull)
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP_IF_NOT_NULL)
		{
			/* Transfer control if current result is non-null */
			if (!*op->resnull)
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP_IF_NOT_TRUE)
		{
			/* Transfer control if current result is null or false */
			if (*op->resnull || !DatumGetBool(*op->resvalue))
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNULL)
		{
			*op->resvalue = BoolGetDatum(*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNOTNULL)
		{
			*op->resvalue = BoolGetDatum(!*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNULL)
		{
			/* out of line implementation: too large */
			ExecEvalRowNull(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNOTNULL)
		{
			/* out of line implementation: too large */
			ExecEvalRowNotNull(state, op, econtext);

			EEO_NEXT();
		}

		/* BooleanTest implementations for all booltesttypes */

		EEO_CASE(EEOP_BOOLTEST_IS_TRUE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(false);
			else
				*op->resvalue = *op->resvalue;
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_TRUE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(true);
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_FALSE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(false);
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_FALSE)
		{
			if (*op->resnull)
				*op->resvalue = BoolGetDatum(true);
			else
				*op->resvalue = *op->resvalue;
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXEC)
		{
			/* out of line implementation: too large */
			ExecEvalParamExec(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXTERN)
		{
			/* out of line implementation: too large */
			ExecEvalParamExtern(state, op, econtext);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_CASE_TESTVAL)
		{
			/*
			 * Normally upper parts of the expression tree have setup the
			 * values to be returned here, but some parts of the system
			 * currently misuse {caseValue,domainValue}_{datum,isNull} to set
			 * run-time data.  So if no values have been set-up, use
			 * ExprContext's.  This isn't pretty, but also not *that* ugly,
			 * and this is unlikely to be performance sensitive enough to
			 * worry about an extra branch.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->caseValue_datum;
				*op->resnull = econtext->caseValue_isNull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ANN_ORDERBY_REF)
		{
			AnnOrderByRefExprRuntime *runtime = op->d.annorderby.runtime;
			AnnIndexScanState *scanstate = runtime->scanstate;

			if (scanstate == NULL)
				ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("ANN ORDER BY reference is not owned by an AnnIndexScan")));

			if (scanstate->iss_AnnOrderByExpectedQuality != ANN_ORDERBY_DISTANCE_UNAVAILABLE) {
				if (!scanstate->iss_AnnOrderByValid ||
					scanstate->iss_AnnOrderByQuality != runtime->quality ||
					scanstate->iss_AnnOrderByExpectedQuality != runtime->quality)
					ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("ANN ORDER BY distance publication does not match the planned reference")));
				if (scanstate->iss_AnnOrderByHeapRecheck) {
					if (runtime->quality != ANN_ORDERBY_DISTANCE_EXACT_L2)
						ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("only exact L2 supports an ANN heap distance recheck")));
					scanstate->iss_AnnOrderByValue = ExecEvalExpr(runtime->fallback, econtext,
						&scanstate->iss_AnnOrderByIsNull, NULL);
					scanstate->iss_AnnOrderByHeapRecheck = false;
					scanstate->iss_AnnOrderByFallbacks++;
				}
				*op->resvalue = scanstate->iss_AnnOrderByValue;
				*op->resnull = scanstate->iss_AnnOrderByIsNull;
				scanstate->iss_AnnOrderByReads++;
			} else {
				scanstate->iss_AnnOrderByFallbacks++;
				*op->resvalue = ExecEvalExpr(runtime->fallback, econtext, op->resnull, NULL);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_TESTVAL)
		{
			/*
			 * See EEOP_CASE_TESTVAL comment.
			 */
			if (op->d.casetest.value)
			{
				*op->resvalue = *op->d.casetest.value;
				*op->resnull = *op->d.casetest.isnull;
			}
			else
			{
				*op->resvalue = econtext->domainValue_datum;
				*op->resnull = econtext->domainValue_isNull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_MAKE_READONLY)
		{
			/*
			 * Force a varlena value that might be read multiple times to R/O
			 */
			if (!*op->d.make_readonly.isnull)
				*op->resvalue =
					MakeExpandedObjectReadOnlyInternal(*op->d.make_readonly.value);
			*op->resnull = *op->d.make_readonly.isnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_IOCOERCE)
		{
			/*
			 * Evaluate a CoerceViaIO node.  This can be quite a hot path, so
			 * inline as much work as possible.  The source value is in our
			 * result variable.
			 */
			char	   *str;

			/* call output function (similar to OutputFunctionCall) */
			if (*op->resnull)
			{
				/* output functions are not called on nulls */
				str = NULL;
			}
			else
			{
				FunctionCallInfo fcinfo_out;

				fcinfo_out = op->d.iocoerce.fcinfo_data_out;
				fcinfo_out->arg[0] = *op->resvalue;
				fcinfo_out->argnull[0] = false;

				fcinfo_out->isnull = false;
				str = DatumGetCString(FunctionCallInvoke(fcinfo_out));

				/* OutputFunctionCall assumes result isn't null */
				Assert(!fcinfo_out->isnull);
			}

			/* call input function (similar to InputFunctionCall) */
			if (!op->d.iocoerce.finfo_in->fn_strict || str != NULL)
			{
				FunctionCallInfo fcinfo_in;

				fcinfo_in = op->d.iocoerce.fcinfo_data_in;
				fcinfo_in->arg[0] = PointerGetDatum(str);
				fcinfo_in->argnull[0] = *op->resnull;
				/* second and third arguments are already set up */

				fcinfo_in->isnull = false;
				*op->resvalue = FunctionCallInvoke(fcinfo_in);

				/* Should get null result if and only if str is NULL */
				if (str == NULL)
				{
					Assert(*op->resnull);
					Assert(fcinfo_in->isnull);
				}
				else
				{
					Assert(!*op->resnull);
					Assert(!fcinfo_in->isnull);
				}
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DISTINCT)
		{
			/*
			 * IS DISTINCT FROM must evaluate arguments (already done into
			 * fcinfo->args) to determine whether they are NULL; if either is
			 * NULL then the result is determined.  If neither is NULL, then
			 * proceed to evaluate the comparison function, which is just the
			 * type's standard equality operator.  We need not care whether
			 * that function is strict.  Because the handling of nulls is
			 * different, we can't just reuse EEOP_FUNCEXPR.
			 */
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* check function arguments for NULLness */
			if (fcinfo->argnull[0] && fcinfo->argnull[1])
			{
				/* Both NULL? Then is not distinct... */
				*op->resvalue = BoolGetDatum(false);
				*op->resnull = false;
			}
			else if (fcinfo->argnull[0] || fcinfo->argnull[1])
			{
				/* Only one is NULL? Then is distinct... */
				*op->resvalue = BoolGetDatum(true);
				*op->resnull = false;
			}
			else
			{
				/* Neither null, so apply the equality function */
				Datum		eqresult;

				fcinfo->isnull = false;
				eqresult = op->d.func.fn_addr(fcinfo);
				/* Must invert result of "="; safe to do even if null */
				*op->resvalue = BoolGetDatum(!DatumGetBool(eqresult));
				*op->resnull = fcinfo->isnull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLIF)
		{
			/*
			 * The arguments are already evaluated into fcinfo->args.
			 */
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* if either argument is NULL they can't be equal */
			if (!fcinfo->argnull[0] && !fcinfo->argnull[1])
			{
				Datum		result;

				fcinfo->isnull = false;
				result = op->d.func.fn_addr(fcinfo);

				/* if the arguments are equal return null */
				if (!fcinfo->isnull && DatumGetBool(result))
				{
					*op->resvalue = (Datum) 0;
					*op->resnull = true;

					EEO_NEXT();
				}
			}

			/* Arguments aren't equal, so return the first one */
			*op->resvalue = fcinfo->arg[0];
			*op->resnull = fcinfo->argnull[0];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CURRENTOFEXPR)
		{
			/* error invocation uses space, and shouldn't ever occur */
			ExecEvalCurrentOfExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYCOERCE)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayCoerce(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROW)
		{
			/* too complex for an inline implementation */
			ExecEvalRow(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROWCOMPARE_STEP)
		{
			FunctionCallInfo fcinfo = op->d.rowcompare_step.fcinfo_data;

			/* force NULL result if strict fn and NULL input */
			if (op->d.rowcompare_step.finfo->fn_strict &&
				(fcinfo->argnull[0] || fcinfo->argnull[1]))
			{
				*op->resnull = true;
				EEO_JUMP(op->d.rowcompare_step.jumpnull);
			}

			/* Apply comparison function */
			fcinfo->isnull = false;
			*op->resvalue = (op->d.rowcompare_step.fn_addr) (fcinfo);

			/* force NULL result if NULL function result */
			if (fcinfo->isnull)
			{
				*op->resnull = true;
				EEO_JUMP(op->d.rowcompare_step.jumpnull);
			}
			*op->resnull = false;

			/* If unequal, no need to compare remaining columns */
			if (DatumGetInt32(*op->resvalue) != 0)
			{
				EEO_JUMP(op->d.rowcompare_step.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ROWCOMPARE_FINAL)
		{
			int32		cmpresult = DatumGetInt32(*op->resvalue);
			RowCompareType rctype = op->d.rowcompare_final.rctype;

			*op->resnull = false;
			switch (rctype)
			{
					/* EQ and NE cases aren't allowed here */
				case ROWCOMPARE_LT:
					*op->resvalue = BoolGetDatum(cmpresult < 0);
					break;
				case ROWCOMPARE_LE:
					*op->resvalue = BoolGetDatum(cmpresult <= 0);
					break;
				case ROWCOMPARE_GE:
					*op->resvalue = BoolGetDatum(cmpresult >= 0);
					break;
				case ROWCOMPARE_GT:
					*op->resvalue = BoolGetDatum(cmpresult > 0);
					break;
				default:
					Assert(false);
					break;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_MINMAX)
		{
			/* too complex for an inline implementation */
			ExecEvalMinMax(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSELECT)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldSelect(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSTORE_DEFORM)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldStoreDeForm(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FIELDSTORE_FORM)
		{
			/* too complex for an inline implementation */
			ExecEvalFieldStoreForm(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYREF_SUBSCRIPT)
		{
			/* Process an array subscript */

			/* too complex for an inline implementation */
			if (ExecEvalArrayRefSubscript(state, op, econtext))
			{
				EEO_NEXT();
			}
			else
			{
				/* Subscript is null, short-circuit ArrayRef to NULL */
				EEO_JUMP(op->d.arrayref_subscript.jumpdone);
			}
		}

		EEO_CASE(EEOP_ARRAYREF_OLD)
		{
			/*
			 * Fetch the old value in an arrayref assignment, in case it's
			 * referenced (via a CaseTestExpr) inside the assignment
			 * expression.
			 */

			/* too complex for an inline implementation */
			ExecEvalArrayRefOld(state, op);

			EEO_NEXT();
		}

		/*
		 * Perform ArrayRef assignment
		 */
		EEO_CASE(EEOP_ARRAYREF_ASSIGN)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefAssign(state, op);

			EEO_NEXT();
		}

		/*
		 * Fetch subset of an array.
		 */
		EEO_CASE(EEOP_ARRAYREF_FETCH)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayRefFetch(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONVERT_ROWTYPE)
		{
			/* too complex for an inline implementation */
			ExecEvalConvertRowtype(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCALARARRAYOP)
		{
			/* too complex for an inline implementation */
			ExecEvalScalarArrayOp(state, op);

			EEO_NEXT();
		}

        EEO_CASE(EEOP_HASHED_SCALARARRAYOP)
        {
            /* too complex for an inline implementation */
            ExecEvalHashedScalarArrayOp(state, op, econtext);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_DOMAIN_NOTNULL)
		{
			/* too complex for an inline implementation */
			ExecEvalConstraintNotNull(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_DOMAIN_CHECK)
		{
			/* too complex for an inline implementation */
			ExecEvalConstraintCheck(state, op);

			EEO_NEXT();
		}

        /* evaluate a strict aggregate deserialization function */
        EEO_CASE(EEOP_AGG_STRICT_DESERIALIZE)
        {
            bool *argnull = op->d.agg_deserialize.fcinfo_data->argnull;

            /* Don't call a strict deserialization function with NULL input */
            if (argnull[0])
                EEO_JUMP(op->d.agg_deserialize.jumpnull);

            /* fallthrough */
        }

        /* evaluate aggregate deserialization function (non-strict portion) */
        EEO_CASE(EEOP_AGG_DESERIALIZE)
        {
            FunctionCallInfo fcinfo = op->d.agg_deserialize.fcinfo_data;
            AggState *aggstate = op->d.agg_deserialize.aggstate;
            MemoryContext oldContext;

            /*
             * We run the deserialization functions in per-input-tuple memory
             * context.
             */
            oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);
            fcinfo->isnull = false;
            *op->resvalue = FunctionCallInvoke(fcinfo);
            *op->resnull = fcinfo->isnull;
            MemoryContextSwitchTo(oldContext);

            EEO_NEXT();
        }

        /*
         * Check that a strict aggregate transition / combination function's
         * input is not NULL.
         */
        EEO_CASE(EEOP_AGG_STRICT_INPUT_CHECK)
        {
            int argno;
            bool *nulls = op->d.agg_strict_input_check.nulls;
            int nargs = op->d.agg_strict_input_check.nargs;

            for (argno = 0; argno < nargs; argno++) {
                if (nulls[argno])
                    EEO_JUMP(op->d.agg_strict_input_check.jumpnull);
            }
            EEO_NEXT();
        }

        EEO_CASE(EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

            if (pergroup->noTransValue) {
                /* If transValue has not yet been initialized, do so now. */
                ExecAggInitGroup(aggstate, pertrans, pergroup, op->d.agg_trans.aggcontext);
                /* copied trans value from input, done this round */
            } else if (likely(!pergroup->transValueIsNull)) {
                /* invoke transition function, unless prevented by strictness */
                ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

            if (pergroup->noCollectValue) {
                /* If transValue has not yet been initialized, do so now. */
                ExecAggInitCollectGroup(aggstate, pertrans, pergroup,
                                 op->d.agg_trans.aggcontext);
                /* copied trans value from input, done this round */
            } else if (likely(!pergroup->collectValueIsNull)) {
                /* invoke transition function, unless prevented by strictness */
                ExecAggCollectPlainTransByVal(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

            if (likely(!pergroup->transValueIsNull)) {
                ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(pertrans->transtypeByVal);

            if (likely(!pergroup->collectValueIsNull)) {
                ExecAggCollectPlainTransByVal(aggstate, pertrans, pergroup,
                                              op->d.agg_trans.aggcontext,
                                              op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(pertrans->transtypeByVal);

            ExecAggPlainTransByVal(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_BYVAL)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(pertrans->transtypeByVal);

            ExecAggCollectPlainTransByVal(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(!pertrans->transtypeByVal);

            if (pergroup->noTransValue) {
                ExecAggInitGroup(aggstate, pertrans, pergroup, op->d.agg_trans.aggcontext);
            } else if (likely(!pergroup->transValueIsNull)) {
                ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                       op->d.agg_trans.aggcontext,
                                       op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_INIT_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];


            Assert(!pertrans->transtypeByVal);

            if (pergroup->noCollectValue) {
                ExecAggInitCollectGroup(aggstate, pertrans, pergroup, op->d.agg_trans.aggcontext);
            } else if (likely(!pergroup->collectValueIsNull)) {
                ExecAggCollectPlainTransByRef(aggstate, pertrans, pergroup,
                                              op->d.agg_trans.aggcontext,
                                              op->d.agg_trans.setno);
            }

            EEO_NEXT();
        }
        

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            if (likely(!pergroup->transValueIsNull))
                ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                    op->d.agg_trans.aggcontext,
                                    op->d.agg_trans.setno);
            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_STRICT_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            if (likely(!pergroup->collectValueIsNull))
                ExecAggCollectPlainTransByRef(aggstate, pertrans, pergroup,
                                    op->d.agg_trans.aggcontext,
                                    op->d.agg_trans.setno);
            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_PLAIN_TRANS_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            ExecAggPlainTransByRef(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
        EEO_CASE(EEOP_AGG_COLLECT_PLAIN_TRANS_BYREF)
        {
            AggState   *aggstate = castNode(AggState, state->parent);
            AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            ExecAggCollectPlainTransByRef(aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

        /* process single-column ordered aggregate datum */
        EEO_CASE(EEOP_AGG_ORDERED_TRANS_DATUM)
        {
            /* too complex for an inline implementation */
            ExecEvalAggOrderedTransDatum(state, op, econtext);

            EEO_NEXT();
        }

        /* process multi-column ordered aggregate tuple */
        EEO_CASE(EEOP_AGG_ORDERED_TRANS_TUPLE)
        {
            /* too complex for an inline implementation */
            ExecEvalAggOrderedTransTuple(state, op, econtext);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_XMLEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalXmlExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_AGGREF)
		{
			/*
			 * Returns a Datum whose value is the precomputed aggregate value
			 * found in the given expression context.
			 */
			AggrefExprState *aggref = op->d.aggref.astate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resvalue = econtext->ecxt_aggvalues[aggref->aggno];
			*op->resnull = econtext->ecxt_aggnulls[aggref->aggno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_GROUPING_FUNC)
		{
			/* too complex/uncommon for an inline implementation */
			ExecEvalGroupingFunc(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_WINDOW_FUNC)
		{
			/*
			 * Like Aggref, just return a precomputed value from the econtext.
			 */
			WindowFuncExprState *wfunc = op->d.window_func.wfstate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resvalue = econtext->ecxt_aggvalues[wfunc->wfuncno];
			*op->resnull = econtext->ecxt_aggnulls[wfunc->wfuncno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SUBPLAN)
		{
			/* too complex for an inline implementation */
			ExecEvalSubPlan(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ALTERNATIVE_SUBPLAN)
		{
			/* too complex for an inline implementation */
			ExecEvalAlternativeSubPlan(state, op, econtext);

			EEO_NEXT();
		}

		/* rownum */
		EEO_CASE(EEOP_ROWNUM)
		{
			if (op->d.rownum.typeCompat) {
        		*op->resvalue = DirectFunctionCall1(int8_numeric, Int64GetDatum(op->d.rownum.RownumState->ps_rownum + 1));
    		} else {
        		*op->resvalue = Int64GetDatum(op->d.rownum.RownumState->ps_rownum + 1);
    		}
			*op->resnull = false;

			EEO_NEXT();
		}

		/* grouping_id */
		EEO_CASE(EEOP_GROUPING_ID)
		{
			int groupingId = 0;

    		for (int i = 0; i < op->d.grouping_id.GroupingIdState->current_phase; i++) {
    		    groupingId += op->d.grouping_id.GroupingIdState->phases[i].numsets;
    		}
    		groupingId += op->d.grouping_id.GroupingIdState->projected_set + 1;

    		*op->resvalue = (Datum)groupingId;
			*op->resnull = false;

			EEO_NEXT();
		}

		/* hash_filter */
		EEO_CASE(EEOP_HASH_FILTER)
		{
			ExecEvalHashFilter(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_USERVAR_OR_SETVARIABLE)
		{
			Const* con = op->d.uservar.con;
			*op->resnull = con->constisnull;

			if (econtext->is_cursor && con->consttype == REFCURSOROID) {
				CopyCursorInfoData(&econtext->cursor_data, &con->cursor_data);
				econtext->dno = con->cursor_data.cur_dno;
			}
			*op->resvalue = con->constvalue;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_USERSET_ELEM)
		{
            UserSetElem *elem = op->d.userset.useexpr;
            Node *node = NULL;
            UserSetElem elemcopy;
            elemcopy.xpr = elem->xpr;
            elemcopy.name = elem->name;

            node = eval_const_expression_value(NULL, (Node *)elem->val, NULL);
            if (nodeTag(node) == T_Const) {
                elemcopy.val = (Expr *)const_expression_to_const(node);
            } else {
                elemcopy.val = (Expr *)const_expression_to_const(QueryRewriteNonConstant(node));
            }

            check_set_user_message(&elemcopy);

            *op->resvalue = ((Const *)elemcopy.val)->constvalue;

			*op->resnull = false;

            EEO_NEXT();
		}

        EEO_CASE(EEOP_PREFIX_BTYEA)
        {
            if (*op->resnull) {
                *op->resvalue = (Datum)0;
                *op->resnull = true;
            } else {
                *op->resvalue = PointerGetDatum(bytea_substring(*op->resvalue, 1, op->d.prefix_key.pkey->length, false));
                *op->resnull = false;
            }

            EEO_NEXT();
        }

        EEO_CASE(EEOP_PREFIX_TEXT)
        {
            if (*op->resnull) {
                *op->resvalue = (Datum)0;
                *op->resnull = true;
            } else {
                *op->resvalue = PointerGetDatum(text_substring_with_encoding(
                    *op->resvalue, 1, op->d.prefix_key.pkey->length, false, op->d.prefix_key.encoding));
                *op->resnull = false;
            }

            EEO_NEXT();
        }

		EEO_CASE(EEOP_NANTEST_ISNAN)
		{
			ExecEvalNan(state, op, econtext);
 
			EEO_NEXT();
		}

		EEO_CASE(EEOP_NANTEST_ISNOTNAN)
		{
			ExecEvalNotNan(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INFINITETEST_ISINFINITE)
		{
			ExecEvalInfinite(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INFINITETEST_ISNOTINFINITE)
		{
			ExecEvalNotInfinite(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_LAST)
		{
			/* unreachable */
			Assert(false);
			goto out;
		}
	}

out:
	*isnull = state->resnull;
	return state->resvalue;
}
```

### I. 表达式相等与引用节点的基础框架

<a id="source-106"></a>

#### _equalVar()

来源：`src/common/backend/nodes/equalfuncs.cpp:187-200`；范围：**完整函数**。

内容 SHA256：`789ca7896fb4b30b6ec05d9ef355d2f732b9528201eefe0e4fb8e146ea1c26f9`。

```cpp
static bool _equalVar(const Var* a, const Var* b)
{
    COMPARE_SCALAR_FIELD(varno);
    COMPARE_SCALAR_FIELD(varattno);
    COMPARE_SCALAR_FIELD(vartype);
    COMPARE_SCALAR_FIELD(vartypmod);
    COMPARE_SCALAR_FIELD(varcollid);
    COMPARE_SCALAR_FIELD(varlevelsup);
    COMPARE_SCALAR_FIELD(varnoold);
    COMPARE_SCALAR_FIELD(varoattno);
    COMPARE_LOCATION_FIELD(location);

    return true;
}
```

<a id="source-107"></a>

#### _equalConst()

来源：`src/common/backend/nodes/equalfuncs.cpp:202-220`；范围：**完整函数**。

内容 SHA256：`adb9613829f87709d17b96f29661e37dd6dedb7e48158a8cd6f7e7756794ae18`。

```cpp
static bool _equalConst(const Const* a, const Const* b)
{
    COMPARE_SCALAR_FIELD(consttype);
    COMPARE_SCALAR_FIELD(consttypmod);
    COMPARE_SCALAR_FIELD(constcollid);
    COMPARE_SCALAR_FIELD(constlen);
    COMPARE_SCALAR_FIELD(constisnull);
    COMPARE_SCALAR_FIELD(constbyval);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(ismaxvalue);

    /*
     * We treat all NULL constants of the same type as equal. Someday this
     * might need to change?  But datumIsEqual doesn't work on nulls, so...
     */
    if (a->constisnull)
        return true;
    return datumIsEqual(a->constvalue, b->constvalue, a->constbyval, a->constlen);
}
```

<a id="source-108"></a>

#### _euqalCommonExprPart()

来源：`src/common/backend/nodes/equalfuncs.cpp:356-379`；范围：**完整函数**。

内容 SHA256：`b7e24c4a46b1ba1f759fe29b412f7dcd46f7dc011a21dd307c3caae5507a5fb9`。

```cpp
template <typename T>
static bool _euqalCommonExprPart(const T* a, const T* b)
{
    COMPARE_SCALAR_FIELD(opno);

    /*
     * Special-case opfuncid: it is allowable for it to differ if one node
     * contains zero and the other doesn't.  This just means that the one node
     * isn't as far along in the parse/plan pipeline and hasn't had the
     * opfuncid cache filled yet.
     */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0) {
        return false;
    }

    COMPARE_SCALAR_FIELD(opresulttype);
    COMPARE_SCALAR_FIELD(opretset);
    COMPARE_SCALAR_FIELD(opcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}
```

<a id="source-109"></a>

#### _equalOpExpr()

来源：`src/common/backend/nodes/equalfuncs.cpp:380-383`；范围：**完整函数**。

内容 SHA256：`abbbe9390655325aa554827320dbb18d1d1bfc0d42e409196429500db8093745`。

```cpp
static bool _equalOpExpr(const OpExpr* a, const OpExpr* b)
{
    return _euqalCommonExprPart<OpExpr>(a, b);
}
```

<a id="source-110"></a>

#### _equalAnnOrderByRef()

来源：`src/common/backend/nodes/equalfuncs.cpp:592-599`；范围：**完整函数**。

内容 SHA256：`ead8d8a5f5528ea64f945fa81b5b17e1606131df50f407138c7826866133a2ed`。

```cpp
static bool _equalAnnOrderByRef(const AnnOrderByRef* a, const AnnOrderByRef* b)
{
    COMPARE_SCALAR_FIELD(orderby_index);
    COMPARE_SCALAR_FIELD(quality);
    COMPARE_NODE_FIELD(original_expr);

    return true;
}
```

<a id="source-111"></a>

#### 字段比较宏定义（包含 location 的空操作语义）

来源：`src/common/backend/nodes/equalfuncs.cpp:40-102`；范围：**明确标注的源码片段**。

内容 SHA256：`b3fc09da5b10b3c419f8659197d8085605093d1e9a58d6f17f36f6f327351acd`。

非函数定义上下文。_euqalCommonExprPart 的名称按源仓原始拼写保留；opfuncid 仅一方为 0 时仍允许匹配，location 不参与相等判定。

```cpp

/*
 * Macros to simplify comparison of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

/* Compare a simple scalar field (int, float, bool, enum, etc) */
#define COMPARE_SCALAR_FIELD(fldname)  \
    do {                               \
        if (a->fldname != b->fldname) {\
            return false;              \
        }                              \
    } while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname)          \
    do {                                     \
        if (!equal(a->fldname, b->fldname)) {\
            return false;                    \
        }                                    \
    } while (0)

/* Compare a field that is a pointer to a Bitmapset */
#define COMPARE_BITMAPSET_FIELD(fldname)         \
    do {                                         \
        if (!bms_equal(a->fldname, b->fldname)) {\
            return false;                        \
        }                                        \
    } while (0)

/* Compare a field that is a pointer to a C string, or perhaps NULL */
#define COMPARE_STRING_FIELD(fldname)           \
    do {                                        \
        if (!equalstr(a->fldname, b->fldname)) {\
            return false;                       \
        }                                       \
    } while (0)

/* Macro for comparing string fields that might be NULL */
#define equalstr(a, b) (((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : ((a) == (b)))

/* Compare a field that is a pointer to a simple palloc'd object of size sz */
#define COMPARE_POINTER_FIELD(fldname, sz)             \
    do {                                               \
        if (memcmp(a->fldname, b->fldname, (sz)) != 0) \
            return false;                              \
    } while (0)

/* Check whether a field is null */
#define CHECK_ANY_NULL(fildname)                                \
    do {                                                        \
        if (a->fildname == nullptr || b->fildname == nullptr) { \
            return false;                                       \
        }                                                       \
    } while (0)

/* Compare a parse location field (this is a no-op, per note above) */
#define COMPARE_LOCATION_FIELD(fldname) ((void)0)

/* Compare a CoercionForm field (also a no-op, per comment in primnodes.h) */
#define COMPARE_COERCIONFORM_FIELD(fldname) ((void)0)
```

<a id="source-112"></a>

#### _copyAnnOrderByRef()

来源：`src/common/backend/nodes/copyfuncs.cpp:3360-3369`；范围：**完整函数**。

内容 SHA256：`0bbeeda559af629785656ac83520362acc83104bdf9b50a38d99c6ebfbb380e2`。

```cpp
static AnnOrderByRef* _copyAnnOrderByRef(const AnnOrderByRef* from)
{
    AnnOrderByRef* newnode = makeNode(AnnOrderByRef);

    COPY_SCALAR_FIELD(orderby_index);
    COPY_SCALAR_FIELD(quality);
    COPY_NODE_FIELD(original_expr);

    return newnode;
}
```

<a id="source-113"></a>

#### _outAnnOrderByRef()

来源：`src/common/backend/nodes/outfuncs.cpp:3215-3222`；范围：**完整函数**。

内容 SHA256：`33d4f942177dae217f5c1dd4887321899980ac5f0927ab705d49e1401267195f`。

```cpp
static void _outAnnOrderByRef(StringInfo str, AnnOrderByRef* node)
{
    WRITE_NODE_TYPE("ANNORDERBYREF");

    WRITE_INT_FIELD(orderby_index);
    WRITE_INT_FIELD(quality);
    WRITE_NODE_FIELD(original_expr);
}
```

<a id="source-114"></a>

#### _readAnnOrderByRef()

来源：`src/common/backend/nodes/readfuncs.cpp:3028-3037`；范围：**完整函数**。

内容 SHA256：`4f0ebaee045816331ac54ad500470c1d445d677ab559d20896dc30b8d13089f1`。

```cpp
static AnnOrderByRef* _readAnnOrderByRef(void)
{
    READ_LOCALS(AnnOrderByRef);

    READ_INT_FIELD(orderby_index);
    READ_INT_FIELD(quality);
    READ_NODE_FIELD(original_expr);

    READ_DONE();
}
```


## 7. 本轮无 WHERE 放行的实际 diff

此 diff 只展示本地已有未提交修改，不代表本轮新增内核改动。不要把本文件其它完整函数误认成全部都是本次新增代码。

```diff
diff --git a/src/gausskernel/optimizer/prep/preptopk.cpp b/src/gausskernel/optimizer/prep/preptopk.cpp
index 5a1766a1d5..d456fdff7a 100644
--- a/src/gausskernel/optimizer/prep/preptopk.cpp
+++ b/src/gausskernel/optimizer/prep/preptopk.cpp
@@ -225,7 +225,7 @@ preprocess_union_all_required_columns(PlannerInfo *root)
          oldNcols,
          newNcols,
          wherePushed ? "true" : "false",
-         wherePushed ? "false" : "true");
+         outer->jointree != NULL && outer->jointree->quals != NULL ? "true" : "false");
 
     return true;
 }
@@ -243,8 +243,9 @@ required_columns_outer_query_is_safe(Query *query)
     if (query == NULL || query->commandType != CMD_SELECT)
         return false;
 
-    if (query->jointree == NULL || query->jointree->quals == NULL)
-        return false; /* WHERE is mandatory for the initial feature contract */
+    /* An absent predicate needs no pushdown; keep the same projection checks. */
+    if (query->jointree == NULL)
+        return false;
 
     if (query->setOperations != NULL || query->hasAggs || query->groupClause != NIL ||
         query->havingQual != NULL || query->distinctClause != NIL ||
```

## 8. 可独立复制的小数据回归输入及已有计划证据

下面 SQL 原文来自已通过的源仓原生回归，包含专用 schema 的重建/清理。它不是 GV 原样可运行脚本：目标必须替换向量类型、AM、opclass、Cosine 操作符和必要 GUC。保留外层无 WHERE、重复 id、UNION ALL＋NOT EXISTS、只有 snapshot 有向量索引的结构。KGN 近似结果需按目标契约验证；不要直接要求与精确源仓数值逐位相同。

```sql
-- Required-column pruning now accepts an absent outer WHERE.
-- Keep the complete UNION/Anti Join safety checks: only the syntactic WHERE
-- requirement is relaxed. Repeated user projections must keep seven columns.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS union_all_no_where_test CASCADE;
CREATE SCHEMA union_all_no_where_test;
SET search_path = union_all_no_where_test, public;

CREATE TABLE delta_t
(
    id bigint, row_id bigint, embedding vector(4),
    n1 integer, n2 integer, n3 integer, n4 integer, unused_text text
);
CREATE TABLE snapshot_t
(
    id bigint, row_id bigint, embedding vector(4),
    n1 integer, n2 integer, n3 integer, n4 integer, unused_text text
);
CREATE TABLE delete_t(row_id bigint);

-- Small deterministic data: both distances increase strictly with the first
-- coordinate. No snapshot primary key; only snapshot has vector indexes.
INSERT INTO delta_t
SELECT 100+g, 100+g, ('[' || (12+g) || ',2,2,2]')::vector,
       g, g+1, g+2, g+3, 'unused-delta'
FROM generate_series(1,4) g;
INSERT INTO snapshot_t
SELECT g, g, ('[' || (2+g) || ',2,2,2]')::vector,
       g, g+1, g+2, g+3, 'unused-snapshot'
FROM generate_series(1,6) g;
INSERT INTO delete_t VALUES (2), (5);

CREATE INDEX delete_row_id_idx ON delete_t(row_id);
CREATE INDEX snapshot_l2_idx ON snapshot_t
    USING diskann(embedding vector_l2_ops);
CREATE INDEX snapshot_cosine_idx ON snapshot_t
    USING diskann(embedding vector_cosine_ops);

CREATE VIEW union_view AS
SELECT id, embedding, n1, n2, n3, n4, unused_text FROM delta_t
UNION ALL
SELECT s.id, s.embedding, s.n1, s.n2, s.n3, s.n4, s.unused_text
FROM snapshot_t s
WHERE NOT EXISTS (SELECT 1 FROM delete_t d WHERE d.row_id=s.row_id);

ANALYZE delta_t;
ANALYZE snapshot_t;
ANALYZE delete_t;

SET query_dop = 1;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_nestloop = on;
SET enable_union_all_diskann_topk_pushdown = on;

-- L2: local Top-K alone does not export the branch resjunk key through UNION.
-- Pruning must materialize that key, then setrefs can build transparent Vars.
SET ann_orderby_distance_reuse = l2;
SET enable_union_all_required_columns_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <-> '[2,2,2,2]'::vector LIMIT 100;

SET enable_union_all_required_columns_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <-> '[2,2,2,2]'::vector LIMIT 100;
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <-> '[2,2,2,2]'::vector LIMIT 100;

-- Cosine exercises the approximate-consumer gate. Do not relax the Result
-- whitelist to compensate for a missing branch-local scalar projection.
SET ann_orderby_distance_reuse = cosine;
SET enable_union_all_required_columns_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <=> '[2,2,2,2]'::vector LIMIT 100;

SET enable_union_all_required_columns_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <=> '[2,2,2,2]'::vector LIMIT 100;
SELECT id,id,id,n1,n2,n3,n4 FROM union_view
ORDER BY embedding <=> '[2,2,2,2]'::vector LIMIT 100;

-- Each dynamic SELECT is planned independently, so the assertion aggregate
-- cannot disable pruning/reuse inside the query being checked. Sorting only
-- the captured IDs avoids depending on an unordered query's emission order.
CREATE FUNCTION capture_ids(query_text text) RETURNS SETOF bigint AS $$
BEGIN
    RETURN QUERY EXECUTE query_text;
END;
$$ LANGUAGE plpgsql;

SET ann_orderby_distance_reuse = l2;
SET enable_union_all_required_columns_pushdown = off;
SELECT 'plain' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view') AS captured(id);
SELECT 'limit_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view LIMIT 100') AS captured(id);
SELECT 'order_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids(
    'SELECT id FROM union_view ORDER BY embedding <-> ''[2,2,2,2]''::vector') AS captured(id);

SET enable_union_all_required_columns_pushdown = on;
SELECT 'plain' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view') AS captured(id);
SELECT 'limit_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids('SELECT id FROM union_view LIMIT 100') AS captured(id);
SELECT 'order_only' AS scenario, array_agg(id ORDER BY id) AS ids
FROM capture_ids(
    'SELECT id FROM union_view ORDER BY embedding <-> ''[2,2,2,2]''::vector') AS captured(id);

RESET ann_orderby_distance_reuse;
RESET enable_union_all_required_columns_pushdown;
RESET enable_union_all_diskann_topk_pushdown;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET enable_hashjoin;
RESET enable_bitmapscan;
RESET enable_seqscan;
RESET query_dop;
RESET search_path;
DROP SCHEMA union_all_no_where_test CASCADE;
RESET client_min_messages;
```

下面是此前 1024 维完整结构测试保存的真实计划。只缩略长向量常量，保留节点结构与输出项。**Result 没有消除，但 snapshot 扫描已复用距离。**它证明 UNION ALL 形态与该设计不矛盾；不证明 GV 当前分支已经修好。

```text
-- 仅缩略1024维常量，不改节点或输出；完整常量见同名完整计划。
Limit
  Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
  ->  Result
        Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
        ->  Merge Append
              Sort Key: ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
              ->  Limit
                    Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                    ->  Sort
                          Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                          Sort Key: ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                          ->  Seq Scan on d_lake_zhongyan.zhongyan_10yi_dt1
                                Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", (zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
              ->  Limit
                    Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                    ->  Nested Loop Anti Join
                          Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                          Join Filter: (d.row_id = s.row_id)
                          ->  Ann Index Scan using zhongyan_10yi_vsi_cos on d_lake_zhongyan.zhongyan_10yi_snap1 s
                                Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))), s.row_id
                                Order By: (s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
                          ->  Materialize
                                Output: d.row_id
                                ->  Index Only Scan using zhongyan_10yi_del1_idx on d_lake_zhongyan.zhongyan_10yi_del1 d
                                      Output: d.row_id
```

## 9. 完整性与来源校验

| 原始文件 | SHA256（整个文件） |
|---|---|
| `src/common/backend/nodes/copyfuncs.cpp` | `d385b956a1b5e58d5fa01c82f3cb74a3d268540c02b6b5fefd49cbf090698c82` |
| `src/common/backend/nodes/equalfuncs.cpp` | `d9baf88a28dddc7d0a5ac1a812584f532672af1ed52d811e1dee352da2dbd549` |
| `src/common/backend/nodes/outfuncs.cpp` | `b0a48e12e848fd5235c76b37e97c1922437b5131a6a1e26d9a65c6a1979e5b4d` |
| `src/common/backend/nodes/readfuncs.cpp` | `aab5d0703024be8406d7aa4ab866ae138e7ddb9bd7f0310f5086b1510e4cc33b` |
| `src/common/backend/utils/adt/ruleutils.cpp` | `91ce2b9996ba1cbcee5444472d3b265a8380600aa1bcacfa590fa51c6b1e858b` |
| `src/common/backend/utils/misc/guc/guc_sql.cpp` | `5fde7b192a8a66793377c32339a311de4cdfc10940896cf6be61342ee632d8a9` |
| `src/gausskernel/optimizer/commands/explain.cpp` | `6bc1efa815dd31233f051783646913a43f69a2535d53e63c3acbc5ea16e0b490` |
| `src/gausskernel/optimizer/plan/createplan.cpp` | `5272575d59021e48253d88dfdbcd4e4539a35d1638c2ce13114a0c769f3ceb4e` |
| `src/gausskernel/optimizer/plan/initsplan.cpp` | `725b0bd8fb4bcf8e97eff603f20b8309b3a61f6ceaba8ed3ffa1a5864e19266a` |
| `src/gausskernel/optimizer/plan/planner.cpp` | `83c584d97bfb7503746fc1f6de9dd48dbbef7cc68e0a9787bb8f34ddae4fce79` |
| `src/gausskernel/optimizer/plan/setrefs.cpp` | `cc5855b65de7063e52a77e7aa161d07394aaeb2a8bac07e5cb2429898bfe297e` |
| `src/gausskernel/optimizer/prep/preptopk.cpp` | `deea19a3c0155100912dbbc9db7b68a85a5dd9d1ff637f85f8d1b711382c33fd` |
| `src/gausskernel/optimizer/prep/prepunion.cpp` | `a58a85f96da7e7c7418562149337f1bcc9810059a624c92aebd37968a8c9118c` |
| `src/gausskernel/optimizer/util/planmem_walker.cpp` | `70f99edbd6703f8d401d24ced781ed8a6136678e64d18cfb9e7b6a69e7e153c3` |
| `src/gausskernel/runtime/executor/execExpr.cpp` | `d96a2811f04d883637d21e6ddb7f76b156a602e20d914765bacaeae249d4c75e` |
| `src/gausskernel/runtime/executor/execExprInterp.cpp` | `658fecf8513feedb5fb1fe3913ff8493c19f8fb9319778ae27c903b7bcb87f4a` |
| `src/gausskernel/runtime/executor/execQual.cpp` | `65c5183005506f2a09facce96c8f252cf7d201c7497d023279111a2085121179` |
| `src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp` | `7117c4f9ad95a0322c9fc2d514869546ce3268bddbdd2f6ab1691c0668ffdc81` |
| `src/include/access/relscan.h` | `cc56964a766efe3e4b59c81a1350c20faf723c322241b19a2a9450c2084b2e64` |
| `src/include/knl/knl_guc/knl_session_attr_sql.h` | `710792a495683109d687ee64290334ab80fabc7cbbffa186e6a37de73434e001` |
| `src/include/miscadmin.h` | `047de06b37ae45ea4f44137802bec321d157d0947b3fe974dbb8d698327f88a7` |
| `src/include/nodes/execnodes.h` | `4b89568b4a783ee26d4832034f0462058f763f3b2f43edbfe6fe88e931290078` |
| `src/include/nodes/primnodes.h` | `fa61b1dbb396de78cc58dea16f27d69a4d5144e79f81687394f1eaf6b2cbdef6` |
| `src/include/nodes/relation.h` | `78fce0b0a748381412f36375d08cd18282e9f05ec1850731e5045443175ae9e1` |
| `src/include/optimizer/prep.h` | `d6f451976b3da391844aa1b34478e7a51766ca3987a449249c2aa656b3d460b2` |
| `src/include/utils/guc.h` | `b601d3bf40fe63ee4308dbce52458c7903e64b182ac170ad5fda4ab96e9f244f` |

附录测试 SQL：`src/test/regress/sql/union_all_required_columns_no_where.sql`；SHA256：`311a02d714f895a657f5ce61e6806e2d9257e2a0a545a3f8ed0585f736ac76b1`。

已有计划证据：`/home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/测试/Result透传与列裁剪联动验证/修复后_cosine_计划缩略.out`；SHA256：`e74504a033245eee012d0ce3d5436460a747242ee89055a058db61a273465574`。


本轮交付仅此 Markdown 文件。已核验每个源码块与当前工作树对应行文本一致、函数索引可达、必要函数覆盖齐全。归档中的旧注释、条件编译和源仓能力边界均保留，不能以文件名或某一段源码替代目标环境的实际执行证据。
