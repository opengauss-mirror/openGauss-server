# openGauss UNION ALL 必需列源头裁剪详细设计

使用说明（2026-09-12 汇总核对）：本文保留字段生命周期、算法和初期设计推演；当前两个特性的协同边界统一见[协同设计说明](UNION_ALL_LIMIT下推与列裁剪协同设计说明.md)，全部相关资料见[设计总入口](README.md)。§3 是改写前的证据，§7、§14、§22 保留初期结构/拆分/评审草案，不是当前源码定义或迁移文件清单；不得据此新增不存在的模块或遗漏实际修复。

| 项目 | 内容 |
|---|---|
| 文档状态 | P0～P2 已实现，单机编译与回归通过；P3 分布式验证和默认开启评估未执行 |
| 目标特性 | UNION ALL Required Columns Pushdown（必需列源头裁剪） |
| 目标分支 | openGauss integration/multimodal-stacked |
| 初始开发基线 | bf59a66d8b |
| 当前核对代码 | HEAD 098d77c8b76d 加尚未提交的六类正确性修复；仅 HEAD 不含完整修复 |
| 目标 SQL 约束 | 外层 WHERE 必须存在；ORDER BY、LIMIT 各自可选 |
| 核心目标 | 不属于最终输出、谓词、排序或内部语义依赖的字段，不进入源表扫描输出，也不在 Append、SubqueryScan、Join、Sort、Stream 间逐层传输 |
| 与现有特性的关系 | 必需列裁剪独立于 DiskANN Top-K；仅当 Top-K GUC 开启且 ORDER BY 与 LIMIT 同时满足现有安全条件时，额外启用分支 Top-K |
| 初始交付代码量快照 | 内核 +1,151/-1 行；回归及框架接入 +1,044/-2 行，其中 SQL 342 行、expected 693 行；不含后续修复 |

## 0. 实现与验收状态（2026-09-09）

安全复核补充：初始特性提交 `098d77c8b76d` 及旧 patch 存在六个已复现的正确性缺陷，修正以[深度复核与缺陷修复报告](../测试/UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md)为准。阶段一现在返回“本 Query 改写成功”的 bool；阶段三只在该值为 true 且外层无残留 quals 时执行，不能把继承的扫描策略标志当成资格。外层/叶子 ROWNUM、SRF，以及 UNION 容器 LIMIT、外层 WITH TIES/PERCENT 均需保守回退。下文原有代码量和 1/1、2/2 结果是初始交付快照，不包含这轮修复；最终验证见新报告 §5。

本设计的目标场景已经完成开发，并在 openGauss 原生 `pg_regress` 单机框架中通过。新能力由独立 GUC `enable_union_all_required_columns_pushdown` 控制，默认值为 `off`；它与 `enable_union_all_diskann_topk_pushdown` 可独立组合。

实际实现采用三段式编排：

1. `preprocess_union_all_required_columns()` 在原列号上把安全的外层 WHERE 映射到所有叶子，删除外层精确重检，并压紧 UNION ALL 的位置型输出。
2. 原有 `preprocess_union_all_diskann_topk()` 在 ORDER BY + LIMIT 满足条件时决定是否增加分支 Top-K。
3. `preprocess_union_all_order_key_projection()` 在 Top-K 判断完成后，把可安全处理的非 Var 排序表达式物化为叶子输出。完整 vector 没有最终投影等其他消费者时，才不再越过 UNION 边界。

实现阶段相对原设计有两项有意调整：

- 为复用现有 UNION/叶子发现、表达式映射和 Top-K 安全逻辑，代码集中在 `preptopk.cpp`，没有新增 `prepunioncols.cpp`。
- 实测发现 `createplan.cpp::use_physical_tlist()` 会在叶子 Query 已压紧后重新把基表物理列加入扫描输出，因此新增 planner 私有标志，在本次已压紧查询的后代基表扫描中禁止物理 targetlist 回扩。这是保证字段从源扫描 `Output` 消失所必需的定向修复。

已通过的验收包括：WHERE、WHERE+ORDER BY、WHERE+LIMIT、WHERE+ORDER BY+LIMIT 四种目标 SQL；DiskANN 计划；`delta_t`、`snapshot_t`、`delete_t` 字段生命周期；GUC off/on 结果等价；UNION ALL 重复行；排序位置；四叶集合树；参数化计划；volatile 输出保留；UNION distinct、叶子 LIMIT 和 security barrier 安全降级。

原生回归结果：专用用例 `1/1` 通过；与既有 `diskann_vector` 联跑 `2/2` 通过。P3 所列多 DN、Stream/RemoteQuery 以及默认开启评估不在本次单机场景交付范围内，不能据此宣称已经完成分布式验收。详细证据见 `../测试/UNION_ALL必需列源头裁剪开发与测试报告.md`。

## 1. 设计问题与结论

设计基线中的计划存在列宽放大问题，而且不只发生在 snapshot_t；以下描述的是本特性要修复的问题，不是声称修复后的当前计划仍有这些现象：

1. snapshot_t 在四种目标 SQL 组合中都会把未使用的 title 带到 Merge Anti Join 之上；没有 ORDER BY 时，embedding 也被无效携带。
2. delta_t 在普通 WHERE、ORDER BY 或 LIMIT 场景下已经能按上层需求裁剪；但现有 Top-K 改写给叶子增加 ORDER BY + LIMIT 后，叶子变成不可拉平子查询，完整的 row_id、title、embedding 又会进入局部 Sort 和 Limit。
3. delete_t 当前样例只输出反连接所需的 row_id，已经是最小列集；仍需增加回归，保证将来 delete 分支增加过滤列时，过滤列只活到其最后使用节点，不进入反连接输出。
4. 根因不是 DiskANN 扫描器读取了 title，而是集合运算仍以视图的完整位置型 targetlist 规划。上层只取 row_id 的信息没有穿过 UNION ALL 边界。
5. 推荐在通用 SetOp 规划之前，对本次 planner 私有的 Query 树做一次原子化“物理输出压紧”：先确定列的最后消费者，再把纯 UNION ALL 及所有叶子的输出重建为连续的最小列集。后续 Append、SubqueryScan、Sort、Stream 和 Executor 无需认识被删除的列。
6. 不采用“把未用列替换成带类型 NULL”的方案作为最终交付。它可以避免计算原表达式，但仍保留空列槽位、tuple descriptor 宽度和层间传输接口，不完全符合“从源头杜绝层层传输”。

## 2. 目标 SQL

第一期只承诺以下结构。WHERE 是必需条件；ORDER BY 和 LIMIT 独立可选：

    SELECT row_id
    FROM gs_1w_1bucket
    WHERE <外层过滤条件>
    [ORDER BY embedding <-> <query_vector> ASC [, ...]]
    [LIMIT <positive_count>];

底层视图的典型结构为：

    SELECT row_id, title, embedding
    FROM delta_t
    UNION ALL
    SELECT s.row_id, s.title, s.embedding
    FROM snapshot_t s
    WHERE NOT EXISTS (
        SELECT 1
        FROM delete_t d
        WHERE d.row_id = s.row_id
    );

需要覆盖四种组合：

| WHERE | ORDER BY | LIMIT | 必需列裁剪 | 分支排序 | 分支 LIMIT |
|---|---|---|---|---|---|
| 有 | 无 | 无 | 必须 | 无 | 无 |
| 有 | 有 | 无 | 必须 | 不主动增加；由 CBO 决定 | 无 |
| 有 | 无 | 有 | 必须 | 无 | 禁止下推 |
| 有 | 有 | 有 | 必须 | 满足安全条件时增加 | 满足 DiskANN Top-K 条件时增加 |

其中 LIMIT without ORDER BY 合法但结果本身没有稳定次序。第一期保持全局 LIMIT，不向各分支复制 LIMIT，避免引入不必要的行为差异。

## 3. 改写前的实测证据（设计基线）

测试环境：

| 项目 | 值 |
|---|---|
| 实例 | /home/qiumc/data/code/cpp/openGauss-server/topk_test_data |
| 端口 | 55440 |
| 数据库 | postgres |
| Schema | union_all_topk_onecol_test |
| 开关 | enable_union_all_diskann_topk_pushdown=on，enable_seqscan=off |

小数据已经足够暴露计划接口问题：delta_t 20 行、snapshot_t 20 行、delete_t 3 行。

### 3.1 WHERE only

外层只需要 row_id，但 snapshot 叶子的输出仍是三列：

    Result
      Output: delta_t.row_id
      -> Append
           -> Index Only Scan on delta_t
                Output: delta_t.row_id
           -> Subquery Scan
                Output: row_id
                -> Merge Anti Join
                     Output: s.row_id, s.title, s.embedding
                     -> Index Scan on snapshot_t s
                          Output: s.row_id, s.title, s.embedding
                     -> Index Only Scan on delete_t d
                          Output: d.row_id

结论：delta_t 已窄化；snapshot_t 的 title、embedding 是无效输出。

### 3.2 WHERE + ORDER BY

embedding 是计算距离所必需的输入，但 title 仍无用途：

    Sort
      Output: row_id, distance
      -> Append
           -> Index Scan on delta_t
                Output: row_id, embedding
           -> Subquery Scan
                Output: row_id, embedding
                -> Merge Anti Join
                     Output: s.row_id, s.title, s.embedding

结论：title 应从 snapshot 源头删除；embedding 应只活到距离值计算完成，之后以 float8 排序键代替，不应穿过全局 Append 后再重复携带完整 vector。

### 3.3 WHERE + LIMIT

计划在 snapshot 分支仍携带三列，问题与 WHERE only 相同。LIMIT 不会让 title 或 embedding 变为必需列。

### 3.4 WHERE + ORDER BY + LIMIT

现有 Top-K 改写给两个叶子增加局部 Sort 和 Limit，但保留了完整叶子 targetlist：

    Append
      -> Subquery Scan
           Output: row_id, embedding
           Filter: row_id > 5
           -> Limit
                Output: row_id, title, embedding, distance
                -> Sort
                     Output: row_id, title, embedding, distance
      -> Subquery Scan
           Output: row_id, embedding
           Filter: row_id > 5
           -> Limit
                Output: row_id, title, embedding, distance
                -> Sort
                     -> Merge Anti Join
                          Output: row_id, title, embedding, distance

同时，外层 WHERE 已被复制到叶子，却仍作为 Subquery Scan Filter 再执行一次。它保证了当前改写的保守正确性，但也使仅供 WHERE 使用的字段无法在叶子内及时终止生命周期。

## 4. 根因分析

### 4.1 通用子查询裁剪显式跳过 SetOp

src/gausskernel/optimizer/path/allpaths.cpp 的 remove_unused_subquery_outputs() 已能收集上层 reltarget 和未下推谓词使用的列；但 4522～4528 行遇到 subquery->setOperations 后直接返回。

即使不是集合运算，该函数也只把未使用表达式换成同类型 NULL，以保持原列号，而不是物理删除 targetlist 项。

### 4.2 UNION ALL 按完整位置型 Schema 规划

src/gausskernel/optimizer/prep/prepunion.cpp 的 plan_set_operations() 从顶层 SetOperationStmt.colTypes、colCollations 和左叶 targetlist 开始规划。generate_setop_tlist() 与 generate_append_tlist() 按每个集合列生成连续输出。

src/gausskernel/optimizer/prep/prepjointree.cpp 的 pull_up_union_leaf_queries() 也通过 make_setop_translation_list() 为所有输出位置建立 AppendRelInfo.translated_vars。

因此，只要集合子查询仍声明 row_id、title、embedding 三列，复杂 snapshot 叶子就会被要求提供三列。

### 4.3 Top-K 改写复制完整叶子接口

src/gausskernel/optimizer/prep/preptopk.cpp 的 prepare_leaf_diskann_topk() 只映射外层 WHERE、ORDER BY 并设置 LIMIT；它不会按外层真实需求重建叶子 targetlist。局部 ORDER BY + LIMIT 又会阻止简单叶子被拉平，delta_t 因而也重新暴露完整三列。

### 4.4 EXPLAIN Output 的准确含义

本设计优化的是 Plan targetlist、TupleTableSlot、投影表达式、detoast/deform、Sort 元组、Stream 序列化和节点间传输宽度。

对 heap 行存表，删除 title 的 Output 不等于完全不读取 title 所在的数据页；整页和 tuple header 仍可能被读取，解码后续属性也可能遍历前面的属性。输出层目标及可能收益为：

- title 不作为执行计划输出属性；
- 没有谓词或表达式依赖时，不再为向上输出 title 而求值、detoast 或复制它；不能仅凭 EXPLAIN Output 承诺所有存储解码路径都不访问该属性；
- title 不进入 Join、Append、Sort、Limit、Material、Stream 的 tuple descriptor；
- 列存路径可能进一步避免无关列 I/O，需另行验证；
- 只有 row_id 时，合适索引可使 Index Only Scan 成为候选，是否回表仍取决于实际存储/可见性条件。

## 5. 设计原则

### 5.1 以“最后消费者”判断必需性

字段不是简单分成“要”与“不要”，而是拥有生命周期：

| 依赖类别 | 示例 | 最后消费者 |
|---|---|---|
| 最终投影 | row_id | 顶层输出 |
| 外层 WHERE | title LIKE 'x%' | 叶子 Filter；成功下推后不再越过 UNION |
| ORDER BY 输入 | embedding | 距离表达式计算节点 |
| ORDER BY 结果 | distance float8 | 全局 Sort/Limit |
| 叶子连接键 | snapshot.row_id | Anti Join，同时也可能是最终输出 |
| delete 连接键 | delete.row_id | Anti Join 内侧 |
| 叶子本地过滤列 | delete.version | delete Filter |
| 安全语义列 | volatile/SRF、rowmark、分布键 | 对应语义节点 |

规则是：源表只输出到其上层仍有消费者的最小属性或表达式；一个属性完成最后一次使用后，应立即从上层 PathTarget/targetlist 消失。

### 5.2 逻辑列号与物理输出分离，但不把映射带入 Executor

视图在 Catalog 中仍有原始三列，用户可继续查询 title、embedding。只对本次 planner 使用的 Query 副本做物理压紧：

    原逻辑列：1=row_id, 2=title, 3=embedding
    WHERE only 物理列：1=row_id
    WHERE + ORDER 物理列：1=row_id, 2=__ua_order_1(float8)

外层表达式及集合叶子在同一次原子改写中一起重编号。改写完成后，所有位置仍是 1..N 的普通连续 targetlist。通用 SetOp、Append、SubqueryScan、setrefs 和 Executor 不需要长期保存 old_attno -> new_attno 映射。

这比在 SubqueryScanPath、Plan 和 Executor 中维护稀疏逻辑列号风险更低，也避免 NULL 占位。

### 5.3 各项能力允许安全降级

1. 必需列压紧：只删除完全无消费者的集合输出。
2. WHERE 分支化：安全时把外层 WHERE 精确映射到所有叶子并删除外层重复谓词。
3. Top-K：仅独立 GUC 开启且 ORDER BY + LIMIT 同时满足 DiskANN 规则时复制分支排序和 LIMIT。
4. 排序键输出：第一阶段成功、无残留外层 WHERE 且表达式安全时，用叶子计算结果代替跨 UNION 传输的无其他消费者的宽输入。

后一级失败不得撤销前一级已经证明安全的收益。例如 ORDER BY 不能转成分支排序键时，仍可删除无关 title，只是 embedding 继续作为排序输入穿过 UNION。

## 6. 总体改写流程

实际调用时机位于 `subquery_planner()` 中已有 `reduce_orderby`、单机 `preprocess_rownum()` 之后，`pull_up_subqueries()` 及后续 UNION ALL 展平之前。

当前采用三个独立阶段，不是一个入口包揽所有改写：

    1. required columns：识别形状，在完整 Query 副本上先映射 WHERE；
       收集必需列，压紧所有位置型接口和引用；全部通过才安装副本并返回 true。
    2. DiskANN Top-K：在仍能识别真实外层距离表达式时检查自己的 GUC 和安全门；
       准备全部叶子副本，成功后统一增加叶子 ORDER BY 与 LIMIT。
    3. order-key projection：仅本 Query 的阶段一返回 true 时尝试；
       仍有外层 WHERE 则跳过，否则安全地改为叶子计算键、外层引用键的输出接口。

“先映射 WHERE、后重编号”和“先识别 Top-K、后改写外层排序键为 Var”都是关键约束。若先删除第 2 列，原 embedding 的 varattno=3 会失去正确对应关系；若先把距离操作符替换为 Var，Top-K 识别会失去原操作符。

每个阶段分别原子提交。后续阶段失败不撤销前一阶段已经安全提交的结果；但同一阶段不能提交部分叶子接口。

即使 `newNcols == oldNcols`，当前阶段一也没有提前返回：仍可能下移 WHERE、安装接口并设置扫描策略，随后进行排序键输出改写。不能按“内外列数相同”判断未触发。

## 7. 初期辅助结构草案（非当前源码定义）

以下保留设计期的数据组织草案。当前实际辅助类型为 `RequiredColumnVarMapContext`、`UnionAllOrderKey`、`DiskAnnTopKPreparedLeaf`，字段以 `preptopk.cpp` 为准；没有实现下列完整 `UnionAllColumnMap`、`UnionAllRewriteState` 结构。这些辅助状态不进入序列化 Plan：

    typedef struct UnionAllColumnMap {
        int old_ncols;
        int new_ncols;
        AttrNumber *old_to_new;      /* 0 表示删除 */
        AttrNumber *new_to_old;
        Bitmapset *final_cols;
        Bitmapset *where_cols;
        Bitmapset *order_input_cols;
        Bitmapset *semantic_cols;
        List *synthetic_order_keys;  /* UnionAllOrderKey */
    } UnionAllColumnMap;

    typedef struct UnionAllOrderKey {
        Index sortgroupref;
        Expr *outer_expr;
        List *leaf_exprs;            /* 与 leaves 一一对应 */
        Oid type;
        int32 typmod;
        Oid collation;
        AttrNumber new_resno;
        bool materialized;
    } UnionAllOrderKey;

    typedef struct UnionAllRewriteState {
        Query *outer;
        Index union_rti;
        RangeTblEntry *union_rte;
        Query *union_query;
        List *leaves;
        UnionAllColumnMap map;
        bool where_fully_pushed;
        bool topk_safe;
        const char *fallback_reason;
    } UnionAllRewriteState;

映射只在 Query 重写期间存在。提交后由连续 targetlist 本身表达物理布局。

## 8. 必需列收集

### 8.1 初始集合

对目标 UNION RTE 收集：

    required =
        Vars(外层非 resjunk 最终 targetlist)
      ∪ Vars(外层 WHERE)
      ∪ Vars(外层 ORDER BY 对应 TargetEntry)
      ∪ safety_required

LIMIT 表达式通常不引用 UNION 输出；若出现相关 Var 或复杂表达式，第一期拒绝而不是猜测。

### 8.2 WHERE 成功分支化后的缩减

若同一个 WHERE 已安全映射到每个叶子，并满足删除外层重检的条件：

    required -= 仅由外层 WHERE 引入的列

这些列仍存在于对应叶子的 Filter 输入中，但不再是集合输出。例如：

    SELECT row_id
    FROM view
    WHERE title LIKE 'a%';

可变成：

    delta scan/filter: 读取 title，输出 row_id
    snapshot scan/filter: 读取 title，输出 row_id
    Append: 仅 row_id

### 8.3 ORDER BY 标量键物化后的缩减

对每个可安全映射的排序表达式，在每个叶子生成相同类型的标量输出：

    __ua_order_1 = embedding <-> query_vector

然后：

    required -= 仅由该 ORDER BY 表达式引入的原始列
    required += __ua_order_1

外层排序 TargetEntry 改为引用 UNION 的新标量列，并保持 resjunk，用户最终仍只看到 row_id。

如果表达式不能安全映射，则保留原始依赖列。例如 embedding 继续作为集合输出，但 title 仍可删除。

### 8.4 叶子内部依赖

集合输出需求和叶子内部需求必须分开：

- snapshot.row_id 既是最终输出，也是反连接键；
- delete.row_id 只是反连接键；
- embedding 是距离函数输入；
- 叶子 WHERE 使用的列即使不输出，也必须留在扫描/过滤节点；
- RLS、security qual、分布键、分区键、rowmark junk 列按现有 planner 规则保留。

不得把 delete_t 的内部列错误加入 UNION 输出。

## 9. WHERE 下推与外层重检消除

### 9.1 等价性

纯 UNION ALL 满足：

    filter(P, R1 UNION ALL ... UNION ALL Rn)
      = filter(P, R1) UNION ALL ... UNION ALL filter(P, Rn)

前提是每个叶子的列映射、类型、typmod、collation 和表达式语义完全一致。

### 9.2 安全条件

第一期沿用并收紧现有 check_outer_qual_pushdown_safe()：

- 外层 WHERE 必须存在；
- 只引用目标 UNION RTE 的普通列；
- 无 whole-row Var、system attribute、跨层 Var；
- 无 SubLink、aggregate、window、SRF；
- 无 volatile 函数；
- security_barrier、RLS 安全边界或非 leakproof 风险时不删除外层谓词；
- 每个叶子的对应表达式类型、typmod、collation 一致；
- 所有叶子映射成功后才提交。

### 9.3 两种结果

精确下推成功：

- WHERE 加到所有叶子；
- 外层 jointree->quals 删除；
- 不再出现重复的 Subquery Scan Filter；
- 仅供 WHERE 使用的字段在叶子内最后一次使用后立即删除。

只能保守下推或不能证明：

- 外层 WHERE 保留；
- WHERE 引用列属于必要列，必须通过 UNION；
- 仍可删除与最终输出、WHERE、ORDER BY 都无关的字段；
- EXPLAIN DEBUG1 记录未删除重检的原因。

## 10. ORDER BY 与排序键生命周期

### 10.1 ORDER BY 不等于 Top-K

ORDER BY 存在但 LIMIT 不存在时：

- 可以把距离表达式物化为每个叶子的 float8 输出；
- 不向叶子强制增加 Sort；
- 不增加叶子 LIMIT；
- 全局排序保留；
- CBO 仍可自行选择有序索引路径、MergeAppend 或 Append + Sort。

### 10.2 原始 vector 的处理

embedding 对距离计算是必要字段，因此不能承诺源表完全不访问它。必须承诺：

- 不需要 ORDER BY 且 WHERE/输出均不引用 embedding 时，任何源扫描 Output 都不出现 embedding；
- 需要距离时，源扫描或 ANN 索引可以读取 embedding；
- 一旦得到 float8 距离值，分支对上的输出只保留 row_id + distance；
- 完整 vector 不进入全局 Append、Sort、Limit 或 Stream。

对 delta 简单分支，距离表达式通常可直接成为 Scan/AnnIndexScan 输出。对 snapshot 反连接分支，snapshot scan 可输出 row_id + embedding，Anti Join 的分支输出改为 row_id + distance；title 从最底层即消失。

后续若要把距离计算进一步压到 Anti Join 的 snapshot 输入侧，可使用 ProjectionPath 做“最后消费者下沉”，但这不是解决 title 泄漏的前置条件。

### 10.3 完整排序语义

必须整体映射所有排序键，保留：

- 排序键次序；
- sort operator；
- ASC/DESC；
- NULLS FIRST/LAST；
- collation；
- 唯一键 tie-break。

Top-K 首版仍按现有 DiskANN 约束接受的排序方向和 NULL 规则执行；普通列裁剪本身不需要限制为 DiskANN。

## 11. 物理压紧算法

### 11.1 构造新集合接口

在 planner 私有副本中：

1. 遍历 unionQuery->targetList 的非 resjunk 项；
2. 保留 required 中的原始列，按原相对顺序排列；
3. 追加可物化的 synthetic order keys；
4. 将 resno 重排为 1..new_ncols；
5. 同步生成 old_to_new 与 new_to_old。

示例：

    原集合输出：
      1 row_id
      2 title
      3 embedding

    SELECT row_id ... WHERE row_id > 5：
      old_to_new = [1, 0, 0]
      新输出 = [row_id]

    SELECT row_id ... WHERE row_id > 5 ORDER BY distance：
      old_to_new = [1, 0, 0]
      新输出 = [row_id, __ua_order_1(float8)]

### 11.2 同步修改每个 SetOperationStmt

仅支持 op=SETOP_UNION 且 all=true。对树中每个 SetOperationStmt：

- colTypes 按新列集重建；
- colTypmods 按新列集重建；
- colCollations 按新列集重建；
- groupClauses 必须为 NIL；
- larg、rarg 递归保持相同物理列数与顺序。

UNION、INTERSECT、EXCEPT 不允许执行此压紧，因为去重/集合语义可能依赖所有输出列。

### 11.3 同步修改叶子 Query

每个叶子执行：

- 对原始列保留对应 TargetEntry 表达式；
- 删除无需求列；
- 追加该叶映射后的标量排序表达式；
- 保留真正必要的 resjunk、rowmark、volatile/SRF 项；
- 重建连续 resno；
- 重建 sortClause 对应的 ressortgroupref；
- 保持 jointree、NOT EXISTS、RLS、权限信息不变。

叶子原始 RTE 的 selectedCols 不应因执行投影裁剪而减少，避免改变权限检查语义；只压缩本次规划使用的 Query 输出接口。

### 11.4 同步修改外层引用

使用受限 expression_tree_mutator，只改：

    varlevelsup == 0
    varno == union_rti
    varattno > 0

普通保留列按 old_to_new 改 varattno。ORDER BY 对应 TargetEntry 改成新 synthetic key 的 Var。同步压紧 union RTE 的 eref->colnames，仅影响本次 planner 副本。

whole-row Var(varattno=0) 直接拒绝优化。

### 11.5 原子提交

所有验证和构造都在副本中完成。提交前检查：

- 所有叶子可见输出数相同；
- 每个位置的 type、typmod、collation 相容；
- resno 连续；
- 每个 sortClause 可找到 ressortgroupref；
- 每个外层 Var 的新 attno 有效；
- SetOperationStmt 三个列元数据列表长度一致；
- 没有引用已删除列。

只交换 outer targetlist、jointree、目标 RTE 的 subquery/eref 等已准备指针。任一检查失败时完全不修改原 Query。

## 12. 与现有 DiskANN Top-K 的整合

当前在 `preptopk.cpp` 复用结构发现、Catalog 校验和叶子表达式映射，由 `planner.cpp` 统一编排，而不是新增另一个总入口：

    bool rewritten = preprocess_union_all_required_columns(root);
    preprocess_union_all_diskann_topk(root);
    if (rewritten)
        preprocess_union_all_order_key_projection(root);

`rewritten` 只表示本 Query 第一阶段成功，不能用可继承的 `root->union_all_required_columns_pruned` 代替。排序键输出函数还检查外层残留 quals 等条件。

当 `enable_union_all_diskann_topk_pushdown=on` 且 ORDER BY + LIMIT 合法时：

- 局部 ORDER BY 使用叶子已经生成的真实距离表达式；
- 局部 LIMIT 位于叶子的 WHERE、JOIN、NOT EXISTS 之后；
- 全局 ORDER BY + LIMIT 的语义保留；后续可能以 Merge Append 满足顺序，不能要求永远有一个独立物理 Sort；
- 列裁剪及排序键输出也成功时，UNION 仅携带上层仍需的原始列和排序键；仍被最终 SELECT 使用的 vector 等列不能删除。

snapshot 分支必须保持：

    snapshot/filter/delete anti join
      -> 计算或输出 distance
      -> local Sort/ordered ANN path
      -> local Limit

禁止先取 snapshot Top-K 再执行 delete anti join，否则删除记录会占用候选名额。

本设计不自动解决“带 Anti Join 的 snapshot 分支无法选择 DiskANN 有序路径”。那是有序反连接或过滤式 ANN 的另一项优化；本设计先保证即使该分支使用普通扫描和 Sort，也不再携带 title 等无关列。

## 13. 四种场景的预期计划

以下是结构性预期，不强制具体 Join 算法或索引名称。

### 13.1 WHERE only

    Append
      Output: row_id
      -> delta scan
           Output: row_id
           Filter/Index Cond: outer_where
      -> snapshot Anti Join
           Output: s.row_id
           -> snapshot scan
                Output: s.row_id
                Filter/Index Cond: outer_where
           -> delete scan
                Output: d.row_id

禁止出现：

- delta_t.title、delta_t.embedding；
- snapshot_t.title、snapshot_t.embedding；
- delete_t 除内部谓词和连接键之外的输出。

### 13.2 WHERE + ORDER BY

    Sort
      Output: row_id, __ua_order_1
      Sort Key: __ua_order_1
      -> Append
           Output: row_id, __ua_order_1
           -> delta branch
                Output: row_id, distance
           -> snapshot Anti Join
                Output: row_id, distance
                -> snapshot scan
                     Output: row_id, embedding
                -> delete scan
                     Output: row_id

title 在任意层都不得出现。embedding 只能存在于计算 distance 之前，不得出现在 Append 和全局 Sort 输入中。

### 13.3 WHERE + LIMIT

    Limit
      Output: row_id
      -> Append
           Output: row_id
           -> delta branch: row_id
           -> snapshot Anti Join: row_id

只有全局 Limit；不得新增分支 Limit。

### 13.4 WHERE + ORDER BY + LIMIT

    Global Limit
      -> Global Sort
           Output: row_id, __ua_order_1
           -> Append
                Output: row_id, __ua_order_1
                -> Local Limit
                     -> Ann Index Scan or Scan + Local Sort
                          Output: row_id, distance
                -> Local Limit
                     -> Local Sort/ordered path
                          -> snapshot Anti Join
                               Output: row_id, distance
                               -> snapshot scan
                                    Output: row_id, embedding
                               -> delete scan
                                    Output: row_id

精确 WHERE 下推后，不应再出现重复的 Subquery Scan Filter。

## 14. 初期模块拆分与代码量估算（历史草案）

本节保留实现前的估算，不能用作当前迁移清单。实际未新增 `prepunioncols.cpp`，也未按此建议修改 `allpaths.cpp`；代码集中在 `preptopk.cpp`。实际必须包含 `createplan.cpp::use_physical_tlist()` 的定向保护及 `PlannerInfo` 标志，完整活动文件、原生用例和构建入口见[设计总入口](README.md) §3。

### 14.1 新增模块

建议新增：

- src/gausskernel/optimizer/prep/prepunioncols.cpp
- 对应 optimizer Makefile 对象
- src/include/optimizer/prep.h 中的入口声明

职责：

- 结构识别；
- 必需列和生命周期分析；
- WHERE/ORDER 映射；
- UNION ALL Query 树压紧；
- 原子提交；
- DEBUG1 拒绝原因。

### 14.2 修改现有模块

| 文件 | 改动 |
|---|---|
| optimizer/plan/planner.cpp | 在 pull_up_subqueries 前调用统一入口 |
| optimizer/prep/preptopk.cpp | 抽出可复用的结构、Catalog、表达式映射与 Top-K 校验；接受已压紧的叶子上下文 |
| optimizer/path/allpaths.cpp | 保留普通子查询逻辑；增加注释/断言，明确纯 UNION ALL 已在 prep 阶段压紧，不以 typed NULL 作为本特性结果 |
| include/optimizer/prep.h | 新入口及内部共享声明 |
| common/backend/utils/misc/guc/guc_sql.cpp | 新增灰度 GUC |
| include/knl/knl_guc/knl_session_attr_sql.h | 保存 GUC |
| test/regress | 新 SQL、expected 和 schedule 项 |

若实现验证表明 Query 树压紧后通用 planner 能正确重算 PathTarget.width，则无需修改 prepunion.cpp、createplan.cpp、setrefs.cpp 和 Executor。这是首选结果。

只有在发现某条 openGauss 特有路径缓存旧 targetlist 时，才定向修复该路径；不预先把 old/new attno 映射扩散到 Plan 节点。

### 14.3 代码量分解

| 部分 | 生产代码估算 |
|---|---:|
| 结构识别、依赖收集、安全校验 | 180～260 行 |
| WHERE 映射与重检消除 | 120～180 行 |
| ORDER 标量键生成和生命周期处理 | 130～220 行 |
| SetOperationStmt/叶子/外层引用压紧 | 220～320 行 |
| Top-K 重构接入 | 100～160 行 |
| GUC、日志、声明、构建接入 | 50～80 行 |
| 合计 | 800～1,200 行 |

回归 SQL 和 expected 约 400～700 行。估算误差约 ±20%，主要取决于分布式 Stream/RemoteQuery 是否暴露旧列号假设。

## 15. GUC 与兼容策略

新增：

    enable_union_all_required_columns_pushdown

当前默认 off，完整回归后再评估默认 on。它独立于：

    enable_union_all_diskann_topk_pushdown

组合行为：

| 必需列开关 | Top-K 开关 | 行为 |
|---|---|---|
| off | off | 不执行两个特性的改写；仍有当前版本的原生优化和公共安全修复 |
| on | off | WHERE 场景做列裁剪/安全谓词分支化及可选排序键输出；不由本特性新增分支 ORDER BY、LIMIT |
| off | on | 执行当前安全检查后的 Top-K 行为，可能继续宽列传输 |
| on | on | 列压紧及 WHERE → Top-K 判断 → 排序键输出；每一步须满足各自条件 |

长期应让列裁剪成为通用优化，不依赖 ANN GUC。

## 16. 安全边界与降级

第一期完整压紧仅支持：

- CMD_SELECT；
- 外层单一目标 UNION 子查询/视图；
- 外层 WHERE 非空；
- 集合树全部为 UNION ALL；
- 叶子为普通 SELECT，可含 WHERE、JOIN、NOT EXISTS；
- 无 outer join 语义歧义；
- 输出列位置和类型可在所有叶子一一映射。

以下情况整体跳过或只做已证明安全的部分：

- whole-row Var 或 system column 等无法按可见输出位置安全映射的引用；`SELECT *` 已展开成普通列时不因选择全部列而自动跳过；
- UNION distinct、INTERSECT、EXCEPT；
- recursive UNION；
- security barrier、无法证明安全的 RLS/权限边界；
- 外层聚合、GROUP BY、HAVING、DISTINCT、Window、row lock、DML；
- lateral/correlated 外层引用；
- 目标列表含 SRF 时保守拒绝；未使用的 volatile 等有语义输出不能直接删除，当前会保留必要输出或按对应安全门降级；
- 叶子自身 DISTINCT、GROUP、Window、OFFSET、WITH TIES 等会影响行语义的结构；
- 类型、typmod、collation 不一致；
- system column 或 whole-row Var；
- 分布键、分区键、RemoteQuery 必须使用而需求收集未覆盖的列。

降级原则：

1. 不能删除外层 WHERE 时，把 WHERE 引用列视为必要列；
2. 不能物化排序键时，保留 ORDER BY 原始输入列；
3. 不能做 Top-K 时，保留全局 ORDER BY/LIMIT；
4. 任一重编号一致性检查失败时，不提交物理压紧；
5. 不允许生成“部分叶子已改、部分叶子未改”的 Query。

## 17. Cost、内存和分布式要求

物理压紧后必须基于新 targetlist 重新计算：

- PathTarget.width；
- Sort 每行宽度和内存成本；
- Material/Hash 的 tuple width；
- Stream/RemoteQuery 序列化宽度；
- Append 和 SubqueryScan 的 tuple descriptor；
- vector 与 row executor 的 projection info。

不得沿用原 row_id + title + embedding 的 cached width，否则即使执行输出变窄，CBO 仍会按宽行错误选择计划。

本特性不保证估算行数或候选成本数值不变：WHERE 移动、重检消除和列宽变化会让后续规划器重新估算并选择路径。它不另造行数估计算法，正确性要求不改变的是：

- SQL 规定的过滤和行数语义；
- DiskANN 召回语义；
- MVCC、RLS 和权限检查；
- UNION ALL 重复行语义；
- 最终排序与 LIMIT 语义。

## 18. 可观测性

不新增执行计划节点；EXPLAIN VERBOSE 的 Output 就是主要验收接口。

当前实际 DEBUG1 成功日志形状如下，数值随 SQL 而变，并非每次都出现全部三行：

    union_all_required_columns: old=3 new=2 where_pushed=true outer_recheck=false
    preprocess_union_all_diskann_topk: successfully pushed ORDER BY + LIMIT to 2 leaves
    union_all_required_columns: order_keys=1 raw_columns=1 materialized=true

Top-K GUC 关闭时，该入口的日志为 `preprocess_union_all_diskann_topk: GUC disabled`；排序键 `materialized=true` 不能当成 Top-K 成功标志。阶段一 `old=3 new=3` 也可能成功改写。

阶段一拒绝日志中的实际 reason 包括：

    outer_query_shape
    unsafe_union_rte
    non_union_all
    inconsistent_output_shape
    unsupported_outer_reference
    compact_validation_failed

不是每个后续安全返回都有细分日志。这里不把尚未实现的 `dropped` 集合、`topk_applied` 或额外 reason 当成当前可观测接口。

DEBUG 日志不得打印查询向量全文或用户字段值。

## 19. 测试方案

用户已明确小数据即可。正确性和计划形状使用当前 20/20/3 行数据就足够；不需要为了本特性制造大数据或性能基准。

### 19.1 核心四象限

对以下四条 SQL 分别测试开关 off/on：

    SELECT row_id FROM view WHERE row_id > 5;

    SELECT row_id FROM view
    WHERE row_id > 5
    ORDER BY embedding <-> vector_const;

    SELECT row_id FROM view
    WHERE row_id > 5
    LIMIT 10;

    SELECT row_id FROM view
    WHERE row_id > 5
    ORDER BY embedding <-> vector_const
    LIMIT 10;

验收：

- 结果用 EXCEPT ALL 双向比较；
- 有顺序的用 row_id 作为 tie-break 后比较有序结果；
- title 不出现在任意 Plan Output；
- 无 ORDER BY 时 embedding 不出现在任意 Plan Output；
- 有 ORDER BY 时 embedding 不越过分支排序键计算点；
- delete_t 只输出连接/过滤必需列；
- LIMIT only 不出现局部 Limit；
- ORDER + LIMIT 且安全时才出现每叶局部 Limit；
- 全局 Sort/Limit 不被删除。

### 19.2 WHERE 字段生命周期

增加：

- WHERE row_id；
- WHERE title；
- WHERE embedding 的安全标量条件；
- WHERE 同时引用多列；
- 无法安全下推的表达式。

当 WHERE 使用 title 且成功下推时，允许 scan Filter 访问 title，但 scan 输出和 Append 输出不能含 title。

### 19.3 投影矩阵

覆盖：

- SELECT row_id；
- SELECT title；
- SELECT row_id, title；
- SELECT embedding；
- 计算表达式；
- SELECT *；
- whole-row；
- 重复引用和别名；
- 列顺序重排。

### 19.4 集合与叶子结构

覆盖：

- 2、4、8 个 UNION ALL 叶子；
- 左深和右深 SetOperationStmt；
- delta 普通表；
- snapshot + NOT EXISTS(delete)；
- delete 侧额外 WHERE 列；
- 叶子 JOIN；
- 类型 coercion、typmod、collation；
- NULL 常量列；
- 分区表和分布式计划。

### 19.5 安全拒绝

覆盖：

- UNION、INTERSECT、EXCEPT；
- volatile 输出；
- SRF 输出；
- security barrier；
- RLS；
- correlated/LATERAL；
- GROUP/DISTINCT/Window；
- row lock；
- OFFSET、WITH TIES；
- whole-row；
- 不匹配类型/collation。

### 19.6 Top-K 组合

覆盖：

- Top-K GUC on/off；
- 必需列 GUC on/off；
- ORDER only；
- LIMIT only；
- ORDER + LIMIT；
- DiskANN 可用/不可用；
- 参数化 query vector；
- 参数化 LIMIT；
- snapshot Anti Join 后局部 Limit 的语义；
- 距离相同情况下的稳定 tie-break。

### 19.7 执行器与成本

除结果外检查：

- EXPLAIN VERBOSE Output；
- EXPLAIN ANALYZE 实际行数；
- Sort width/内存；
- Stream/RemoteQuery 输出列；
- row/vector executor；
- prepared statement 与计划缓存；
- 开关切换不复用不兼容 cached plan。

## 20. 验收标准

功能完成必须同时满足：

1. 目标四种 SQL 全部结果等价；
2. SELECT row_id 且 WHERE 不引用 title 时，delta_t、snapshot_t、delete_t 的任意 Output 均不出现 title；
3. 无 ORDER BY 且其他表达式不引用 embedding 时，任意 Output 均不出现 embedding；
4. ORDER BY 使用 embedding、排序键输出安全成功且原始 embedding 无其他消费者时，Append 及其上层只携带该排序键，不携带完整 embedding；
5. delete_t 仅携带 Anti Join 和本地过滤真正需要的列；
6. WHERE 精确下推成功后，外层不再重复执行同一 Filter；
7. LIMIT without ORDER BY 不产生分支 LIMIT；
8. ORDER BY + LIMIT 的局部 LIMIT 始终位于 snapshot 删除过滤之后；
9. GUC off 停止对应特性改写；不能把它等同于撤销公共安全修复或恢复迁移前旧二进制的计划；
10. 单机编译、基础回归通过，无崩溃、无列号断言、无类型/collation 错误；分布式验收仍是 P3 未完成项，不能记为已通过。

## 21. 分阶段实施

### P0：通用列压紧

- 新 GUC；
- 识别目标 WHERE + pure UNION ALL；
- 只按最终输出、WHERE、ORDER 依赖压紧；
- 修复 snapshot 和 Top-K 后 delta 的 title 泄漏；
- 四象限正确性和 EXPLAIN 回归。

### P1：WHERE 生命周期终止

- 复用通用谓词安全判断；
- 全叶精确下推后删除外层重检；
- WHERE-only 字段不越过 UNION；
- 安全屏障/RLS 降级回归。

### P2：标量排序键

- 分支生成 float8 distance；
- Append/Sort 不携带 vector；
- 与现有 DiskANN Top-K 原子整合；
- ORDER only 与 ORDER + LIMIT 回归。

### P3：分布式与默认开启评估

- Stream/RemoteQuery width；
- prepared plan/GPC；
- 多 DN 回归；
- 根据稳定性决定新 GUC 是否默认 on。

不建议交付只使用 typed NULL 的独立阶段，因为它容易被误认为已经满足“物理不传输”。如开发调试临时采用，必须明确标注为过渡实现，不能通过最终验收。

## 22. 初期评审决策记录（历史草案）

1. 第一版是否只支持外层单一 UNION 视图，还是同时支持 JOIN 中的 UNION RTE。建议先支持单一 RTE，与当前目标 SQL 一致。
2. WHERE 精确下推是否直接复用 allpaths.cpp 的安全判断，还是把共享逻辑抽到新模块。建议抽公共 helper，避免 Top-K 与通用谓词下推出现两套不同安全口径。
3. ORDER BY 标量键是否第一期就做。若只解决 title，P0 可先完成；但要完整满足“完整 vector 不层层传输”，应纳入 P2。
4. 新 GUC 默认值。建议开发期 off，回归和灰度完成后再改 on。
5. snapshot Anti Join 的 DiskANN 有序路径是否另立设计。建议另立，不把列裁剪交付绑在该高风险优化上。

本设计的最终目标不是让 EXPLAIN 看起来少一列，而是让每个字段只活到它的最后一个真实消费者：title 无消费者就从源扫描输出消失；delete 字段止于删除过滤；embedding 止于距离计算；只有 row_id 和标量排序键进入 UNION 之上的公共数据通道。
