# UNION ALL 列裁剪与 ANN 距离复用：联合调用栈

本文按一条 SQL 从规划到执行的先后顺序，说明当前 openGauss 源仓中列裁剪、排序键复用、UNION ALL Top-K 下推、Anti Join 源头距离投影和 DiskANN 距离复用如何接起来。这里的“调用栈”是**跨阶段的调用关系与时序**；优化器与执行器不在同一个瞬时 C/C++ 栈帧中。

代码对应 `openGauss-server` 的 `integration/multimodal-stacked` 工作树；详细实现分别见 [列裁剪源码解读](UNION_ALL必需列源头裁剪源码解读.md) 与 [ANN 距离复用源码深度解读](ANN距离复用源码深度解读.md)。以下计划例子来自仓内 [开关矩阵回归用例](../../../src/test/regress/sql/union_all_ann_feature_matrix.sql) 及其 [期望输出](../../../src/test/regress/expected/union_all_ann_feature_matrix.out)。

## 1. 目标 SQL 与三个开关

```sql
CREATE VIEW union_view AS
SELECT row_id, title, embedding FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS (
    SELECT 1 FROM delete_t d WHERE d.row_id = s.row_id
);

SET enable_union_all_required_columns_pushdown = on;
SET enable_union_all_diskann_topk_pushdown = on;
SET ann_orderby_distance_reuse = cosine;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <=> '[1,1,1,1]'
LIMIT 4;
```

这里的 `<=>` 是**源仓 openGauss** 的 Cosine 操作符；迁移到使用 `<+>` 的目标仓时应按目标仓的操作符与索引算子类适配。`WHERE` 决定这类列裁剪改写是否适用；`ORDER BY` 提供待投影和复用的距离；`LIMIT` 使本例能够走分支 Top-K 下推。三个 GUC 各控制自己的入口，不应把 Top-K 开关视为距离复用开关。

## 2. 规划期调用链

```text
planner()
└─ standard_planner()                                      planner.cpp
   ├─ subquery_planner()
   │  ├─ preprocess_union_all_required_columns()       preptopk.cpp
   │  │  ├─ 将安全的外层 WHERE 推到 UNION ALL 各叶子
   │  │  ├─ 压缩 UNION ALL 的输出列
   │  │  └─ 改写成功：root->union_all_required_columns_pruned = true
   │  │
   │  ├─ preprocess_union_all_diskann_topk()           preptopk.cpp
   │  │  └─ 符合条件时把 ORDER BY + LIMIT 推到各叶子
   │  │
   │  ├─ preprocess_union_all_order_key_projection()   preptopk.cpp
   │  │  └─ 排序键复用：各分支输出标量距离，外层 ORDER BY 引用该输出
   │  │
   │  └─ internal_grouping_planner()
   │     ├─ query_planner()：生成并选择 Path
   │     └─ create_plan()：将选中的 Path 生成 Plan
   │        ├─ create_scan_plan()
   │        │  └─ use_physical_tlist()：已裁剪的源表不再扩成物理整行
   │        └─ 返回 top_plan；snapshot 分支可形成 Scan → Sort → Anti Join → Limit
   │
   ├─ 继续处理 subquery_planner() 返回的 top_plan
   │  └─ push_down_union_all_source_projections()       planner.cpp
   │     └─ push_down_union_all_source_projection_walker()
   │        ├─ 查找 Anti Join 的保留侧及其到源 Scan 的透明节点链
   │        ├─ union_all_safe_vector_distance() 检查候选距离表达式
   │        ├─ union_all_join_uses_source_elsewhere() 检查向量无其他消费者
   │        └─ union_all_replace_source_chain()
   │           └─ 将候选距离表达式送到源 Scan 的 targetlist
   │
   ├─ set_plan_references()                                setrefs.cpp
   │  └─ 上层对该距离输出的使用改为计划槽位引用
   │
   └─ rewrite_ann_orderby_distance_refs()                  planner.cpp
      └─ rewrite_ann_orderby_distance_walker()
         ├─ ann_orderby_join_allows_approx()
         │  └─ 对近似 Cosine：检查 Join 条件是否消费距离
         └─ 在 AnnIndexScan.targetlist 中找到与 indexorderbyorig 相同的表达式
            └─ 用 AnnOrderByRef 替换这一个扫描层 TargetEntry
```

这是同一次 `standard_planner()` 调用的连续流程：它先调用 `subquery_planner()` 完成 Query 改写、路径选择和 Plan 生成；`subquery_planner()` 返回 `top_plan` 后，原来的 `standard_planner()` 栈帧继续执行源头投影下沉、`set_plan_references()` 和 ANN 距离引用注入。图中的“继续处理”是调用返回后的顺序执行，不是第二次调用 `standard_planner()`。

列裁剪成功标志由 [preptopk.cpp](../../../src/gausskernel/optimizer/prep/preptopk.cpp) 设置；它表示**查询块通过改写并提交了结果**，不等同于“输出列数一定减少”。[createplan.cpp](../../../src/gausskernel/optimizer/plan/createplan.cpp) 的 `use_physical_tlist()` 使用该标志，避免扫描输出重新变宽。后续排序键投影只在本次查询块改写成功时尝试；`push_down_union_all_source_projections()` 则受该标志控制，并且在 `set_plan_references()` **之前**运行。`rewrite_ann_orderby_distance_refs()` 在 `set_plan_references()` **之后**运行，且只有距离复用 GUC 非 `off` 才进入。

### `union_all_replace_source_chain()` 如何将距离投影下沉

`union_all_replace_source_chain()` 的任务不是直接修改 Anti Join，而是把 Anti Join 保留侧从源 Scan 到 Join 之间所传递的“原始向量槽位”改成“标量距离槽位”。下面不使用假设计划，直接使用仓内回归 [union_all_ann_feature_matrix.sql](../../../src/test/regress/sql/union_all_ann_feature_matrix.sql) 及对应 [union_all_ann_feature_matrix.out](../../../src/test/regress/expected/union_all_ann_feature_matrix.out) 中已执行产生的 SQL 和完整计划。

#### 真实表、视图与查询

回归用例故意不为 `row_id` 建主键，以稳定形成 `Sort -> Scan` 的 Anti Join 保留侧形态。与本节直接相关的 DDL 是：

```sql
CREATE TABLE delta_t
(
    row_id bigint,
    title text,
    embedding vector(4)
);

CREATE TABLE snapshot_t
(
    row_id bigint,
    title text,
    embedding vector(4)
);

CREATE TABLE delete_t
(
    row_id bigint
);

CREATE INDEX delta_l2_diskann ON delta_t
    USING diskann (embedding vector_l2_ops);
CREATE INDEX snapshot_l2_diskann ON snapshot_t
    USING diskann (embedding vector_l2_ops);

CREATE VIEW union_view AS
SELECT row_id, title, embedding
FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS
(
    SELECT 1
    FROM delete_t d
    WHERE d.row_id = s.row_id
);
```

用例使用下列计划稳定参数：

```sql
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = on;
SET enable_nestloop = off;
```

目标查询是：

```sql
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]'
LIMIT 4;
```

为了单独观察列裁剪及源距离投影，两次规划都保持 Top-K 已开启、ANN 距离复用已关闭，只切换 `enable_union_all_required_columns_pushdown`。因为 `ann_orderby_distance_reuse=off`，下面计划中的变化不能归因于 `AnnOrderByRef`。

#### 真实计划：列裁剪关闭

```sql
SET enable_union_all_diskann_topk_pushdown = on;
SET enable_union_all_required_columns_pushdown = off;
SET ann_orderby_distance_reuse = off;
```

回归实际输出的完整计划为如下。为了对应函数参数，下面在三行 `Output` 后增加了 `source/candidate` 箭头；箭头及其后文字是本文注释，不属于 gsql 原始输出，其余计划内容保持回归输出原样。此时列裁剪开关为 `off`，函数实际不会执行；箭头标出的是开启后将成为函数参数的改写前对应项：

```text
Limit
  Output: "*SELECT* 1".row_id, (("*SELECT* 1".embedding <-> '[0,0,0,0]'::vector))
  ->  Sort
        Output: "*SELECT* 1".row_id, (("*SELECT* 1".embedding <-> '[0,0,0,0]'::vector))
        Sort Key: (("*SELECT* 1".embedding <-> '[0,0,0,0]'::vector))
        ->  Result
              Output: "*SELECT* 1".row_id, ("*SELECT* 1".embedding <-> '[0,0,0,0]'::vector)
              ->  Append
                    ->  Subquery Scan on "*SELECT* 1"
                          Output: "*SELECT* 1".row_id, "*SELECT* 1".embedding
                          Filter: ("*SELECT* 1".row_id > 2)
                          ->  Limit
                                Output: delta_t.row_id, delta_t.title, delta_t.embedding, ((delta_t.embedding <-> '[0,0,0,0]'::vector))
                                ->  Ann Index Scan using delta_l2_diskann on union_all_ann_feature_matrix_test.delta_t
                                      Output: delta_t.row_id, delta_t.title, delta_t.embedding, (delta_t.embedding <-> '[0,0,0,0]'::vector)
                                      Order By: (delta_t.embedding <-> '[0,0,0,0]'::vector)
                                      Filter Mode: Post-Filter
                                      Filter: (delta_t.row_id > 2)
                    ->  Subquery Scan on "*SELECT* 2"
                          Output: "*SELECT* 2".row_id, "*SELECT* 2".embedding
                          Filter: ("*SELECT* 2".row_id > 2)
                          ->  Limit
                                Output: s.row_id, s.title, s.embedding, ((s.embedding <-> '[0,0,0,0]'::vector))
                                ->  Sort
                                      Output: s.row_id, s.title, s.embedding, ((s.embedding <-> '[0,0,0,0]'::vector))
                                      Sort Key: ((s.embedding <-> '[0,0,0,0]'::vector))
                                      ->  Merge Anti Join
                                            Output: s.row_id, s.title, s.embedding, (s.embedding <-> '[0,0,0,0]'::vector)  ← candidate（Anti Join 上的候选距离 TLE）
                                            Merge Cond: (s.row_id = d.row_id)
                                            ->  Sort
                                                  Output: s.row_id, s.title, s.embedding  ← source（透明链上层 Sort 的原向量 TLE）
                                                  Sort Key: s.row_id
                                                  ->  Ann Index Scan using snapshot_l2_diskann on union_all_ann_feature_matrix_test.snapshot_t s
                                                        Output: s.row_id, s.title, s.embedding  ← source（透明链终点 Scan 的原向量 TLE）
                                                        Order By: (s.embedding <-> '[0,0,0,0]'::vector)
                                                        Filter Mode: Post-Filter
                                                        Filter: (s.row_id > 2)
                                            ->  Sort
                                                  Output: d.row_id
                                                  Sort Key: d.row_id
                                                  ->  Seq Scan on union_all_ann_feature_matrix_test.delete_t d
                                                        Output: d.row_id
(42 rows)
```

这份真实计划中，snapshot 分支源扫描仍输出：

```text
s.row_id, s.title, s.embedding
```

Anti Join 再使用 `s.embedding` 形成距离输出；`title` 也仍在下层向上传递。

#### 真实计划：开启列裁剪和源距离投影

保持其他参数不变，只执行：

```sql
SET enable_union_all_required_columns_pushdown = on;
```

再次规划同一条 SQL，回归实际输出的完整计划为如下。同样，三处箭头是文档注释，用来展示上述 `source/candidate` 经过改写和 `setrefs` 后的形态：

```text
Limit
  Output: delta_t.row_id, ((delta_t.embedding <-> '[0,0,0,0]'::vector))
  ->  Result
        Output: delta_t.row_id, ((delta_t.embedding <-> '[0,0,0,0]'::vector))
        ->  Merge Append
              Sort Key: ((delta_t.embedding <-> '[0,0,0,0]'::vector))
              ->  Limit
                    Output: delta_t.row_id, ((delta_t.embedding <-> '[0,0,0,0]'::vector))
                    ->  Ann Index Scan using delta_l2_diskann on union_all_ann_feature_matrix_test.delta_t
                          Output: delta_t.row_id, (delta_t.embedding <-> '[0,0,0,0]'::vector)
                          Order By: (delta_t.embedding <-> '[0,0,0,0]'::vector)
                          Filter Mode: Post-Filter
                          Filter: (delta_t.row_id > 2)
              ->  Limit
                    Output: s.row_id, ((s.embedding <-> '[0,0,0,0]'::vector))
                    ->  Sort
                          Output: s.row_id, ((s.embedding <-> '[0,0,0,0]'::vector))
                          Sort Key: ((s.embedding <-> '[0,0,0,0]'::vector))
                          ->  Merge Anti Join
                                Output: s.row_id, ((s.embedding <-> '[0,0,0,0]'::vector))  ← candidate（setrefs 后已变成对下层距离槽位的引用）
                                Merge Cond: (s.row_id = d.row_id)
                                ->  Sort
                                      Output: s.row_id, ((s.embedding <-> '[0,0,0,0]'::vector))  ← source 改写结果（Sort 透传标量距离）
                                      Sort Key: s.row_id
                                      ->  Ann Index Scan using snapshot_l2_diskann on union_all_ann_feature_matrix_test.snapshot_t s
                                            Output: s.row_id, (s.embedding <-> '[0,0,0,0]'::vector)  ← source 改写结果（Scan 产生标量距离）
                                            Order By: (s.embedding <-> '[0,0,0,0]'::vector)
                                            Filter Mode: Post-Filter
                                            Filter: (s.row_id > 2)
                                ->  Sort
                                      Output: d.row_id
                                      Sort Key: d.row_id
                                      ->  Seq Scan on union_all_ann_feature_matrix_test.delete_t d
                                            Output: d.row_id
(34 rows)
```

与关闭时相比，这份真实计划明确展示了三件事：

1. `title` 从 delta、snapshot 以及 Anti Join 的输出链上消失；
2. snapshot 的 Ann Index Scan 不再向上输出原始 `s.embedding`，而是输出 `s.row_id` 和标量距离；
3. Ann Index Scan 层是单层括号，因为距离复用开关仍为 `off`；Anti Join 及其上方是双层括号，说明上层在引用下层已输出的距离槽位。

##### 不能用最终 EXPLAIN 的单/双括号去做 `equal()`

最终计划中这两行的文字确实不同：

```text
Merge Anti Join
  Output: s.row_id, ((s.embedding <-> '[0,0,0,0]'::vector))

Ann Index Scan
  Output: s.row_id, (s.embedding <-> '[0,0,0,0]'::vector)
```

但 `search_indexed_tlist_for_non_var()` 从未拿这两段最终 EXPLAIN 文字做比较，也不是在最终计划上跨过 Sort，直接比较 Merge Anti Join 和 Ann Index Scan。真实匹配对象是 **Merge Anti Join 与它的直接左子计划 Sort** 在 `setrefs` 之前的内部表达式树。

`set_plan_refs()` 严格按“先处理当前节点，再递归子节点”的顺序工作：

```cpp
/* 先根据当前 plan 类型修改当前节点 */
switch (nodeTag(plan)) {
    case T_MergeJoin:
        set_join_references(...);
        break;
    /* ... */
}

/* 然后才递归左右子计划 */
plan->lefttree = set_plan_refs(root, plan->lefttree, rtoffset);
plan->righttree = set_plan_refs(root, plan->righttree, rtoffset);
```

所以这份计划的真实时序是：

```text
1. union_all_replace_source_chain() 完成

   Merge Anti Join candidate.expr:
       OpExpr(s.embedding <-> query_vector)

   直接左孩子 Sort 的 source.expr:
       OpExpr(s.embedding <-> query_vector)

   Ann Index Scan 的 source.expr:
       OpExpr(s.embedding <-> query_vector)

2. set_plan_refs() 先处理 Merge Anti Join

   build_tlist_index(Sort.targetlist)
       看到的仍是未 setrefs 的完整 OpExpr

   equal(Join candidate OpExpr, Sort source OpExpr) = true

   Join candidate 被改为 OUTER_VAR[2]

3. set_plan_refs() 再递归处理 Sort

   Sort 是透传节点，不自己计算 targetlist。
   set_dummy_tlist_references() 把 Sort 第 2 列改成 OUTER_VAR[2]，
   表示直接读取 Ann Index Scan 的第 2 列。

4. set_plan_refs() 最后处理 Ann Index Scan

   Scan 是真正产生第 2 列的节点；距离复用关闭时，
   该列仍保留为直接求值的 OpExpr。
```

因此，最终内部节点实际是：

```text
Merge Anti Join 第 2 列:  OUTER_VAR[2]        -- 引用 Sort 第 2 列
Sort 第 2 列:             OUTER_VAR[2]        -- 引用 Scan 第 2 列
Ann Index Scan 第 2 列:   OpExpr(<->)         -- 真正产生值
```

EXPLAIN 为了可读性，会把 `OUTER_VAR[2]` 反解析回它所引用的子计划输出表达式，所以 Join 和 Sort 最终显示成双括号，Scan 的直接 `OpExpr` 显示成单括号。括号层数是最终计划的反解析线索，不是 `equal()` 的输入；`equal()` 比较的是第 2 步尚未被转换的两棵 `OpExpr` 表达式树。

上述是用户可见的真实 SQL 和完整 EXPLAIN。下文的 `chain`、`candidate`、`sourceVar` 和 `resno` 是对这份真实计划在 `set_plan_references()` 之前的 C 结构映射，用来说明代码如何产生上述输出；它们不是伪造的 EXPLAIN 文本。

函数定义在 [planner.cpp](../../../src/gausskernel/optimizer/plan/planner.cpp)：

```cpp
static bool union_all_replace_source_chain(
    List* chain,
    TargetEntry* candidate,
    const Var* sourceVar)
```

本例的三个参数是：

```text
chain:
  [
    Sort(snapshot 保留侧),
    AnnIndexScan(snapshot_t)
  ]

candidate:
  Anti Join targetlist 中的 TargetEntry
  expr = s.embedding <-> '[0,0,0,0]'

sourceVar:
  s.embedding
```

`chain` 由 `union_all_collect_source_chain()` 从 Anti Join 保留侧开始，按从上到下的顺序收集。它不包含 Anti Join 本身、`delete_t` 一侧以及 Join 上方的 Append 或 Limit。目前允许的中间透明节点是 `Sort`、`Material` 和 `Hash`，链的终点可以是 `SeqScan`、`IndexScan`、`AnnIndexScan`、`BitmapHeapScan`、`TidScan` 或 `TidRangeScan`。

函数分两个阶段工作：第一遍验证并收集需要修改的 `TargetEntry*`，第二遍才统一修改。这种结构保证失败时不会留下只改一半的计划树。

#### 结合真实计划逐行读完整函数

下面直接在当前工作树 [planner.cpp](../../../src/gausskernel/optimizer/plan/planner.cpp) 的完整函数中加入中文解读。除注释外，执行语句与当前实现一致。

阅读时对照上面的 34 行真实计划：链上的 Sort 是 Merge Anti Join 左侧、按 `s.row_id` 排序的 Sort；再下面是 `snapshot_l2_diskann`。Join 上方按距离排序的 Sort，以及右侧 delete_t 的 Sort，都不在这条链里。

```cpp
/*
 * 对应本节真实计划的 snapshot 分支：
 *
 * Merge Anti Join
 *   candidate：本层输出的 s.embedding <-> '[0,0,0,0]'::vector
 *   左孩子 Sort（Sort Key: s.row_id）
 *     source：准备被替换的 s.embedding 输出项
 *     左孩子 AnnIndexScan(snapshot_l2_diskann)
 *       source：准备被替换的 s.embedding 输出项
 *
 * 参数：
 *   chain     = [snapshot 侧 Sort, snapshot AnnIndexScan]，元素是 Plan*；
 *   candidate = Anti Join 上提供完整距离表达式的 TargetEntry*；
 *   sourceVar = 距离表达式内部的 Var(s.embedding)，不是完整距离表达式。
 *
 * 注意时点：
 *   函数在开启列裁剪的规划过程中、setrefs 之前执行。
 *   前面的查询改写已去掉无用的 title，链上原向量槽位是 resno=2。
 *   文档中关闭列裁剪的最终计划只是效果对照，不是本函数入口的内存快照；
 *   不能把关闭态第 3 列 embedding 的位置直接套到这里。
 *
 * 调用者已经通过下列辅助函数完成前置检查：
 *   union_all_collect_source_chain：找到 Anti Join 保留侧并收集合法透明链；
 *   union_all_safe_vector_distance：确认候选是受支持且维度安全的距离表达式；
 *   union_all_join_uses_source_elsewhere：确认 Join 其他输出和条件不用原向量。
 * 不能脱离这些检查，把本函数单独当作任意表达式下推器。
 */
static bool union_all_replace_source_chain(List* chain, TargetEntry* candidate, const Var* sourceVar)
{
    // 链表游标，不是计划节点；先遍历 chain，再复用它遍历 sources。
    ListCell* lc = NULL;

    // 暂存待修改的 TargetEntry*，初始为空。
    // chain 保存 Plan*；sources 保存去重后的 TargetEntry*，两者不能混用。
    List* sources = NIL;

    /*
     * 第一阶段：检查整条链，保存待修改对象；这一遍不修改任何 expr。
     *
     * make_sort() 可以让 Sort 与 Scan 共用同一个 targetlist。
     * 若先把 Sort 的 embedding 改成距离，Scan 所引用的同一 TLE 也变了；
     * 下一次按 Var(s.embedding) 重新查找，就可能找不到。
     * 因此必须先查齐并保存指针，再统一修改。
     */
    foreach (lc, chain) {
        // 本例第一轮是 snapshot 侧按 row_id 排序的 Sort；
        // 第二轮是该 Sort 下面的 AnnIndexScan。
        Plan* plan = (Plan*)lfirst(lc);

        // 在当前节点 targetlist 中找唯一的原向量输出项：
        // tle->expr 必须是普通 Var，且 equal(tle->expr, sourceVar)。
        // 此处找 s.embedding，不找完整距离 OpExpr。
        // 没找到，或找到多个相同原向量输出项，均返回 NULL。
        TargetEntry* source = union_all_find_source_tle(plan, sourceVar);

        // 内层遍历 targetlist 的游标，不能覆盖外层遍历 chain 的 lc。
        ListCell* tlc = NULL;

        if (source == NULL) {
            // 无法唯一定位被替换槽位，放弃本次下沉。
            // list_free 只释放临时列表容器，不释放它指向的计划 TLE。
            list_free(sources);

            // 尚未进入修改阶段，之前检查过的节点也保持原样。
            return false;
        }

        // 检查当前 Sort/Scan 的所有输出项，包括 resjunk 隐藏输出。
        foreach (tlc, plan->targetlist) {
            // 当前待检查的一个输出项。
            TargetEntry* tle = (TargetEntry*)lfirst(tlc);

            /*
             * tle != source：按指针排除被替换项自身。
             *   source 本来就是 s.embedding，不排除它就会永远拒绝。
             *
             * union_all_expr_uses_var：递归检查其他输出是否仍使用该向量。
             *   本例剩余输出 s.row_id 不使用向量，通过；
             *   若还有 vector_norm(s.embedding)，则拒绝。
             *   Join 的其他消费者由调用者提前检查，这里只检查链上输出。
             */
            if (tle != source && union_all_expr_uses_var((Node*)tle->expr, sourceVar)) {
                // 其他输出仍需要原向量，清理列表并返回失败。
                // 此时同样没有修改任何 source->expr。
                list_free(sources);
                return false;
            }
        }

        // 第一轮 Sort 要保护排序键，第二轮 AnnIndexScan 不进入此分支。
        if (IsA(plan, Sort)) {
            // 确认节点类型后，才能读取 Sort 的专用字段。
            Sort* sort = (Sort*)plan;

            // 检查全部排序键。本例只有 row_id，但代码不能只检查第一个键。
            for (int i = 0; i < sort->numCols; i++) {
                /*
                 * i 是从 0 开始的数组下标；
                 * sortColIdx[i] 和 source->resno 都是从 1 开始的输出槽位编号。
                 *
                 * 本例 sortColIdx[0]=1（row_id），source->resno=2（embedding）；
                 * 1 != 2，因此不会破坏 Sort 的排序键。
                 *
                 * 若二者相等，替换的正是 Sort 用来排序的向量槽位。
                 * 换成 float8 距离会改变排序值及类型，
                 * 原排序操作符等信息也可能不再适配，必须拒绝。
                 */
                if (sort->sortColIdx[i] == source->resno) {
                    list_free(sources);
                    return false;
                }
            }
        }

        /*
         * 当前节点已验证，按 TLE 指针地址去重。
         *
         * Sort 与 Scan 共用一个 TLE 时，sources 最后只有一个对象；
         * 若各自有独立 TLE，即使表达式内容相同，也要记录两个对象。
         * 不能用表达式 equal() 代替指针去重，否则可能遗漏独立对象。
         *
         * 去重负责防止重复赋值；
         * 防止“修改后重新查找失败”的关键，是先保存指针、后面不再查找。
         */
        if (!list_member_ptr(sources, source)) {
            // 保存原 TLE 地址，不复制 TLE，也不在此时改表达式。
            // lappend 返回更新后的列表，需要接回 sources。
            sources = lappend(sources, source);
        }
    }

    /*
     * 第二阶段：整条 chain 验证通过，开始统一修改。
     * 遍历的是已保存的 sources，不再遍历 chain 重新查找原向量。
     */
    foreach (lc, sources) {
        // 直接取得先前已验证、已按地址去重的 TLE。
        TargetEntry* source = (TargetEntry*)lfirst(lc);

        /*
         * 真正改变计划表达式的唯一赋值：
         *   修改前：source->expr = Var(s.embedding)
         *   修改后：source->expr = OpExpr(s.embedding <-> query_vector)
         *
         * copyObject 深拷贝 candidate 的表达式树；
         * 每个不同 TLE 得到独立表达式，避免后续改写相互影响。
         *
         * 只换 expr，保留 resno、resname、resjunk、ressortgroupref。
         * 本例第 2 个槽位编号不变，但结果类型从 vector 变为 float8。
         *
         * candidate 自身未在此修改，后续 setrefs 才把 Join 的候选距离
         * 替换为 OUTER_VAR[2]。此处也不计算距离，只构造规划期表达式树。
         */
        source->expr = (Expr*)copyObject(candidate->expr);
    }

    // 仅释放临时列表容器，不能用 list_free_deep。
    // TLE 仍属于计划树；传入的 chain 则由调用者释放。
    list_free(sources);

    // 表示源头距离投影下沉成功；
    // 尚未形成最终 Var 引用，也尚未注入 AM 距离复用节点 AnnOrderByRef。
    return true;
}
```

#### 第一遍：验证整条链

函数首先建立一个临时指针列表：

```cpp
List* sources = NIL;
```

对 `chain` 中每个 Plan，先通过 `union_all_find_source_tle()` 在该节点的 targetlist 中查找表达式恰好等于 `sourceVar` 的输出项：

```cpp
TargetEntry* source = union_all_find_source_tle(plan, sourceVar);
```

对于上面的 `Sort`，找到的是：

```text
resno=2  s.embedding
```

如果该节点没有这个输出，或存在多个无法唯一确定的同类输出，则整次下沉失败，不修改任何节点。

找到 `source` 后，函数检查同一节点的其他 targetlist 表达式是否仍在使用 `s.embedding`：

```cpp
foreach (tlc, plan->targetlist) {
    TargetEntry* tle = (TargetEntry*)lfirst(tlc);
    if (tle != source &&
        union_all_expr_uses_var((Node*)tle->expr, sourceVar))
        return false;
}
```

例如以下输出不允许下沉：

```text
resno=1  s.row_id
resno=2  s.embedding
resno=3  vector_norm(s.embedding)
```

因为将 `resno=2` 替换成距离后，`resno=3` 仍然需要原始向量。`union_all_join_uses_source_elsewhere()` 负责检查 Join 本身的其他消费者；这里则检查 `chain` 中各节点的输出消费者。

如果当前节点是 `Sort`，还必须保证被替换的槽位不是该 Sort 的排序键：

```cpp
for (int i = 0; i < sort->numCols; i++) {
    if (sort->sortColIdx[i] == source->resno)
        return false;
}
```

本例中 `source->resno=2`，而 Sort 依据 `s.row_id`，即 `sortColIdx[0]=1`，所以可以通过。如果它原本依据第 2 列排序，将第 2 列从 `vector` 改成 `float8` 会改变 Sort 语义，必须拒绝。

节点验证通过后，记录其 `source` 指针：

```cpp
if (!list_member_ptr(sources, source))
    sources = lappend(sources, source);
```

这里的指针去重不能省略。`make_sort()` 等实现可能让透明节点与子节点共享同一份 targetlist，因此 Sort 与 Scan 找到的 `TargetEntry*` 可能是同一个指针。按指针去重可避免同一对象被重复修改，也避免前一次修改后下一个节点再也找不到原 `Var` 的问题。

准确地说，有两个配套动作：**先收集并保存指针、修改时不再重新查找**，解决“原 Var 被改没以后找不到”的问题；**按指针去重**，保证同一个 TLE 只被修改一次。只在原来的“边查边改”循环里增加去重，仍可能在第二个别名的查找阶段失败。

结合当前真实计划中的 Sort/Scan 形态，若二者沿用 `make_sort()` 的共享 targetlist（该构造函数直接执行 `plan->targetlist = lefttree->targetlist`），列表变化如下。`U` 是用于解释的对象符号，不是运行时抓取的地址；EXPLAIN 本身不能显示指针是否共享。

```cpp
/*
 * sources 的变化（假设 Sort/Scan 共用一个原向量 TLE，记为 U）：
 *
 * 进入循环前：sources = NIL。
 *
 * 第一轮，检查 snapshot 侧 Sort：
 *   找到 source=U；其他输出不使用向量，row_id 排序键不占用向量槽位。
 *   检查通过后，sources=[U]。
 *
 * 第二轮，检查 snapshot AnnIndexScan：
 *   原表达式尚未修改，仍能找到 source=U。
 *   list_member_ptr(sources, U) 为 true，不再追加，sources 仍为 [U]。
 *
 * 第二遍统一修改：
 *   直接取 U，将 U->expr 替换为 candidate->expr 的深拷贝。
 *   只赋值一次，Sort 和 Scan 对同一 TLE 的引用同时看到距离表达式。
 */
```

如果二者的 TLE 不共享，则 `sources` 会保存两个不同指针，第二遍分别生成两份独立的距离表达式。**两节点不一定对应两次赋值；赋值次数由不同 TLE 对象数决定。**

#### 第二遍：统一替换为距离表达式

只有整条 `chain` 全部验证通过，函数才执行：

```cpp
foreach (lc, sources) {
    TargetEntry* source = (TargetEntry*)lfirst(lc);
    source->expr = (Expr*)copyObject(candidate->expr);
}
```

修改前：

```text
Sort.targetlist:
  resno=1  s.row_id
  resno=2  s.embedding

AnnIndexScan.targetlist:
  resno=1  s.row_id
  resno=2  s.embedding
```

修改后：

```text
Sort.targetlist:
  resno=1  s.row_id
  resno=2  s.embedding <-> '[0,0,0,0]'

AnnIndexScan.targetlist:
  resno=1  s.row_id
  resno=2  s.embedding <-> '[0,0,0,0]'
```

修改的只是 `TargetEntry.expr`；`resno`、`resname`、`resjunk` 和 `ressortgroupref` 等槽位身份信息保持不变。第 2 个槽位的内容和类型由 `vector` 变成了 `float8`，但整条透明链一起变更，因此不会出现上下层对同一槽位的理解不一致。`copyObject()` 为不同 TargetEntry 生成独立的表达式树，避免后续改写某一份表达式时意外影响另一个节点。

此处不能写成 `source = candidate`：那只会改局部指针，不会替换原计划 TLE 的表达式；也不应直接把整个 `candidate` TLE 塞进链上的 targetlist，因为 Join 输出项的 `resno/resjunk/ressortgroupref` 不一定就是链上那个槽位的属性。现有实现只复制 `candidate->expr`，正是为了保留每一层已有的槽位身份。

最后，回看真实计划里 Scan 的两行：

```text
Output: s.row_id, (s.embedding <-> '[0,0,0,0]'::vector)
Order By: (s.embedding <-> '[0,0,0,0]'::vector)
```

本函数替换的是 `Output` 对应的 targetlist 项，没有删除基础表的 `embedding` 列，也没有删除或改写索引的 `Order By`。Scan 仍可从自己的扫描元组读取向量来计算投影和执行必要过滤；变化是原向量不再作为独立输出列传给上方 Sort/Join。对 `source->expr` 的复制也只是在规划期构造表达式树，这个函数本身不调用距离函数、不读取 AM 的运行时距离。

#### `set_plan_references()` 将 Join 表达式改成槽位引用

`union_all_replace_source_chain()` 返回时，Anti Join targetlist 中仍然是完整的：

```text
s.embedding <-> '[0,0,0,0]'
```

但它的保留侧子计划现在已经输出完全相同的表达式。随后 [setrefs.cpp](../../../src/gausskernel/optimizer/plan/setrefs.cpp) 中的 `set_join_references()` 为左右子计划 targetlist 建立索引，并通过：

```cpp
join->plan.targetlist = fix_join_expr(
    root,
    join->plan.targetlist,
    outer_itlist,
    inner_itlist,
    (Index)0,
    rtoffset);
```

重写 Join targetlist。这一段容易被误解成“本特性又专门改了 Join 上的 `candidate`”。实际上，`union_all_replace_source_chain()` 只修改子计划链上的源 `TargetEntry`；Join 上的 `candidate` 是后续被 openGauss 原有 `setrefs` 机制自动改成子计划槽位引用的。

##### `targetlist` 是父子计划的输出契约

规划阶段并没有一个“此距离已经在运行时计算完成”的布尔标志。规划器判断子计划能否提供某个结果，依据的是子计划的 `targetlist`：

```text
TargetEntry(resno=1, expr=s.row_id)
TargetEntry(resno=2, expr=s.embedding <-> query_vector)
```

这等价于子计划向父计划承诺：

```text
输出 slot 第 1 列是 row_id
输出 slot 第 2 列是 distance
```

“已经计算好”是执行时序上的说法：父 Join 消费某行时，子计划必须先生成该行的输出 slot。规划器此时只在建立这份契约，并没有计算真实数值。

##### `has_non_vars` 只表示“子计划输出了复杂表达式”

`set_join_references()` 先对左右直接子计划分别调用：

```cpp
outer_itlist = build_tlist_index(outer_plan->targetlist);
inner_itlist = build_tlist_index(inner_plan->targetlist);
```

`build_tlist_index()` 扫描每个 `TargetEntry`：

```cpp
if (IsA(tle->expr, Var)) {
    /* 记录 varno、varattno 与 resno 的对应关系 */
} else if (IsA(tle->expr, PlaceHolderVar)) {
    itlist->has_ph_vars = true;
} else {
    itlist->has_non_vars = true;
}
```

因此，发现 `s.embedding <-> query_vector` 这个 `OpExpr` 时设置 `has_non_vars=true`，不是在识别 DiskANN、`<->` 或“距离”。它只是一个通用的能力与快速路径标记：

```text
has_non_vars = false
  子 targetlist 只有普通 Var，无须线性扫描它来匹配复杂表达式

has_non_vars = true
  子 targetlist 至少有一个非 Var 表达式，父节点可尝试整体复用
```

`indexed_tlist` 这个名字容易令人以为所有表达式都建了哈希索引。实际上，普通 `Var` 会建立紧凑的对应信息；非 `Var` 仍保留在原 targetlist 中，匹配时由 `tlist_member()` 线性扫描。`has_non_vars` 可避免在明知没有复杂输出时做无效扫描。

##### `search_indexed_tlist_for_non_var()` 的精确行为

`fix_join_expr_mutator()` 处理一个非 `Var` 节点时，先尝试在左子计划中匹配完整表达式，再尝试右子计划：

```cpp
if (context->outer_itlist && context->outer_itlist->has_non_vars) {
    newvar = search_indexed_tlist_for_non_var(
        node, context->outer_itlist, OUTER_VAR);
    if (newvar != NULL)
        return (Node*)newvar;
}
if (context->inner_itlist && context->inner_itlist->has_non_vars) {
    newvar = search_indexed_tlist_for_non_var(
        node, context->inner_itlist, INNER_VAR);
    if (newvar != NULL)
        return (Node*)newvar;
}
```

普通非 Stream 场景中，`search_indexed_tlist_for_non_var()` 的核心只有两步。第一步，在子计划 targetlist 中找第一个表达式结构相同的 `TargetEntry`：

```cpp
tle = tlist_member(node, itlist->tlist);
```

`tlist_member()` 对每个 `tlentry->expr` 调用 `equal()`，比较的是表达式树结构，而不是指针地址。操作符 OID、参数树、`Var` 身份、常量、类型等结构属性必须相等。因为源链中的表达式来自：

```cpp
copyObject(candidate->expr)
```

所以它们虽然位于不同内存地址，却与 Join 上的 `candidate->expr` 结构相同，能够稳定命中。这不是“数学等价”判定；如果两个表达式只是计算结果等价、但表达式树不同，就不会匹配。

第二步，若找到 `tle`，就构造对该输出槽位的 `Var`：

```cpp
newvar = makeVarFromTargetEntry(newvarno, tle);
newvar->varnoold = 0;
newvar->varoattno = 0;
```

`makeVarFromTargetEntry()` 取用 `tle->resno`、`exprType(tle->expr)`、typmod 和 collation，生成类似下面的内部引用：

```text
Var
  varno    = OUTER_VAR
  varattno = 2
  vartype  = FLOAT8OID
```

`varnoold` 和 `varoattno` 置零，是因为这个新 `Var` 并不是对某张基表原始字段的引用，而是对子计划计算结果列的引用。若没有找到结构相同的 `TargetEntry`，函数返回 `NULL`，调用者再尝试另一个子计划或继续递归处理内部子表达式。Stream 计划下会改用 `tlist_member_except_aggref()` 处理聚合和 `RelabelType` 特殊情况；本文的距离 `OpExpr` 主链按上述普通路径理解即可。

##### 不是“外层任何表达式都搜索所有后代”

`fix_join_expr_mutator()` 是自顶向下处理当前表达式树的：

1. 当前节点是普通 `Var` 时，走 `search_indexed_tlist_for_var()`；
2. 当前节点是非 `Var` 时，若子 targetlist 有复杂输出，先匹配当前完整子树；
3. 完整匹配成功，立即返回槽位 `Var`，不再遍历其内部；
4. 完整匹配失败，才递归进入它的子表达式，因此也可只复用一个内部子表达式。

例如父节点需要：

```text
sqrt(s.embedding <-> query_vector) + 1
```

子节点只输出：

```text
resno=2: s.embedding <-> query_vector
```

最外层加法和 `sqrt()` 都无法整体匹配，递归到距离子表达式时才命中，最终形成：

```text
sqrt(OUTER_VAR[2]) + 1
```

对 Join，`set_join_references()` 会处理 `joinqual`、`nulleqqual`、Join 的 `targetlist`、`var_list`、`qual`，以及相应的 `mergeclauses` 或 `hashclauses`。它不是回到原始 `Query` 中遍历“任意地方”，也不会越过直接子计划搜索孙计划。例如 `Join -> Sort -> Scan` 中，Join 只搜索 `Sort.targetlist`；每一层各自建立槽位引用，所以源距离投影必须在整条透明 `chain` 上保持。其他一元上层节点会由 `fix_upper_expr()` 使用类似规则处理，但仍然只以其直接子计划 targetlist 为输入契约。

Outer Join 还有额外安全边界。`joinqual` 在 Join 语义之下，可先使用两侧输入；但处理 Join 上方的 targetlist 和 `qual` 前，`set_join_references()` 会对可能被补 NULL 的一侧清除 `has_non_vars`。例如 Left/Semi/Anti Join 会清除 `inner_itlist->has_non_vars`，防止上方绕过 Join 的补 NULL 语义而直接复用右侧复杂表达式。

##### `OUTER_VAR` 和 `INNER_VAR` 是执行槽位标识

`OUTER_VAR` 与 `INNER_VAR` 是 `Var.varno` 的特殊值，不是表别名，也不等于 Outer Join 的“保留侧/非保留侧”：

```cpp
#define INNER_VAR 65000  /* 引用右侧/inner 子计划输出 */
#define OUTER_VAR 65001  /* 引用左侧/outer 子计划输出 */
```

对一个 Join，`OUTER_VAR[2]` 是解读时的简写，真实节点是：

```text
Var(varno=OUTER_VAR, varattno=2, vartype=FLOAT8OID, ...)
```

它表示从 `ExprContext.ecxt_outertuple` 读取左子计划输出 slot 的第 2 列。`INNER_VAR[1]` 则从 `ecxt_innertuple` 读取右子计划的第 1 列。在本文的 Left Anti Join 中，左子计划恰好也是保留侧，所以距离变成 `OUTER_VAR[2]`；这是本计划形态中两个概念恰好重合，不应推广为“`OUTER_VAR` 永远表示保留侧”。

##### Join 上的 `candidate` 如何“自然”变成引用

`union_all_replace_source_chain()` 执行完后，Join 上的原 `candidate->expr` 仍是：

```text
s.embedding <-> query_vector
```

但左子计划的 `resno=2` 现在也是结构相同的距离表达式。`set_join_references()` 通过下列赋值用一棵已修正引用的新表达式树替换 Join targetlist：

```cpp
join->plan.targetlist = fix_join_expr(...);
```

`fix_join_expr_mutator()` 在递归进入 `OpExpr` 参数之前，先对完整 `OpExpr` 做上述结构匹配。命中后直接返回 `OUTER_VAR[2]`，不再下钻到 `s.embedding`，因此 Join 层不会重新组装成 `OUTER_VAR[2] <-> query_vector`。这就是“`candidate` 的修改是自然发生的”的准确含义：本特性创造了原生 `setrefs` 能够识别的父子同表达式形态，真正修正 Join 引用的是原有通用机制。

因此最终的数据流是：

```text
AnnIndexScan 计算并输出第 2 列 distance
    ↓
Sort 透传第 2 列 distance（它自身仍按 row_id 排序）
    ↓
Anti Join 通过 OUTER_VAR[2] 直接读取 distance
```

这也解释了 EXPLAIN 中上层距离经常显示为双层括号：上层使用的是子计划已经产生的标量距离，而不是再次从原始向量计算。

#### 与 DiskANN AM 距离复用的边界

`union_all_replace_source_chain()` 完成的是**距离投影下沉**：从“Scan 向上传输原始向量，Anti Join 层计算距离”变成“Scan 输出标量距离，Anti Join 引用该标量”。它本身不会把 SQL 距离表达式替换成 AM 发布值。

随后的 `rewrite_ann_orderby_distance_refs()` 才会确认扫描确实是支持的 DiskANN `AnnIndexScan`，并将扫描 targetlist 中的距离表达式替换为 `AnnOrderByRef`。因此两步的边界是：

```text
union_all_replace_source_chain()
  → 距离在 Scan 层产生并作为标量向上传递

rewrite_ann_orderby_distance_refs()
  → 满足准入条件时，Scan 从 AM 已发布的距离中取值
```

### 排序键复用：列裁剪内部的一步

这里把 `preprocess_union_all_order_key_projection()` 实现的能力称为**排序键复用**：各 UNION ALL 分支把 `embedding <=> 查询向量` 算成标量排序键，外层 `ORDER BY` 引用这个输出位置。这样原始 `embedding` 无须为了外层排序继续跨 UNION ALL 传输。它复用的是**分支产生的排序键**，并不意味着该分支已经读取 DiskANN AM 算好的距离；后者需要独立的 `AnnOrderByRef` 改写和执行期发布值。

排序键复用依赖本查询的列裁剪改写成功、存在适合的 `ORDER BY`，以及排序键投影自身的安全检查；**不依赖 Top-K 下推成功，也不要求有 `LIMIT`**。`subquery_planner()` 始终先尝试 Top-K，再在 `unionAllColumnsRewritten` 为真时尝试排序键投影，这是为了让 Top-K 先识别原始排序操作符，并不是把 Top-K 成功作为排序键投影的门槛。Top-K 开关为 `off` 时，仍可形成“分支输出标量距离 → Append → 全局 Sort 引用该距离”的计划，只是不会由 Top-K 改写新增分支 `ORDER BY + LIMIT`。

现有 [列裁剪回归用例](../../../src/test/regress/sql/union_all_required_columns.sql) 专门设置 Top-K `off`、列裁剪 `on`；其 [实际期望计划](../../../src/test/regress/expected/union_all_required_columns.out) 摘录如下，距离复用并未参与：

```text
Limit
  -> Sort
       Sort Key: ((delta_t.embedding <-> '[0,0,0,0]'::vector))
       -> Result
            -> Append
                 -> Seq Scan on delta_t
                      Output: delta_t.row_id, (delta_t.embedding <-> ...)
                 -> Merge Anti Join
                      Output: s.row_id, ((s.embedding <-> ...))
                      -> Seq Scan on snapshot_t s
                           Output: s.row_id, (s.embedding <-> ...)
```

这是排序键复用仍在工作的例子：计划只有一个全局 `Limit` 和 `Sort`，各分支输出的是标量距离；扫描层的单层括号表示普通 SQL 距离计算，不表示 AM 距离复用。若排序键投影的安全条件不满足，原始 `embedding` 可能仍需传到外层排序位置；应检查该阶段的准入结果，而不能单凭 Top-K 为 `off` 断言“列裁剪失效”。

在这个 snapshot/delete 场景中，列裁剪与距离复用的实际接点是 `union_all_replace_source_chain()`：它把原先位于 Anti Join 输出中的 `s.embedding <=> 常量` 放到保留侧 Scan 输出中。之后距离复用注入器才有一个扫描层 TargetEntry 可匹配。`ann_orderby_join_allows_approx()`只负责判断近似值能否穿过 Join；它**不创建**扫描层的距离输出。`s.row_id = d.row_id` 与距离无关，可以透传；若 Join 条件用距离决定匹配，则不得让近似 Cosine 改变匹配结果。L2 的质量标记为精确，其准入路径与近似 Cosine 的 Join 检查不同。

### 计划上能看见的边界变化

固定 Top-K 为 `on`、距离复用为 `off`，只切换列裁剪：

```text
列裁剪 off：
  Merge Anti Join
    Output: s.row_id, s.title, s.embedding, (s.embedding <=> ...)
    -> Ann Index Scan on snapshot_t s
         Output: s.row_id, s.title, s.embedding

列裁剪 on、复用 off：
  Merge Anti Join
    Output: s.row_id, ((s.embedding <=> ...))
    -> Ann Index Scan on snapshot_t s
         Output: s.row_id, (s.embedding <=> ...)

列裁剪 on、复用 cosine：
  Merge Anti Join
    Output: s.row_id, ((s.embedding <=> ...))
    -> Ann Index Scan on snapshot_t s
         Output: s.row_id, ((s.embedding <=> ...))
```

计划片段省略了中间 Sort 和完整向量常量，不能当作独立 EXPLAIN 的逐字输出。第一步把**距离计算的所在层**移到 Scan；第二步把 Scan 的距离表达式改为读取 AM 发布值。尤其要注意：上层的双层括号表示引用下层输出，**不能单凭它证明 AM 距离复用**；需要检查 Ann Index Scan 自身的 Output。完整对照见上述回归期望输出中的 Cosine 四阶段计划。

## 3. 执行期调用链

```text
ExecInitAnnIndexScan()                                   nodeAnnIndexscan.cpp
└─ 找到计划中的 AnnOrderByRef，核对运行时 GUC/距离质量
   └─ 设置 IndexScanDesc.xs_ann_orderby.requested / requested_mode

ExecAnnIndexScan()
└─ AnnIndexOrderedNext() 或 AnnIndexNext()
   └─ scan_handler_idx_getnext() → index_getnext()
      └─ index_getnext_tid() → DiskANN AM 的 diskanngettuple()
         └─ diskanngettuple_internal()                     diskannscan.cpp
            ├─ GetDistance() 为最终候选取得全向量内部距离
            ├─ DiskAnnConvertOrderByDistance()
            │  ├─ L2：对内部平方距离开方
            │  └─ Cosine：按当前 AM 的转换公式给出近似 SQL 排序尺度
            ├─ candidate.outputDistance
            └─ 发布到 IndexScanDesc.xs_ann_orderby

AnnIndexNext() 取得可见 heap 行并放入 slot
└─ AnnIndexBindOrderByValue()
   └─ 核对 epoch、row token、质量与 heap 身份后绑定到扫描状态

AnnIndexOrderedNext() 需要内部有序物化时
└─ 直接读取已绑定的 iss_AnnOrderByValue 作为排序键

AnnOrderByRef 求值
└─ ExecEvalAnnOrderByRef() / 表达式解释引擎等价分支
   └─ 返回当前扫描状态中的 iss_AnnOrderByValue
```

`AnnIndexOrderedNext()` 是扫描执行器内部可能采用的有序物化路径，不代表计划树一定有一个额外 `Sort` 节点；snapshot 分支为满足 `row_id` Merge Join 条件或距离排序而出现的计划树 `Sort` 是另一回事。AM 发布值先与**当前可见 heap 行**绑定，执行器再交给上层 Sort、Anti Join、Limit、Merge Append。若不满足运行时条件，表达式回退到原始计算；特殊的 L2 heap recheck 也会对该行重新计算并缓存，不能将发布值无条件用于所有 TID。

## 4. 开关关闭或改写不命中时

| 条件 | 对这条联合链路的影响 |
|---|---|
| 列裁剪 GUC 为 `off`，或当前查询未通过列裁剪安全检查 | 不设置本查询块的成功标志；本路径不执行 Anti Join 到 Scan 的源头距离投影。其他查询若扫描本来就输出距离，仍可独立复用。 |
| Top-K GUC 为 `off`，或无适用的 `LIMIT` | 不做此处的分支 Top-K 下推；列裁剪及其排序键复用仍可独立成功，不能据此断言距离复用必定关闭。 |
| 无 `ORDER BY` 距离 | 没有可匹配的 `indexorderbyorig`，自然没有这条 AM 距离复用链路。 |
| 距离复用 GUC 为 `off` | 不注入 `AnnOrderByRef`；列裁剪和源头距离投影仍可发生，扫描层会执行原始距离表达式。 |
| Join 条件消费距离 | 近似 Cosine 的 Join 安全检查不放行；不能将近似值用于决定行是否匹配。 |

因此，对典型 `delta UNION ALL (snapshot ANTI JOIN delete)` 查询，联合能力的核心关系是：**列裁剪改写成功 → 距离投影进入 snapshot 扫描层 → Join 安全检查允许标量距离透传 → AnnOrderByRef 读取 DiskANN 发布值**。其中任一步均需满足自身的形态与安全条件，不能仅凭三个 GUC 均为 `on` 推断扫描层一定复用。
