# openGauss UNION ALL 必需列源头裁剪源码解读

| 项目 | 内容 |
|---|---|
| 解读对象 | 当前工作树中的 UNION ALL Required Columns Pushdown 实现 |
| 源码仓库 | `/home/qiumc/data/code/cpp/openGauss-server` |
| 分支/基线 | `integration/multimodal-stacked` / 初始特性提交 `098d77c8b76d` + 本轮安全修复工作树 |
| 核心实现 | `src/gausskernel/optimizer/prep/preptopk.cpp` |
| 目标 SQL | 外层 WHERE 必须存在；ORDER BY、LIMIT 分别可选 |
| 控制开关 | `enable_union_all_required_columns_pushdown`，默认 `off` |
| 文档状态 | 已按 2026-09-09 工作树代码核对，并补充关键 C++ 逐段精读 |

## 0. 深度复核后的安全修正

对初始提交 `098d77c8b76d` 的小数据复核发现六个可复现问题，修复说明见[深度复核与缺陷修复报告](../测试/UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md)。正文原有行号主要对应初始提交，定位应以函数名为准。手写或迁移时必须包含下面的修正：

- 阶段一入口现在返回 `bool`，表示**当前 Query**是否成功改写；`PlannerInfo.union_all_required_columns_pruned` 仍可继承，但只用于后代扫描禁止 physical targetlist 回扩。不能以继承的 true 直接批准子查询排序键改写，否则聚合子查询的 Aggref 会被放进非 Agg 叶节点。
- 阶段三还要求外层没有残留 quals；若 WHERE 未完全下推，保留外层排序表达式的计算位置。即使 immutable 的 `100 / row_id`，提前计算也可能对本应先过滤掉的 0 抛错。
- 叶子和外层出现 `ROWNUM` 时保守回退。移动 WHERE 会改变 ROWNUM 编号，不能把它当成普通、无副作用的标量表达式。
- 既有 Top-K 入口必须检查 UNION 容器自身的 LIMIT/OFFSET 等语义边界，并拒绝外层 SRF、WITH TIES/PERCENT；只检查叶子安全并不足够。

SRF 安全门需要 `hasTargetSRFs` 与 `expression_returns_set(targetList)` 两种检查。旧表达式框架的 parser 未必设置 hasTargetSRFs，本轮第一版只检查 flag 时仍返回错结果；直接扫描外层/叶子 targetlist 才覆盖这一分支。

## 1. 先看结论

这个特性不是在 Executor 收到宽元组后再丢弃列，而是在通用 SetOp/Append 规划之前改写 planner 私有的 `Query` 树：

1. 把安全的外层 WHERE 映射到每个 UNION ALL 叶子；
2. 计算 UNION 边界上真正需要保留的列；
3. 同步压紧叶子 targetlist、UNION targetlist、SetOperationStmt 类型元数据和外层 Var 列号；
4. 可选地让现有 DiskANN Top-K 逻辑给每个叶子增加 ORDER BY + LIMIT；
5. 将 `embedding <-> query_vector` 等派生排序表达式物化成叶子标量列，使完整 vector 不再穿过 UNION；
6. 在 `createplan` 阶段阻止基表 physical targetlist 把已删除的列重新加回扫描输出。

最终效果是字段只存活到最后一个消费者：

- `title` 没有消费者时，不进入源扫描 `Output`；
- `title` 只用于 WHERE 时，允许出现在源扫描 `Filter`，但不向上输出；
- `delete_t.reason` 只活到 delete 扫描过滤；
- `embedding` 只活到距离计算；
- UNION 以上只传最终结果列和标量 distance。

实现没有新增 Plan 节点，也没有修改 Executor 节点。主要验收接口就是 `EXPLAIN (VERBOSE ON, COSTS OFF)` 中每个节点的 `Output`。

## 2. 为什么普通投影裁剪不够

典型视图有三列：

```sql
CREATE VIEW union_view AS
SELECT row_id, title, embedding
FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS
(
    SELECT 1 FROM delete_t d WHERE d.row_id = s.row_id
);
```

用户只查询：

```sql
SELECT row_id
FROM union_view
WHERE row_id > 2;
```

普通子查询裁剪知道外层只需要 `row_id`，但 UNION ALL 是位置型集合接口：`SetOperationStmt.colTypes`、各叶子 targetlist、UNION Query targetlist 和外层 `Var.varattno` 必须保持同一列数、次序与类型。只改其中一处会造成列号错位或类型错误。

此外，openGauss 在生成基表 Scan Plan 时倾向使用包含整行属性的 physical targetlist，以减少普通场景的投影开销。即使 Query 层已经裁剪，`createplan.cpp` 仍可能把 `title`、`embedding` 加回 Scan `Output`。

因此本特性同时解决两层问题：

```text
Query/SetOp 层：同步压紧位置型输出接口
        ↓
Path/Plan 层：禁止本次查询的基表扫描重新扩成物理整行
```

## 3. 源码文件职责

| 文件 | 作用 |
|---|---|
| `src/gausskernel/optimizer/prep/preptopk.cpp` | 必需列分析、WHERE 映射、SetOp 压紧、排序键物化，并复用既有 Top-K 逻辑 |
| `src/gausskernel/optimizer/plan/planner.cpp` | 安排三阶段调用顺序，并向子 planner 传播压紧状态 |
| `src/gausskernel/optimizer/plan/createplan.cpp` | 阻止已压紧查询的基表 physical targetlist 回扩 |
| `src/common/backend/utils/misc/guc/guc_sql.cpp` | 注册新 GUC，默认关闭 |
| `src/include/knl/knl_guc/knl_session_attr_sql.h` | 保存 session 级 GUC 值 |
| `src/include/nodes/relation.h` | 在 `PlannerInfo` 中保存本次查询已压紧标志 |
| `src/include/optimizer/prep.h` | 声明三个预处理入口 |
| `src/test/regress/sql/union_all_required_columns.sql` | 完整计划和正确性回归 |
| `src/test/regress/expected/union_all_required_columns.out` | 原生 EXPLAIN/结果 golden |

生产代码相对基线为 +1,151/-1 行，其中 `preptopk.cpp` 为 +1,104/-1 行。

### 3.1 读代码前必须认识的 Query 树字段

这项改写操作的不是最终 `Plan`，而是 parser/rewrite 已经生成、planner 即将消费的 `Query`。下面几个字段如果不分清，后面的列号压紧会很难理解。

| 类型/字段 | 本实现中的含义 |
|---|---|
| `Query.targetList` | 当前查询层输出及 resjunk 表达式；外层 ORDER BY 距离通常在这里有一个 resjunk TLE |
| `RangeTblEntry` / `RTE_SUBQUERY` | 外层 FROM 中的 UNION 视图，以及 SetOp 树每个叶子的子 Query 容器 |
| `Query.setOperations` | UNION ALL 的 `SetOperationStmt` 树根 |
| `SetOperationStmt.colTypes` | 每个集合输出位置的类型 OID 列表 |
| `colTypmods` / `colCollations` | 与 `colTypes` 严格等长、同位置的 typmod/collation |
| `TargetEntry.resno` | 本层 targetlist 中的物理输出序号 |
| `TargetEntry.resjunk` | 为排序等内部语义保留、但不返回用户的输出项 |
| `TargetEntry.ressortgroupref` | SortGroupClause 找 TargetEntry 使用的稳定引用号，不等同于 `resno` |
| `Var.varno` | 当前 Query 的 RTE 编号 |
| `Var.varattno` | 该 RTE 的输出位置；本实现要重写的核心字段 |
| `Var.varnoold/varoattno` | EXPLAIN、权限或来源追踪使用的原始来源信息，满足条件时同步更新 |
| `RangeTblEntry.eref->colnames` | RTE 暴露给上层的位置型列名列表 |

对目标查询，Query 树大致是：

```text
outer Query
  targetList
    TLE row_id                     expr = Var(unionRti, 1)
    TLE distance [resjunk]         expr = OpExpr(Var(unionRti, 3), const)
  jointree.quals                   expr = Var(unionRti, 1) > 2
  rtable[unionRti]                 RTE_SUBQUERY
    subquery = union Query
      targetList                   指向左侧第一个叶子的三列 Var
      setOperations                SetOperationStmt(UNION ALL)
      rtable
        leaf RTE 1 -> delta Query
        leaf RTE 2 -> snapshot Query（内部含 NOT EXISTS）
```

这里有两套不同的编号：外层 `unionRti` 是外层 Query 的 RTE 编号；叶子内部 `delta_t`、`snapshot_t` 的 `varno` 属于各自叶子 Query。表达式下推不能简单改一个 `varno`，必须通过叶子 targetlist 做位置替换。

### 3.2 三个临时结构逐字段解释

`preptopk.cpp:55-75` 定义三个仅在 planner 内使用的结构：

```cpp
typedef struct DiskAnnTopKPreparedLeaf {
    RangeTblEntry *ownerRte;
    Query *rewrittenQuery;
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
```

#### `DiskAnnTopKPreparedLeaf`

- `ownerRte` 指向当前工作 Query 树里的原叶子 RTE；最终提交时修改的是 `ownerRte->subquery`。
- `rewrittenQuery` 是该叶 Query 的深拷贝，先在副本中增加 ORDER BY/LIMIT。
- 两者分开保存，使代码可以先准备所有叶子，再一次性提交；任何叶失败时，其他叶的原指针都没有变化。

#### `RequiredColumnVarMapContext`

- `unionRti` 限定 mutator 只改目标 UNION RTE，不能误改同一表达式中的其他 Var。
- `oldNcols` 是本阶段压紧前的可见集合列数，用于拒绝越界列号。
- `oldToNew[oldAttno]` 为 0 表示该旧列已删除，大于 0 表示新列号。
- `failed` 不是立刻抛 ERROR，而是让 expression mutator 完成安全返回后，由调用者放弃整个副本。

#### `UnionAllOrderKey`

- `sortRef` 把 key 与外层 `SortGroupClause.tleSortGroupRef` 关联起来；不能用 targetlist 下标替代，因为 resno 会重排。
- `leafExprs` 按叶子遍历顺序保存每个叶子的本地等价表达式。
- `type/typmod/collation` 是 synthetic SetOp 列必须追加的三元元数据。
- `newAttno` 在第二次压紧时分配，是外层排序 TLE 最终引用的 UNION 输出位置。

这些结构都用 `palloc/palloc0` 分配在 planner 生命周期的 MemoryContext 中，不进入 CachedPlan/Plan 序列化，也不需要逐个 `pfree`。

### 3.3 阶段一主入口：每一块代码为什么存在

`preprocess_union_all_required_columns()` 的关键骨架如下：

```cpp
if (!u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown)
    return false;

if (!required_columns_outer_query_is_safe(outer))
    return false;

work = (Query *)copyObject(outer);
unionRte = get_single_union_subquery_rte(work, &unionRti);
if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral)
    return false;

if (!collect_pure_union_all_leaves(unionRte->subquery, &leaves))
    return false;

oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
if (oldNcols <= 0 ||
    !validate_required_column_shape(unionRte->subquery, leaves, oldNcols))
    return false;

wherePushed = prepare_outer_where_pushdown(work, unionRti, unionRte, leaves);

if (!collect_required_output_columns(work, unionRti, oldNcols, &required))
    return false;

preserve_semantically_required_outputs(leaves, oldNcols, &required);
if (bms_is_empty(required))
    required = bms_add_member(required, 1);

if (!compact_union_all_query(work, unionRti, unionRte,
                             leaves, required, oldNcols))
    return false;

outer->targetList = work->targetList;
outer->jointree = work->jointree;
outer->rtable = work->rtable;
root->union_all_required_columns_pruned = true;
return true;
```

逐块看：

1. 第一个 `if` 是零成本 GUC 旁路。默认 off 时不复制 Query，也不扫描 targetlist。
2. 外层结构门在复制前运行，避免明显不支持的 SQL 支付深拷贝成本。
3. `copyObject(outer)` 是阶段一的事务边界；其后的任何 `return` 都只是丢弃 planner context 中的副本。
4. `get_single_union_subquery_rte()` 建立“只有一个目标 RTE”的假设，因此后面可固定 `unionRti=1`。
5. security barrier 和 lateral 在表达式映射前拒绝，防止跨安全边界或参数化边界移动谓词。
6. 先收集全部叶子并校验，再移动 WHERE；不能边遍历边改。
7. WHERE 处理必须早于 required 收集。只有这样，成功下推后仅供 WHERE 使用的列才不会被加入 UNION 输出集合。
8. `preserve_semantically_required_outputs()` 是对纯“上层引用分析”的补充，保护副作用和行数语义。
9. `compact_union_all_query()` 同步重写所有位置接口；成功后才设置 planner 标志。
10. 不直接执行 `root->parse = work`，而是保留 `outer` 对象身份，只安装被验证的三个子树指针。

这里有一个重要语义：`prepare_outer_where_pushdown()` 返回 `false` 不会导致阶段一整体失败。此时外层 quals 仍在 `work` 中，`collect_required_output_columns()` 会把 WHERE 引用列加入 required；其他无消费者列仍可安全裁剪。

这种部分优化只能裁列，不能继续把外层排序表达式移到残留过滤之前。阶段三通过检查外层 `jointree->quals` 非空时返回来保留求值位置。

### 3.4 外层安全门逐条件说明

`required_columns_outer_query_is_safe()` 的核心条件是：

```cpp
if (query == NULL || query->commandType != CMD_SELECT)
    return false;

if (query->jointree == NULL || query->jointree->quals == NULL)
    return false;

if (query->setOperations != NULL || query->hasAggs ||
    query->groupClause != NIL || query->havingQual != NULL ||
    query->distinctClause != NIL || query->hasWindowFuncs ||
    query->windowClause != NIL || query->cteList != NIL ||
    query->hasForUpdate || query->hasTargetSRFs || query->hasSubLinks ||
    query->hasRecursive || query->hasModifyingCTE ||
    query->hasRowSecurity || query->limitOffset != NULL ||
    query->limitIsPercent || query->limitWithTies)
    return false;

// 老表达式框架不保证设置 hasTargetSRFs；必须直接检查表达式。
if (expression_returns_set((Node *)query->targetList) ||
    expression_contains_rownum((Node *)query->targetList) ||
    ContainRownumQual(query))
    return false;
```

这些字段不是随意罗列：

| 条件 | 拒绝原因 |
|---|---|
| `commandType != CMD_SELECT` | DML targetlist、RETURNING 和 row identity 有额外语义 |
| `jointree->quals == NULL` | 第一阶段产品契约要求 WHERE 必须存在 |
| 外层 `setOperations` | 实现只处理 FROM 中的 UNION RTE，不处理 outer 自己就是 SetOp 的形状 |
| Agg/Group/Having/Distinct/Window | 输出引用与行数语义不再是简单逐行投影 |
| CTE/Recursive/ModifyingCTE | 可能有物化、递归工作表或写入副作用 |
| ForUpdate | 需要 rowmark junk 列与锁定语义 |
| TargetSRFs | targetlist 可改变行数 |
| ROWNUM | WHERE 或 ORDER 的移动可能改变行编号，需同时检查 targetlist 与 quals |
| SubLinks | 外层关联层级和 pull-up 时序需要额外证明 |
| RowSecurity | 不能在当前逻辑下跨 RLS 边界移动谓词或缩减接口 |
| OFFSET/PERCENT/WITH TIES | 分支 Limit 与全局行语义更复杂 |

随后代码要求：

```cpp
list_length(query->rtable) == 1
list_length(fromExpr->fromlist) == 1
IsA(linitial(fromExpr->fromlist), RangeTblRef)
rtindex == 1
```

这四个条件共同保证 `get_single_union_subquery_rte()` 返回的唯一 RTE 与 jointree 真正引用的是同一个对象，而不是 rtable 中有未出现在 FROM 的附加 RTE。

### 3.5 纯 UNION ALL 叶子递归如何工作

关键递归在 `collect_setop_leaves()`：

```cpp
if (IsA(node, SetOperationStmt)) {
    SetOperationStmt *setop = (SetOperationStmt *)node;
    if (setop->op != SETOP_UNION || !setop->all)
        return false;
    if (!collect_setop_leaves(setop->larg, rtable, leaves))
        return false;
    if (!collect_setop_leaves(setop->rarg, rtable, leaves))
        return false;
    return true;
} else if (IsA(node, RangeTblRef)) {
    RangeTblEntry *rte = rt_fetch(rtr->rtindex, rtable);
    if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
        return false;
    *leaves = lappend(*leaves, rte);
    return true;
}
```

遍历顺序是左子树再右子树，所以 `leaves` 的顺序与 SetOp 输出顺序一致。阶段三的 `leafExprs[leafIndex]` 正是依赖这个稳定顺序。递归函数只接受两种 Node：中间 `SetOperationStmt` 或叶 `RangeTblRef`；出现其他节点类型立即失败。

注意：这里收集的是“拥有叶子 Query 的 RTE”，而不是叶 Query 内部的基表 RTE。snapshot 叶内部可以有 `snapshot_t`、`delete_t` 以及 NOT EXISTS，整体仍只是 SetOp 的一个叶子。

### 3.6 WHERE 采用先准备、后写入的两遍算法

`prepare_outer_where_pushdown()` 没有在第一次循环中直接改叶子：

```cpp
foreach (lc, leaves) {
    RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
    Expr *mapped = map_outer_qual_to_leaf(outer, unionRti, unionRte,
                                         leafRte->subquery,
                                         (Expr *)outerQual);
    if (mapped == NULL)
        return false;
    mappedQuals = lappend(mappedQuals, mapped);
}

forboth(lc, leaves, mq, mappedQuals) {
    RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
    if (!append_outer_qual_to_leaf(leafRte->subquery,
                                  (Expr *)lfirst(mq)))
        return false;
}

outer->jointree->quals = NULL;
```

第一遍只生成 `mappedQuals`。假设第 3 个叶子映射失败，第 1、2 个叶子还没有被写入。第二遍用 `forboth` 保证第 N 个叶子与第 N 个 mapped expression 配对。

即便第二遍的 `append_outer_qual_to_leaf()` 意外失败，修改也只发生在阶段一的 `work` 深拷贝上，原 `root->parse` 仍不变。这形成两层原子性：函数内先准备列表，入口外再使用整棵 Query 副本。

最后才把外层 quals 设为 NULL。不能更早删除，否则某个叶子映射失败时会丢失过滤条件。

### 3.7 `ReplaceVarsFromTargetList` 到底替换了什么

WHERE 与 ORDER 映射共享以下模式：

```cpp
List *leafMapTlist = build_visible_output_map_tlist(leaf);

if (!validate_union_leaf_output_types(unionRte->subquery,
                                      leafMapTlist, vars))
    return NULL;

Expr *mapped = (Expr *)ReplaceVarsFromTargetList(
    (Node *)outerExpr,
    unionRti,
    0,
    unionRte,
    leafMapTlist,
    REPLACEVARS_REPORT_ERROR,
    0,
    NULL);
```

`build_visible_output_map_tlist()` 会跳过叶子 resjunk，并人为构造连续的 `resno=1..N`：

```cpp
foreach (lc, leaf->targetList) {
    TargetEntry *tle = (TargetEntry *)lfirst(lc);
    if (tle->resjunk)
        continue;
    result = lappend(result,
        makeTargetEntry(tle->expr, resno++, NULL, false));
}
```

假设 outer expression 为：

```text
OpExpr(<->)
  left  = Var(varno=unionRti, varattno=3)
  right = Const(vector)
```

对于 delta 叶，第 3 个 visible TLE 是 `Var(delta_rti, embedding_attno)`，替换后得到：

```text
OpExpr(<->)
  left  = Var(varno=delta_rti, varattno=embedding_attno)
  right = Const(vector)
```

对于 snapshot 叶，第 3 个 visible TLE 是 `s.embedding`，替换后得到 snapshot Query 本地表达式。叶内部 NOT EXISTS 的 delete Var 不参与替换。

调用前后有三层保护：

1. 所有 outer Var 必须是 level 0、指向 `unionRti`、`varattno>0`；
2. `validate_union_leaf_output_types()` 按 attno 比较 UNION TLE 与叶 TLE 的 type、typmod、collation；
3. `leaf_local_expr_is_valid()` 再确认映射结果无 volatile/SubLink、无上层 Var，而且每个本地 `varno` 都能在叶 rtable 中取到。

`REPLACEVARS_REPORT_ERROR` 理论上会在无法替换时报告错误，但前置检查已经把不支持形状变成普通 `false/NULL` 降级路径。

### 3.8 WHERE 与叶子原条件如何合并

`append_outer_qual_to_leaf()` 使用：

```cpp
leafCopy->jointree->quals = (Node *)make_and_qual(
    (Node *)leafCopy->jointree->quals,
    (Node *)mappedQual);
```

因此 snapshot 原有 NOT EXISTS 条件不会被覆盖。概念上从：

```text
snapshot WHERE NOT EXISTS(delete)
```

变为：

```text
snapshot WHERE NOT EXISTS(delete)
           AND mapped_outer_where
```

`make_and_qual()` 会处理原 quals 为 NULL 的情况，避免人为制造多余 BoolExpr 层。若 leaf 的 `jointree` 本身为 NULL，代码先构造空 `FromExpr`，但正常 SELECT 叶通常已有 jointree。

### 3.9 required Bitmapset 的精确来源

最内层收集器：

```cpp
vars = pull_vars_of_level(expr, 0);
foreach (lc, vars) {
    Var *var = (Var *)lfirst(lc);
    if (var->varlevelsup != 0 || var->varno != unionRti ||
        var->varattno <= 0 || var->varattno > oldNcols)
        return false;
    *required = bms_add_member(*required, var->varattno);
}
```

这里把真实的 1-based `varattno` 直接作为 Bitmapset member。Bitmapset 并不要求成员从 0 开始，因此后续也是 `bms_is_member(pos, required)`，没有 `attno-1` 转换。

外层调用顺序为：

```cpp
foreach (outer->targetList)
    add_required_columns_from_expr(tle->expr, ...);

add_required_columns_from_expr(outer->jointree->quals, ...);
add_required_columns_from_expr(outer->limitCount, ...);
add_required_columns_from_expr(outer->limitOffset, ...);
```

targetlist 没有过滤 `resjunk`，这是有意的：`ORDER BY embedding <-> const` 即使未出现在 SELECT 列表，也会以 resjunk TLE 存在，阶段一必须暂时保留 embedding。

WHERE 若已成功下推，`work->jointree->quals` 已为 NULL，`title` 这类 filter-only 列不会进入集合 required；叶子 quals 中的本地 Var 不需要也不应该作为 UNION 输出收集。

### 3.10 volatile/SRF 为什么按“输出位置”保留

代码检查每个叶子的第 `pos` 个可见输出：

```cpp
if (tle->ressortgroupref != 0 ||
    expression_returns_set((Node *)tle->expr) ||
    contain_volatile_functions((Node *)tle->expr) ||
    checkExprHasSubLink((Node *)tle->expr))
    *required = bms_add_member(*required, pos);
```

UNION ALL 是位置接口。如果 delta 第 2 列是 volatile，而 snapshot 第 2 列只是普通 title，也必须保留双方第 2 个位置；不能只在一个叶子保留，否则集合列数不同。

`ressortgroupref != 0` 也保留，因为叶子内部排序/分组语义通过 sortgroupref 找 TLE。虽然阶段一安全门拒绝已有叶子 ORDER/LIMIT，保留这一项可以防止未来 parser/rewrite 形态变化时静默删除语义 TLE。

### 3.11 `oldToNew` 不是辅助信息，而是提交协议

`compact_union_all_query()` 先构造：

```cpp
oldToNew = (AttrNumber *)palloc0(
    (oldNcols + 1) * sizeof(AttrNumber));

for (pos = 1; pos <= oldNcols; pos++) {
    if (bms_is_member(pos, required))
        oldToNew[pos] = (AttrNumber)nextAttno++;
}
```

`palloc0` 让未保留位置天然为 0，无需第二次初始化。按旧列顺序递增 `nextAttno`，所以保留列的相对次序不变。

随后必须用同一个数组依次处理：

```text
1. outer expressions 中的 Var
2. 每个 leaf Query targetlist
3. 每个 leaf RTE 的 eref->colnames
4. union Query targetlist
5. 全部 SetOperationStmt 元数据
6. outer 中 union RTE 的 eref->colnames
```

它们不是六种独立优化，而是同一位置接口的六份表示。少改任何一份，后续 planner 都可能按不同 schema 理解同一个 tuple。

### 3.12 外层 Var mutator 的关键分支

`required_column_var_mutator()` 的核心是：

```cpp
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
```

逐分支说明：

- 非目标 RTE 或非本层 Var 原样深拷贝，mutator 不能越过 Query level 修改相关子查询里的 Var。
- `varattno<=0` 同时覆盖 whole-row 0 和 system attribute 负数，当前实现一律拒绝。
- `oldToNew==0` 说明 required 分析认为列可删，但表达式树中仍有引用；这是内部一致性失败，不能把 Var 指到 0。
- `varattno` 是当前 Query 真正使用的编号，必须修改。
- `varoattno` 只有仍精确描述同一个 unionRti/旧 attno 时才修改，避免覆盖更深层原始来源信息。
- 非 Var 节点交给 `expression_tree_mutator()` 递归，所以 OpExpr、FuncExpr、BoolExpr 内部的目标 Var 都能更新。

`remap_outer_query_vars()` 对 outer targetlist、剩余 quals、limitCount、limitOffset 分别调用 mutator。虽然普通 SQL 的 LIMIT 不能引用当前行列，保留这一步可以在遇到异常 parse tree 时安全失败，而不是留下旧列号。

### 3.13 叶子 targetlist 为什么分两遍重建

`compact_leaf_targetlist()` 第一遍只处理非 resjunk：

```cpp
if (tle->resjunk)
    continue;

oldPos++;
if (oldToNew[oldPos] == 0)
    continue;

copy = (TargetEntry *)copyObject(tle);
copy->resno = (AttrNumber)nextResno++;
result = lappend(result, copy);
```

第二遍再追加 resjunk：

```cpp
if (!tle->resjunk)
    continue;
copy = (TargetEntry *)copyObject(tle);
copy->resno = (AttrNumber)nextResno++;
result = lappend(result, copy);
```

原因有两个：

1. SetOp 的可见接口只按非 resjunk 位置对齐，`oldPos` 不能把 resjunk 算进去；
2. openGauss targetlist 约定可见列在前、内部 junk 在后，重建后仍保持该布局。

代码保留 resjunk 的 `ressortgroupref`，只更新 `resno`。SortGroupClause 通过 ressortgroupref 找 TLE，不依赖旧 resno。

### 3.14 UNION Query targetlist 的额外限制

`compact_union_targetlist()` 比叶子版本更严格：

```cpp
if (tle->resjunk)
    return failure;

if (!IsA(tle->expr, Var))
    return failure;

var = (Var *)tle->expr;
if (var->varlevelsup != 0 || var->varattno != oldPos)
    return failure;
```

SetOp Query 的 targetlist 按 parser 约定引用左侧第一个叶子的输出。这里不尝试兼容任意表达式，而是验证第 N 个 TLE 确实是第 N 个输出 Var，然后同时修改：

```cpp
copy->resno = oldToNew[oldPos];
var->varattno = oldToNew[oldPos];
if (var->varoattno == oldPos)
    var->varoattno = oldToNew[oldPos];
```

如果未来 parser 让 SetOp targetlist 包含非 Var coercion，这里会保守回退，而不是假定它仍可按位置改写。这是一个需要在未来类型 coercion 扩展中重点修改的安全门。

### 3.15 SetOp 三份元数据如何同步压紧

`compact_setop_metadata()` 先验证当前节点：

```cpp
if (op->op != SETOP_UNION || !op->all || op->groupClauses != NIL ||
    list_length(op->colTypes) != oldNcols ||
    list_length(op->colTypmods) != oldNcols ||
    list_length(op->colCollations) != oldNcols)
    return false;
```

然后用同一个旧位置构造三个新列表：

```cpp
for (pos = 1; pos <= oldNcols; pos++) {
    if (oldToNew[pos] == 0)
        continue;
    types = lappend_oid(types,
        list_nth_oid(op->colTypes, pos - 1));
    typmods = lappend_int(typmods,
        list_nth_int(op->colTypmods, pos - 1));
    collations = lappend_oid(collations,
        list_nth_oid(op->colCollations, pos - 1));
}
```

这里 `list_nth_*` 使用 0-based 下标，所以是 `pos-1`；与 Bitmapset 中直接使用 1-based attno 的地方不要混淆。

代码先递归处理 `larg/rarg`，全部成功后才把新列表赋给当前 `op`：

```cpp
if (!compact_setop_metadata(op->larg, ...) ||
    !compact_setop_metadata(op->rarg, ...))
    return false;

op->colTypes = types;
op->colTypmods = typmods;
op->colCollations = collations;
```

即使某个深层节点失败，局部修改仍只在阶段一 `work` 副本中，不会污染原 Query。

### 3.16 阶段三入口为何重新复制 Query

`preprocess_union_all_order_key_projection()` 开头：

```cpp
if (!u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown ||
    !root->union_all_required_columns_pruned ||
    outer->sortClause == NIL)
    return;

if (outer->jointree != NULL && outer->jointree->quals != NULL)
    return;

work = (Query *)copyObject(outer);
```

这里没有复用阶段一的 `work`，因为阶段一已经提交，阶段二还可能把 ORDER BY/LIMIT 加入叶子。阶段三必须以“阶段一 + 可选阶段二”的最新 Query 为输入，再建立自己的原子提交边界。

`validate_order_projection_shape()` 没有调用 `leaf_query_is_safe()`，这是有意的：阶段二刚刚加入的叶子 sortClause/limitCount 此时是合法输入，若仍使用阶段一的叶子安全门，ORDER+LIMIT 场景会总是拒绝标量键投影。

因此调用方还必须用阶段一返回值确认本层曾经通过安全检查。不能用从 parent root 继承的扫描裁剪标志代替这个条件。

### 3.17 `prepare_union_all_order_keys()` 如何保留排序语义

函数通过 sortClause 找 TLE：

```cpp
SortGroupClause *sc = (SortGroupClause *)lfirst(scCell);
TargetEntry *outerTle =
    get_sortgroupclause_tle(sc, outer->targetList);
```

不能用 `list_nth(targetList, n)`，因为 sortClause 保存的是 `tleSortGroupRef`，而 targetlist 中可能混有用户列和 resjunk。

随后分三类：

```cpp
if (IsA(outerTle->expr, Var))
    continue;

vars = pull_vars_of_level((Node *)outerTle->expr, 0);
if (vars == NIL)
    continue;
```

- 直接 Var：已经可以作为窄 UNION 列使用，不制造重复 synthetic key；例如 `ORDER BY row_id`。
- 无 Var 的常量：各行相等，不需要每叶输出。
- 有 Var 的派生表达式：创建 `UnionAllOrderKey`。

key 元数据从原外层表达式提取：

```cpp
key->sortRef = outerTle->ressortgroupref;
key->type = exprType((Node *)outerTle->expr);
key->typmod = exprTypmod((Node *)outerTle->expr);
key->collation = exprCollation((Node *)outerTle->expr);
```

每个叶子映射后还要与这三项逐一相等，才追加到 `key->leafExprs`。因此 SetOp 新增列在每个分支拥有一致的 tuple descriptor。

Sort operator、ASC/DESC、NULLS FIRST/LAST 并不复制到 `UnionAllOrderKey`，因为这些语义仍完整保存在原 `SortGroupClause` 中；key 只负责提供值。

### 3.18 第二次 required 收集为什么跳过匹配的 sortRef

阶段三使用：

```cpp
if (find_union_all_order_key(keys,
                             tle->ressortgroupref) != NULL)
    continue;
```

命中说明该 TLE 的整个表达式将被 synthetic key Var 替换，所以不再收集其内部原始列。这里按 `ressortgroupref` 关联，而不是用 `equal(expr, mappedExpr)`，因为外层表达式与叶子本地表达式的 varno 本就不同。

未命中的 targetlist、剩余 quals、LIMIT/OFFSET 仍走普通 required 收集。之后再次调用 `preserve_semantically_required_outputs()`，防止第二次压紧误删 volatile/SRF 输出。

对于阶段一已得到的 `{row_id, embedding}` 接口，阶段三典型状态为：

```text
oldNcols = 2
rawRequired = {1}
oldToNew[1] = 1
oldToNew[2] = 0
key->newAttno = 2
```

注意：这里的旧第 2 列是“阶段一后的 embedding”，不是最初三列接口中的 title。

### 3.19 叶子排序键如何从 resjunk 变成集合可见列

阶段二为叶子 ORDER BY 新增的距离表达式通常是 resjunk。阶段三必须让它成为 SetOp 可见输出，否则 UNION 不能传这个值。

`rewrite_leaf_order_projection()` 先保留 rawRequired 原始列，然后：

```cpp
Expr *mapped = (Expr *)list_nth(key->leafExprs, leafIndex);
TargetEntry *oldTle = tlist_member((Node *)mapped, oldTarget);

newTle = makeTargetEntry((Expr *)copyObject(mapped),
                         (AttrNumber)nextResno++,
                         pstrdup(name),
                         false);
if (oldTle != NULL)
    newTle->ressortgroupref = oldTle->ressortgroupref;
```

关键点：

- 新 TLE 的 `resjunk=false`，因为它现在是 UNION 内部可见列；外层最终用户是否看到由 outer targetlist 决定。
- 若阶段二已有相同表达式，继承其 `ressortgroupref`，叶子 sortClause 仍能找到新 TLE。
- `nextResno` 在 raw 列之后连续分配。
- 名称 `__ua_order_N` 只用于 planner 私有 RTE 接口，不改变 catalog 中视图定义。

最后处理旧 resjunk：

```cpp
if (!tle->resjunk ||
    leaf_expr_is_order_key(tle->expr, keys, leafIndex))
    continue;
```

与 synthetic key 相同的旧 resjunk 被丢弃，避免同一距离在 targetlist 中出现两份；其他 junk 原样保留并重新编号。

### 3.20 外层距离表达式如何变成一个 Var

`rewrite_outer_order_projection()` 按 sortRef 找 key：

```cpp
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
```

命中的 TLE 整体替换，不是在原 OpExpr 内只换 embedding Var。结果从：

```text
OpExpr(Var(unionRti, embedding), query_vector)
```

变为：

```text
Var(unionRti, __ua_order_attno, FLOAT8OID)
```

TLE 本身的 `ressortgroupref` 没变，sortClause 也没变，所以 Sort 仍按同一个逻辑键排序。未命中的 TLE 仍需用本轮 `oldToNew` 更新普通 Var 列号。

如果用户同时 SELECT 这个距离表达式并用它 ORDER BY，同一个带 sortRef 的 TLE 会被替换为 synthetic key Var，既避免重复计算，也保持它是用户可见还是 resjunk 的原属性。

### 3.21 UNION synthetic key 为何引用 leftmost RTE

`rewrite_union_order_projection()` 先读取：

```cpp
leftmostVar = (Var *)
    ((TargetEntry *)linitial(unionQuery->targetList))->expr;
leftmostRti = leftmostVar->varno;
```

SetOp Query targetlist 按现有 parser 约定引用左侧第一个叶 RTE。新增 synthetic TLE 也必须遵循同一约定：

```cpp
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
```

这并不意味着运行时只从左叶取 distance。`SetOperationStmt` 决定所有叶子第 N 个位置合并成同一个集合列；union Query targetlist 的 leftmost Var 是该集合输出类型/来源的代表表达方式。

随后执行顺序是：

```cpp
compact_setop_metadata(... old raw columns ...);
append_setop_order_key_metadata(... keys ...);
```

必须先删掉 embedding 的旧元数据，再追加 float8 key。反过来做会让 `compact_setop_metadata(oldNcols)` 面对已经增长的列表长度，校验失败。

`append_setop_order_key_metadata()` 递归给每个 SetOperationStmt 追加完全相同的 key 三元组，保证多层 UNION 的内部接口也包含 synthetic 列。

### 3.22 Top-K 叶子为什么也是两阶段提交

Top-K 主入口先做：

```cpp
foreach (lc, leaves) {
    Query *copy = (Query *)copyObject(leafRte->subquery);
    prepared = lappend(prepared,
        make_prepared_leaf(leafRte, copy));
}

foreach (lc, prepared) {
    if (!prepare_leaf_diskann_topk(...,
                                   item->rewrittenQuery,
                                   outerQual))
        return;
}

commit_prepared_leaves(prepared);
```

第一轮只复制，第二轮只修改副本，第三轮才执行：

```cpp
item->ownerRte->subquery = item->rewrittenQuery;
```

这样不会出现第一个叶子已经有 LIMIT、第二个叶子映射失败但仍继续使用半改写集合的状态。

`prepare_leaf_diskann_topk()` 对每个 sortClause：

1. 用 `get_sortgroupclause_tle()` 找外层表达式；
2. 映射到叶子本地表达式；
3. 用 `tlist_member()` 复用已有等价 TLE，否则新增 resjunk；
4. 用 `assignSortGroupRef()` 建立叶子 sort 引用；
5. 复制 SortGroupClause，只替换 `tleSortGroupRef`；
6. 最后复制 `limitCount`。

它不删除全局 sortClause/limitCount。局部 Limit 放入“整个 snapshot 叶 Query”，所以叶 Query 内部的 NOT EXISTS/Anti Join 先完成，再由叶顶层排序和截断；不是先截断 snapshot 表再删除。

### 3.23 Top-K 为什么必须在两个列阶段之间

阶段一结束时，embedding 仍是一个真实 UNION 列，外层距离 TLE 仍是：

```text
OpExpr(vector_distance, Var(unionRti, embedding), Const/Param)
```

`is_diskann_vector_order_expr()` 需要这个 OpExpr 才能查 `pg_operator`、`pg_amop` 和 DiskANN opfamily。

阶段三结束后，外层 TLE 已变为：

```text
Var(unionRti, synthetic_float8_attno)
```

这时再做 DiskANN 识别只会看到 float8 Var，无法知道它来自哪个 access-method ordering operator。因此固定顺序是：

```text
原始列压紧（保留排序输入）
→ Top-K 识别和叶子 ORDER/LIMIT
→ 排序输入替换为标量 key
```

### 3.24 `createplan` 防回扩代码逐行解释

`create_scan_plan()` 原逻辑为：

```cpp
if (use_physical_tlist(root, rel) &&
    best_path->pathtype != T_ForeignScan) {
    if (best_path->pathtype == T_IndexOnlyScan)
        tlist = copyObject(indexinfo->indextlist);
    else
        tlist = build_physical_tlist(root, rel);
} else {
    tlist = build_path_tlist(root, best_path);
}
```

physical 分支可能把整张表或整个索引的属性都加入 Scan Plan。新增判断位于 `use_physical_tlist()` 最前面：

```cpp
if (root->union_all_required_columns_pruned &&
    rel->rtekind == RTE_RELATION)
    return false;
```

逐项含义：

- 判断放在函数最前，优先级高于原来“什么 RTE 可以用 physical tlist”的判断。
- 只对 `RTE_RELATION` 返回 false；函数、VALUES、CTE 等其他 RTE 继续使用既有规则。
- planner flag 从外层向所有子 root 继承，因此 delta、snapshot、delete 的基表 Scan 都走 `build_path_tlist()`。
- `build_path_tlist()` 根据 PathTarget 构造输出；PathTarget 已由压紧后的 Query 和内部 join/filter 需求计算。
- Filter 中引用 title/reason 不要求它成为 Scan 输出。扫描节点可取属性求值 qual，但只把父节点需要的表达式写入结果 slot。
- 对 Index Only Scan 也不再直接复制完整 `indextlist`，因此 `Output` 可以只剩索引中真正需要的 row_id。

这段改动只影响阶段一成功的 Query 树。如果安全校验失败，flag 不会设置，所有 Scan 使用原 `use_physical_tlist()` 行为。

### 3.25 三个阶段的原子性边界对照

| 阶段 | 工作副本 | 写入发生点 | 失败结果 |
|---|---|---|---|
| 必需列/WHERE | 整个 outer Query 的 `copyObject` | 安装 outer 的 targetList/jointree/rtable | 原 outer 完全不变 |
| Top-K | 每个 leaf Query 单独 `copyObject` | `commit_prepared_leaves()` 替换全部叶 subquery | 所有 ownerRte 保持原叶 |
| 标量排序键 | 当前 outer Query 再次 `copyObject` | 安装 outer 的 targetList/jointree/rtable | 保留阶段一/二结果，不做阶段三 |

这解释了为什么阶段三失败不会撤销安全的 title 裁剪，Top-K 失败也不会撤销阶段一：三项能力是串联调用，但各自有独立提交边界。

### 3.26 目标 SQL 的内存结构逐字段快照

仍以 `SELECT row_id ... WHERE ... ORDER BY distance LIMIT 4` 为例。

#### 初始状态

```text
outer.targetList
  TLE resno=1, resjunk=false, expr=Var(unionRti,1:row_id)
  TLE resno=2, resjunk=true,  ressortgroupref=1,
      expr=OpExpr(Var(unionRti,3:embedding), vector_const)

unionRte.eref->colnames = [row_id, title, embedding]
unionQuery.targetList   = [Var(left,1), Var(left,2), Var(left,3)]
setop.colTypes          = [INT8OID, TEXTOID, VECTOROID]
```

#### 阶段一提交后

```text
outer.jointree->quals   = NULL
delta.leaf.quals       += delta.row_id > 2
snapshot.leaf.quals    += s.row_id > 2

required               = {1,3}
oldToNew               = [unused,1,0,2]

unionRte.eref->colnames = [row_id, embedding]
unionQuery.targetList   = [Var(left,1), Var(left,2)]
setop.colTypes          = [INT8OID, VECTOROID]
outer distance Var      = Var(unionRti,2)
```

#### 阶段二提交后

```text
delta leaf targetList
  visible row_id
  visible embedding
  resjunk distance, ressortgroupref=X
delta leaf sortClause -> X
delta leaf limitCount = 4

snapshot leaf 同样增加本地 distance/sort/limit
```

#### 阶段三提交后

```text
rawRequired             = {1}
raw oldToNew            = [unused,1,0]
key.newAttno            = 2

每叶 targetList
  TLE resno=1, visible row_id
  TLE resno=2, visible distance, ressortgroupref=X
  原 resjunk distance 被去重

unionRte.eref->colnames = [row_id, __ua_order_1]
setop.colTypes           = [INT8OID, FLOAT8OID]
outer sort TLE expr      = Var(unionRti,2:FLOAT8OID)
```

#### Plan 生成时

```text
root->union_all_required_columns_pruned = true
use_physical_tlist(base relation)       = false

delta scan Output                       = row_id, distance
snapshot scan Output                    = row_id, embedding
snapshot Anti Join Output               = row_id, distance
delete scan Output                      = row_id
Merge Append/Append Output              = row_id, distance
```

snapshot Scan 仍输出 embedding 是正确的：Anti Join 上层需要用它计算 distance。关键是 title 已消失，且 embedding 在 distance 生成后不再越过叶子公共接口。

### 3.27 函数级调用图

阶段一完整调用关系：

```text
preprocess_union_all_required_columns
  ├─ required_columns_outer_query_is_safe
  ├─ copyObject(outer)
  ├─ get_single_union_subquery_rte
  ├─ collect_pure_union_all_leaves
  │    └─ collect_setop_leaves [recursive]
  ├─ count_visible_tlist_entries
  ├─ validate_required_column_shape
  │    └─ leaf_query_is_safe
  ├─ prepare_outer_where_pushdown
  │    ├─ check_outer_qual_pushdown_safe
  │    │    ├─ build_visible_output_map_tlist
  │    │    └─ validate_union_leaf_output_types
  │    ├─ map_outer_qual_to_leaf
  │    │    ├─ ReplaceVarsFromTargetList
  │    │    └─ leaf_local_expr_is_valid
  │    └─ append_outer_qual_to_leaf
  ├─ collect_required_output_columns
  │    └─ add_required_columns_from_expr
  ├─ preserve_semantically_required_outputs
  └─ compact_union_all_query
       ├─ remap_outer_query_vars
       │    └─ required_column_var_mutator [recursive]
       ├─ compact_leaf_targetlist
       ├─ compact_name_list
       ├─ compact_union_targetlist
       └─ compact_setop_metadata [recursive]
```

阶段三完整调用关系：

```text
preprocess_union_all_order_key_projection
  ├─ copyObject(outer)
  ├─ collect_pure_union_all_leaves
  ├─ validate_order_projection_shape
  ├─ prepare_union_all_order_keys
  │    ├─ get_sortgroupclause_tle
  │    └─ map_sort_expr_to_leaf [每 key × 每 leaf]
  ├─ collect_order_projection_raw_columns
  ├─ preserve_semantically_required_outputs
  └─ rewrite_union_order_projection
       ├─ rewrite_outer_order_projection
       ├─ rewrite_leaf_order_projection [每 leaf]
       │    └─ leaf_expr_is_order_key
       ├─ compact_union_targetlist
       ├─ compact_name_list
       ├─ compact_setop_metadata [recursive]
       └─ append_setop_order_key_metadata [recursive]
```

### 3.28 代码审查时应逐项验证的不变量

后续修改这段代码时，至少要逐项确认：

1. 所有叶子可见输出列数相同；
2. 每个位置的 type、typmod、collation 相同；
3. 所有可见 TargetEntry.resno 连续且从 1 开始；
4. resjunk 位于可见输出之后；
5. 每个 SortGroupClause 都能按 ressortgroupref 找到 TLE；
6. 外层所有目标 UNION Var 都指向未删除的新 attno；
7. 每层 SetOperationStmt 元数据长度等于新可见列数；
8. 每个 `eref->colnames` 与对应可见输出等长；
9. volatile/SRF/SubLink 输出位置没有被删除；
10. 外层 WHERE 只有在所有叶子完成等价映射后才删除；
11. Top-K 只在所有叶子准备成功后提交；
12. synthetic key 在每个叶子和每层 SetOp 的类型完全一致；
13. GUC off 或任一安全门失败时返回 false、不新设置 planner flag；已有的父层扫描策略继承值不等于本层成功；
14. `use_physical_tlist()` 只在 flag 为 true 的基表路径上禁用；
15. 全局 ORDER BY/LIMIT 始终保留。

## 4. 总调用链

入口位于 `subquery_planner()`，处在 `preprocess_rownum()` 之后、`pull_up_subqueries()` 之前：

```text
subquery_planner
  ├─ preprocess_rownum
  ├─ preprocess_union_all_required_columns      阶段一：WHERE + 原始列压紧
  ├─ preprocess_union_all_diskann_topk          阶段二：可选分支 Top-K
  ├─ preprocess_union_all_order_key_projection  阶段三：标量排序键
  ├─ pull_up_subqueries
  └─ flatten_simple_union_all / 后续通用规划
```

对应代码在 `planner.cpp:1713`、`1717`、`1725`。

这个次序不能随意交换：

- 阶段一必须在旧列号仍完整时映射 WHERE；
- 阶段二必须看到原始的向量距离 `OpExpr`，才能识别 DiskANN operator family；
- 阶段三若先执行，会把距离表达式替换成一个普通 `Var(float8)`，Top-K 识别将失去原始算子信息；
- 三个阶段都必须早于通用子查询拉平，否则原 UNION RTE 和叶子 Query 边界已经改变。

## 5. GUC 与 planner 状态

### 5.1 GUC 注册

`guc_sql.cpp:856` 注册：

```text
enable_union_all_required_columns_pushdown
  context:  PGC_USERSET
  node:     NODE_ALL
  group:    QUERY_TUNING_METHOD
  default:  false
```

session 值保存在：

```cpp
u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown
```

它与已有的 `enable_union_all_diskann_topk_pushdown` 独立：

| 必需列 GUC | Top-K GUC | 行为 |
|---|---|---|
| off | off | 原始规划路径 |
| on | off | WHERE/列压紧/标量排序键；不增加叶子 Limit |
| off | on | 既有 Top-K 行为，可能保留宽列 |
| on | on | 先压紧，再加安全 Top-K，最后传标量排序键 |

### 5.2 `PlannerInfo` 标志

`relation.h:529` 新增：

```cpp
bool union_all_required_columns_pruned;
```

阶段一成功提交后将其设为 `true`。`subquery_planner()` 创建子 root 时，从 `parent_root` 继承该值：

```cpp
root->union_all_required_columns_pruned =
    parent_root != NULL && parent_root->union_all_required_columns_pruned;
```

这样 UNION 叶子、snapshot 子查询和 NOT EXISTS 转换出的下层规划都知道自己属于一个已压紧的查询树。该标志只存在于 planner 内存中，不写入 Plan 序列化结构，也不影响后续其他 SQL。

## 6. 阶段一：必需列与 WHERE 生命周期

主入口是 `preptopk.cpp:142` 的 `preprocess_union_all_required_columns()`。

### 6.1 入口流程

伪代码如下：

```text
if GUC off:
    return
if outer query shape unsafe:
    return

work = deep copy of outer Query
unionRte = find the only UNION subquery RTE
leaves = recursively collect pure UNION ALL leaves
validate positional shape and leaf safety

wherePushed = map WHERE to every leaf
required = collect outer consumers after WHERE processing
required += volatile/SRF/sort semantic outputs
if required empty:
    retain first column as carrier

compact every positional interface using oldToNew
if every check succeeds:
    install work.targetList/jointree/rtable
    mark PlannerInfo as pruned
else:
    leave original Query untouched
```

### 6.2 为什么先复制整棵 Query

入口在 `preptopk.cpp:166` 执行：

```cpp
work = (Query *)copyObject(outer);
```

后面的动作会同时修改多片结构：叶子 WHERE、叶子 targetlist、叶子 alias、SetOp 元数据、UNION targetlist 和外层 Var。任何一步失败都不能留下“一个叶子已改、另一个未改”的半提交状态。

实现因此使用 copy-on-write：所有校验和重写在 `work` 上完成，最后只把 `targetList`、`jointree`、`rtable` 三个准备好的指针装回原 `outer`。失败路径直接返回，原树不变。

### 6.3 外层结构门

`required_columns_outer_query_is_safe()` 位于 `preptopk.cpp:235`。第一期刻意限制为：

- `CMD_SELECT`；
- 外层 WHERE 非空；
- 外层只有一个 RTE 和一个 `RangeTblRef`；
- 该 RTE 是目标 UNION 子查询；
- 无外层 SetOp、聚合、GROUP/HAVING、DISTINCT、Window；
- 无 CTE、递归、修改型 CTE、行锁；
- 无外层 SRF、ROWNUM、SubLink、RLS；SRF 不能只检查 flag，还需直接扫描表达式；
- 无 OFFSET、PERCENT、WITH TIES。

这不是说这些结构永远不能优化，而是当前实现没有为它们证明完整的列号和求值语义，因此选择保守降级。

### 6.4 识别纯 UNION ALL

`get_single_union_subquery_rte()` 要求外层 rtable 中唯一对象是 `RTE_SUBQUERY`，且其 `subquery->setOperations` 非空。

`collect_pure_union_all_leaves()` 和 `collect_setop_leaves()` 递归遍历 SetOp 树：

- 中间节点必须是 `SetOperationStmt`；
- `op == SETOP_UNION`；
- `all == true`；
- 叶节点必须是指向子 Query 的 `RangeTblRef`。

因此 UNION distinct、INTERSECT、EXCEPT 会在修改前直接退出。四叶、八叶等嵌套形状可以递归处理，不依赖固定叶子数。

### 6.5 叶子和位置元数据校验

`validate_required_column_shape()` 检查：

- UNION Query 自身没有 ORDER/LIMIT、聚合、窗口、CTE、RLS 等复杂语义；
- UNION targetlist 全部为可见输出，不含 resjunk；
- 顶层 `SetOperationStmt` 的 `colTypes`、`colTypmods`、`colCollations` 长度均等于原始列数；
- 每个叶子满足 `leaf_query_is_safe()`；
- 每个叶子可见 targetlist 列数与 UNION 一致；
- 每个叶子 RTE 的 `eref->colnames` 长度一致；
- 叶子 RTE 不是 security barrier 或 lateral。

这一步建立后续“第 N 个集合列在每个叶子都对应第 N 个输出”的前提。

### 6.6 WHERE 如何下推并终止列生命周期

`prepare_outer_where_pushdown()` 先调用 `check_outer_qual_pushdown_safe()`，再为每个叶子调用 `map_outer_qual_to_leaf()`。

安全条件包括：

- 无 SubLink、volatile、SRF、aggregate、window；
- 所有 level-0 Var 都引用目标 UNION RTE；
- 只能是普通输出列，不能是 whole-row/system column；
- 对每个叶子，映射位置的 type、typmod、collation 相同；
- 映射后的表达式只含该叶本地、有效 RTE 的 Var。

真正的映射使用 `ReplaceVarsFromTargetList()`：

```text
outer Var(union_rti, attno=2)
        ↓ 第 2 个集合输出
leaf targetlist 第 2 项表达式
        ↓
delta_t.title 或 s.title
```

所有叶子都映射成功后，`append_outer_qual_to_leaf()` 用 AND 合并叶子原有条件，并将：

```cpp
outer->jointree->quals = NULL;
```

这一步非常重要。以 `WHERE title IS NOT NULL` 为例：

- 叶子扫描 Filter 仍需读取 `title`；
- 外层 WHERE 已不存在；
- 后续必需列收集不再把 `title` 当作 UNION 输出；
- `title` 的最后消费者就停在各叶子 Filter。

如果 WHERE 不能安全下推，函数返回 `false`，外层谓词保留。后续仍可裁掉完全无关的列，但 WHERE 引用列必须继续穿过 UNION，保证正确性。

### 6.7 必需列如何收集

`collect_required_output_columns()` 扫描：

- 外层全部 targetlist，包括 ORDER BY 自动生成的 resjunk TargetEntry；
- 尚未删除的外层 jointree quals；
- LIMIT/OFFSET 表达式。

`add_required_columns_from_expr()` 使用 `pull_vars_of_level(expr, 0)`，只接受指向目标 UNION RTE 的普通 Var，并把 `varattno` 加入 `Bitmapset`。

例如原集合接口为：

```text
1 = row_id
2 = title
3 = embedding
```

`SELECT row_id ... WHERE row_id > 2` 在 WHERE 成功下推后，最终集合为：

```text
required = {1}
```

`SELECT row_id ... ORDER BY embedding <-> const` 此时外层 targetlist 还有距离表达式对应的 resjunk 项，所以阶段一得到：

```text
required = {1, 3}
```

排序键在阶段三才进一步从原始 `embedding` 转换为标量 distance。

### 6.8 为什么还要保留语义输出

`preserve_semantically_required_outputs()` 检查每个叶子的同一可见输出位置。只要任一叶在该位置包含以下语义，就把该位置加入 required：

- `ressortgroupref`；
- set-returning expression；
- volatile expression；
- SubLink。

原因是一个“外层没有引用”的表达式也可能有副作用或改变行数。比如视图输出含 `nextval()`，删除该输出会少执行序列调用。回归通过比较 GUC off/on 的 `last_value=13` 看护这一点。

如果最终 required 为空，代码保留第 1 列作为 carrier。legacy SetOp/Append 路径对零列 tuple 支持不完整，保留一列比制造空接口更安全。

## 7. 位置型输出压紧

核心入口是 `compact_union_all_query()`，位于 `preptopk.cpp:676`。

### 7.1 `oldToNew` 映射

代码按原列相对次序分配连续新列号：

```text
原始：row_id(1), title(2), embedding(3)
需要：row_id, embedding

oldToNew[1] = 1
oldToNew[2] = 0   表示删除
oldToNew[3] = 2
```

新接口永远是连续的 `1..N`，因此不需要把稀疏列号映射带进 Path、Plan 或 Executor。

### 7.2 同步修改的结构

| 函数 | 修改对象 |
|---|---|
| `remap_outer_query_vars()` | 外层 targetlist、剩余 quals、LIMIT/OFFSET 中引用 UNION 的 Var |
| `compact_leaf_targetlist()` | 每个叶子的可见输出；resjunk 重新编号后追加 |
| `compact_name_list()` | 叶子 RTE 和 UNION RTE 的 alias 列名 |
| `compact_union_targetlist()` | UNION Query 指向左叶输出的 Var、TargetEntry.resno |
| `compact_setop_metadata()` | 每个 SetOperationStmt 的类型、typmod、collation 列表 |

`required_column_var_mutator()` 只改：

```text
varlevelsup == 0
varno       == unionRti
varattno    > 0
```

发现 whole-row、system attribute、超界列号或指向被删除列的残留引用时，会设置 `failed`，使整个副本放弃提交。

### 7.3 SetOperationStmt 为什么必须递归改

多叶 UNION ALL 不是只有顶层一个集合节点。例如四叶树可能是：

```text
              UNION ALL
              /       \
         UNION ALL   UNION ALL
          /   \       /    \
         L1   L2     L3    L4
```

每个中间 `SetOperationStmt` 都带自己的 `colTypes`、`colTypmods`、`colCollations`。`compact_setop_metadata()` 递归处理左右子树，遇到 `RangeTblRef` 才停止。只改顶层会让内部节点仍宣称输出三列，最终在通用 SetOp 规划或 setrefs 阶段发生错位。

### 7.4 提交点

所有压紧成功后只执行：

```cpp
outer->targetList = work->targetList;
outer->jointree = work->jointree;
outer->rtable = work->rtable;
root->union_all_required_columns_pruned = true;
```

保留原 `Query *outer` 的对象身份，可以避免调用者持有的 Query 指针失效。

## 8. 阶段二：复用现有 DiskANN Top-K

`preprocess_union_all_diskann_topk()` 位于 `preptopk.cpp:1193`。这部分是现有 UNION ALL Top-K 能力，本特性把它放在两次列处理之间。

### 8.1 Top-K 识别

`diskann_topk_outer_query_is_safe()` 要求简单 SELECT、非空 ORDER BY、合法正 LIMIT、无 OFFSET，并至少有一个 DiskANN 向量排序键。

`is_diskann_vector_order_expr()` 不只按函数名判断，而是：

1. 确认表达式是二元 `OpExpr`；
2. 确认返回类型是 `float8`；
3. 从 `pg_operator` 校验参数类型；
4. 从 `pg_amop` 找 operator family；
5. 确认 opfamily 的 access method 为 `DISKANN_AM_OID` 且用途为 `AMOP_ORDER`。

### 8.2 两阶段叶子提交

Top-K 入口先复制所有叶子到 `DiskAnnTopKPreparedLeaf`，再依次调用 `prepare_leaf_diskann_topk()`：

- 映射尚存在的外层 WHERE；
- 映射每个排序表达式；
- 在叶子 targetlist 中查找或增加 resjunk 排序项；
- 复制完整 `SortGroupClause`；
- 复制全局 limitCount 为叶子 limitCount。

全部叶子成功后，`commit_prepared_leaves()` 才替换原叶 Query。全局 ORDER BY 和 LIMIT 始终保留，因此局部 Top-K 只是减少每个分支向全局层提供的候选数。

在本特性 GUC 开启时，阶段一已经把安全 WHERE 放入叶子并删除外层 recheck，所以这里通常看到 `outerQual == NULL`，不会重复增加谓词。

## 9. 阶段三：将 vector 转成标量排序键

主入口是 `preptopk.cpp:731` 的 `preprocess_union_all_order_key_projection()`。

### 9.1 入口条件

只有同时满足以下条件才运行：

- 必需列 GUC 为 on；
- 当前 Query 的阶段一返回 true；继承的 `root->union_all_required_columns_pruned` 不能替代；
- 外层 `sortClause` 非空；
- 外层没有未下推的 quals。

这意味着 ORDER BY 不需要 LIMIT 也能得到标量键优化；Top-K GUC 可以保持 off。

### 9.2 找到需要物化的排序表达式

`prepare_union_all_order_keys()` 遍历外层 sortClause：

- 直接 Var 已经足够窄，跳过；
- 常量排序键不需要叶子输出，跳过；
- 其他表达式的所有 Var 必须来自目标 UNION RTE；
- 调用 `map_sort_expr_to_leaf()` 为每个叶子构造等价本地表达式；
- 每叶结果的 type、typmod、collation 必须完全相同。

每个派生排序键记录在 `UnionAllOrderKey`：

```text
sortRef     外层 SortGroupClause 的引用号
leafExprs   每个叶子的本地表达式
type        结果类型
typmod      类型修饰
collation   排序规则
newAttno    新 UNION 接口中的列号
```

### 9.3 第二次必需列收集

阶段一后，当前 UNION 可能已经从三列变为：

```text
1 = row_id
2 = embedding
```

`collect_order_projection_raw_columns()` 再收集一次原始列需求，但会跳过即将由 synthetic key 替换的排序 TargetEntry。对于最终只输出 row_id 的距离排序，得到：

```text
rawRequired = {1}
```

随后给 synthetic key 分配新列号 2。

### 9.4 重写叶子

`rewrite_leaf_order_projection()`：

1. 保留 `rawRequired` 对应的原始可见列；
2. 为每个排序键追加可见 TargetEntry；
3. 名称为 `__ua_order_1`、`__ua_order_2`；
4. 表达式为本叶的实际距离计算；
5. 删除与新 key 相同的旧 resjunk 副本；
6. 保留其他叶子内部 resjunk 项。

于是两个叶子的公共接口都变成：

```text
row_id bigint
__ua_order_1 float8
```

### 9.5 重写 UNION 与外层排序

`rewrite_union_order_projection()` 完成四件事：

- 再次按 `oldToNew` 压紧 UNION 原始列；
- 将 synthetic key 追加到 UNION targetlist 和 alias 列名；
- 递归压紧旧 SetOp 元数据，再为每层追加 key 的 type/typmod/collation；
- 将外层排序 TargetEntry 的原距离表达式替换为 `Var(unionRti, key->newAttno)`。

外层 `SortGroupClause` 和 `ressortgroupref` 不变，因此 ASC/DESC、NULLS、collation 和多键顺序仍由原语义控制。改变的只是排序值从哪里获得：从“UNION 上方重新读取 embedding 计算”，变成“叶子已计算好的 float8”。

## 10. `createplan`：为什么还需要防回扩

Query targetlist 压紧后，PathTarget 已经只含真正需要的属性。但 `create_scan_plan()` 默认可能调用：

```cpp
build_physical_tlist(root, rel)
```

它会按基表物理 rowtype 构造完整扫描输出。这在普通查询里可能省掉 projection，但会破坏本特性“从源头不产生死列”的目标。

`createplan.cpp:1582` 因此在 `use_physical_tlist()` 最前面增加：

```cpp
if (root->union_all_required_columns_pruned && rel->rtekind == RTE_RELATION)
    return false;
```

之后 `create_scan_plan()` 走 `build_path_tlist(root, best_path)`，只按 Path 真正需要的表达式生成 Scan `Output`。

这也是 `delete_t` 能正确裁剪的关键。必需列 preprocessor 并没有专门遍历 delete 表并删除 `reason`；它保留 NOT EXISTS 子查询语义。通用 planner 知道：

- `d.row_id` 是 Anti Join 键，需要向 join 输出；
- `d.reason` 只用于本地 Filter；
- `unused_delete` 无消费者。

禁止 physical tlist 回扩后，Scan 可以读取 `reason` 完成过滤，但只向 Anti Join 输出 `d.row_id`。

代价是这类基表路径不能利用“整行零投影”的快捷方式，会执行窄 targetlist 投影。该取舍被限制在已成功压紧的单个查询树及其后代，不是全局关闭 physical tlist。

## 11. 一条目标 SQL 的完整演变

以此 SQL 为例：

```sql
SELECT row_id
FROM union_view
WHERE row_id > 2
ORDER BY embedding <-> '[0,0,0,0]'
LIMIT 4;
```

### 11.1 解析/重写后的初始接口

```text
UNION 输出：1=row_id, 2=title, 3=embedding
外层输出：row_id
外层 resjunk：embedding <-> const
外层 WHERE：row_id > 2
```

### 11.2 阶段一

```text
WHERE 映射到 delta 和 snapshot 叶子
外层 WHERE 删除
required = {row_id, embedding} = {1,3}
oldToNew = [1→1, 2→0, 3→2]

UNION 输出：1=row_id, 2=embedding
title 从 UNION 接口消失
```

### 11.3 阶段二

```text
识别 embedding <-> const 属于 DiskANN order operator
把 ORDER BY 和 LIMIT 4 加到每个叶子
全局 ORDER BY/LIMIT 保留
snapshot 的 NOT EXISTS/Anti Join 仍位于叶子 Limit 下方
```

这里的“下方”按 EXPLAIN 树表示先执行 Anti Join，再 Sort/Limit；因此删除记录不会占用 snapshot 分支的 Top-K 名额。

### 11.4 阶段三

```text
当前旧接口：1=row_id, 2=embedding
rawRequired = {1}
synthetic key 新列号 = 2

每叶输出：1=row_id, 2=distance(float8)
UNION 输出：1=row_id, 2=distance(float8)
外层排序表达式：Var(unionRti, 2)
embedding 在叶子完成距离计算后终止
```

### 11.5 createplan

```text
union_all_required_columns_pruned = true
base relation 不构造 physical tlist
扫描 Output 不重新出现 title 或其他死列
```

最终典型计划结构：

```text
Global Limit
  -> Merge Append / Global Sort
       Output: row_id, distance
       -> Local Limit
            -> Ann Index Scan or Scan + Sort
                 Output: row_id, distance
       -> Local Limit
            -> Sort
                 -> Anti Join
                      Output: row_id, distance
                      -> snapshot scan
                           Output: row_id, embedding
                      -> delete scan
                           Output: row_id
```

具体选择 Hash Anti Join 还是 Merge Anti Join、普通 Index Scan 还是 Ann Index Scan，由 CBO 和数据分布决定；回归关注的是字段生命周期与 Limit 相对语义位置。

## 12. 四类 SQL 分别经过哪些阶段

| SQL 形态 | 阶段一 | 阶段二 | 阶段三 | 结果接口 |
|---|---|---|---|---|
| WHERE | WHERE 下推，原始列压紧 | 跳过 | 跳过 | `row_id` |
| WHERE + ORDER BY | WHERE 下推，保留排序输入 | 无 LIMIT，跳过 Top-K | 物化排序表达式 | `row_id + distance` |
| WHERE + LIMIT | WHERE 下推，原始列压紧 | 无 ORDER，跳过 | 跳过 | `row_id`，只有全局 Limit |
| WHERE + ORDER BY + LIMIT | WHERE 下推，保留排序输入 | 安全时每叶 Top-K | 物化排序表达式 | `row_id + distance`，全局及局部 Limit |

## 13. 安全回退设计

这个实现的主要策略是“证明不了就不改”，不是尝试修复部分结构后继续规划。

### 13.1 整体跳过

典型整体跳过条件：

- 新 GUC off；
- 外层没有 WHERE；
- 外层不是单一 UNION 子查询；
- UNION distinct/INTERSECT/EXCEPT；
- whole-row/system column；
- security barrier、lateral、RLS；
- 聚合、窗口、CTE、递归、行锁；
- 叶子已有 ORDER/LIMIT；
- 各叶列数或 type/typmod/collation 不一致；
- 列号或 alias 元数据不能同步验证。

### 13.2 部分能力降级

三种能力可以分别降级：

- WHERE 不能下推：保留外层 recheck 和其所需列，仍可删其他死列；
- Top-K 不满足 DiskANN 条件：不增加叶子 Limit，仍可做必需列与标量键；
- 排序键不能在所有叶子等价物化：保留阶段一后的原始排序输入列。

### 13.3 原子性边界

阶段一、Top-K、阶段三各自使用副本或 prepared leaves，阶段内部原子提交。后阶段失败不会回滚前阶段已经完成且独立证明安全的优化。

## 14. 可观测性与调试入口

### 14.1 DEBUG1 日志

阶段一成功：

```text
union_all_required_columns: old=3 new=1 where_pushed=true outer_recheck=false
```

阶段一拒绝原因包括：

```text
outer_query_shape
unsafe_union_rte
non_union_all
inconsistent_output_shape
unsupported_outer_reference
compact_validation_failed
```

阶段三成功：

```text
union_all_required_columns: order_keys=1 raw_columns=1 materialized=true
```

阶段三失败：

```text
union_all_required_columns: order projection skipped reason=rewrite_failed
```

Top-K 仍使用原有 `preprocess_union_all_diskann_topk` 和 `prepare_leaf_diskann_topk` DEBUG1 日志。

### 14.2 建议断点

按排查顺序可设置：

```text
preprocess_union_all_required_columns
prepare_outer_where_pushdown
collect_required_output_columns
compact_union_all_query
compact_setop_metadata
preprocess_union_all_diskann_topk
prepare_leaf_diskann_topk
preprocess_union_all_order_key_projection
rewrite_union_order_projection
use_physical_tlist
```

重点观察变量：

```text
unionRti
oldNcols / newNcols
required / rawRequired
oldToNew[]
leaves
keys / key->newAttno
root->union_all_required_columns_pruned
```

### 14.3 EXPLAIN 判读

推荐：

```sql
SET enable_union_all_required_columns_pushdown = on;
EXPLAIN (VERBOSE ON, COSTS OFF) ...;
```

需要区分：

- `Filter: title ...` 表示该列是本节点表达式输入，合法；
- `Output: ..., title, ...` 表示该列继续向父节点传输，若无上层消费者就是问题；
- heap Scan Output 变窄不等价于数据页只物理读取这些字节；它严格表示不再将死列 deform/detoast/project/copy 到上层 slot。

## 15. 回归如何对应源码风险

测试入口为 `src/test/regress/sql/union_all_required_columns.sql`。

### 15.1 计划看护

| SQL 区域 | 源码风险 |
|---|---|
| 四种 WHERE/ORDER/LIMIT 组合，102～131 行 | 三阶段组合与 Limit 相对位置 |
| DiskANN 计划，133～140 行 | Top-K 识别和 Ann Index Scan 窄输出 |
| delete 过滤，142～147 行 | internal Filter 列不进入 Anti Join 输出 |
| Top-K GUC off，149～158 行 | 列裁剪不依赖 Top-K |
| 必需列 GUC off，160～169 行 | 默认关闭和旧计划兼容 |
| 四叶 UNION，171～185 行 | 递归 SetOp 元数据与非连续列号重排 |
| distinct/leaf LIMIT/security barrier，299～320 行 | 安全门和原路径回退 |

这些测试直接保存完整 `EXPLAIN VERBOSE`，不是只返回一个布尔断言。因此任一 Scan、Join、Append、Sort、Limit 的 Output 或层级变化都会形成 golden diff。

### 15.2 正确性看护

- 显式结果保留 `row_id=3` 的 UNION ALL 重复行；
- 四象限分别在 GUC off/on 下执行，用双向 `EXCEPT ALL` 比较；
- 有序结果按位置比较，不只比较无序集合；
- volatile 视图用 sequence 最终值证明未丢副作用；
- 投影重排验证 `varattno` 映射；
- 参数化 WHERE、vector、LIMIT 验证 prepared plan；
- 回退结构继续核对结果。

## 16. 构建和测试入口

修改 `.cpp` 后按工作区统一 Debug/O0 脚本：

```bash
BUILD=/home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh
$BUILD incremental
```

若修改 `relation.h`、`prep.h` 或 GUC 结构头文件，当前构建未启用可靠头文件依赖，必须使用：

```bash
$BUILD full
```

定向原生回归：

```bash
repo=/home/qiumc/data/code/cpp/openGauss-server
install=$repo/mppdb_temp_install
gcclib=/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3/gcc/lib64

env \
  GAUSSHOME=$install \
  SSL_LIB_PATH=$install/lib \
  LD_LIBRARY_PATH=$install/lib:$gcclib \
  make -C $repo/src/test/regress union_all_required_columns_check
```

与既有 DiskANN 回归联跑：

```bash
env \
  GAUSSHOME=$install \
  SSL_LIB_PATH=$install/lib \
  LD_LIBRARY_PATH=$install/lib:$gcclib \
  make -C $repo/src/test/regress \
    union_all_required_columns_check EXTRA_TESTS=diskann_vector
```

上面 `1/1`、`2/2` 是初始交付的历史记录。安全复核已增加 `union_all_required_columns_safety`，专用 schedule 现在包含两个用例；本轮实际联跑命令和结果以[深度复核报告 §5](../测试/UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md#5-最终构建与回归结果)为准。

## 17. 维护时最容易踩的坑

### 17.1 只删 targetlist，不改 SetOp 元数据

会造成列数、类型或 collation 错位。新增集合层级时必须确保每个中间 SetOperationStmt 都递归更新。

### 17.2 先改列号，再映射 WHERE/ORDER

外层 `varattno=3` 原本代表 embedding；若先删 title 而没有保存映射，它可能错误指向新第 2 列或直接越界。原则始终是“先按旧位置完成表达式映射，再压紧并统一重编号”。

### 17.3 过早把 ORDER BY 改成标量 Var

Top-K 识别依赖原始 DiskANN `OpExpr`。排序键投影必须位于 Top-K 判断之后。

### 17.4 忘记 physical targetlist

Query/Path 看起来已经只有 row_id，但最终 EXPLAIN Scan 又出现全表列时，应首先检查 `use_physical_tlist()` 和 planner 标志传播。

### 17.5 把 Filter 输入误判成 Output 泄漏

本地谓词当然需要读取列。目标是该列在最后消费者之后不再进入父节点 slot，而不是禁止扫描节点求值它。

### 17.6 为通过计划 golden 强制具体 Join 算法

本特性的语义核心是字段生命周期。Hash Anti Join 与 Merge Anti Join 都可以正确；除非索引路径本身是验收目标，不应把某个 join method 误当成必需语义。

### 17.7 扩大支持范围却不补安全门

新增 lateral、RLS、CTE、outer join、分布式 RemoteQuery 等能力时，需要同时回答权限、泄漏安全、列号、分布键和序列化接口问题，并增加拒绝/成功两类回归。

## 18. 当前边界与后续方向

当前实现完成目标单机 SQL，但仍有明确边界：

- 外层必须有 WHERE；
- 外层必须是单一 UNION 子查询；
- 集合树必须全部是 UNION ALL；
- 不覆盖 outer join、lateral、RLS、递归、CTE 等复杂结构；
- GUC 默认保持 off；
- 多 DN、Stream/RemoteQuery 尚未形成实测 golden；
- 没有改变 heap 页读取方式，只改变属性求值、slot 和节点间传输；
- snapshot Anti Join 是否能使用 DiskANN 有序路径是另一项优化，不属于列裁剪本身。

后续扩展优先级建议：

1. 多 DN 下验证分布键、Stream/RemoteQuery 输出和序列化宽度；
2. 为 RLS、安全屏障和 lateral 建立明确的 leakproof/权限边界；
3. 评估 `use_physical_tlist` 禁用带来的投影 CPU 与宽元组收益；
4. 扩展到 JOIN 中的 UNION RTE，而不是只支持单一 FROM；
5. 足够回归和灰度后再讨论 GUC 默认开启。

## 19. 从零手写实现指南

这一章不是继续解释现有代码，而是假设读者回到未实现状态，按什么顺序可以重新写出同等能力，并在每一步避免把 Query 树写坏。

### 19.1 开始编码前先冻结五条契约

先把下面五条写在开发笔记顶部。实现过程中如果某个改动违背其中一条，应退回重新设计，而不是继续加补丁兜底。

1. **逻辑结果不变**：UNION ALL 的重复行、WHERE、排序、全局 LIMIT、volatile/SRF 求值次数都不改变。
2. **位置接口全改或全不改**：叶子、UNION targetlist、SetOp 元数据、alias、外层 Var 必须使用同一映射。
3. **先映射表达式，后改列号**：WHERE 和 ORDER 必须在旧位置仍有效时映射到叶子。
4. **每个阶段原子提交**：不能把半改写 Query 留给后续 planner。
5. **证明不了就回退**：首版宁可少优化，也不能猜测 RLS、lateral、whole-row 或复杂 SetOp 的语义。

目标输入范围也要先固定：

```text
外层 CMD_SELECT
外层 WHERE 必须存在
FROM 中只有一个 RTE_SUBQUERY
该 subquery 是 pure UNION ALL
叶子是普通 SELECT，可含普通 WHERE/JOIN/NOT EXISTS
ORDER BY、LIMIT 各自可选
```

不要一开始就支持 JOIN 中多个 UNION RTE、outer join、RLS 或多 DN；这些范围会显著扩大 Var level、权限和分布键问题。

### 19.2 推荐的编码顺序

最稳妥的实现顺序不是按最终调用顺序一次写完，而是分七个可验证阶段：

| 编码阶段 | 先完成什么 | 当场验收 |
|---|---|---|
| A | GUC、PlannerInfo flag、函数声明、planner 空入口 | GUC 可 SHOW/SET，GUC off 计划不变 |
| B | 外层形状门、纯 UNION ALL 叶子发现、类型校验 | 支持形状进入 DEBUG1；不支持形状安全跳过 |
| C | WHERE 映射与两遍提交 | WHERE 出现在每个叶子，外层重复 Filter 消失，结果不变 |
| D | required 收集、oldToNew、五类位置接口压紧 | WHERE-only 的 Append/Join 接口变窄，四叶 UNION 不崩溃 |
| E | createplan physical tlist 防回扩 | delta/snapshot/delete Scan Output 不再出现死列 |
| F | 标量排序键物化 | Append 以上只传 float8 distance，不传 vector |
| G | 与 Top-K 组合、全量拒绝路径和原生回归 | 四象限、GUC 矩阵、volatile、fallback 全通过 |

阶段 D 之前不要急着接 Top-K。先把普通 WHERE-only 位置压紧写对，问题更容易定位。

### 19.3 openGauss 代码风格与可用基础设施

手写时优先复用内核已有 Node/List/MemoryContext API，不引入 STL 容器或裸 `malloc`：

| 需求 | 应使用 |
|---|---|
| 深拷贝 Query/Expr/TLE | `copyObject()` |
| planner 生命周期内分配 | `palloc()` / `palloc0()` |
| Node 类型检查 | `IsA(node, Type)` |
| List 遍历 | `foreach` / `forboth` |
| OID/int 列表 | `lappend_oid()` / `lappend_int()` |
| 获取 RTE | `rt_fetch()` |
| 按 resno 找 TLE | `get_tle_by_resno()` |
| 按 sortgroupref 找 TLE | `get_sortgroupclause_tle()` |
| 表达式相等 | `equal()` / `tlist_member()` |
| 收集本层 Var | `pull_vars_of_level(expr, 0)` |
| 替换 RTE 输出 Var | `ReplaceVarsFromTargetList()` |
| 通用表达式递归 | `expression_tree_mutator()` |
| required 集合 | `Bitmapset` / `bms_add_member()` |

planner MemoryContext 会整体回收临时列表和副本，不需要在每条失败路径逐个释放 Query Node。临时 `pull_vars_of_level()` 返回的 List 可以用 `list_free_ext()` 释放 list cell，但不能随意释放其中指向表达式树的 Node。

### 19.4 第一步：先搭 GUC、状态和空入口

#### GUC 字段

在 `knl_session_attr_sql` 中增加：

```cpp
bool enable_union_all_required_columns_pushdown;
```

在 `InitSqlConfigureNamesBool()` 中注册为：

```cpp
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

默认必须先用 `false`。功能尚未完成时默认 on 会让所有符合形状的查询进入半成品路径，也无法用原计划作结果对照。

#### PlannerInfo 状态

增加：

```cpp
bool union_all_required_columns_pruned;
```

在每次 `subquery_planner()` 初始化 root 时写明：

```cpp
root->union_all_required_columns_pruned =
    parent_root != NULL &&
    parent_root->union_all_required_columns_pruned;
```

不能只依赖 `makeNode()` 的零初始化：顶层确实应为 false，但子 root 必须主动继承 true，createplan 才能约束所有后代基表。

#### 入口声明

在 `optimizer/prep.h` 声明：

```cpp
extern bool preprocess_union_all_required_columns(PlannerInfo *root);
extern void preprocess_union_all_order_key_projection(PlannerInfo *root);
```

先实现两个只检查 GUC 后返回的空函数（阶段一返回 false），完成一次编译。这样可以把头文件、链接和 GUC 问题与后续 Query 改写问题分开。

修改任何头文件后，当前工作树没有可靠的 `--enable-depend`，最终必须 full build，不能只相信增量链接成功。

### 19.5 第二步：把入口放到唯一正确的 planner 窗口

在 `subquery_planner()` 中按下面顺序调用：

```cpp
bool unionAllColumnsRewritten = preprocess_union_all_required_columns(root);
preprocess_union_all_diskann_topk(root);
if (unionAllColumnsRewritten)
    preprocess_union_all_order_key_projection(root);

parse->jointree = (FromExpr *)pull_up_subqueries(
    root, (Node *)parse->jointree);
```

位置要求：

- 在 `preprocess_rownum()` 之后；
- 在 `pull_up_subqueries()` 之前；
- 在 `flatten_simple_union_all()` 之前；
- required columns 在 Top-K 之前；
- order-key projection 在 Top-K 之后。

此时先不要写任何改写，只验证调用时能看到：

```text
outer->rtable 长度 = 1
唯一 RTE 的 rtekind = RTE_SUBQUERY
unionRte->subquery->setOperations != NULL
```

如果把入口放在 `pull_up_subqueries()` 之后，简单 UNION 叶可能已经转成 appendrel，本文后续所有基于 RTE_SUBQUERY/SetOperationStmt 的代码都不再成立。

### 19.6 第三步：从底向上写结构识别函数

推荐先写下面四个函数，再写主入口：

```cpp
static RangeTblEntry *get_single_union_subquery_rte(
    Query *outer, Index *unionRtiOut);

static bool collect_setop_leaves(
    Node *node, List *rtable, List **leaves);

static bool collect_pure_union_all_leaves(
    Query *query, List **leaves);

static bool leaf_query_is_safe(Query *leaf);
```

实现检查表：

- `get_single...` 同时检查 rtable 长度、RTE 类型、subquery 非空、setOperations 非空；成功时才写 `*unionRtiOut=1`。
- `collect_setop_leaves()` 对中间节点先检查 `SETOP_UNION && all`，再左、右递归。
- 叶子必须是能从当前 union Query rtable 取到的 `RTE_SUBQUERY`。
- `leaf_query_is_safe()` 至少拒绝已有 sort/limit/offset/setOperations、Agg/Group/Having/Distinct/Window/CTE/ForUpdate/SRF/Recursive/ModifyingCTE/RLS；还必须直接检查 targetlist 中的 SRF/ROWNUM 和 quals 中的 ROWNUM。

先用 DEBUG1 输出叶子数，验证 2 叶和 4 叶；再用 UNION distinct、叶子 LIMIT、RLS 验证返回 false。结构发现阶段绝对不能修改 Query。

### 19.7 第四步：写表达式映射的三个原语

先实现：

```cpp
static List *build_visible_output_map_tlist(Query *leaf);
static bool validate_union_leaf_output_types(
    Query *unionQuery, List *leafMapTlist, List *vars);
static bool leaf_local_expr_is_valid(
    Query *leaf, Expr *mappedExpr);
```

再用它们实现一个通用映射模式：

```cpp
static Expr *map_expr_to_leaf(
    RangeTblEntry *unionRte,
    Index unionRti,
    Query *leaf,
    Expr *outerExpr)
{
    if (checkExprHasSubLink((Node *)outerExpr) ||
        contain_vars_of_level_or_above((Node *)outerExpr, 1))
        return NULL;

    List *vars = pull_vars_of_level((Node *)outerExpr, 0);
    /* 验证每个 Var 都指向 unionRti 且 varattno > 0 */

    List *mapTlist = build_visible_output_map_tlist(leaf);
    if (!validate_union_leaf_output_types(
            unionRte->subquery, mapTlist, vars))
        return NULL;

    Expr *mapped = (Expr *)ReplaceVarsFromTargetList(
        (Node *)outerExpr, unionRti, 0, unionRte,
        mapTlist, REPLACEVARS_REPORT_ERROR, 0, NULL);

    return leaf_local_expr_is_valid(leaf, mapped)
               ? mapped
               : NULL;
}
```

现有代码分别保留 `map_sort_expr_to_leaf()` 和 `map_outer_qual_to_leaf()` 两个函数；手写时可以先用一个内部公共 helper，再用两个薄包装表达不同调用语义。

这一阶段要用三个最小例子验证映射结果：

```text
outer Var(row_id)              -> delta.row_id / s.row_id
outer Var(title) IS NOT NULL   -> delta.title / s.title
outer embedding <-> Const      -> delta.embedding <-> Const / s.embedding <-> Const
```

还要验证 whole-row、其他 RTE Var、上层相关 Var、type/typmod/collation 不一致均返回 NULL。

### 19.8 第五步：手写 WHERE 安全门和两遍提交

先写纯检查函数 `check_outer_qual_pushdown_safe()`，不要在里面修改任何树。它需要完成：

```text
拒绝 SubLink
拒绝 volatile
拒绝 SRF/Agg/Window
检查所有本层 Var 属于 unionRti 且 attno > 0
对每个叶子的对应输出做 type/typmod/collation 校验
```

再写 `prepare_outer_where_pushdown()`：

```cpp
List *mappedQuals = NIL;

foreach (leaf) {
    Expr *mapped = map_outer_qual_to_leaf(...);
    if (mapped == NULL)
        return false;
    mappedQuals = lappend(mappedQuals, mapped);
}

forboth (leaf, mappedQual) {
    if (!append_outer_qual_to_leaf(leafQuery, mappedQual))
        return false;
}

outer->jointree->quals = NULL;
return true;
```

手写时最容易犯的错误是：

```cpp
/* 错误：边映射边直接写叶子 */
foreach (leaf) {
    mapped = map(...);
    leaf->jointree->quals = and(leaf->quals, mapped);
}
```

如果后面的叶子失败，前面的叶子已经改变。即便外层还有 Query 副本，局部函数也失去清晰的原子性；后续若被复用到非副本路径就会出问题。

本阶段验收不要看列裁剪，只看：

- GUC on 时每个叶子出现等价 Filter/Index Cond；
- 外层 Subquery Scan 不再重复 Filter；
- delete NOT EXISTS 条件仍在；
- GUC off/on 结果相同。

### 19.9 第六步：手写 required 收集器

最小函数集：

```cpp
static int count_visible_tlist_entries(List *targetList);
static bool add_required_columns_from_expr(
    Node *expr, Index unionRti, int oldNcols,
    Bitmapset **required);
static bool collect_required_output_columns(
    Query *outer, Index unionRti, int oldNcols,
    Bitmapset **required);
static void preserve_semantically_required_outputs(
    List *leaves, int oldNcols, Bitmapset **required);
```

按以下顺序收集：

```text
outer.targetList 的每一个 TLE，包括 resjunk
仍存在的 outer.jointree->quals
limitCount
limitOffset
每个叶子可见输出位置中的 ressortgroupref/SRF/volatile/SubLink
```

不要扫描叶子 quals 来决定 UNION 输出。叶子本地 Filter 读取列，不代表该列还要穿过集合边界。

不要写成：

```cpp
/* 错误：漏掉 ORDER BY resjunk */
if (!tle->resjunk)
    collect(tle->expr);
```

这样 ORDER BY 使用的 embedding 会在阶段一被删除，随后排序表达式映射失败或引用错列。

不要把 `bms_add_member(required, varattno - 1)` 与当前代码混用。本文实现始终把真实 attno 作为 member，查询也用同一个 attno。

### 19.10 第七步：先写五个独立压紧 helper

建议按难度从低到高写：

```cpp
compact_name_list
compact_leaf_targetlist
required_column_var_mutator + remap_outer_query_vars
compact_union_targetlist
compact_setop_metadata
```

每个 helper 都接收同一个 `oldToNew`，并返回 `bool` 或通过 `bool *ok` 报告失败。

#### `compact_name_list`

只按位置复制 `oldToNew[pos]>0` 的 String Node。先验证原 list 长度严格等于 oldNcols。

#### `compact_leaf_targetlist`

第一遍处理 visible TLE，`oldPos` 只对非 resjunk 自增；第二遍追加所有 resjunk。所有保留 TLE 必须 `copyObject()`，并重写连续 resno。

#### outer Var mutator

只改 level 0 且 varno==unionRti。被删除列、whole-row、system attno、越界 attno 都设置 `failed=true`。不要静默留下旧 Var。

#### `compact_union_targetlist`

要求无 resjunk、每个 TLE expr 是 Var、`varattno==oldPos`。同时改 TLE.resno、Var.varattno，并有条件改 varoattno。

#### `compact_setop_metadata`

对每一层检查 pure UNION ALL、groupClauses NIL、三份列表长度一致；按 old position 构造三个新列表；左右递归成功后再赋值。

每写完一个 helper，建议在 DEBUG 构建中人为构造失败条件，确认返回 false 而不是 Assert/崩溃。

### 19.11 第八步：把压紧 helper 组成一个事务

`compact_union_all_query()` 应保持下面的严格顺序：

```cpp
build_old_to_new(required);

if (!remap_outer_query_vars(...))
    return false;

foreach (leaf) {
    leaf->targetList = compact_leaf_targetlist(...);
    leafRte->eref->colnames = compact_name_list(...);
}

unionQuery->targetList = compact_union_targetlist(...);
if (!compact_setop_metadata(...))
    return false;

unionRte->eref->colnames = compact_name_list(...);
return ok;
```

从逻辑上这些修改没有必须谁先谁后的执行语义，因为都发生在副本里；但固定顺序使调试时能清楚判断是哪一份接口首先不一致，也与从外向内再回到外层 RTE alias 的数据关系相符。

主入口必须在 `work=copyObject(outer)` 上调用它。不要把 `unionRte` 从原 outer 取出后传给 work 的 helper；原树和副本中的 RTE 指针不是同一个对象。

阶段一成功后，记录：

```cpp
root->union_all_required_columns_pruned = true;
```

失败路径不能提前设置 flag，否则 createplan 会在未压紧查询上禁用 physical tlist，GUC off/on 的旧计划契约也会被破坏。

与此同时，阶段一必须返回 true，失败则返回 false，让调用者得到本层结果。子 root 从父层继承的 flag 只表示扫描输出策略，不能据此跳过本层安全检查。

### 19.12 第九步：补 `use_physical_tlist`，才算“源头裁剪”完成

先运行 WHERE-only EXPLAIN。如果 Append 已经窄，但 Scan Output 仍列出整表字段，说明 Query 改写正确、Plan 构造又回扩了。

只在 `use_physical_tlist()` 增加：

```cpp
if (root->union_all_required_columns_pruned &&
    rel->rtekind == RTE_RELATION)
    return false;
```

不要改成全局：

```cpp
/* 错误：影响所有查询 */
if (rel->rtekind == RTE_RELATION)
    return false;
```

也不要只在 outer root 判断，因为真正的 delta/snapshot/delete Scan 是在子 `subquery_planner` root 中创建，必须依赖 flag 继承。

本阶段通过标准：

```text
WHERE row_id：三个源扫描 Output 只有 row_id
WHERE title：title 只在 Filter，Output 不含 title
delete reason：reason 只在 delete Filter，Output 只有 d.row_id
```

### 19.13 第十步：从 ORDER BY 的 sortRef 开始写 synthetic key

先定义 `UnionAllOrderKey`，再按下面顺序实现：

```text
find_union_all_order_key
validate_order_projection_shape
prepare_union_all_order_keys
collect_order_projection_raw_columns
leaf_expr_is_order_key
rewrite_leaf_order_projection
rewrite_outer_order_projection
append_setop_order_key_metadata
rewrite_union_order_projection
preprocess_union_all_order_key_projection
```

不要从“给叶子 targetlist append distance”开始写，因为在那之前必须知道：

- 哪些 sortClause 是派生表达式；
- 每叶是否都能映射；
- 结果三元类型是否一致；
- 哪些原始列在 key 替换后仍被其他表达式使用；
- 新 key 在 SetOp 接口中的统一 attno。

`prepare_union_all_order_keys()` 必须按 sortClause 顺序生成 key，才能让 `__ua_order_1` 与第一个派生排序键稳定对应。直接 Var 和常量跳过，但其他键失败时应放弃整个阶段三，不能只物化一部分排序表达式。

### 19.14 第十一步：第二次列压紧要用“当前列号”

阶段三重新计算：

```cpp
oldNcols = count_visible_tlist_entries(
    unionRte->subquery->targetList);
```

这里禁止复用阶段一的 original oldNcols，也禁止保存原 `oldToNew` 跨阶段使用。阶段一可能已经把 `[row_id,title,embedding]` 变成 `[row_id,embedding]`，阶段三面对的是新的 2 列接口。

正确的第二次映射：

```text
当前 old columns = [row_id(1), embedding(2)]
rawRequired       = {1}
raw oldToNew      = [1→1, 2→0]
synthetic distance newAttno = 2
```

错误地继续使用原始 `embedding=3` 会导致外层 Var 越界、SetOp 元数据长度不一致或排序指向不存在列。

### 19.15 第十二步：叶子、UNION、外层必须一起增加 key

阶段三不是只把外层表达式换成 Var。完整操作是：

```text
每叶 targetlist：追加 visible distance TLE
每叶 eref：追加 __ua_order_N 名称
UNION targetlist：追加指向 leftmost leaf 第 N 列的 Var
UNION RTE eref：追加 __ua_order_N 名称
每层 SetOperationStmt：追加 type/typmod/collation
外层排序 TLE：替换为 Var(unionRti,newAttno)
```

叶子若已有相同的 resjunk distance，必须：

```text
继承它的 ressortgroupref
新增一份 visible TLE
删除旧 resjunk 副本
```

否则可能出现两个等价 distance TLE，叶子 sortClause 仍指向旧 junk，而 SetOp 传另一份；既浪费计算，也容易在 setrefs 阶段产生不一致。

### 19.16 第十三步：最后才组合 Top-K

若分支 Top-K 代码已存在，不要把它揉进 required compaction 的同一个大函数。保持三个入口：

```text
required columns
Top-K
order projection
```

组合时检查：

- 阶段一仍保留 ORDER 输入 embedding；
- Top-K 能看到原始 vector distance OpExpr；
- Top-K 对所有叶子先复制、全部准备成功后再替换；
- local LIMIT 写在叶 Query 顶层；
- snapshot NOT EXISTS 仍在该 Query 内部，因此逻辑上先删除再截断；
- global sort/limit 不删除；
- 阶段三允许叶子已存在 sort/limit，不能再调用阶段一的 `leaf_query_is_safe()` 拒绝它。

如果 Top-K GUC off，阶段三仍应运行并生成标量 key；因此阶段三的入口条件不能依赖 Top-K 成功标志。

### 19.17 手写时必须加入的 DEBUG1

至少保留稳定、无用户值的日志：

```text
阶段一拒绝 reason code
oldNcols/newNcols
where_pushed/outer_recheck
order key 数量
rawRequired 数量
Top-K 提交叶子数
```

不要打印完整 Query vector、title、谓词常量或 `nodeToString(parse)` 到默认日志。这些可能很大，也可能包含用户数据。

建议临时调试时额外打印 `oldToNew`，但正式代码应只保留计数和 reason code。

### 19.18 从零实现时的测试推进顺序

不要等 1,100 行全部写完才第一次跑 SQL。按下面顺序逐步建立 golden：

#### Checkpoint 1：GUC 旁路

```sql
SET enable_union_all_required_columns_pushdown = off;
EXPLAIN (VERBOSE ON, COSTS OFF) ...;
```

应与基线完全一致。

#### Checkpoint 2：WHERE row_id

只开阶段一和 createplan guard：

```text
Append 输出 row_id
delta 输出 row_id
snapshot Anti Join 输出 row_id
delete 输出 row_id
外层重复 Filter 消失
```

#### Checkpoint 3：WHERE title

```text
title 出现在 delta/snapshot Filter
title 不出现在 Scan Output/Join Output/Append Output
```

#### Checkpoint 4：四叶 UNION

验证每个内部 SetOperationStmt 都压到相同一列，避免只改顶层的假成功。

#### Checkpoint 5：ORDER only

```text
叶子可以读取 embedding
叶子公共输出为 row_id + distance
Append 和全局 Sort 不携带 embedding
无 local Limit
```

#### Checkpoint 6：LIMIT only

只有 global Limit，绝不能给叶子复制 LIMIT。

#### Checkpoint 7：ORDER + LIMIT

安全时每叶一个 local Limit，snapshot Anti Join 位于 local Limit 之下，全局 Limit 仍在。

#### Checkpoint 8：正确性

必须同时做：

```text
显式结果
EXCEPT ALL 双向比较
有序位置比较
UNION ALL 重复行
volatile/SRF
prepared parameter
unsupported fallback
```

集合比较必须用 `EXCEPT ALL`，不能用 `EXCEPT`；后者会吞掉重复计数差异。

### 19.19 可直接照着执行的实现完成定义

当下面所有条件为真，才可以说“基本实现完成”：

- [ ] GUC 注册、默认 off、session SET 生效；
- [ ] 顶层 root 初始 false，子 root 正确继承；
- [ ] 入口位于 rownum 之后、subquery pull-up 之前；
- [ ] 只识别单一 RTE_SUBQUERY + pure UNION ALL；
- [ ] 所有表达式映射前校验 Var level/RTE/attno；
- [ ] 每个映射位置校验 type/typmod/collation；
- [ ] WHERE 先准备所有叶，再写入，最后删外层 quals；
- [ ] required 收集包含 targetlist resjunk；
- [ ] volatile/SRF/SubLink/ressortgroupref 输出位置保留；
- [ ] required 为空时有安全 carrier；
- [ ] oldToNew 使用连续新 attno；
- [ ] 外层 Var、叶 targetlist、UNION targetlist、SetOp 元数据、eref 全同步；
- [ ] 可见 TLE 在前，resjunk 在后，resno 连续；
- [ ] `ressortgroupref` 没有被当作 `resno` 重写；
- [ ] 阶段一只在完整副本成功后提交；
- [ ] physical tlist 只对已压紧查询的 RTE_RELATION 禁用；
- [ ] order key 对所有叶子映射成功后才提交；
- [ ] synthetic key 在每层 SetOp 追加完整类型三元组；
- [ ] outer 距离 TLE 整体替换为 synthetic Var；
- [ ] Top-K 位于两个列阶段之间；
- [ ] local Limit 在整个叶 Query 顶层，全局 Limit 保留；
- [ ] GUC off/on 结果和重复数一致；
- [ ] 原生 `pg_regress` SQL/expected/schedule 已接入；
- [ ] Debug/full build 与相关回归通过。

## 20. 主要错误模式及规避方法

下面按严重度列出从零实现时最可能出现的问题。

### 20.1 会产生错误结果或崩溃的错误

| 错误写法 | 后果 | 正确做法 |
|---|---|---|
| 只删叶子 TLE，不改 SetOp 元数据 | 列数/类型不一致、setrefs 报错或错列 | 同一 oldToNew 更新所有位置表示 |
| 先压列号再映射 WHERE | embedding/title 指向错位 | 所有叶映射成功后再 compact |
| 第一个叶成功就立即写原树 | 后叶失败留下半改写 | Query/leaf copy + 最终 commit |
| 删除 volatile/SRF 未用输出 | 副作用次数或行数变化 | 按集合输出位置强制 required |
| 用 `EXCEPT` 检查 UNION ALL | 漏掉重复数错误 | 使用双向 `EXCEPT ALL` |
| 把 local Limit 放到 Anti Join 输入 | 被删除行占候选名额 | LIMIT 放在完整 snapshot 叶 Query 顶层 |
| 删除全局 Limit/Sort | 局部 Top-K 不能保证全局结果 | 全局节点始终保留 |
| 只校验 OID，不校验 typmod/collation | 字符/向量接口可能语义不一致 | 三者逐位置全部相等 |
| mutator 修改其他 varno/varlevelsup | 破坏相关子查询或叶内部引用 | 严格限定 level 0 + unionRti |
| 把 whole-row attno=0 当普通列 | oldToNew 越界或丢整行语义 | attno<=0 直接拒绝 |

### 20.2 结果正确但优化目标失效的错误

| 错误写法 | 表现 | 正确做法 |
|---|---|---|
| 用 typed NULL 占原列位置 | 表达式不算了，但 tuple 仍有空槽位 | 物理删除并连续重编号 |
| 忘记 `use_physical_tlist` | Append 窄，Scan Output 又变宽 | planner flag + createplan guard |
| key 仍为 resjunk、不加入 SetOp | 外层还需原 embedding | 将 key 变成叶/SetOp 可见内部列 |
| synthetic key 前不删原输入 | 同时传 embedding 和 distance | 第二次 rawRequired 压紧 |
| 阶段三依赖 Top-K GUC | ORDER-only 仍传 vector | 不依赖 Top-K 成功；要求本层阶段一成功、有排序且无残留 quals |
| outer TLE 只改内部 Var、不整体替换 | 全局层仍重复计算距离 | 命中 sortRef 后整个 expr 替换为 key Var |

### 20.3 容易造成脆弱回归或维护困难的错误

| 错误写法 | 问题 | 正确做法 |
|---|---|---|
| golden 强制 Hash/Merge Anti Join 某一种 | 小数据成本变化就无意义失败 | 看护列 Output 和语义相对位置 |
| 用字符串搜索 EXPLAIN 做唯一测试 | 可能漏层级、类型或完整输出变化 | 保存原生完整 EXPLAIN golden |
| DEBUG1 打印完整向量/Query | 日志巨大且泄漏用户数据 | 只打印计数、布尔和 reason code |
| 把新 oldToNew 长期放进 Plan | 扩大 Executor/序列化改动面 | planner 内完成连续接口，Plan 无感知 |
| 缩减 RTE 权限位图 | 改变列权限检查 | 只改执行 targetlist，不改权限元数据 |
| 修改头文件后只增量编译 | ABI 混合、结果不可信 | full build |

### 20.4 最终人工审查的四张表

提交前应为每条目标 SQL 手工写出以下四张映射表。

#### 表一：原始集合列

```text
old attno | 名称      | 类型      | outer consumers
1         | row_id    | bigint    | SELECT, WHERE
2         | title     | text      | none / WHERE
3         | embedding | vector(4) | ORDER BY
```

#### 表二：阶段一 required

```text
old attno | 保留? | 原因
1         | yes   | final output
2         | no    | WHERE 已下推，最后消费者在 leaf
3         | yes   | outer sort TLE 尚需原始输入
```

#### 表三：阶段一 oldToNew

```text
1 -> 1
2 -> 0
3 -> 2
```

#### 表四：阶段三接口

```text
current raw column 1 row_id    -> new 1
current raw column 2 embedding -> drop
synthetic distance             -> new 2
SetOp types                    -> [bigint, float8]
```

如果无法为某条 SQL 明确写出这四张表，说明 required 分析或排序键生命周期仍没有想清楚，不应继续编码。

## 21. 相关材料

- 详细设计：`../设计/UNION_ALL必需列源头裁剪详细设计.md`
- 开发与测试报告：`../测试/UNION_ALL必需列源头裁剪开发与测试报告.md`
- 当前交付 patch：`../执行包/opengauss_union_all_required_columns_pushdown.patch`
