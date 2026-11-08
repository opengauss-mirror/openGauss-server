> 历史全文归档（2026-09-12）：正文中的“当前”、状态、代码位置和测试结论均指原文版本，不代表最新实现。当前行为及安全修复以[协同设计说明](../UNION_ALL_LIMIT下推与列裁剪协同设计说明.md)为准；来源及校验信息见[归档清单](README.md)。除本提示和目录迁移所需的相对链接调整外，原正文完整保留。

# openGauss UNION ALL + DiskANN Top-K 下推限制清单

本文基于当前提交 `6df94c2ccb`、设计文档
`openGauss_UNION_ALL分支过滤与DiskANN_TopK协同下推设计.md`、代码解读文档
`openGauss_UNION_ALL与DiskANN_TopK完整代码解读.md` 以及
`src/gausskernel/optimizer/prep/preptopk.cpp` 整理。

本文中的“不支持”通常表示不执行本次 Top-K 下推，SQL 仍进入 openGauss
原生规划流程，并不表示 SQL 本身报错。对于设计上不支持、但当前代码没有正确
拦截的场景，本文明确标记为“代码漏检”，不能将其视为已经支持。

严格支持的基本形态是：

```sql
SELECT <普通输出列>
FROM (
    <叶子1>
    UNION ALL
    <叶子2>
    ...
) u
[WHERE <能映射到全部叶子的安全谓词>]
ORDER BY <DiskANN距离表达式> ASC NULLS LAST
         [, <其他可映射排序键> ...]
LIMIT <正整数或外部参数>;
```

## 1. 外层 Query 不支持的场景

### 1.1 外层 Query 整体结构

#### 1.1.1 特性开关未开启

GUC 默认值为 `false`。未执行以下命令时，所有 SQL 都不会进行本特性改写：

```sql
SET enable_union_all_diskann_topk_pushdown = on;
```

#### 1.1.2 外层不是 SELECT

当前只处理 `CMD_SELECT`，不处理：

- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- 其他非 SELECT Query

#### 1.1.3 外层直接是集合操作

以下写法的外层 Query 自身带有 `setOperations`，不支持：

```sql
SELECT ... FROM t1
UNION ALL
SELECT ... FROM t2
ORDER BY ...
LIMIT 10;
```

目标 UNION ALL 必须位于外层 SELECT 的唯一子查询数据源中，例如：

```sql
SELECT *
FROM (
    SELECT ... FROM t1
    UNION ALL
    SELECT ... FROM t2
) u
ORDER BY ...
LIMIT 10;
```

通过视图访问时，视图展开后也必须形成上述结构。

#### 1.1.4 外层不是单一子查询数据源

当前代码要求：

```text
outer->rtable长度为1
outer->jointree非空且为FromExpr
outer->jointree->fromlist长度为1
rtable第1项为RTE_SUBQUERY
RTE.subquery非空且包含setOperations
```

因此不支持：

- 外层多个 FROM 项；
- 外层 JOIN；
- 外层直接访问普通表；
- 外层直接访问函数；
- 外层直接访问 `VALUES`；
- 外层唯一来源不是目标 UNION ALL 子查询。

示例：

```sql
-- 不支持：外层JOIN
SELECT u.*
FROM union_view u
JOIN other_table t ON t.id = u.id
ORDER BY u.embedding <-> $1
LIMIT 10;

-- 不支持：外层多个FROM项
SELECT u.*
FROM union_view u, other_table t
ORDER BY u.embedding <-> $1
LIMIT 10;
```

当前实现还存在两个边界问题：

1. 使用 `list_length(outer->rtable) == 1` 判断单一来源，条件过严。某些 Query
   的有效 FROM 来源只有一个，但 `rtable` 中保留了辅助 RTE，也会被误拒绝。
2. 只检查 `fromlist` 长度，没有明确验证唯一节点就是引用目标 UNION RTE 的
   `RangeTblRef`。

#### 1.1.5 外层包含复杂查询语义

以下结构不支持：

- 聚合函数；
- `GROUP BY`；
- `HAVING`；
- `DISTINCT`；
- 窗口函数；
- CTE；
- 递归 CTE；
- `FOR UPDATE` 等行锁；
- 外层自身的 `UNION`、`INTERSECT`、`EXCEPT`；
- 外层任何位置存在 SubLink；
- 安全屏障视图；
- ROWNUM 等尚未完成等价性证明的特殊语义。

#### 1.1.6 外层目标列表的当前漏检

设计上不支持以下外层目标表达式：

- SRF，例如 `generate_series()`；
- volatile 表达式，例如 `nextval()`；
- 可能因局部 LIMIT 改变求值次数的表达式。

当前代码没有检查 `outer->hasTargetSRFs`，也没有完整遍历外层 targetList
检查 volatile 表达式。这属于代码漏检，不能视为已经支持。

### 1.2 外层 WHERE 不支持的场景

#### 1.2.1 支持范围

外层 WHERE 可以为空。非空时，整个 `outer->jointree->quals` 必须能安全
映射到全部 UNION ALL 叶子。

可以支持只引用 UNION 输出列的稳定逐行谓词，例如：

```sql
WHERE title LIKE 'delta%'
  AND row_id > 100
```

通常可支持：

- `AND`
- `OR`
- 普通比较
- `IS NULL`
- `LIKE`
- stable 或 immutable 函数

#### 1.2.2 含 SubLink

不支持：

- `EXISTS`
- `NOT EXISTS`
- `IN (SELECT ...)`
- 标量子查询
- 其他 SubLink

这里拒绝的是准备下推的外层 WHERE。叶子原来已有的相关 `NOT EXISTS`
可以保留，不属于这一限制。

示例：

```sql
SELECT *
FROM union_view u
WHERE u.id IN (SELECT id FROM other_table)
ORDER BY u.embedding <-> $1
LIMIT 10;
```

#### 1.2.3 含 volatile、SRF、聚合或窗口表达式

不支持：

- `random()`
- `nextval()`
- `clock_timestamp()`
- 其他 volatile 函数
- SRF
- 聚合表达式
- 窗口表达式

外层 WHERE 会保留，同时还会复制到每个叶子。volatile 表达式重复执行会改变
返回结果、求值次数或副作用，因此不能下推。

#### 1.2.4 引用范围不安全

WHERE 中的每个当前层 Var 必须满足：

```text
varlevelsup == 0
varno        == unionRti
varattno     > 0
```

因此不支持：

- `varlevelsup >= 1` 的父层相关引用；
- LATERAL 相关引用；
- UNION RTE 之外的其他 RTE；
- whole-row Var；
- `ctid`、`tableoid` 等系统属性；
- `PlaceHolderVar`；
- 不能定位到 UNION 可见输出列的引用。

当前 `check_outer_qual_pushdown_safe()` 只收集 `varlevelsup=0` 的 Var，没有
在该函数中直接拒绝更高层 Var。非零层级 Var 会在后续
`map_outer_qual_to_leaf()` 中被拒绝，因此不会提交错误改写，但失败较晚。

LATERAL 目前也没有统一、明确的入口检查，主要依赖后续 Var 映射间接回退，
属于实现边界不完整。

#### 1.2.5 不支持部分谓词下推

当前不会把安全和不安全条件拆开。例如：

```sql
WHERE u.title LIKE 'delta%'
  AND u.id IN (SELECT id FROM other_table)
```

不会只下推 `title LIKE 'delta%'`，而是放弃整次 Top-K 下推。

同样，当前不会对 OR 的不同分支做选择性拆分。

#### 1.2.6 无法映射到所有叶子

只要存在一个叶子满足以下任一条件，整个改写回退：

- 缺少 WHERE 引用的对应输出列；
- 对应列的 type 不一致；
- 对应列的 typmod 不一致；
- 对应列的 collation 不一致；
- 映射结果含 `varlevelsup != 0` 的 Var；
- 映射结果中的 `varno` 不能从叶子 `rtable` 解析；
- 映射结果含 SubLink；
- 映射结果含 volatile 表达式。

### 1.3 外层 ORDER BY 不支持的场景

#### 1.3.1 没有 ORDER BY

没有 ORDER BY 不是 Top-K 排序查询，不触发本特性：

```sql
SELECT *
FROM union_view
LIMIT 10;
```

#### 1.3.2 没有 DiskANN 排序键

ORDER BY 至少需要一个排序项满足：

1. 去掉顶层 `RelabelType` 后是二元 `OpExpr`；
2. 返回类型为 `float8`；
3. 参数类型与 `pg_operator` 登记一致；
4. 在 `pg_amop` 中登记为 `AMOP_ORDER`；
5. 所属 `pg_opfamily.opfmethod` 为 `DISKANN_AM_OID`。

因此不支持：

- 只有普通标量排序；
- 只有非 DiskANN 向量索引操作符；
- 使用普通距离函数而不是 Catalog 登记的操作符；
- 距离操作被其他表达式包裹，导致顶层不再是目标 `OpExpr`；
- 距离操作符返回类型不是 `float8`；
- 操作符未登记为 ordering operator；
- 操作符族不属于 DiskANN AM。

示例：

```sql
-- 不支持：只有标量排序
SELECT *
FROM union_view
ORDER BY row_id
LIMIT 10;

-- 不支持：函数形式
SELECT *
FROM union_view
ORDER BY l2_distance(embedding, $1)
LIMIT 10;

-- 不支持：距离操作被算术表达式包裹
SELECT *
FROM union_view
ORDER BY (embedding <-> $1) + 1
LIMIT 10;
```

#### 1.3.3 当前 DiskANN 操作符范围

当前代码仓 Catalog 登记：

| 操作符 | 距离语义 | 当前特性 |
|---|---|---|
| `<->` | L2 | 支持识别 |
| `<#>` | 负内积 | 支持识别 |
| `<=>` | 余弦距离 | 支持识别 |
| `<+>` | L1 | DiskANN未登记，不支持 |

当前 `is_diskann_vector_order_expr()` 没有直接比较左右输入类型是否为
`VECTOROID`，主要依赖 Catalog 中的 DiskANN opfamily 关系间接保证。这是
防御性检查缺口。

#### 1.3.4 完整排序列表无法映射

外层 ORDER BY 可以包含多个向量键，也可以包含普通标量 tie-breaker，但是
完整排序列表都必须下推。

任一排序项存在以下情况都会导致回退：

- 含 SubLink；
- 含 `varlevelsup >= 1` 的相关外层 Var；
- 引用 UNION RTE 之外的其他 RTE；
- 使用 whole-row Var；
- 使用系统属性；
- 无法映射到任意一个叶子的可见输出列；
- UNION 输出与叶子输出的 type、typmod 或 collation 不一致；
- 映射后含 volatile 函数；
- 映射后含 SubLink；
- 映射后含非叶子本地 Var。

#### 1.3.5 排序方向

设计只支持：

```sql
ASC NULLS LAST
```

设计上不支持：

- `DESC`
- `NULLS FIRST`
- 多排序键升降序混合

但当前代码的方向检查没有真正返回失败，这些 SQL 可能被错误纳入改写，而不是
按设计回退。这属于代码漏检，不能视为已经支持。

### 1.4 外层 LIMIT 不支持的场景

#### 1.4.1 无 LIMIT 或存在 OFFSET

不支持：

- 没有 LIMIT；
- `LIMIT ALL`；
- 存在 OFFSET；
- `OFFSET 0`；
- `LIMIT NULL`；
- 常量 `LIMIT 0`；
- 负数 LIMIT。

示例：

```sql
-- 不支持：无LIMIT
SELECT *
FROM union_view
ORDER BY embedding <-> $1;

-- 不支持：OFFSET非空
SELECT *
FROM union_view
ORDER BY embedding <-> $1
LIMIT 10 OFFSET 5;
```

#### 1.4.2 LIMIT 表达式不符合当前识别范围

当前实际支持：

- 正整数 `int8 Const`；
- 普通 `LIMIT 3` 形成的整数转换节点；
- `PARAM_EXTERN`，例如 `LIMIT $2`。

明确不支持：

- 非整数常量；
- 普通算术表达式；
- 多参数函数表达式；
- SubLink；
- volatile 表达式；
- 非 `PARAM_EXTERN` 参数。

例如：

```sql
SELECT *
FROM union_view
ORDER BY embedding <-> $1
LIMIT 1 + 2;
```

#### 1.4.3 LIMIT FuncExpr 校验过宽

当前代码把“一个正整数 Const 参数的非易变 FuncExpr”也当作可能的隐式转换，
因此：

```sql
LIMIT abs(3::bigint)
```

可能被错误接受。代码检查的是函数输入参数，而没有证明该函数是安全的隐式类型
转换，也没有验证函数实际返回值。

这可能造成：

- 支持范围失控；
- 函数在外层和多个叶子重复执行；
- LIMIT 行数估计不准确；
- 路径选择不准确。

#### 1.4.4 WITH TIES 和 PERCENT

设计不支持：

- `FETCH ... WITH TIES`
- `FETCH ... PERCENT`

当前代码没有检查 `limitWithTies` 和 `limitIsPercent`，写入叶子时却将二者
强制设为 `false`。

其中：

- WITH TIES 可能漏掉与第 K 行并列的结果；
- PERCENT 可能改变每个分支和全局的返回行数。

这是当前实现中的正确性缺陷，不能依赖代码自动安全回退。

## 2. 内层 Query 不支持的场景

这里的“内层 Query”包括：

1. 外层唯一 RTE 指向的 UNION Query；
2. UNION Query 的 `setOperations` 树；
3. `RangeTblRef` 指向的各个叶子 Query。

### 2.1 内层不是纯 UNION ALL

集合操作树的内部节点必须满足：

```text
nodeTag == T_SetOperationStmt
op      == SETOP_UNION
all     == true
```

不支持：

- `UNION` 去重；
- `INTERSECT`；
- `EXCEPT`；
- `UNION ALL` 与上述操作混合；
- 不是纯 UNION ALL 的集合操作树。

纯 UNION ALL 的分支数量没有硬编码上限，二分支、三分支和更多分支都可以递归
收集，纯 UNION ALL 的括号嵌套也可以处理。

### 2.2 叶子不是有效子查询

集合操作树的每个叶子必须是：

```text
RangeTblRef
→ 通过rtindex取得RangeTblEntry
→ RTE.rtekind == RTE_SUBQUERY
→ RTE.subquery != NULL
```

以下情况不支持：

- 叶子节点不是 `RangeTblRef`；
- `rtindex` 不能定位有效 RTE；
- 叶子 RTE 不是 `RTE_SUBQUERY`；
- `RTE.subquery` 为空。

### 2.3 叶子已有 ORDER BY、LIMIT 或集合操作

叶子必须尚未包含本特性需要新增的排序和截断语义。

不支持：

- 叶子已有 `ORDER BY`；
- 叶子已有 `LIMIT`；
- 叶子已有 `OFFSET`；
- 叶子自身仍有 `setOperations`。

即使叶子原有 ORDER BY 或 LIMIT 在某些特定 SQL 中可能与外层兼容，当前实现
也不尝试合并或证明等价性，而是直接回退。

### 2.4 叶子包含复杂查询语义

不支持：

- 聚合；
- `GROUP BY`；
- `HAVING`；
- `DISTINCT`；
- 窗口函数；
- CTE；
- 行锁；
- 尚未完成等价性证明的特殊查询语义。

### 2.5 叶子允许的结构

以下结构本身不是限制：

- 普通表扫描；
- 叶子内部 JOIN；
- 叶子已有 WHERE；
- 叶子已有相关 `NOT EXISTS`；
- 不同叶子的同一输出列来自不同 `varno`。

例如 snapshot 叶子的删除过滤可以保留：

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM delete_docs d
    WHERE d.row_id = s.row_id
)
```

### 2.6 叶子输出表达式不安全

外层 WHERE 或 ORDER BY 映射到叶子后，表达式必须满足：

```text
所有Var.varlevelsup == 0
所有Var.varno能从叶子rtable解析
不含SubLink
不含volatile函数
```

因此不支持：

- 映射后的表达式引用父层 Query；
- 映射后的 Var 指向无效 RTE；
- 映射后的表达式包含 SubLink；
- 映射后的表达式包含 volatile 函数；
- 映射结果无法形成有效叶子本地表达式。

### 2.7 叶子 targetList 无法建立输出位置映射

叶子映射只使用非 resjunk TargetEntry，并重新编号为：

```text
1, 2, 3, ... N
```

以下情况不支持：

- 外层引用的第 N 个 UNION 输出在叶子中不存在；
- 对应输出只存在于不应参与映射的 resjunk TLE；
- 输出位置无法一一对应；
- 输出表达式的类型信息与 UNION 统一输出不一致。

### 2.8 叶子当前漏检

当前 `leaf_query_is_safe()` 没有完整检查：

- `leaf->hasTargetSRFs`；
- 叶子 targetList 中未被 WHERE/ORDER BY 引用的 volatile 表达式；
- 叶子已有 quals 中的全部 volatile 表达式；
- 叶子安全屏障语义；
- 叶子内部更复杂的 LATERAL 或 PlaceHolderVar 边界。

这些场景不能视为已经安全支持。局部 LIMIT 可能改变 SRF 输出行数或 volatile
表达式的求值次数。

## 3. 内外层配合联合检查不支持的场景

单独看外层和叶子都满足基本结构，并不表示一定能够改写。还必须验证外层表达式
能够按 UNION 输出位置安全转换为每个叶子的本地表达式。

### 3.1 外层 UNION RTE 与内层 UNION Query 关系不成立

必须满足：

```text
外层Query
└── 唯一有效RangeTblRef
    └── unionRte：RTE_SUBQUERY
        └── unionRte->subquery：UNION Query
            ├── setOperations：纯UNION ALL
            └── rtable：持有各叶子RTE
```

以下情况不支持：

- 外层唯一 FROM 节点不指向目标 `unionRte`；
- `unionRte->subquery` 不是 UNION Query；
- UNION Query 的 `setOperations` 为空；
- `RangeTblRef.rtindex` 无法在同层 UNION Query.rtable 中找到叶子 RTE；
- 外层 RTE 与实际 UNION Query 的对应关系不清晰或不唯一。

### 3.2 外层 Var 不能按 UNION 输出位置映射

外层表达式中的：

```text
Var(varno=unionRti, varattno=N, varlevelsup=0)
```

必须映射为每个叶子的第 N 个非 resjunk 输出表达式。

以下情况不支持：

- `varno != unionRti`；
- `varlevelsup != 0`；
- `varattno <= 0`；
- UNION Query 中不存在第 N 个输出 TLE；
- 某个叶子不存在第 N 个可见输出 TLE；
- 叶子的第 N 个输出依赖更高查询层；
- 替换后仍残留外层 UNION RTE 的 Var。

### 3.3 UNION 输出与叶子输出类型不一致

对于外层 WHERE 和 ORDER BY 实际引用的每一列，必须逐叶子比较：

```text
exprType
exprTypmod
exprCollation
```

任一项不一致，整个改写回退。

典型不支持场景：

- 某个叶子需要额外类型转换后才能形成 UNION 统一输出；
- 某个叶子的 typmod 不一致；
- text 类型相同但 collation 不一致；
- 外层按转换后的值排序，而叶子只能按转换前的值排序。

不能只比较列名，也不能只比较类型 OID。

### 3.4 外层 WHERE 不能完整下推到全部叶子

当前不支持：

- WHERE 只能映射到部分叶子；
- WHERE 只有部分子条件安全；
- 某个叶子缺少谓词引用列；
- 某个叶子的映射表达式含非本地 Var；
- 某个叶子的映射表达式含 volatile 或 SubLink；
- 某个叶子类型、typmod 或 collation 不匹配。

必须满足：

```text
所有叶子都能映射外层完整WHERE
或者
所有叶子都不提交改写
```

### 3.5 外层完整 ORDER BY 不能下推到全部叶子

当前不支持：

- 只向部分叶子下推排序；
- 只下推 ORDER BY 中的向量键、遗漏标量 tie-breaker；
- 某个排序项只能映射到部分叶子；
- 某个叶子无法为映射后的排序表达式建立 TLE；
- 排序 TLE 无法获得正确 `ressortgroupref`；
- `SortGroupClause.tleSortGroupRef` 无法指向叶子排序 TLE。

完整排序列表必须保持：

```text
排序表达式
排序方向
NULLS顺序
tie-breaker顺序
```

并在每个叶子和外层保持一致。

### 3.6 WHERE、ORDER BY 与 LIMIT 的执行顺序不成立

叶子改写后的语义顺序必须是：

```text
叶子原有JOIN/WHERE/NOT EXISTS
→ 映射后的外层WHERE
→ 完整ORDER BY
→ 局部LIMIT K
```

以下形态不支持：

```text
叶子
→ ORDER BY
→ LIMIT K
→ 外层WHERE
```

否则，不满足 WHERE 的近邻会提前占用 K 个名额，导致少行或漏行。

映射后的外层 WHERE 必须与叶子已有 quals 使用 AND 合并，不能覆盖叶子原条件。

例如必须形成：

```sql
WHERE NOT EXISTS (...)
  AND title LIKE 'delta%'
```

不能只保留 `title LIKE 'delta%'`，否则已删除数据会重新进入结果。

### 3.7 外层全局语义不能保留

局部下推后，外层仍必须保留：

- 原始 WHERE；
- 完整全局 ORDER BY；
- 全局 LIMIT。

不支持删除外层全局排序或 LIMIT，也不能把局部结果直接当作最终结果。

正确数据流是：

```text
每个叶子过滤后局部Top-K
→ UNION ALL
→ 全局ORDER BY
→ 全局LIMIT K
```

首版还保留外层原始 WHERE，叶子过滤用于保证局部 Top-K 正确，外层再次过滤
作为语义保护。

### 3.8 任一叶子失败时不能形成完整改写

当前采用全有或全无策略：

```text
复制所有叶子
→ 逐叶子准备WHERE、ORDER BY、LIMIT
→ 任一失败：不提交任何叶子
→ 全部成功：统一替换所有叶子Query
```

导致整次改写回退的联合失败包括：

- 某个叶子结构不安全；
- 某个叶子输出位置缺失；
- 某个叶子类型不一致；
- 某个叶子 WHERE 映射失败；
- 某个叶子 ORDER BY 映射失败；
- 某个叶子 mapped expression 不属于本地查询层。

从 Top-K 数学语义看，部分分支下推同一套 ORDER BY + LIMIT 并不必然错误；
当前全有或全无主要用于简化失败回退并保持 Query 树改写状态一致。

### 3.9 操作符满足要求但叶子没有可用 DiskANN 索引

当前预处理只验证 ORDER BY 操作符属于 DiskANN opfamily，不验证每个叶子一定
存在可用 DiskANN 索引。

因此：

- 某个叶子没有 DiskANN 索引，不一定阻止 Query 改写；
- 改写后该叶子可能使用 `Seq Scan + Sort + Limit`；
- 是否选择 `Ann Index Scan` 仍由原生 Path 代价比较决定；
- 本特性不会强制指定索引；
- 多排序键虽然能下推，DiskANN 索引未必能满足完整 pathkeys，仍可能增加 Sort。

这不是 SQL 正确性限制，但意味着“改写成功”不等于“必然获得 DiskANN 性能收益”。

### 3.10 代价模型无法正确比较联合计划

当前 DiskANN 代价模型尚未充分考虑：

- K；
- WHERE 过滤选择率；
- `diskann_probes`；
- PQ 等搜索参数；
- 召回率。

因此即使内外层联合改写成功，也可能：

- 没有选择 DiskANN Path；
- 选择成本不合理的路径；
- 低估或高估不同叶子的局部 Top-K 成本；
- 无法准确比较 `Ann Index Scan` 与 `Seq Scan + Sort + Limit`。

### 3.11 当前不能依赖自动安全回退的联合场景

以下场景设计上不支持，但当前检查不完整：

| 场景 | 检查缺口 | 主要风险 |
|---|---|---|
| `DESC`、`NULLS FIRST`、混合方向 | 排序方向检查未生效 | 违反DiskANN正向扫描能力边界 |
| `FETCH WITH TIES` | 外层未拒绝，叶子强制关闭 | 漏掉并列行 |
| `FETCH PERCENT` | 外层未拒绝，叶子强制关闭 | 返回行数改变 |
| 外层 targetList 含 SRF | 未检查 `hasTargetSRFs` | SRF与LIMIT执行关系改变 |
| 叶子 targetList 含 SRF | 叶子检查遗漏 | 叶子输出行数改变 |
| 叶子含 volatile 表达式 | 未完整深层检查 | 求值次数或副作用改变 |
| 外层 targetList 含 volatile 表达式 | 未完整检查 | 求值次数改变 |
| `LIMIT abs(3)` 等函数 | 被误认为隐式转换 | 支持范围和成本估算失控 |
| LATERAL | 无统一显式检查 | 依赖后续映射间接回退 |
| 叶子安全屏障 | 只明确检查外层unionRte | 安全边界不完整 |
| 非vector输入操作符 | 未直接比较 `VECTOROID` | 依赖Catalog正确性 |
| 外层FROM与RTE对应关系 | 只检查数量 | 没有显式验证唯一RangeTblRef指向目标RTE |

其中 WITH TIES、PERCENT、SRF 和 volatile 表达式具有直接语义风险，应优先修复。

### 3.12 联合检查的最终判定条件

当前能够稳定纳入本特性的 SQL，应同时满足：

```text
GUC开启
AND 外层是简单SELECT
AND 外层只有一个有效UNION ALL子查询来源
AND 外层WHERE为空或能完整映射到所有叶子
AND 外层ORDER BY至少含一个DiskANN ordering operator
AND 完整ORDER BY能映射到所有叶子
AND 外层LIMIT属于当前支持形式
AND 无OFFSET
AND 内层集合操作树为纯UNION ALL
AND 所有叶子都是有效RTE_SUBQUERY
AND 每个叶子自身无ORDER BY/LIMIT/复杂查询语义
AND 所有相关输出列的type/typmod/collation一致
AND 所有映射后的Var均属于对应叶子当前查询层
AND 叶子原有过滤与映射后WHERE都位于局部LIMIT之前
AND 外层全局WHERE/ORDER BY/LIMIT继续保留
```

只要其中一项无法证明，当前实现就应放弃本次 Top-K 下推并保留原始 Query。
