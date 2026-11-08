# ANN 距离复用 Join 透明透传详细设计

## 1. 背景与问题

DiskANN 距离复用会把 `Ann Index Scan` 中与 `ORDER BY` 完全相同的距离投影替换为内部 `AnnOrderByRef`。执行时，该引用直接读取 AM 已发布并完成尺度转换的距离，避免再次对 heap 向量执行完整距离计算。

Cosine 复用值属于显式允许的近似距离。原实现只允许它经过 `Limit`、`Sort`、`Material`、`Append`、`MergeAppend`、`SubqueryScan` 和透明 `Result`，所有 Join 都会进入未知消费者的失败关闭分支。典型的 snapshot/delete 反连接因此出现不必要的回退：

```text
Merge Anti Join
  Merge Cond: s.row_id = d.row_id
  -> Ann Index Scan snapshot_cosine_diskann
       Output: s.row_id, (s.embedding <=> query)     -- 重新计算精确距离
```

该 Anti Join 只使用 `row_id` 判断 snapshot 行是否被删除。距离只是随保留行向上传递，直到上层 `Sort` 或 `Merge Append` 才参与排序。把这列换成允许的近似 Cosine 不会改变 Join 是否匹配，只可能产生已经由 GUC 明确接受的排序精度差异。

## 2. 目标与非目标

### 2.1 目标

- 允许标量 `NestLoop`、`MergeJoin`、`HashJoin` 透明传递可复用的近似距离。
- Hash Join 的探测侧和经 `Hash` 的构建侧均可在安全条件下复用；包括 Hash Right Anti Join 的 snapshot 构建侧。
- Join 条件、Join 后过滤和参数传递不得消费被替换的距离。
- Join 输出必须只是 `Var/Const` 透传，不能在该节点重新加工距离。
- 未能证明安全时保留原距离表达式，继续执行精确计算。
- 保持“不同 UNION ALL 分支可分别使用近似值和精确值”的既定质量约定。

### 2.2 非目标

- 不改变 DiskANN 搜索、rerank、距离转换或逐行发布协议。
- 不改变 L2/Cosine 的质量定义和 GUC 含义。
- 不允许近似距离决定 Join 匹配、WHERE 过滤、分组、去重或聚合结果。
- 不开放向量化 Join、Stream、远程计划及未知自定义 Join。
- 不调整 CBO 成本、Join 顺序或 Join 算法选择。

## 3. 语义边界

### 3.1 允许：距离只是 Join 的随行输出

典型 SQL：

```sql
SELECT s.row_id, s.embedding <=> '[1,1,1,1]'::vector AS distance
FROM snapshot_t s
WHERE NOT EXISTS
(
    SELECT 1
    FROM delete_t d
    WHERE d.row_id = s.row_id
)
ORDER BY distance
LIMIT 10;
```

数据流为：

```text
snapshot Ann Index Scan 产生 row_id、distance
    ↓
Anti Join 只按 row_id 删除命中的行，distance 原样透传
    ↓
Sort / Merge Append 使用 distance 排序
```

这里精确值与近似值处于相同的 Cosine 距离尺度。允许的变化仅是近距离候选之间的顺序精度，不是 `d²` 与 `d` 之类的单位混用。

### 3.2 禁止：Join 消费距离决定结果集合

例如：

```sql
SELECT q.row_id
FROM
(
    SELECT row_id, embedding <=> '[1,1,1,1]'::vector AS distance
    FROM delta_t
    ORDER BY distance
    LIMIT 10
) q
JOIN cosine_gate g ON q.distance < g.max_distance;
```

此时误差可能使一行从匹配变成不匹配，影响结果集合，而不只是排序精度。因此嵌套的 Ann Index Scan 必须保留原表达式：

```text
Nested Loop
  Join Filter: (q.distance < g.max_distance)
  -> Ann Index Scan
       Output: row_id, (embedding <=> query)          -- 单括号，禁止复用
```

## 4. 优化器位置与处理时机

改写仍位于 `standard_planner()` 中所有计划和子计划完成 `set_plan_references()` 之后、构造最终 `PlannedStmt` 之前：

```text
set_plan_references()
    ↓
rewrite_ann_orderby_distance_refs()
    ↓
rewrite_ann_orderby_distance_walker()
    ↓
ann_orderby_node_allows_approx()
```

选择该时机的原因是：

- Join 的透传输出已经表现为 `OUTER_VAR/INNER_VAR` 等物理槽位引用。
- 可以基于最终计划节点类型和条件字段做准入判断。
- 被替换的仍然只是 `AnnIndexScan.targetlist` 中与 `indexorderbyorig` 完全相等的完整表达式，不会改写 SQL 语义树中的任意相似子表达式。

## 5. 实现设计

### 5.1 距离消费识别

新增 `ann_orderby_join_expr_consumes_distance()`，递归检查 Join 的语义表达式。出现以下任一情况就视为消费距离：

1. `Var` 或 `Param` 去除 domain 后的类型为 `FLOAT8OID`。在 `setrefs` 后计划中，独立的距离子输出通常以 float8 槽位引用出现。该规则会保守拒绝无关的 float8 Join key，属于有意的失败关闭。
2. `Var` 或 `Param` 去除 domain 后满足 `type_is_rowtype()`，包括匿名 RECORD 和命名复合类型。这类值可能把距离藏在整行内部，不能仅凭外层类型不是 float8 就认为安全。
3. 表达式中已经出现 `AnnOrderByRef`。
4. 表达式直接包含内置 L2 或 Cosine 距离操作符。

此实现宁可少复用，也不能把近似距离误送入关系语义。

#### 5.1.1 P1 修正：整行引用不是独立于距离的标量

旧实现只判断 `exprType(node) == FLOAT8OID`，会漏过如下条件：

```sql
JOIN gates g ON q = ROW(g.id, exact_distance)
```

其中 `q` 是子查询产生的 `(id, distance)`。`q.*` 在计划中是 RECORD Var，表达式 walker 把 Var/Param 当作叶子，不会自动展开记录字段。旧实现因此会让近似距离进入记录相等判断，实测把关闭复用时的 1 行变成 0 行。Merge Join 的整行键、LATERAL 传入的整行参数也存在同一问题。

修正后的核心判定为：

```cpp
if (IsA(node, Var) || IsA(node, Param)) {
    Oid typeId = getBaseType(exprType(node));
    if (typeId == FLOAT8OID || type_is_rowtype(typeId)) {
        return true;
    }
}
```

不能仅检查 `Var.varattno == 0`：setrefs 后整行值可能先放入子节点的一个普通输出槽位，再以正数属性号被引用。应检查引用类型，而不是只检查原始整行语法标记。domain 必须先解包，以免同样的类型包装绕过规则。

本修复统一作用于 Join 的所有条件字段及 `nestParams.paramval`，不是只补 Nested Loop 的 Join Filter。targetlist 仍可按既有规则透传 Var；保守拒绝发生在整行值进入 Join 语义或参数传递时，整数 row_id 连接继续允许复用。若未来需要放开复合值，应先实现字段级依赖追踪。

### 5.2 Join 字段覆盖

`ann_orderby_join_allows_approx()` 检查以下字段：

| 公共/节点字段 | 影响 | 准入要求 |
|---|---|---|
| `plan.qual` | Join 后过滤 | 不得消费距离 |
| `joinqual` | ON/USING 匹配 | 不得消费距离 |
| `nulleqqual` | NULL 相等语义 | 不得消费距离 |
| `NestLoop.nestParams` | 外侧向内侧传参 | 参数值不得是距离 |
| `MergeJoin.mergeclauses` | Merge 匹配键 | 不得消费距离 |
| `HashJoin.hashclauses` | Hash 匹配键 | 不得消费距离 |
| SPQ `hashqualclauses` | SPQ 附加 Hash 条件 | 编译启用时同样检查 |

只对 `T_NestLoop`、`T_MergeJoin`、`T_HashJoin` 返回可能安全；其他 Join 类型继续返回 false。

### 5.3 输出透明性

Join 通过条件检查后，仍要接受原有透明节点公共检查：targetlist 中每个表达式必须是 `Var` 或 `Const`。

因此以下形式会失败关闭：

```text
Join Output: distance + 0.0
Join Output: CASE WHEN ... THEN distance ...
Join Output: function(distance)
```

而 setrefs 后的简单槽位透传可以通过。EXPLAIN 为可读性可能把 Var 向下展开成距离文本，不能据此误判 Join 节点重新执行了距离函数。

### 5.4 分支状态恢复

`rewrite_ann_orderby_distance_walker()` 进入节点前保存父级 `approximatePlanSafe`，退出该节点及其子树后恢复。一个不安全 Join 只阻断其自身子树，不会错误关闭 `Append/MergeAppend` 的其他兄弟分支。

### 5.5 P2 修正：Hash 构建侧透传

P1 版已放行 `T_HashJoin`，却遗漏其右子树中的 `T_Hash`。因此按整数 id 连接时，探测侧可以复用，构建侧却在访问 Hash 时被设为 `approximatePlanSafe=false`。这是漏优化；不涉及重新定义距离精度。

本次在 `ann_orderby_node_allows_approx()` 增加 `T_Hash` 分支，要求有且只有左侧输入，然后继续执行公共检查：`plan.qual` 为空、targetlist 仅含 `Var/Const`。核心为：

```cpp
case T_Hash:
    if (((Plan*)node)->lefttree == NULL || ((Plan*)node)->righttree != NULL) {
        return false;
    }
    break;  // 继续执行公共 qual/targetlist 检查
```

这里的 `break` 只是准许继承已有的安全状态，不能把 `approximatePlanSafe` 主动设回 true。父 HashJoin 的 `hashclauses`、`joinqual`、`nulleqqual` 和 `plan.qual` 已由 §5.2 检查；上层任何消费者拒绝后，Hash 及其后代仍处于拒绝状态。P1 的 RECORD/复合值保护同样适用。

源仓 Hash 的键表达式保存在父 HashJoin。`planmem_walker.cpp` 的 `T_Hash` 分支遍历普通 Plan 字段，额外键由父节点遍历。目标仓如果在 Hash 自身增加了 `hashkeys`、过滤器等表达式字段，必须补充其距离消费检查，不能直接复制这个白名单分支。

#### 5.5.1 为什么不需要修改执行器

距离进入 Hash 前已经由 `AnnIndexScan` 投影为当前行的 float8 字段：

1. `setrefs.cpp` 为 Hash 调用 `set_dummy_tlist_references()`，用槽位引用描述子节点输出。Hash 自身不执行一次距离投影。
2. `MultiExecHash()` 从子节点取 slot；Hash key 由父 HashJoin 初始化，安全场景只读取整数 id。
3. `ExecHashTableInsert()` 把 slot 转为 `MinimalTuple`，将完整 tuple 字节复制到 Hash 表，距离字段随之保存。
4. 需要分批时，`ExecHashJoinSaveTuple()` 保存完整 tuple 和 hash 值；读回时 `ExecHashJoinGetSavedTuple()` 恢复同一 tuple。它不重新访问 ANN 扫描的“当前距离”缓存。
5. 重扫可能复用已有 Hash 表，也可能因参数变化重建。复用表读取已物化的字段；重建重新拉取子节点并保存各行的新字段。P2 测试用改变整数过滤参数的 LATERAL 路径实际触发重建。

所以正常驻留、落盘、读回都通过已有 tuple 协议传递标量。新增执行器缓存反而会增加额外生命周期；本次内核变更仅为 `planner.cpp` 中的 10 行（含注释）。

## 6. 修改前后计划

### 6.1 修改前

```text
-> Merge Anti Join
     Merge Cond: (s.row_id = d.row_id)
     -> Sort
          Sort Key: s.row_id
          -> Ann Index Scan using snapshot_cosine_diskann
               Output: s.row_id, (s.embedding <=> '[1,1,1,1]'::vector)
               Order By: (s.embedding <=> '[1,1,1,1]'::vector)
```

DiskANN 仍负责搜索与 `Order By`，但 Output 单括号表示扫描投影继续计算原始 Cosine 表达式。

### 6.2 修改后

```text
-> Merge Anti Join
     Merge Cond: (s.row_id = d.row_id)
     -> Sort
          Sort Key: s.row_id
          -> Ann Index Scan using snapshot_cosine_diskann
               Output: s.row_id, ((s.embedding <=> '[1,1,1,1]'::vector))
               Order By: (s.embedding <=> '[1,1,1,1]'::vector)
```

Ann Index Scan 自身的双括号表示 target entry 已改成 `AnnOrderByRef`。运行时是否没有 fallback，仍由现有执行器计数和距离复用回归共同证明，不能只依赖 EXPLAIN 外观。

### 6.3 P2 的 Hash Right Anti Join

P1 版 snapshot 的路径：

```text
Hash Right Anti Join
  Hash Cond: (d.row_id = s.row_id)
  -> Seq Scan on delete_t d
  -> Hash
       -> Ann Index Scan using snapshot_cosine_diskann on snapshot_t s
            Output: s.row_id, (s.embedding <=> '[1,1,1,1]'::vector)
```

P2 修复后，相同 Hash 构建侧使用内部引用：

```text
Hash Right Anti Join
  Hash Cond: (d.row_id = s.row_id)
  -> Seq Scan on delete_t d
  -> Hash
       -> Ann Index Scan using snapshot_cosine_diskann on snapshot_t s
            Output: s.row_id, ((s.embedding <=> '[1,1,1,1]'::vector))
```

完整计划保存在原生回归 `union_all_ann_feature_matrix.out`；上述片段只突出 P2 的变化。

## 7. 测试设计

`union_all_ann_feature_matrix.sql` 使用无主键的 delta/snapshot/delete 结构，并建立 L2 与 Cosine 两组 DiskANN 索引。

### 7.1 四阶段开关矩阵

L2 和 Cosine 分别执行：

1. Top-K 下推、列裁剪、距离复用全关。
2. 只打开 UNION ALL Top-K 下推。
3. 再打开必需列裁剪。
4. 最后打开对应的 L2 或 Cosine 距离复用。

该矩阵同时看护分支 Limit、源头 Output 列集合以及 Ann Index Scan 的单/双括号变化。

### 7.2 Join 正向看护

Cosine 全开后，`Merge Anti Join` 只按 `row_id` 连接，delta 与 snapshot 两个 DiskANN 扫描都应显示双括号距离输出。

### 7.3 Join 负向看护

新增 `cosine_gate`，让外层 Nested Loop 的 Join Filter 消费子查询 `distance`。嵌套 Ann Index Scan 必须保持单括号。

### 7.4 操作符负向看护

本 openGauss 树中 `<+>` 是 L1，不是 Cosine。即使所有相关 GUC 都打开，`<+>` 用例也不得误入 DiskANN Cosine 距离复用。

### 7.5 P1 计划与执行结果看护

在原 `union_all_ann_feature_matrix.sql` 内新增独立的 8 行向量数据，使用 `LIMIT 40` 排除候选截断因素：

- Nested Loop 整行相等条件、Merge Join 整行键、LATERAL 整行参数：检查完整 EXPLAIN，底层扫描必须保持原距离计算。
- 上述三种路径及直接 FLOAT8 距离条件、FLOAT8 参数：关闭/开启复用均应返回 `{1}`，`ref_scans=0`、`approximate_rows=0`。
- 按整数 id 的 Nested Loop/Merge Join：开关前后仍返回全部 8 个 ID，开启时应保留 AM 引用，防止把 P1 修成“所有 Join 全部禁用”。
- 在 `enable_expr_fusion=off/on` 两种设置下都执行结果看护。

测试通过 PL/pgSQL `EXECUTE` 独立规划被测 SELECT，避免外层结果捕获所需的聚合/连接反过来禁用其距离复用。数值检查使用 `float8send()`，不只依赖小数打印或 EXPLAIN 括号。

### 7.6 P2 计划、结果和物化生命周期

同一 SQL 文件追加以下看护，仍使用 openGauss 原生 expected 比较：

- 完整计划：普通 Hash Join 的构建侧/探测侧，以及三个开关均打开的 delta/snapshot/delete Hash Right Anti Join。
- 正反结果：按 id 连接、距离作为 Hash key、距离作为 Join Filter、含距离的 RECORD 条件。后三者必须保持精确值，并且底层扫描不生成 AM 引用。
- 分支独立性：安全/不安全分支按两种顺序组成 UNION ALL，只安全分支复用。
- 重复匹配、LEFT JOIN 的 NULL 扩展和空构建侧：保持开关前后的行数、ID 多重集和 NULL 数量。
- 32 行向量、每行 4kB PLAIN payload：4MB work_mem 驻留；64kB work_mem 实际分批；LATERAL 改变整数参数触发三次 Hash 重建，结果为 32+31+30=93 行。
- 生命周期用例在 L2/Cosine、两种表达式执行引擎下执行；以直接 ANN 复用扫描为数值基线，逐位比较距离并比较 payload，避免用精确 Cosine 误判允许的近似结果。
- `EXPLAIN ANALYZE` 实际观察 Hash、Batches 和 loops，回归只输出稳定布尔值。若没有真正落盘或重扫，相应布尔值为 false，不能把用例当作通过。

## 8. 验收标准

- 透明 `row_id` Anti Join 下，snapshot Cosine Ann Index Scan 由单括号变为双括号。
- Join Filter 消费 distance 时，底层 Ann Index Scan 仍为单括号。
- 整行/RECORD 包含 distance 并用于 Join 匹配或参数时，底层扫描同样保持单括号，开关前后匹配 ID 不变。
- 安全 Hash 构建侧/探测侧都复用；距离条件仍阻断；分批和三次参数化重扫后逐行距离、payload 与独立扫描基线一致。
- GUC 关闭时计划行为不变。
- L2/Cosine 四阶段计划与各开关职责一致。
- `<+>` 在 openGauss 中保持 L1 负向行为。
- `union_all_required_columns_check` 和 `ann_orderby_distance_reuse_check` 两组专用回归全部通过。

## 9. 已知保守边界

- 当前把 Join 条件中的任意 float8 或复合类型 `Var/Param`（含 domain）都视为可能的距离引用，因此无关 float8/复合 Join key 也可能阻断复用。这是安全的假阴性，不是错误复用。
- 仅支持标量 `NestLoop/MergeJoin/HashJoin`；其他节点需单独证明字段语义和执行协议。
- P2 已补上普通标量 Hash 构建侧，仍遵守父级安全检查。向量化、Stream、远程及目标分支新增的 Hash 语义未自动纳入支持范围。
- 本功能只放宽优化器准入，不改变近似 Cosine 的既有质量承诺，也不把计划双括号当作唯一运行时证据。
