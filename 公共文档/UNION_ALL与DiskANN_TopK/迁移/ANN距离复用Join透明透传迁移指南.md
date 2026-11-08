# ANN 距离复用 Join 透明透传迁移指南

## 1. 迁移范围

本指南主体迁移一个小功能点：允许符合约束的 `NestLoop`、`MergeJoin`、`HashJoin` 透明传递近似 ANN 排序距离。包内另附源头投影下推的原始提交补丁，用于补齐目标仓缺失的前置能力。

Join 小功能的源码快照及原三份 Join 补丁范围限定为：

```text
openGauss-server/src/gausskernel/optimizer/plan/planner.cpp
openGauss-server/src/test/regress/sql/union_all_ann_feature_matrix.sql
openGauss-server/src/test/regress/expected/union_all_ann_feature_matrix.out
```

本版包含 Join 透明透传、P1 整行距离依赖漏判修复及 P2 Hash 构建侧透传。包内 `planner.cpp` 以源仓库 `3dbecdd31268` 为基底，只叠加该小功能及 P1/P2，不包含此前 `union_all_replace_source_chain()` 的无主键 core 修复。

补充依赖补丁 `patch/UNION_ALL源头投影下推_3dbecdd31268.patch` 原样导出提交 `3dbecdd312686c8ce337169616f4f505f403fb51`，父提交为 `81be3a14b69fef7bda0bb2a683a24f2675ba90c5`。它另涉及 `union_all_required_columns.sql`、`union_all_required_columns_safety.sql` 及对应两份 expected，加上 `planner.cpp` 共 5 个文件（+327/-10）。父提交本身及当前未提交的共享 targetlist 修复不在此历史提交补丁中。

完整源码只用于比对，不得整文件覆盖差异较大的目标仓。本包仍不打包完整基础距离复用与 AM 基础设施；下方前置条件缺一不可。

## 2. 迁移前提

目标仓库必须已经具备以下基础能力：

- `AnnOrderByRef` 或等价的 AM 距离引用节点。
- `rewrite_ann_orderby_distance_refs()` 及其计划树 walker。
- L2/Cosine 距离复用 GUC 和质量标记。
- DiskANN 或目标 KGN AM 已能发布对应距离。
- UNION ALL Top-K 下推和必需列裁剪若需复现完整矩阵，也应已迁移。
- 典型 Anti Join 分支须具备源头投影下推及 setrefs 前的调用：扫描先输出距离，setrefs 再将上层表达式改成槽位引用，最后安全检查决定是否注入 AM 引用。仅迁移 Join 白名单不会自动给扫描补齐距离输出；缺少这一步时使用本包新增的 `3dbecdd31268` 依赖补丁适配。
- 无主键矩阵会经过 `Sort -> scan`：目标仓的源头投影改写必须已正确处理共享 targetlist（先收集匹配项、去重后再修改，或具备等价保护）。源仓工作树有此前的独立 core 修复，本小功能包刻意不携带它；不能把本包对 HEAD 的 `git apply --check` 成功当作所有运行前置条件已满足。

若目标仓库尚无这些基础设施，本补丁不能独立提供完整距离复用能力，不能通过“补齐编译错误”的方式猜测接口。

## 3. openGauss 与 GV/KGN 的关键差异

| 项目 | 当前 openGauss 源仓 | GV/KGN 目标仓 |
|---|---|---|
| L2 操作符 | `<->` | `<->` |
| Cosine 操作符 | `<=>` | `<+>` |
| ANN AM | DiskANN | KGN |
| 距离质量 | L2 exact、Cosine approximate | 按 KGN 已定义的转换公式和质量声明 |

迁移时不能照抄 openGauss 的 Cosine 操作符 OID。必须从目标仓库目录或系统表确认 `<+>` 对应的操作符 OID，并替换 `VECTOR_COSINE_DISTANCE_OPERATOR_OID` 或目标分支中的等价常量。

目标 L2 也可能是近似值。本小功能只判断 Join 是否透明，不负责重新定义距离质量；必须沿用目标仓库已经确认的 KGN 距离转换公式和 `exact/approximate` 契约。

## 4. 推荐迁移步骤

### 4.1 先定位目标入口

在目标仓库查找：

```text
rewrite_ann_orderby_distance_refs
rewrite_ann_orderby_distance_walker
ann_orderby_node_allows_approx
AnnOrderByRef
```

若函数已改名，应按职责定位“setrefs 后将 ANN ORDER BY 投影替换为 AM 结果引用”的入口，不能仅按源文件行号粘贴。

### 4.2 移植距离消费检查

移植以下职责：

```text
ann_orderby_join_expr_consumes_distance()
ann_orderby_join_allows_approx()
```

目标仓库必须核对：

- `Join`、`NestLoop`、`MergeJoin`、`HashJoin` 的结构字段是否同名。
- 是否存在额外 Join qual、向量化 Join 或分布式 Join 字段。
- `NestLoopParam.paramval` 是否仍在 setrefs 后表示外侧参数来源。
- 目标距离引用节点名称和类型查询接口。
- KGN L2/Cosine 操作符 OID。
- KGN 操作符的真实返回类型。若不是源仓的 FLOAT8，应将标量距离引用判定适配为目标类型，不能只替换操作符 OID。
- `getBaseType()`、`type_is_rowtype()` 及声明头文件在目标仓的对应接口。不能只照搬旧版 FLOAT8 判定；P1 修复要求同时拒绝可能含距离的 RECORD/复合引用，并解包 domain。

任何无法确认语义的字段或节点都应失败关闭。

### 4.3 修改透明节点准入

在 `ann_orderby_node_allows_approx()` 或目标等价函数中加入标量三类 Join。通过 Join 专用检查后，仍须执行公共 targetlist 检查：每项只能是 `Var/Const`。

Join 的 `plan.qual` 可以非空，但必须已经由 Join 专用检查证明不消费距离；其他透明节点仍保持原来的空 qual 要求。

P2 还需加入 `T_Hash`：仅接受有左输入、无右输入的普通 Hash，并继续经过公共空 qual 和 `Var/Const` targetlist 检查。父 HashJoin 拒绝时，Hash 必须继续继承 false，不能主动恢复 true；Hash key 或过滤消费距离时，构建侧和探测侧仍按既有安全门回退。

源仓 Hash 的所有键表达式由父 HashJoin 保存和初始化。迁移到差异较大的目标仓时，应检查 Hash 是否新增 `hashkeys`、运行时过滤或其他表达式消费者；若有，必须逐一证明与距离无关。还需确认 Hash 存储、批次落盘和重扫保存的是完整的子 tuple，距离已是其中的物化字段，而不是稍后再读取扫描节点的当前缓存。源仓已具备这一协议，P2 无执行器改动。

### 4.4 适配测试操作符

源 SQL 的 Cosine 为：

```sql
ORDER BY embedding <=> '[1,1,1,1]'
```

迁移到 GV/KGN 时改为：

```sql
ORDER BY embedding <+> '[1,1,1,1]'
```

同时删除或改写源包中“`<+>` 是 L1”的 openGauss 专用负向用例，因为该结论在 GV/KGN 不成立。目标仓应改用一个确定不属于 KGN Cosine opclass 的操作符作为负向看护。

不要使用零向量做 Cosine 查询向量；零向量没有定义良好的 Cosine 方向，会让计划测试与数值异常混在一起。

## 5. Patch 使用方式

先核对目标是否已具备 `push_down_union_all_source_projections()` 或等价实现。若缺失，先从新增的 `patch/UNION_ALL源头投影下推_3dbecdd31268.patch` 迁移函数、上下文声明和 setrefs 前调用点，同时补齐 §5.1 的共享 targetlist 保护。该补丁是 `git format-patch` 导出的原始提交，不含这一后续修复；不能原样迁入旧函数后就运行无主键测试。

目标已有 Join/P1/P2 而只缺源头投影时，仅补缺失依赖及安全保护，不要再次应用 Join 补丁。若目标没有父提交 `81be3a14b6` 的等价能力，应先核对其列裁剪/Top-K 安全修复；补丁上下文匹配不代表语义前提满足。

以下操作及选择表只针对前置能力具备后的 Join 部分。

若目标仓库与源仓库足够接近，可先在目标仓库根目录执行：

```bash
git apply --check patch/ANN距离复用_Join透明透传.patch
git apply patch/ANN距离复用_Join透明透传.patch
```

如果 `--check` 失败，不要使用 `--reject` 后忽略冲突。应按以下顺序人工迁移：

1. 对照包内 `planner.cpp` 的 Join helper、Join/Hash 白名单、qual 检查及类型查询头文件相关差异，不依赖固定 hunk 数或行号。
2. 将 helper 接到目标仓库的近似消费者安全门。
3. 按 KGN 的操作符和质量枚举改写判断。
4. 将 SQL 按 GV 操作符语义调整后加入目标回归框架。
5. 在目标环境重新生成 expected，不直接照搬源仓计划文本。

补丁选择按目标当前状态决定：

| 目标状态 | 应用顺序 |
|---|---|
| 尚未迁移 Join 小功能 | 仅应用完整 `ANN距离复用_Join透明透传.patch` |
| 已迁移原 Join 功能，但没有 P1/P2 | 先应用 `ANN距离复用_Join透明透传_P1增量修复.patch`，再应用 `ANN距离复用_Join透明透传_P2增量修复.patch` |
| 已迁移上一版含 P1 的完整包 | 仅应用 `ANN距离复用_Join透明透传_P2增量修复.patch` |

Join 完整补丁与 P1/P2 增量路径二选一；不要在完整补丁后再次叠加增量。新增源头投影依赖补丁不是 Join 增量链的一部分，按缺失能力单独补齐。所有 patch 路径从源码仓的 `src/` 开始，在目标源码仓根目录使用默认 `-p1`。

### 5.1 共享 targetlist 前置修复的可运行复现

迁移包的 `docs/共享targetlist前置修复复现/` 提供了独立复现用例：

- `01_无主键完整复现.sql`：创建无主键的 delta/snapshot/delete 表和 UNION ALL 视图，强制 `Merge Anti Join -> Sort -> Seq Scan`，依次执行列裁剪关闭、列裁剪打开、三个 GUC 打开的目标 SQL。
- `02_当前已修复实例实测.out`：当前修复内核的完整输出，B 阶段正常返回 `101、102、3、3`，最后通过动态执行再次验证结果并回滚全部对象。
- `README.md`：触发条件、旧版本危险边界、根因和修复算法。

该用例专门验证源头列裁剪的共享 targetlist 保护，**不是** Join P1 的 RECORD 距离消费用例。未包含 `union_all_replace_source_chain()` 修复的旧版本，可能在 B 阶段的 `EXPLAIN` 或执行过程中 backend core；只能在隔离开发实例上运行，不能用生产实例复现。不要为旧版本制作“固定 expected”：旧版本的验收现象是进程异常退出，修复版本的验收是无 core、结果正确、事务回滚。

修复实现必须满足完整顺序：

1. 遍历 source chain，先验证每层都找到匹配的 `TargetEntry`，且没有其他表达式/排序键依赖原始 source Var。
2. 将匹配的 `TargetEntry*` 保存到临时列表，并按指针去重；Sort 和 Scan 可能引用同一个 entry。
3. 完成全部校验后，再遍历去重后的 entry 列表统一执行 `copyObject(candidate->expr)` 替换。

旧实现的危险点是第二遍重新调用 `union_all_find_source_tle()`：第一处共享 entry 已改写后，后续节点无法再按原始 Var 找到它，得到 NULL 后被解引用。迁移时不能只复制“结果相同”的代码片段，必须保留上述先验证、去重、后修改的时序。

## 6. 迁移后必须审查的安全点

### 6.1 正向场景

```text
Merge Anti Join
  Merge Cond: snapshot.row_id = delete.row_id
```

Join 条件只使用 row_id、targetlist 只透传距离时，snapshot KGN/ANN Index Scan 应使用内部距离引用。

P2 同样要求 `Hash Right Anti Join -> Hash -> snapshot KGN/ANN Index Scan` 的构建侧复用；还应分别固定普通 Hash Join 的 ANN 构建侧与探测侧，避免仅测到已支持的一侧。

### 6.2 负向场景

以下情况必须保持原表达式：

- `Join Filter: distance < threshold`。
- Merge/Hash Join key 直接或间接引用距离。
- Nested Loop 通过 `nestParams` 传递距离。
- `Join Filter`/Merge key/Hash key 使用包含距离的整行 `q.*`，即使引用类型是 RECORD 而不是 float8。
- Nested Loop 通过 `nestParams` 传入可能包含距离的整行/复合值，包括 domain 包装类型；不能仅检查 `varattno == 0`。
- Join targetlist 对距离做算术、CASE 或函数加工。
- 未识别的 Join/Stream/远程/向量化执行节点。

### 6.3 分支独立性

一个不安全 Join 只能关闭自己的子树，不能污染 `Append/MergeAppend` 的兄弟分支。退出子树时必须恢复父级 `approximatePlanSafe`。

### 6.4 精确与近似并存

项目已接受同一归并比较域中部分分支使用近似距离、部分分支使用精确距离；它们必须处于同一种距离尺度。迁移不得擅自增加“所有分支必须同时复用”的限制，也不得把 KGN 内部其他分数单位直接暴露给上层排序。

## 7. 编译与测试

仅修改 `.cpp` 时可按目标仓规则增量编译；如果目标适配需要修改节点结构或头文件，必须全量编译，避免 ABI 混合。

源 openGauss 的验证入口为：

```bash
make -C src/test/regress union_all_required_columns_check
make -C src/test/regress ann_orderby_distance_reuse_check
```

若目标 schedule 尚未登记本文件，应在合适的原生回归 schedule 中加入 `test: union_all_ann_feature_matrix`。本包按约定只携带三个源码/测试文件，不携带源仓 schedule；不能只复制 SQL/expected 而漏掉测试入口。上面的两组 make target 是源仓的现有入口，目标仓按自己的框架登记。

目标 GV/KGN 至少应覆盖：

1. L2 四阶段 GUC 矩阵。
2. Cosine 四阶段 GUC 矩阵，操作符改为 `<+>`。
3. row_id Anti Join 正向透传。
4. Join Filter 消费 distance 的负向回退。
5. 距离复用关闭时的原计划。
6. 查询结果、行数和 LIMIT 边界验证。
7. P1 新增整行 Nested Loop、Merge Join、LATERAL 三条路径：复用开关前后必须保留同一个匹配 ID，而不是把 1 行变成 0 行。
8. `enable_expr_fusion` 或目标等价开关的两种设置；简单整数 Join 必须继续复用，负向消费者必须继续精算。
9. P2：Hash 构建侧和探测侧正向、距离 Hash key/Join Filter/RECORD 负向、安全与不安全 UNION ALL 分支两种排列。
10. Hash 的重复匹配、LEFT JOIN NULL 扩展、空构建侧；比较开关前后 ID 多重集、行数和 NULL 数量。
11. 真实内存驻留/分批溢写/三次参数变化重建，在 L2/Cosine 和两种表达式引擎下分别验证。使用相同 AM 公式的独立扫描输出为距离基线。

新增的 `join_guard_capture()` 使用动态 SQL，避免结果断言所需的外层聚合改变被测计划。`ref_scans` 在源仓通过 Ann Index Scan 自身的 Output 双括号识别内部引用；目标 EXPLAIN 格式不同必须适配该检查，不能把“未识别计划文本”冒充禁用了复用。数值比较也必须按 KGN 的公式与近似契约适配；源仓正向 `approximate_rows=8` 是此 8 行 DiskANN 数据的观测，不能要求 KGN 在所有数据上必然同样有 8 个差异。

### 7.1 P1 验收必须能触发旧缺陷

先确认目标索引真的被选择、旧实现确实复用了距离，并选到“AM 近似值与原 SQL 精算值不同”的候选。记录比较的精确常量应从该行原 SQL 距离计算得到；使用足够大的 LIMIT 排除候选截断。否则即使错误地复用，近似值偶然相同也可能让结果测试假通过。

除行集对照外，还必须验证负向计划不产生内部距离引用。不要为使测试通过而把带缺陷的结果更新到 expected。

### 7.2 P2 验收必须观察物化路径

源测试 `hash_lifecycle_check()` 用 32 行、每行 4kB PLAIN payload，在 4MB 和 64kB `work_mem` 下观察 `EXPLAIN ANALYZE` 的 Hash、Batches 和 loops。输出 `batched=true` 才说明实际分批，`rescanned=true` 才说明 Hash 真正重复执行；仅设置低 work_mem 或写 LATERAL 语法不构成证据。目标内存策略不同，应适配行宽/内存设置，直到实际观察到这些路径，不能把 false 写入目标 golden 当作通过。

数值验收逐行检查 `float8send(distance)` 与同模式直接 AM 复用扫描完全相同，同时核对完整 payload；避免把“仍然精算但数值差不多”当作复用成功。源仓 L2 精确，目标 KGN L2 近似时，应让 L2 与 Cosine 一样服从近似消费者安全门，并使用 KGN 自身已有的转换结果作为基线。

## 8. 计划验收

正向场景的目标扫描应出现内部距离引用；若目标 EXPLAIN 同样用双括号表示引用，形态类似：

```text
Ann Index Scan using snapshot_kgn_idx
  Output: row_id, ((embedding <+> query_vector))
  Order By: (embedding <+> query_vector)
```

负向场景应保留原表达式：

```text
Join Filter: (distance < threshold)
-> Ann Index Scan
     Output: row_id, (embedding <+> query_vector)
```

双括号只能证明计划节点已生成内部引用。最终还应使用目标仓库的执行计数器或等价诊断证明正常行读取 AM 值，fallback 次数符合预期。

## 9. 常见迁移错误

- 把 openGauss `<=>` 原样带到 GV，实际未命中 KGN Cosine opclass。
- 只加入 `T_MergeJoin` 白名单，没有检查 `mergeclauses`、`joinqual` 和 `plan.qual`。
- 看到 EXPLAIN 上层显示双括号，就误认为底层扫描已经复用。
- 将源仓 `planner.cpp` 整文件覆盖目标仓，带入不相关 optimizer 修改。
- 直接复制源 expected，而没有按目标操作符、索引名和计划节点重新生成。
- 为提高命中率取消 float8/未知节点的保守拒绝，导致近似值进入 Join 语义。
- 认为 RECORD Var 没有显式 float8 子节点就安全；通用表达式 walker 不会展开引用值内部的字段来源。
- 只修 Join Filter，漏掉 Merge/Hash 条件、Join 后过滤或 `nestParams`。

## 10. 本版交付与剩余边界

- 补充依赖：`patch/UNION_ALL源头投影下推_3dbecdd31268.patch`，原样导出源头投影提交及其测试，缺少这项能力的目标先补齐；不包含后续共享 targetlist 修复。
- 主补丁：`patch/ANN距离复用_Join透明透传.patch`，包含该小功能及 P1/P2，适合尚未迁移 Join 放开部分的分支。
- P1 增量：`patch/ANN距离复用_Join透明透传_P1增量修复.patch`，从原 Join 版本升级到 P1 版本。
- P2 增量：`patch/ANN距离复用_Join透明透传_P2增量修复.patch`，从上一版含 P1 的包升级到本版。完整补丁和增量链的应用结果都与三个源码快照一致。
- `openGauss-server/` 下仍保留原层次的三个文件，与主补丁结果一致；新增依赖涉及的另四个测试文件以 patch 形式交付；`docs/` 包含设计、迁移和验证说明。原三份 Join patch 及三个源码快照保持不变。
- P2 已支持普通标量 Hash 构建侧；仍不开放向量化、分布式及其他未验证执行域。详见 `P2修复验证与交付说明.md`。
- 原来的三文件旧包不含 P1，不应继续作为迁移输入；以本次更新的完整迁移包为准。

## 11. 回滚

回滚本小功能需移除两个 Join helper，并从近似透明节点白名单中删除 `NestLoop/MergeJoin/HashJoin/Hash` 分支。仅回滚 P2 时删除新增 `T_Hash` 分支，保留 P1 和其他 Join 检查。距离复用基础设施、AM 发布协议和 UNION ALL 其他能力不需要回滚。
