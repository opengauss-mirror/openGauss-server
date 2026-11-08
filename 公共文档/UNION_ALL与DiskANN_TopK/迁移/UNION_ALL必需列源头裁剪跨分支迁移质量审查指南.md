# UNION ALL 必需列源头裁剪跨分支迁移质量审查指南

## 1. 目的与结论

本文用于以下场景：`UNION ALL 必需列源头裁剪` 已从当前 openGauss 分支迁移到另一个同源但代码差异较大的分支，需要让大模型完成迁移后的代码审查、修正和验证。

迁移质量不能用“patch 应用成功”“代码能编译”或“目标 SQL 能执行”来判断。最低可信标准应是：

1. 目标分支中的实现仍满足原特性的语义不变量；
2. 所有位置型列接口完成一致的列号和元数据重映射；
3. 任一安全条件不满足时，整项优化原子回退，不留下半改写 Query；
4. GUC 关闭时与目标分支原行为一致；
5. GUC 开启时，计划确实从源扫描处消除无关列；
6. 开启和关闭优化的查询结果严格等价；
7. 目标分支已有功能、分布式规划和既有回归没有被破坏。

大模型最应该关注的不是源码文件和行号是否与原分支相同，而是每项设计意图在目标分支中由哪个真实入口、结构和 API 承担。代码差异越大，越应做“三方语义对照”：

```text
原分支特性提交相对其基线的变化
                ↓ 提取设计意图和不变量
目标分支迁移前代码 ───────→ 目标分支迁移后代码
```

没有任何单次大模型审查能够“保证”生产质量，但按本文完成静态审查、Debug/full 构建、计划看护、结果等价、负向回退和目标分支兼容性回归后，可以达到迁移的基本质量门。

## 2. 审查时使用的权威材料

初始提交 `098d77c8b76d` 在后续深度复核中复现了六个缺陷。迁移必须同时包含[深度复核与缺陷修复报告](../测试/UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md)所列修正；旧交付 patch 并不会自动包含后续修复。

原实现的基准材料如下：

| 材料 | 位置 |
|---|---|
| 原特性提交 | `098d77c8b76d56eace4af8fddaf8d94b394b996c` |
| 详细设计 | [UNION_ALL必需列源头裁剪详细设计.md](../设计/UNION_ALL必需列源头裁剪详细设计.md) |
| 源码解读 | [UNION_ALL必需列源头裁剪源码解读.md](../源码解读/UNION_ALL必需列源头裁剪源码解读.md) |
| 开发与测试报告 | [UNION_ALL必需列源头裁剪开发与测试报告.md](../测试/UNION_ALL必需列源头裁剪开发与测试报告.md) |
| 原交付 patch | [opengauss_union_all_required_columns_pushdown.patch](../执行包/opengauss_union_all_required_columns_pushdown.patch) |

开始审查前，还必须向大模型提供或让其只读确认：

- 目标仓库绝对路径；
- 目标分支名称和当前 HEAD；
- 迁移前基线 commit；
- 迁移 commit，或明确的迁移 diff 范围；
- 目标分支构建、安装和回归命令；
- 测试实例端口、数据库和 schema；
- 目标分支是否用于多 DN、Stream、并行或其他特殊执行环境；
- 工作树中哪些 dirty 文件属于用户，不能修改或提交。

若无法明确迁移前基线，大模型至少应先找出迁移所改的全部文件，并说明审查结论的证据边界，不能把整个目标分支与原分支做无差别文本比较。

## 3. 先锁定不可改变的语义契约

本特性的目标 SQL 契约是：

- 外层 `WHERE` 必须存在；
- 外层 `ORDER BY` 可有可无；
- 外层 `LIMIT` 可有可无；
- 数据来源是可安全改写的纯 `UNION ALL`；
- 最终查询只需要的列，应从叶子扫描开始就不再携带其他无消费者列；
- 只在叶子本地使用的过滤列可以参与扫描过滤，但不能继续向上传输；
- 任一叶子不能安全改写时，整个 UNION 优化都不生效。

目标效果不能简单理解为“最上层只输出一列”。例如：

```sql
SELECT row_id
FROM union_view
WHERE title LIKE 'A%'
ORDER BY embedding <-> '[0.1,0.2,0.3,12.8]'
LIMIT 10;
```

正确的字段生命周期是：

| 字段 | 最后消费者 | 应有行为 |
|---|---|---|
| `row_id` | 最终投影、可能也用于反删除连接 | 保留至最外层 |
| `title` | 叶子本地 WHERE | 可出现在源扫描 `Filter`，不应向上层 `Output` |
| `embedding` | 叶子本地距离计算 | 不应以 vector 形式穿过 UNION 边界 |
| synthetic distance | 局部/全局排序 | 以标量列穿过 UNION，最终可为 resjunk |
| `delete_t.reason` 等删除表过滤列 | 删除侧本地过滤 | 不应从删除侧扫描向上多传 |

大模型必须围绕“最后消费者”审查，而不能见到某列出现在任意扫描节点就直接判断失败。列出现在 `Filter`、`Index Cond`、`Join Cond` 中是允许的，关键是它是否还出现在不必要的节点 `Output` 中并被层层传输。

## 4. 迁移后最应关注的十二类细节

### 4.1 规划改写入口是否仍在正确阶段

原实现位于 `subquery_planner()`，在 rownum 预处理之后、子查询 pull-up 和简单 UNION ALL flatten 之前，顺序为：

```text
preprocess_rownum
  → preprocess_union_all_required_columns
  → preprocess_union_all_diskann_topk
  → preprocess_union_all_order_key_projection
  → pull_up_subqueries / flatten_simple_union_all
```

迁移时不能只查到三个函数被调用就算通过。大模型应画出目标分支真实的 planner 时间线并回答：

1. 该点上 view/rewrite 生成的 UNION 子 Query 是否已经存在；
2. 该点上 UNION RTE、叶子 Query 和旧列号是否仍完整；
3. 该点之后是否才发生 `pull_up_subqueries`、`flatten_simple_union_all` 或同类扁平化；
4. 目标分支是否把 rownum、CTE、security barrier 或 SetOp 处理移动过；
5. 三个阶段之间是否插入了会重写 targetlist 或 sortClause 的新逻辑。

三个阶段的相对顺序是阻断项：

- 阶段一要在旧列号仍可映射时下推 WHERE 并压紧原始列；
- Top-K 阶段必须仍能看见原始 vector 距离 `OpExpr`；
- 排序键物化阶段随后把距离变成叶子计算出的标量列；
- 三者必须发生在 UNION 被通用逻辑拉平之前。

还必须分清“当前 Query 阶段一返回 true”与“PlannerInfo 从父查询继承扫描裁剪策略”。只有前者才能批准本层排序键改写；否则子查询可能尚有聚合/SRF/其他安全门限制。本次曾实测误用继承 flag 导致 `Aggref found in non-Agg plan node`。

如果目标分支不存在相同函数名，应寻找承担相同生命周期的实际入口，不能为了保持文本相同而生搬原位置。

### 4.2 目标分支 AST 和优化器 API 差异

大模型要逐一核对目标分支中的真实结构定义，而不是依赖原分支记忆：

- `Query` 是否新增或删除了 `limitIsPercent`、`limitWithTies`、`hasTargetSRFs`、`hasModifyingCTE`、`hasRowSecurity` 等字段；
- `SetOperationStmt` 的 `colTypes`、`colTypmods`、`colCollations`、`groupClauses` 是否仍有相同含义和长度约束；
- `RangeTblEntry` 的 security barrier、lateral、权限和列名字段是否变化；
- `TargetEntry.resno`、`resjunk`、`ressortgroupref` 的处理是否变化；
- `Var` 的 `varno`、`varattno`、`varlevelsup`、`varnoold`、`varoattno` 是否存在分支特有语义；
- `ReplaceVarsFromTargetList`、`pull_vars_of_level`、`expression_tree_mutator` 等函数的签名、返回值和失败行为是否变化；
- `PlannerInfo` 是否存在多个初始化入口、复制入口或特殊 planner root；
- `use_physical_tlist()`、`build_path_tlist()` 或替代逻辑是否增加了列存、向量、分区、外表等分支。

审查报告应列出“原实现依赖的 API → 目标分支对应 API → 差异 → 适配结论”，不能只给一句“接口已适配”。

### 4.3 `oldToNew` 列号映射是否完整且一致

这是最容易产生错结果或崩溃的部分。对每次压紧，大模型必须确认同一个 `oldToNew` 映射同步更新了：

1. 外层 Query 中引用 UNION RTE 的全部 level-0 `Var.varattno`；
2. 每个 UNION 叶子的可见 targetlist；
3. 叶子 RTE 的 `eref->colnames`；
4. UNION Query 自身 targetlist；
5. UNION RTE 对外暴露的 `eref->colnames`；
6. SetOp 树每一层的 `colTypes`；
7. SetOp 树每一层的 `colTypmods`；
8. SetOp 树每一层的 `colCollations`；
9. 如目标分支新增了位置型元数据，也必须同步处理。

必须特别审查以下编号陷阱：

- SQL attribute number 和 `TargetEntry.resno` 从 1 开始，C 数组/List 下标常从 0 开始；
- `ressortgroupref` 是稳定引用号，不等于 `resno`，不应随列压紧机械改写；
- `resjunk` 项不能混入 SetOp 可见列数，但也不能因最终不返回给用户就随意删除；
- whole-row `Var.varattno == 0` 和系统列负 attno 不能送入普通映射数组；
- `varlevelsup != 0` 的相关引用不能当成本层 UNION 列改写；
- `varnoold/varoattno` 只有在目标分支语义允许且确实指向该接口时才同步修改。

建议让大模型为每条典型 SQL 手工列出四张表：原始集合列、阶段一 required、阶段一 old-to-new、阶段三最终接口。四张表无法自洽时，不应接受代码。

### 4.4 required-column 收集是否覆盖所有真实消费者

required 列不能只从最终非 junk targetlist 收集，还应考虑：

- 外层 targetlist 的全部表达式，包括 resjunk 排序表达式；
- 未被成功下推或仍保留在外层的 quals；
- sort/group/distinct/window/returning/row mark 等目标分支允许出现的消费者；
- LIMIT/OFFSET 表达式中可能存在的引用；
- volatile expression、SRF、SubLink 及具有语义副作用的输出位置；
- 目标分支新增的 planner 字段所引用的表达式。

如果某类 Query 结构没有被正确分析，应安全拒绝该形状，而不是推测它不需要列。required 为空时还要检查是否保留合法 carrier，避免构造零列 SetOp 或触发分支特有断言。

### 4.5 WHERE 下推是否等价、完整并且原子

外层 WHERE 是本特性的必要入口条件。审查时需要确认：

- 只把引用目标 UNION RTE 的 level-0 Var 映射到叶子表达式；
- 通过叶子 targetlist 做位置替换，而不是只修改 `varno`；
- 每个映射位置校验 type、typmod、collation；
- 所有叶子都能映射成功后才从外层删除原 quals；
- 原叶子的已有 quals 与下推 quals 组合方式正确；
- BoolExpr、RelabelType、CollateExpr、Param 等分支特有节点能被正确复制或拒绝；
- 不能跨 RLS、security barrier、lateral、相关子查询边界非法下推；
- volatile、SRF、SubLink 或无法证明安全的表达式触发整体回退。

最佳实现模式仍是 copy-on-write：先复制外层 Query，准备所有叶子的改写副本，全部成功后一次性提交。大模型要检查失败路径是否可能已经修改原树、某个原叶子或共享 List。只要存在“前几个叶已改、后一个叶失败”的路径，就是阻断问题。

WHERE 不能下推时，阶段一仍可保留 WHERE 引用列并裁掉其他死列；此时阶段三必须跳过，避免把排序表达式移动到残留 Filter 之前。典型复现是带 volatile 输出的叶子、拒绝 0 的 volatile 外层谓词，以及 `ORDER BY 100 / row_id`：错误改写会产生除零。

还要检查保存的指针是否指向工作副本，而不是原始 Query；跨 `copyObject()` 后混用两棵树的 RTE 指针，是迁移时非常隐蔽的错误。

### 4.6 排序键物化是否真正截断 vector 生命周期

当存在外层 `ORDER BY embedding <-> query_vector` 时，阶段一不能直接删掉 embedding，因为 Top-K 识别和距离计算仍需要它。阶段三才应：

1. 按 `SortGroupClause.tleSortGroupRef` 找到外层排序 TLE；
2. 将该排序表达式映射到每个叶子的本地表达式；
3. 在每个叶子追加相同类型的 synthetic scalar key；
4. 在 SetOp 树每一层追加相同的 type/typmod/collation 元数据；
5. 将外层排序 TLE 的整个表达式替换成对 synthetic key 的 Var；
6. 再删除 UNION 边界上已无消费者的原始 embedding。

需要重点检查：

- 是否错误地按 targetlist 下标或 resno 匹配 sort key；
- 是否只替换了距离表达式内部的 Var，导致外层仍重复计算；
- 是否只给部分叶子或 SetOp 根追加 key；
- 多个 ORDER key 是否保持顺序、方向、nulls first/last 和操作符语义；
- synthetic key 是否在所有叶子上 type、typmod、collation 一致；
- ORDER BY 不存在时，阶段三能无副作用返回；
- Top-K GUC 关闭但 required-column GUC 开启时，ORDER-only 场景仍能物化标量 key。

EXPLAIN 中只看最终 `SELECT row_id` 不够。必须确认 UNION/Append 以上不再传 vector，叶子只在距离计算最后一次使用 vector。

### 4.7 Top-K、过滤、反删除与 LIMIT 的相对位置

迁移可能恰好遇到目标分支 Top-K 实现不同，因此要审查语义位置而不是节点名称。核心要求是：

- 全局 ORDER BY 和全局 LIMIT 始终保留；
- 局部 Top-K 只是候选裁剪，不能替代全局排序和截断；
- 局部 LIMIT 必须包在完整叶 Query 之外；
- snapshot 叶中的 anti-delete、RLS、用户 WHERE 等必须先完成，再进入该叶的局部 LIMIT；
- 不能让已删除记录先占据局部候选名额，然后再做 Anti Join；
- 没有 ORDER BY 时不能凭 LIMIT 任意做不等价的分支下推；
- 没有 LIMIT 时 ORDER BY 所需的列生命周期优化仍应独立正确；
- 只有 LIMIT、没有 ORDER BY 时，结果行身份本来可能无确定顺序，测试不应错误要求固定 ID 顺序。

特别补查三个已复现缺口：UNION 容器自身有 LIMIT 时禁止跨越；外层 SRF 可以输出零行，输入行的局部 K 不能代表输出行的 K；WITH TIES 不能降成叶子普通 LIMIT。PERCENT 也应按专门语义拒绝。

典型正确相对关系应为：

```text
Global Limit（若有）
  → Global Sort（若有）
    → Append / UNION ALL
      → Leaf-local Limit（仅安全 Top-K 场景）
        → Leaf-local Sort 或 ANN Index Scan
          → 完整叶语义：WHERE、Anti Join、RLS 等
            → Source Scan
```

计划中 Hash Anti Join 与 Merge Anti Join 的变化通常不是迁移失败；`Output`、过滤位置、Limit 的上下关系及结果语义才是看护重点。

### 4.8 physical targetlist 是否在 Plan 阶段把列重新扩回来

Query 树压紧成功并不等于源扫描已变窄。目标分支可能在 `createplan` 或 path target 构造时重新选择整行 physical targetlist。

大模型必须：

1. 找到目标分支所有 physical targetlist 选择路径；
2. 检查 `PlannerInfo` 的“本次查询已裁剪”标志是否正确初始化；
3. 检查创建子 planner root 时是否传播该标志；
4. 确认只对已成功裁剪查询的 `RTE_RELATION` 禁止 physical tlist；
5. 确认失败回退或 GUC off 时标志仍为 false；
6. 检查普通 heap/index scan 之外的目标分支路径。

特别容易漏掉：

- Index Scan / Index Only Scan；
- ANN Index Scan；
- CStore Scan；
- 分区表和分区迭代器；
- Foreign Scan；
- Bitmap 路径；
- Stream/RemoteQuery 的传输 targetlist；
- 并行 worker 或子规划器创建的独立 `PlannerInfo`。

如果 Append 已经只输出 `row_id`，但 `snapshot_t` 扫描仍输出 `row_id,title,embedding`，说明迁移仍未完成。若 `title` 只用于扫描 Filter，它可以出现在 Filter 表达式，但不应作为向上输出列。

### 4.9 GUC 生命周期、默认行为和分布式传播

至少核对：

- 名称仍是 `enable_union_all_required_columns_pushdown`，或有明确兼容说明；
- 类型为 bool；
- 默认 `off`；
- 用户级 `SET`/`SHOW` 生效；
- 新 session 默认值正确；
- 字段放在目标分支正确的 session attribute 中；
- GUC off 时不会设置 planner flag，也不会调用任何有副作用的改写；
- 它与 `enable_union_all_diskann_topk_pushdown` 独立，不应互相隐式开启；
- prepared statement、generic/custom plan 和 plan cache 对 GUC 变化的行为符合目标分支现有约定；
- 多 DN 或 worker 环境中，GUC 和相关 planner 状态能传播到实际规划/执行节点。

若目标分支对 planner GUC 有统一的 plan-cache invalidation 标志、序列化表或 worker 白名单，必须接入。单机 session 中 `SET` 成功不能证明分布式节点已收到该值。

### 4.10 安全门是否因迁移被削弱

无法证明安全的 Query 形状必须整体回退。至少覆盖：

- 不是纯 `UNION ALL`，包括 UNION DISTINCT、INTERSECT、EXCEPT；
- 外层没有 WHERE；
- 叶子已有 LIMIT/OFFSET，或目标分支支持的百分比/with ties LIMIT；
- agg、grouping sets、window、distinct、row marks；
- modifying CTE、INSERT/UPDATE/DELETE/MERGE 等非只读结构；
- RLS、security barrier view；
- lateral、相关引用或 whole-row/system attribute；
- volatile expression、SRF、SubLink；
- 叶子输出数量不一致；
- 同位置 type、typmod、collation 不一致；
- 无法唯一识别 UNION RTE；
- 目标分支新增但原实现不了解的语义字段。

`ROWNUM` 需要单独检查外层/叶子 targetlist 和 quals。它在此分支不是普通 volatile 函数，移动 WHERE 可以改变编号，不能只依赖 `contain_volatile_functions()`。

SRF 不能只检查 `hasTargetSRFs`：该分支旧表达式框架下，parser 可能不设置这个 flag。外层和叶子还必须直接调用 `expression_returns_set(targetList)`；测试至少分别覆盖 `enable_expr_fusion=off/on`。

目标分支已有的更严格安全检查应保留，不能为了贴近原提交而用旧函数整体覆盖新函数。原则是“迁移特性逻辑进入目标分支”，而不是“把目标分支对应区域退回原分支版本”。

### 4.11 权限、RLS、来源追踪和内存所有权

列传输裁剪不等于权限裁剪。大模型要确认没有顺手缩小：

- `requiredPerms`；
- `selectedCols`、`insertedCols`、`updatedCols`；
- RLS/security quals；
- 审计、来源追踪或依赖收集需要的元数据。

否则用户可能绕过本来应执行的列权限检查。

同时检查内存所有权：

- 所有新节点和 List 位于正确 planner memory context；
- `copyObject()` 后不会引用寿命更短的临时对象；
- 不会把栈上数组挂入 Query；
- List 修改遵循目标分支宏/API，避免遍历中破坏共享链；
- 失败路径不会释放仍被原树引用的对象；
- 不把仅用于 planner 的映射结构意外放入 Plan、序列化或执行阶段。

### 4.12 迁移 diff 是否纯净

大模型最终必须按“目标分支迁移前基线 → 迁移后”审查，而不是只看工作树状态。应确认：

- diff 中每个文件都能映射到 GUC、planner、Query 改写、Plan 防回扩、测试或文档之一；
- 没有生成文件、构建产物、数据目录和日志；
- 没有覆盖目标分支无关修复；
- 没有大块机械格式化掩盖真实修改；
- 没有为通过回归而无解释地重写大量 golden；
- `git diff --check` 无空白错误；
- 提交只包含本特性相关内容。

## 5. 大模型应执行的审查流程

### 阶段 0：只读盘点，不立即改代码

先执行并记录：

```bash
git -C "$TARGET_REPO" status --short
git -C "$TARGET_REPO" branch --show-current
git -C "$TARGET_REPO" rev-parse HEAD
git -C "$TARGET_REPO" diff --stat "$TARGET_BASE".."$PORT_COMMIT"
git -C "$TARGET_REPO" diff --check "$TARGET_BASE".."$PORT_COMMIT"
```

然后用 `rg` 定位目标分支真实入口：

```bash
rg -n "enable_union_all_required_columns_pushdown" "$TARGET_REPO"
rg -n "preprocess_union_all_(required_columns|diskann_topk|order_key_projection)" "$TARGET_REPO/src"
rg -n "subquery_planner|pull_up_subqueries|flatten_simple_union_all" "$TARGET_REPO/src/gausskernel/optimizer"
rg -n "use_physical_tlist|build_path_tlist" "$TARGET_REPO/src"
rg -n "union_all_required_columns_pruned" "$TARGET_REPO/src"
```

此阶段输出：

- 工作树边界；
- 迁移文件清单；
- 目标分支 planner 时间线；
- API 差异初表；
- 尚未验证的假设。

不要在这一阶段修改代码或 expected 文件。

### 阶段 1：做语义映射表

至少完成下表：

| 原设计职责 | 原实现位置 | 目标分支位置 | 差异 | 迁移是否完整 | 证据 |
|---|---|---|---|---|---|
| GUC 注册和 session 字段 |  |  |  |  | file:line |
| PlannerInfo flag |  |  |  |  | file:line |
| 三阶段调用顺序 |  |  |  |  | file:line |
| WHERE 映射 |  |  |  |  | file:line |
| required 收集 |  |  |  |  | file:line |
| oldToNew 全接口压紧 |  |  |  |  | file:line |
| order key 物化 |  |  |  |  | file:line |
| Top-K 原子提交 |  |  |  |  | file:line |
| physical tlist 防回扩 |  |  |  |  | file:line |
| 正向/负向回归 |  |  |  |  | file:line |

如果目标分支无需某一项，要给出目标分支代码证据和理由，不能留空或写“应该一样”。

### 阶段 2：静态不变量审查

逐函数跟踪一条典型 SQL，至少回答：

1. 原始三列各自被哪个表达式引用；
2. WHERE 如何映射到每个叶子；
3. required bitmap 如何形成；
4. oldToNew 如何生成；
5. 哪些结构使用该映射；
6. Top-K 在哪一步识别原始距离表达式；
7. synthetic distance 在哪一步追加；
8. embedding 在哪一步不再穿过 UNION；
9. planner flag 在哪一步设置并向子 planner 传播；
10. Scan Plan 为什么不会恢复整行 physical tlist；
11. 任一叶失败时从哪个副本回退；
12. GUC off 时哪些入口完全不生效。

每个结论都应给出目标分支 `file:line` 证据。只复述设计文档而没有目标代码证据，不能算审查完成。

### 阶段 3：编译与自动化验证

迁移涉及头文件时必须 full build；不能以增量编译结果作为最终证据。建议顺序：

1. 目标分支 Debug + assertion 全量构建；
2. 启动隔离测试实例；
3. 运行本特性定向 native regression；
4. 运行相关 DiskANN/vector/SetOp/planner 回归；
5. 运行 `EXPLAIN (VERBOSE ON, COSTS OFF)` 计划看护；
6. 运行 GUC on/off 结果等价测试；
7. 运行负向形状，确认安全回退；
8. 如用于多 DN，再运行真实多 DN/Stream 计划与结果验证。

Debug/O0 只能用于功能验证。若要给出性能结论，必须另用目标分支 Release/O2 构建，且不能把 Debug 数字写入性能结论。

### 阶段 4：先解释失败，再修改

遇到 golden 差异时，先分类：

- 目标分支成本模型导致 Hash/Merge、Seq/Index 选择变化；
- `Output` 中出现不该向上传输的列；
- Limit/Sort/Anti Join 的相对位置变化；
- 结果、重复数或顺序语义变化；
- 目标分支 EXPLAIN 文本格式变化；
- 迁移确实遗漏代码。

不得直接复制新输出覆盖 expected 来让测试变绿。只有确认语义和计划目标正确后，才能更新目标分支 golden，并在审查报告中解释差异。

### 阶段 5：修复后闭环

每个修复都要回到对应质量门重跑，最终报告包括：

- 修复前根因；
- 修改文件和关键位置；
- 为什么不会扩大优化适用范围；
- 新增或加强的回归；
- 构建和测试原始结果；
- 未覆盖风险。

## 6. 最低测试矩阵

### 6.1 GUC 二维组合

至少覆盖：

| required-column GUC | DiskANN Top-K GUC | 目的 |
|---|---|---|
| off | off | 原分支行为基线 |
| on | off | 验证普通 WHERE、ORDER-only 生命周期优化 |
| off | on | 验证已有 Top-K 不依赖新特性 |
| on | on | 验证三阶段协作 |

`enable_union_all_required_columns_pushdown=off` 必须保持迁移前计划和结果行为；新 GUC 默认必须为 off。

### 6.2 四个目标 SQL 象限

| WHERE | ORDER BY | LIMIT | 主要看护点 |
|---|---|---|---|
| 有 | 无 | 无 | WHERE 下推，源扫描只输出最终列；过滤列只活到叶子 |
| 有 | 有 | 无 | 距离在叶子计算，UNION 以上只传 row_id + scalar key |
| 有 | 无 | 有 | 只保留全局 Limit，不做无序局部 Top-K |
| 有 | 有 | 有 | 局部 Top-K + 全局 Sort/Limit，反删除在局部 Limit 之下 |

还应加入至少一个 WHERE 使用非最终列的用例，例如最终只返回 `row_id`、WHERE 使用 `title`。这能证明“局部需要”与“向上传输需要”被正确区分。

### 6.3 计划看护要点

完整保存原生：

```sql
EXPLAIN (VERBOSE ON, COSTS OFF)
...
```

不要只截取某个 Scan 节点。典型计划至少看护：

- `delta_t` 扫描不输出未使用的 `title`；
- `snapshot_t` 扫描不输出未使用的 `title`；
- `delete_t` 只输出连接/过滤真正需要的列；
- WHERE 使用的列允许出现在 Filter，但不继续向上 Output；
- vector 只在叶子距离计算处出现；
- Append/UNION 以上传递 row_id 和 scalar distance，而不是 embedding；
- snapshot 的 Anti Join 先排除删除记录，局部 Limit 后截断；
- 全局 Sort 和 Limit 没被删除；
- GUC off 时计划保持目标分支原形态。

不要把特定 join 算法或 scan 方法当成唯一正确答案，除非它本身就是待看护特性的契约。

### 6.4 查询结果正确性

测试数据量可以很小，但必须刻意包含：

- 两个叶子都有行；
- 重复 `row_id`，验证 `UNION ALL` 重复语义；
- delete 表命中和未命中的 snapshot 行；
- WHERE 命中/不命中的 `title`；
- 相同或接近的距离值；
- LIMIT 边界前后的行；
- NULL 或目标类型允许的边界值。

推荐以 GUC off 结果为基准，执行双向多重集差分：

```sql
(optimized_query EXCEPT ALL baseline_query)
UNION ALL
(baseline_query EXCEPT ALL optimized_query);
```

差分必须为空。不能用普通 `EXCEPT`，因为它会掩盖重复次数错误。

有确定 ORDER BY 时，还要比较有序结果的位置和值；仅有 LIMIT、没有 ORDER BY 时，SQL 不承诺固定行身份，除非测试人为提供确定性路径，否则应比较谓词满足性、行数和多重集允许范围，不应把偶然扫描顺序写成语义契约。

建议补充：

- prepared statement 的首次计划和重复执行；
- query vector 为 Param；
- LIMIT 为常量和 Param；
- EXPLAIN ANALYZE，确保执行阶段无崩溃、断言和类型错误。

### 6.5 必须回退的负向用例

至少测试：

- 外层无 WHERE；
- UNION DISTINCT；
- INTERSECT/EXCEPT；
- 叶子已有 LIMIT/OFFSET；
- `SELECT *` 或 whole-row 引用；
- volatile 函数；
- SRF；
- SubLink/相关子查询；
- lateral；
- aggregation/window/distinct；
- security barrier view；
- RLS 表；
- 不同 typmod/collation 的集合列；
- 三层或四层嵌套 UNION ALL；
- 多于两个叶子；
- 目标分支新增的特殊 SetOp 或 Limit 语法。

“回退”不是报错，而是优化不开启、查询仍按目标分支原语义规划执行。对于权限和 RLS 用例，还必须确认不能因裁剪少做权限检查。

### 6.6 兼容性回归

除本特性用例外，至少执行：

- 目标分支原有 UNION/INTERSECT/EXCEPT 回归；
- view/subquery pull-up 回归；
- planner targetlist 和 resjunk 相关回归；
- DiskANN/vector 回归；
- 原有 UNION ALL Top-K 回归；
- 权限/RLS/security barrier 相关回归；
- 若涉及分布式：Stream、RemoteQuery、DN GUC 同步相关回归。

如果目标分支存在独立的 CStore、分区表或向量索引回归，也应加入，因为 physical targetlist 的分支差异容易只在这些路径暴露。

## 7. 多 DN 或分布式分支的附加质量门

原单机验证不能自动证明多 DN 正确。如果目标分支用于分布式环境，大模型要额外关注：

- Coordinator 与 DN 是否都能识别 GUC；
- GUC 是否在 remote session/worker 中同步；
- Stream/RemoteQuery 的 output 列号与压紧后的 SetOp 接口一致；
- 分布键、排序键、hash key 是否因压列号而错位；
- `Var.varno/varattno` 是否在 remote plan 序列化前已完成 setrefs；
- synthetic distance 的类型和 collation 是否可序列化；
- CN 和 DN 的 planned statement 是否使用一致的节点结构版本；
- explain on CN 不仅看起来变窄，DN 实际发送 tuple 也确实不携带死列；
- replicated/distributed 表、数据重分布和多 Stream 组合下结果一致。

如未运行真实多 DN 测试，最终结论必须明确写“单机基本质量通过，多 DN 未验证”，不能写成通用完成。

## 8. 问题分级与提交阻断标准

### 8.1 Blocker：必须修复后才能接受迁移

- GUC on/off 出现结果差异、重复数差异或崩溃；
- 某些叶子已改写、另一些叶子失败后留下半改写树；
- oldToNew 未同步 SetOp 元数据、RTE 列名或外层 Var；
- 错改相关 Var、whole-row/system column；
- 外层 WHERE 被删除但没有成功下推到全部叶子；
- local Limit 位于反删除、RLS 或必要过滤之前；
- 全局 Sort/Limit 被局部 Top-K 代替；
- 跨 security barrier/RLS 非法下推；
- 默认 GUC 为 on，或 GUC off 仍改变原行为；
- 权限位图被裁剪，导致权限语义变化；
- 目标分支已有修复被原分支旧代码覆盖；
- 提交混入无关代码或用户资产。

### 8.2 Major：基本功能可能正确，但迁移质量不能算完整

- Query 层已窄，physical targetlist 又把死列扩回；
- embedding 仍穿过 UNION，未达到源头裁剪目标；
- 普通 heap 生效但 ANN/CStore/partition/remote 路径失效；
- 多 DN 使用场景中 GUC 或列映射未验证传播；
- prepared/generic plan 行为未验证；
- 缺少负向回退或 `EXCEPT ALL` 正确性测试；
- expected 被整体刷新但没有解释计划变化；
- 修改头文件后只做增量构建。

### 8.3 Minor：可记录后修，但不能掩盖重要问题

- 注释、命名或日志风格与目标分支不一致；
- 测试名称不够清晰；
- 非关键 EXPLAIN 节点因成本模型发生变化；
- 文档行号过期但链接和语义仍正确。

Hash Anti Join 与 Merge Anti Join、Seq Scan 与 Index Scan 的选择变化一般不应单独判为 Blocker；除非它改变了字段生命周期、节点相对位置或正确性。

## 9. 最终验收清单

只有下列项目都能给出证据，才建议把迁移评价为“基本质量通过”：

- [ ] 已记录目标仓库、分支、基线、迁移 commit 和 dirty 边界；
- [ ] 已完成原设计职责到目标分支代码的语义映射表；
- [ ] 三阶段入口位于目标分支正确的 planner 生命周期中；
- [ ] 阶段顺序仍是 required columns → Top-K → order-key projection；
- [ ] 所有叶子先准备成功、再原子提交；
- [ ] required 收集覆盖 resjunk 和全部真实消费者；
- [ ] oldToNew 同步全部位置型接口和每层 SetOp 元数据；
- [ ] type、typmod、collation 均逐位置校验；
- [ ] volatile/SRF/SubLink/RLS/security barrier 等安全门未削弱；
- [ ] synthetic key 完整替换外层排序表达式；
- [ ] vector 不再跨 UNION 传输；
- [ ] physical targetlist 不会恢复无关列；
- [ ] local Limit 位于完整叶语义之上，全局 Sort/Limit 保留；
- [ ] GUC 默认 off，两个 GUC 的四种组合已验证；
- [ ] 四个 WHERE/ORDER/LIMIT 目标 SQL 象限均有计划看护；
- [ ] GUC on/off 使用 `EXCEPT ALL` 验证结果等价；
- [ ] 负向 Query 安全回退；
- [ ] 修改头文件后完成 Debug/full build；
- [ ] 本特性及相关原有回归通过；
- [ ] 如面向多 DN，已完成真实分布式验证；否则明确标为未验证；
- [ ] `git diff --check` 通过，提交中无无关文件；
- [ ] 没有 Blocker，所有 Major 已修复或明确不进入本次验收范围。

## 10. 可直接交给大模型的审查提示词

下面的提示词适合在迁移完成后直接使用。先替换尖括号中的信息。

```text
你现在要审查并验证 openGauss 的“UNION ALL 必需列源头裁剪”跨分支迁移。

目标仓库：<TARGET_REPO>
目标分支：<TARGET_BRANCH>
迁移前基线：<TARGET_BASE_COMMIT>
迁移提交或 diff 范围：<PORT_COMMIT_OR_RANGE>
构建命令：<BUILD_COMMAND>
回归命令：<REGRESSION_COMMAND>
测试连接：<HOST_PORT_DB_SCHEMA>
部署形态：<SINGLE_NODE_OR_MULTI_DN>

原实现参考：
- commit 098d77c8b76d56eace4af8fddaf8d94b394b996c
- 详细设计：/home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/设计/UNION_ALL必需列源头裁剪详细设计.md
- 源码解读：/home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/源码解读/UNION_ALL必需列源头裁剪源码解读.md
- 测试报告：/home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/测试/UNION_ALL必需列源头裁剪开发与测试报告.md

请按“先只读审查，确认根因后再修复”的顺序工作。不要假定目标分支的文件名、函数签名、Query/SetOperationStmt 字段和 planner 阶段与原分支相同；先读取目标分支实际定义。不要 reset、clean、覆盖或提交无关 dirty 文件，也不要为了让回归通过直接刷新 expected。

本特性的 SQL 契约是：外层 WHERE 必须存在；ORDER BY 和 LIMIT 分别可选。目标是让非必要列从源扫描开始就停止向上传输；只用于 Filter/Join/距离计算的列允许活到其最后消费者，但不能继续出现在无关上层 Output。

必须重点审查：
1. 三阶段入口是否在 rownum 后、subquery pull-up/UNION flatten 前，顺序是否为 required-column → DiskANN Top-K → scalar order-key projection。
2. 目标分支 AST/API 差异是否已逐项适配，尤其是 Query、SetOperationStmt、RangeTblEntry、PlannerInfo、TargetEntry、Var 和 physical targetlist API。
3. WHERE 是否通过叶子 targetlist 等价映射到所有叶子，并且只有全部成功后才删除外层 quals。
4. 是否使用 copy-on-write/原子提交，任一安全门失败时原 Query 完全不变。
5. required-column 是否包含 targetlist resjunk、剩余 quals 和所有真实消费者，并保留 volatile/SRF/SubLink 等语义项。
6. oldToNew 是否同步外层 Var、所有叶 targetlist、UNION targetlist、所有相关 eref->colnames，以及 SetOp 每一层 colTypes/colTypmods/colCollations。
7. 是否正确区分 1-based attno/resno、0-based List/数组下标，以及 ressortgroupref 与 resno；是否拒绝 whole-row、系统列和非 level-0 Var。
8. ORDER BY 距离表达式是否在每个叶子物化为类型一致的 scalar key，外层整个排序 TLE 是否替换为 key Var，原 embedding 是否不再跨 UNION。
9. Top-K 是否仍能在 scalar projection 前识别原始距离 OpExpr；局部 Limit 是否包住完整叶 Query，位于 anti-delete/RLS/WHERE 之后；全局 Sort/Limit 是否始终保留。
10. PlannerInfo 裁剪标志是否在所有初始化和子 root 中正确传播；目标分支所有 heap/index/ANN/CStore/partition/foreign/remote 路径是否可能通过 physical targetlist 把死列重新扩回来。
11. GUC 是否默认 off、session SET/SHOW 生效、与 DiskANN Top-K GUC 独立；prepared plan、worker/DN 和 plan cache 行为是否符合目标分支规则。
12. 权限位图、RLS/security barrier、来源追踪和内存所有权是否保持正确。

先输出一张“原设计职责 → 目标分支代码位置 → API 差异 → 结论 → file:line 证据”的映射表，再输出按 Blocker/Major/Minor 分级的问题。不要只做文本 diff，也不要因 join/scan 算法名称变化直接判错；应关注结果、Output 字段生命周期、Limit/Sort/Anti Join 相对位置和安全回退。

随后执行：
- 修改头文件后的 Debug/full build；
- GUC 的 on/off 与两个 GUC 的 2×2 组合；
- WHERE 必有且 ORDER BY/LIMIT 各自可选的四个 SQL 象限；
- 完整 EXPLAIN (VERBOSE ON, COSTS OFF) 计划看护；
- 用小而有重复、删除记录、过滤命中/不命中的数据做 GUC on/off 双向 EXCEPT ALL；
- no-WHERE、UNION DISTINCT、leaf LIMIT、volatile/SRF/SubLink、whole-row、RLS/security barrier、类型/typmod/collation 等负向回退；
- 原有 SetOp、DiskANN/vector、planner 和权限相关回归；
- 若部署形态是多 DN，增加 GUC 传播、Stream/RemoteQuery output、分布键/排序键列号和真实结果验证。

只有确认问题根因后才修改代码。每个修复都补测试并重跑相应质量门。最终报告必须包含：环境和 commit、迁移文件清单、语义映射表、分级问题及修复、实际执行的命令与结果、典型计划前后差异、正确性证据、未覆盖风险和是否满足“基本质量通过”。没有真实多 DN 验证时必须明确写明，不能外推。
```

## 11. 建议的大模型最终报告格式

```text
1. 审查范围
   - target repo / branch / base / port commit
   - dirty 文件边界

2. 目标分支适配映射
   - 原职责 / 目标位置 / API 差异 / 结论 / file:line

3. Findings
   - [Blocker|Major|Minor] 标题
   - 触发条件
   - 代码证据
   - 影响
   - 修复
   - 覆盖测试

4. 构建与测试
   - 完整命令
   - PASS/FAIL 与原始摘要
   - 四个 SQL 象限
   - GUC 2×2
   - 正确性与负向回退

5. 代表性计划验证
   - 每个源扫描 Output
   - Filter/Join 最后消费者
   - vector 与 scalar key 生命周期
   - local/global Limit 相对位置

6. 残余风险
   - 未测平台、执行路径和部署形态

7. 最终判断
   - 基本质量通过 / 不通过
   - 若不通过，列出全部阻断项
```

## 12. 给迁移执行者的最短版提醒

如果只能让大模型优先检查少数事项，顺序应是：

1. planner 入口和三阶段顺序；
2. copy-on-write 与所有叶子的原子回退；
3. oldToNew 是否更新全部位置型接口；
4. `resno`、`ressortgroupref`、`varattno` 是否混淆；
5. type/typmod/collation 是否一致；
6. physical targetlist 是否把死列加回来；
7. local Limit 是否错误地早于 anti-delete/WHERE；
8. global Sort/Limit 是否保留；
9. GUC off 是否完全保持原行为；
10. 完整计划看护 + `EXCEPT ALL` 结果等价 + 负向安全回退。

这十项里，前八项决定实现是否安全，后两项决定是否有足够证据接受迁移。
