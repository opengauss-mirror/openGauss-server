# GV/KGN 距离槽位被 prune 清空：修复设计与验证指南

## 1. 交给目标仓 AI 的任务说明

本次只处理一个问题：**已有的 KGN ORDER BY 距离输出，仍被上层使用或后续距离引用注入需要，却被 `prune_index_orderby_resjunk_tlist()` 提前清空。**

请在目标仓定位用户报告的 `adjust_plantree.cpp` 中该函数及全部调用点，先确认真实行为，再按本文件增加槽位保护。目标行号约为 733～776，仅供搜索；不能把它当作固定位置。

本地没有 GV 仓库，也没有该函数原文。本文给出的策略辅助函数是完整参考实现；接入位置、目标类型和 AM 识别须由目标 AI 根据真实源码适配。**本文不是声称已经在 GV 编译通过的补丁，也不伪造目标函数的完整替换实现。**

业务前提已经明确：KGN 的 L2 可以使用近似距离，Cosine 也可以近似；目标已有距离转换公式，应遵守目标公式。验收不要求它们等于原始向量的精确 SQL 距离。近似误差不能用来解释意外 NULL、错列、错行身份或仍然重复计算。

修复边界：

- 保留仍被使用、待复用或尚不能证明无用的距离输出。
- 保持输出槽位编号和表达式语义，不重新初始化数据库，不重建业务索引。
- 本修改不负责补出原本不存在的距离 TLE，也不自动适配 `NestLoopVLE / AdaptiveJoin` 的近似透传检查。
- 不取消 `approximatePlanSafe`，不把近似 L2 标成精确 L2，不把 Result 中的距离 OpExpr 直接当作已经复用的槽位。

## 2. 先确认是否真的命中这个缺陷

必须在**同一条 SQL、同一棵计划、同一个扫描节点**上获得以下记录：

1. prune 前存在完整的距离 TargetEntry。
2. 原 prune 条件实际命中该 TLE；记录 `resno / resjunk / ressortgroupref / expr nodeTag`。
3. prune 后该表达式变成 NULL 常量、空指针或被移除。
4. 至少存在一个真实消费者，或者该输出是后续 AM 距离引用的候选。

不能只看到函数被调用，就断言它清理了目标距离；也不能只看到 EXPLAIN 单层括号，就断言距离 TLE 被删。

特别注意：**客户端不显示距离，不等于每一层计划的该 TLE 都是 `resjunk=true`。** UNION 的 `__ua_order_N` 可以是分支接口的可见标量列，到了最外层才成为客户端不可见的排序项。如果实际扫描 TLE 是 `resjunk=false`，而旧 prune 只处理 `resjunk=true`，那这条规则可能根本没有命中它。

### 2.1 查清“置 NULL”的具体含义

| 目标实现动作 | 需要确认的影响 |
|---|---|
| `tle->expr` 替换为一个 NULL `Const` | 结构上仍有表达式，但该槽位返回 SQL NULL；活跃排序或上层引用可能读到 NULL |
| `tle->expr = NULL` | 表达式指针为空，不等同于 SQL NULL；通常会破坏后续表达式处理的前提，须检查目标是否有明确的专用约定 |
| 从 targetlist 删除 TLE，或把 ListCell 置空 | 还可能改变输出位置、列表形状或使槽位索引失效 |
| 替换为另一种 AM 内部引用节点 | 不一定是缺陷；须证明它确实提供同一行、同一距离尺度的值，且上层引用和后续注入支持这种表示 |

**不能等表达式清空之后再从 `indexorderby` 猜着恢复。**旧表达式的参数、引用域、类型或绑定阶段可能已经不同，修复应挡在破坏动作之前。

## 3. 为什么 `resjunk` 和 `ressortgroupref` 不能证明无用

`resjunk` 表示该输出不作为普通结果列交给客户端，不表示执行器内部无人读取它。

以下都是消费者：

- 父节点投影中的槽位 Var，包括 Result 对距离列的引用。
- Sort、MergeAppend 的 `sortColIdx`，即使最终 SELECT 只输出 id。
- Join 条件、输出过滤、参数或其他表达式对该槽位的使用。
- Append/MergeAppend 统一输出接口对每个分支相应位置的要求。
- 已生成的距离引用，或后续准备注入的 AM 距离输出。

`ressortgroupref` 是查询/目标列表中的关联标识，不是全计划通用的槽位编号，也不是“可以删除”的标记。它为 0 的列也可能被 Var 或父节点排序索引读取。

必须保持的条件是：

> 一个槽位只要仍有消费者，就必须继续提供该槽位约定的值；只有完整证明它没有消费者、没有后续注入需求、也没有共享别名影响，才允许按原规则清理。

### 3.1 与当前典型计划的关系

以下仅用于解释槽位关系，不是声称目标已执行出的新计划：

```text
Limit
  Result
    MergeAppend                         需要各分支的距离排序槽位
      Limit → Sort → delta ForeignScan
      Limit
        Anti Join                       输出 snapshot 的标量距离
          IndexScan(KGN)                第 k 个槽位产生 distance
          delete 扫描                   Join 只使用 row_id
```

如果 KGN 扫描第 k 项提供的距离被清成 NULL，上层仍引用第 k 项，链条就不再提供原有距离值。允许 L2 近似，并不意味着允许这个值消失。

## 4. 推荐实施方式：先保守保留，再考虑精确裁剪

### 4.1 首版最小修复

**在旧 prune 已经认定“准备清理的 KGN 排序距离 TLE”之后、真正修改表达式之前，增加保留判断。**

保留条件满足任一项即可：

1. 已有真实消费者。
2. 已经是 `AnnOrderByRef` 或目标对应的有效距离引用节点。
3. 后续 AM 距离复用可能需要这个输出。
4. 无法可靠证明没有消费者。

如果该函数现在没有父节点上下文、完整槽位使用信息，**首版将这些 KGN 距离候选的使用情况视为 UNKNOWN，保守保留即可。**不要为了这个修复临时拼一个只遍历部分节点的“无消费者分析”。

这一版的效果是：减少对 KGN 隐藏距离输出的清理。它不会主动生成新距离表达式，也不会使无索引路径变成 KGN 扫描。可能保留原本确实无用的隐藏输出，增加少量投影或传输成本；复用关闭时也可能保留 SQL 距离计算，必须实测并如实记录。

非 KGN、非本次距离输出的旧清理规则继续走原有逻辑。

### 4.2 完整参考辅助函数

下面的枚举和函数是完整定义，函数不依赖 GV 的节点结构，可以先独立核对策略。输入事实由目标代码提供；不是用几个布尔值代替事实验证。

```cpp
enum AnnOrderByOutputUsage {
    ANN_OUTPUT_LIVE,
    ANN_OUTPUT_PROVEN_DEAD,
    ANN_OUTPUT_UNKNOWN
};

static bool should_keep_kgn_orderby_output(
    bool isKgnDistanceOutput,
    bool isAlreadyAnnOrderByRef,
    bool isResjunk,
    AnnOrderByOutputUsage usage,
    bool laterReuseMayNeedOutput)
{
    // 已有距离引用不能被普通“无用排序表达式清理”破坏。
    // 目标如果使用另一种等价内部节点，应在调用处正确识别。
    if (isAlreadyAnnOrderByRef)
        return true;

    // 非本次目标：不施加本次新增保护，仍交回原有规则判断。
    if (!isKgnDistanceOutput)
        return false;

    // 可见输出不能因为它长得像排序距离就清理。
    if (!isResjunk)
        return true;

    // 注入在后面发生时，必须先留下待匹配的完整距离表达式。
    if (laterReuseMayNeedOutput)
        return true;

    // LIVE 保留；UNKNOWN 也保留。
    // 只有完整证明无消费者，才允许继续原有清理逻辑。
    return usage != ANN_OUTPUT_PROVEN_DEAD;
}
```

返回值约定：

- `true`：保留当前 TLE，跳过这一次清理动作，继续处理其他项。
- `false`：本保护函数没有要求保留；**仍须满足原 prune 的全部条件**，不能凭这个 false 就清理任何 TLE。

不要把“跳过清理当前项”实现成错误的整函数 return；如果函数还需处理其它项，应按原循环结构执行对应的 continue 或等价控制流。

### 4.3 五个参数如何取得

| 参数 | 目标实现要求 | 首版可采用的保守值 |
|---|---|---|
| `isKgnDistanceOutput` | 先确认实际索引 AM 是 KGN，再确认当前 TLE 是原 prune 正在处理的对应排序距离；使用目标真实算子/类型及原表达式匹配逻辑 | 已由旧逻辑识别为 KGN 距离候选时为 true；不能按表达式文本包含 `<->` 判断 |
| `isAlreadyAnnOrderByRef` | 检查真实节点 tag，或目标已经支持的等价距离引用节点 | 真实识别；不能用“双括号”代替 |
| `isResjunk` | 当前节点当前 TLE 的字段 | 直接读取，不从用户 SELECT 是否显示距离推断 |
| `usage` | 当前计划版本、当前引用域下的槽位使用结论 | 没有完整分析时一律 UNKNOWN；发现消费者可标 LIVE |
| `laterReuseMayNeedOutput` | 后续复用流程可能匹配并输出这个距离；使用目标 GUC/算子配置，允许保守预留 | 对启用距离复用的 KGN 候选可保守设 true；不能仅因当前 approximatePlanSafe 为 false 就认定永远不需要 |

`laterReuseMayNeedOutput=false` 不是允许删除的充分条件；上层排序仍可能需要普通 SQL 距离。当前已知的 `approximatePlanSafe=false` 也不是无消费者证明，拒绝 AM 复用后执行器仍可能需要计算并传递普通距离。

`isKgnDistanceOutput=false` 应表示已经确认不属于保护范围，不能表示“识别失败所以当作不是”。若已确认是 KGN 扫描，但当前改写阶段使距离候选映射不确定，应保守保留对应候选并记录 UNKNOWN。

比较表达式时核对引用域：目标 `indexorderby` 可能已经使用索引列编号，而 `indexorderbyorig` 或等价字段保留原始表表达式。不能把不同域的 Var 直接作结构比较，也不能退化成“操作符相同就算同一个距离”。优先在旧清理已经识别出的候选上增加保护，不另造一套更宽松的距离匹配。

### 4.4 在原函数中具体改哪里

目标 AI 应按实际原函数完成以下编辑，而不是替换整套 setrefs：

1. 找到旧逻辑最终决定清理当前 TLE 的分支。
2. 在任何 `expr` 替换、ListCell 修改或 targetlist 删除之前取得当前候选和保护事实。
3. 调用上述辅助函数；返回 true 时保留当前项，记录保护原因并跳过原清理动作。
4. 返回 false 时才继续原逻辑；不得改变旧逻辑的其它匹配条件。
5. 对已经是距离引用节点的项，保证所有会触及它的清理入口均有保护，不能只有一个调用点保护而另一个调用点继续清空。
6. 保留现有错误处理。若进入函数时 `tle` 或 `tle->expr` 已异常为空，应记录此前哪个阶段破坏了它，不要把空指针当成新的“已清理成功”状态。

首版无需改 `create_scan_plan()`、Query 重写或 Join 安全门；但若 prune 前就没有距离输出，则该用例不属于本补丁能单独解决的范围。

### 4.5 不应改动的字段

保护时保留原对象内容，不要为规避旧条件而篡改：

```text
tle->resno
tle->resjunk
tle->ressortgroupref
expr 的类型、typmod、collation
targetlist 顺序及长度
父节点 sortColIdx
上层 Var 的输入侧和槽位编号
```

尤其不能把 `resjunk` 强制改成 false，这可能使隐藏排序列进入客户端输出或改变 UNION 接口。也不能把 `ressortgroupref` 改成 0 来躲避 prune；这样可能破坏排序项关联。

## 5. 真正的消费者分析：怎样才能标 PROVEN_DEAD

首版只需保留 UNKNOWN。若以后要恢复对确实无用距离 TLE 的精确清理，再实现这一节的分析，并单独验证。

### 5.1 槽位标识与版本

清理期间使用当前计划对象与其输出槽位标识，例如 `(当前 Plan*, resno)`；不要把 `resno=6` 当成整棵树的同一列，也不要把某个 `ressortgroupref` 当成全局输出位置。

发生 copyObject、SubqueryScan 消除、计划替换、targetlist 重建或 resno 重排后，旧映射可能失效。必须更新/重建分析，不能跨改写阶段复用旧指针集合；不能确认时退回 UNKNOWN。

### 5.2 至少覆盖的消费者

| 消费来源 | 必须识别的依赖 |
|---|---|
| 最终客户端输出 | 根输出中的可见列，以及生成它们所需的下层槽位；重复 `id,id,id` 不能折成少返回的列 |
| Result 投影/过滤 | targetlist、qual、resconstantqual 中实际读取的输入；真正的 OpExpr 计算不能被当成简单透传 |
| Sort | `sortColIdx` 对其直接输入元组的引用，即使该列不再向客户端显示 |
| MergeAppend | 排序键位置及各子计划对应输出位置，所有分支都要映射 |
| Append | 上层需要的输出位置向各子分支传播，不能只看第一个分支 |
| Limit、Material、Sort 等透传节点 | 将上层对其输出位置的需求映射到实际输入槽位，同时加上本节点自身依赖 |
| Join | targetlist、plan.qual、joinqual、nulleqqual、merge/hash keys、NestLoop 参数，以及目标新增字段 |
| SubqueryScan / ForeignScan | 使用目标真实输出映射和扫描 targetlist，不能假定所有引用都是 OUTER_VAR |
| NestLoopVLE / AdaptiveJoin | 检查目标所有有效执行策略、额外子计划、过滤、参数和输出映射；不认识的路径必须 UNKNOWN |
| SubPlan / InitPlan / Param | 按目标实际传参和执行关系处理；不能只扫普通 lefttree/righttree |
| 后续 AM 注入 | 预留待匹配的完整扫描距离输出，不能只看当前是否已有 AnnOrderByRef |

“没有在父 targetlist 找到 Var”不足以证明 dead：Sort/MergeAppend 的比较键通常记录在专用数组中。

### 5.3 setrefs 前后不能混用引用域

- setrefs 前通常是关系/查询层的 Var，不能简单拿 `varattno` 当成子计划输出 resno。
- setrefs 后，`OUTER_VAR / INNER_VAR` 等表示特定输入侧的槽位；仍须按具体节点映射，不能跨层直接查找同编号。
- 若 prune 在 setrefs 处理中间运行，树可能处于不同局部阶段。此时全树简单扫描很容易得到错误的“无人使用”结论。

首版不依赖完整分析，正是为了避免在混合引用域中误删。后续若做精确分析，宜在引用表示和计划对象稳定的阶段收集需求，再以可靠映射提供给清理逻辑；无法保证映射稳定就继续保留。

### 5.4 共享 targetlist 和 TLE 别名

源仓 Sort、Limit 等节点可能共享下层 targetlist；GV 也应实际检查地址。

清理一个 TLE 可能通过共享指针同时修改另一个节点。仅证明扫描局部无人使用，不足以证明共享该 TLE 的所有位置都无用。

- 首版保守保留 KGN 候选时，无需为此修改或复制目标列表。
- 精确清理时，必须证明所有共享使用方均允许清理，或者先按照目标框架安全地解除共享，并同步所有必需的映射。
- `copy-on-write` 只能解决别名传播，不能把有消费者的槽位变成无用槽位；复制一份再把它清成 NULL，仍可能让父节点读错。

只有「消费者分析完整、映射属于当前计划版本、别名影响已确认、没有后续注入需求」都成立，才允许标记 `ANN_OUTPUT_PROVEN_DEAD`。

## 6. GUC 行为与兼容性

| 状态 | 应采取的动作 |
|---|---|
| 复用 off，上层仍读取距离 | 保留；按普通 SQL 表达式或已有合法机制提供值 |
| 复用 on，后续可能注入 | 保留完整候选，供注入匹配 |
| 已是有效距离引用节点 | 保留，不由普通清理逻辑清空 |
| 使用关系 UNKNOWN | 保留 |
| 已完整证明无消费者且无需后续注入 | 满足旧规则后，才可执行原清理 |
| 非 KGN、非目标距离候选 | 沿用原规则，本补丁不扩大清理范围 |

**槽位正确性保护不能只挂在 `ann_orderby_distance_reuse != off` 上。**如果旧实现关闭复用时也会清掉活跃距离，正确性修复仍应覆盖它。

因此，本补丁不能预先承诺“复用 off 时计划和成本绝对不变”。首版保守保留 UNKNOWN 时也可能多保留无用计算。若必须严格保持 off 的旧行为，需要先证明原 off 路径清掉的项确实无消费者或有其它合法生产者，不能通过 GUC 条件掩盖活跃槽位破坏。

列裁剪开关及 Top-K 下推开关保持各自职责。这个保护判断不应偷偷打开列裁剪或 ORDER BY/LIMIT 下推。

## 7. 与其他阻断点的边界

本补丁有可能一并修复 Result 的引用建立失败：如果原来是槽位先被清掉，setrefs 因而无法匹配完整距离，保留槽位后引用链可能恢复。

但是它不能自动解决：

- 距离从未进入扫描 targetlist。
- 扩展 Join 类型不被源投影下推 walker 识别。
- `NestLoopVLE / AdaptiveJoin` 的近似消费者检查仍拒绝。
- 注入调用早于 setrefs、使用旧计划指针，或 GV 在 setrefs 后重建了表达式。
- AM、操作符、类型、typmod、支持函数或质量模式不匹配。
- 节点已经注入，但 EXPLAIN 未适配普通 IndexScan。

修后应分别报告「活跃槽位保护是否成功」和「距离复用链是否成功」。后者尚未成功时，继续记录第一次安全上下文拒绝点，不应因此撤销已经正确的槽位保护。

## 8. 诊断日志与断点要求

记录同一次执行的以下阶段，实际相对顺序以目标调用栈为准，不要假定 prune 必然在 setrefs 之后：

```text
创建 Scan targetlist 后
源距离投影下推前、后
prune 入口与候选清理动作前、后
setrefs 前、后
距离注入前、后
执行器读取该行距离时
```

建议每条记录包含：

```text
query/run 标识、阶段
Plan 地址、nodeTag、indexid、AM 标识
targetlist 地址、TLE 地址、resno、resjunk、ressortgroupref
expr 地址、nodeTag、结果类型、typmod、collation
距离表达式摘要或结构哈希（避免重复打印 1024 维长常量）
消费者：所属父节点、输入侧、sortColIdx 或 Var(varno,varattno)
usage = LIVE / PROVEN_DEAD / UNKNOWN
已有距离引用、后续复用预留、最终 prune 决策与原因
approximatePlanSafe、quality、node_equal 匹配结果、是否注入
```

需要观察的不是“函数跑了几次”，而是：**哪一个 TLE 在哪一步发生了什么变化，以及谁还在读它。**

预期保护轨迹：

```text
prune 前：resno=k，expr=OpExpr，旧条件命中
保护判断：LIVE / UNKNOWN / reserved-for-reuse
prune 后：resno=k，expr 及元数据保持
setrefs 后：实际父消费者引用正确的距离槽位
注入后：该扫描 TLE.expr=AnnOrderByRef（其它条件通过时）
执行：读取本行 AM 发布距离；不是仅改变 EXPLAIN 括号
```

## 9. 测试输入：保持用户真实 SQL 形状

优先复用已有测试视图和 KGN 索引，不编造 KGN CREATE INDEX 语法，不重建业务对象。

下面是目标 SQL 模板，`<QUERY_VECTOR_1024>` 必须替换成实际合法的 1024 维向量字面量；不要把占位符直接提交数据库。Cosine 使用非零向量。

```sql
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id, id, id,
       "num_1_2B", "num_2_2B", "num_3_2B", "num_4_2B"
FROM d_lake_zhongyan.zhongyan_10yi
ORDER BY embedding <-> '<QUERY_VECTOR_1024>'::floatvector(1024)
LIMIT 100;
```

视图结构必须保留：delta 分支 UNION ALL snapshot 分支，snapshot 上带 NOT EXISTS(delete)，KGN 位于 snapshot；生产形态是 ForeignScan 时，也应覆盖该分支及扩展 Anti Join。

**不要为了观察距离而把距离加进 SELECT 后，只测试改写后的 SQL。**那可能改变 `resjunk` 和投影位置，使旧 prune 根本不命中。原 SQL 的隐藏距离用日志/断点观察。显式投影距离的 SQL 只能作为补充对照。

执行查询结果检查时，去掉 EXPLAIN，仍保持原 SELECT 列表。不能通过再套一层聚合、CTE 或排序验证 SQL 来替代原查询执行，因为可能改变规划形态；应在客户端收集结果后检查。

### 9.1 必测矩阵

| 场景 | 核心断言 |
|---|---|
| 原始无 WHERE、只 ORDER BY 隐藏距离、LIMIT 100 | 旧 prune 原本确实命中；修后活跃距离 TLE 保留，原槽位有效 |
| LIMIT 10，实际候选数大于 10 | 覆盖真正的 Top-K 截断及分支合并，不能只用小数据 LIMIT 100 的全集结果代替 |
| 原始查询的三个重复 id | 客户端实际返回 7 列，三个 id 值一致；隐藏距离没有多返回成第 8 列 |
| 增加外层 WHERE | 槽位保护原则不变；WHERE 是否下推单独记录 |
| L2 / Cosine | 两者按 KGN 近似契约测试，目标操作符分别为 `<->` / `<+>` |
| 原始 Nested Loop Anti Join 与扩展 VLE/AdaptiveJoin | 区分槽位保护成功和节点透传门控通过，记录首次 true→false 的位置 |
| Sort→IndexScan 等共享 targetlist 形态 | 记录共享地址；不能通过别名清掉活跃上层输出 |
| 已注入 AnnOrderByRef 后又进入清理 | 引用节点和其槽位仍保留 |
| 两个不同向量或不同距离表达式 | 不可因操作符相同就保护/替换错 TLE；保持表达式与排序键对应 |
| 距离用于过滤、Join 匹配或 DISTINCT 的负向用例 | 活跃槽位仍保留；既有近似消费者限制没有因本补丁被绕过 |
| 复用 off/on × 列裁剪 off/on，Top-K 固定 on | 四组合验证适用路径上的槽位正确性；没有 KGN 路径不能算覆盖了 KGN 注入 |
| 参数化计划、预编译及 rescan | 验证规划/缓存实际走到被修复的路径；执行时值属于当前参数和当前行 |
| 非 KGN 索引路径 | 本次保护没有改变其旧清理规则 |
| 真正无用距离输出 | 首版 UNKNOWN 保留是预期；精确版只有证明 dead 后才验证继续清理 |

若 SQL 没有生成目标清理候选，应标记为“未覆盖”，不能因查询执行成功就算该测试通过。精确版无法用 SQL 稳定生成 dead 候选时，可在目标原有单测框架中构造完整计划及消费者，验证 LIVE、UNKNOWN、PROVEN_DEAD 三类决策。

表中的复用“on”表示选择目标实际支持的开启枚举，例如 `l2 / cosine / all`；不要照抄成 `SET ann_orderby_distance_reuse = on`。L2 用例至少覆盖 off/l2，Cosine 至少覆盖 off/cosine，并补 all 的组合；目标若名称不同，以其真实 GUC 定义为准。

### 9.2 结果正确性与近似距离验收

必须确认：

1. 活跃距离没有因为清理变成 NULL 或无效表达式；上层读取的列没有错位。
2. 删除表命中的 snapshot 行不出现，payload 与 id 对应，重复输出列与列数正确。
3. 注入成功时，执行器实际读取本行 AM 发布的距离，使用目标已约定的转换公式。
4. 排序验证使用实际参与排序的隐藏键；不要另算精确距离后要求输出按精确值严格排序。
5. 预期可复用的扫描距离没有再次执行原距离函数；单独统计该扫描的 AM 读取、回退和额外上层重算。delta 非 AM 分支继续计算距离是正常现象。
6. 若分支通过 MergeAppend 合并，验证参与合并的分支按它实际提供的排序键有序；本补丁不能制造缺失键或 NULL 键。

不要求近似 L2 与精确 L2 数值相等。开关前后若计划或 ANN 候选发生变化，也不能仅凭最终 id 不完全相同就认定本补丁错误；需要固定或记录候选集合，再检查该集合上的距离传递、删除过滤和排序。

小数据测试可使用 LIMIT 大于已确认完整候选集的数量，验证行集与输出列；但“LIMIT 大于表行数”本身不保证 ANN 已返回完整候选，仍须观察实际召回范围。

## 10. 完成标准与交付清单

目标 AI 应交付：

1. 原函数及调用点的实际源码差异，说明 guard 插入在哪个清理动作之前。
2. 修复前确定命中的轨迹：存在距离 TLE → 被 prune 改写 → 实际消费者仍需它。
3. 修复后同一结构的轨迹：保护生效、槽位/元数据不变、消费者引用有效。
4. 原 SQL 的完整 EXPLAIN，保持距离只在 ORDER BY；另附实际结果检查。
5. 距离引用注入与执行取值证据；若仍被其它门控拒绝，明确列出剩余断点，不能宣称整体复用已修好。
6. 使用目标既有构建和回归框架完成编译与上述适用测试；记录源版本、运行二进制、GUC 和测试范围。
7. 清楚说明首版是否保守保留 UNKNOWN，以及 off 模式是否增加了投影计算；不得声称所有计划零变化。

本补丁验收通过的最低条件是活跃槽位不再被错误清理。整个业务问题验收还要求预期 KGN 路径完成真实 AM 距离复用，两个结论应分别给证据。

## 11. 相关源码与既有文档

- [关键函数源码比对档案](GV距离复用与列裁剪排查_关键函数源码比对档案.md)：完整函数及调用顺序，包括 setrefs、共享 targetlist、注入和取值路径。
- [KGN 距离复用 Result 问题的根因与验证记录](KGN距离复用Result透传问题_根因分析与修复验证.md)：源仓已完成的测试以及 GV 尚需确认的差异。

本地源仓快照基线：`openGauss-server / integration/multimodal-stacked / ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4`，另含此前已测试的无 WHERE 未提交增强。目标 prune 函数不存在于该源仓，本文没有把源仓测试冒充为目标补丁的编译或运行结果。
