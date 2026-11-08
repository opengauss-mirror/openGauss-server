# UNION ALL LIMIT 下推与列裁剪协同设计说明

核对日期：2026-09-12。依据是 `openGauss-server` 的 `integration/multimodal-stacked` 当前源码：HEAD `098d77c8b76d` 加尚未提交的六类安全修复。本次整理没有修改内核、启动服务或重新执行数据库测试。资料总入口见 [README](README.md)。

## 1. 两个特性分别解决什么

| 特性 | 解决的问题 | 当前目标形状 | 不代表什么 |
|---|---|---|---|
| UNION ALL DiskANN Top-K 下推 | 在合并前限制各分支的候选规模，向 CBO 暴露分支有序路径机会 | 外层单一纯 UNION ALL 数据源；满足安全检查的 `ORDER BY + LIMIT`；排序中包含经 Catalog 证明的 DiskANN ordering operator；WHERE 可有可无 | 不是任意 LIMIT 下推，也不保证每个叶子都选择 ANN 索引 |
| UNION ALL 必需列源头裁剪 | 无用字段不再跨扫描、连接、集合和排序接口逐层传输 | 外层 WHERE 必须存在；ORDER BY、LIMIT 各自可选；纯 UNION ALL 及全部叶子通过安全检查 | 不是减少数据页物理读取的保证，也不要求最终 SELECT 列数小于视图列数才能触发 |

典型目标：

```sql
SELECT row_id
FROM gs_1w_1bucket
WHERE row_id > 2
-- 以下两项各自可选；向量类型和运算符以目标分支 Catalog 为准。
ORDER BY embedding <-> '[0.1,0.2,0.3,12.8]'
LIMIT 10;
```

视图通常是 `delta_t UNION ALL (snapshot_t anti join delete_t)`。只查询 `row_id` 并不表示扫描可以无条件丢弃 `embedding`、连接键或过滤列：它们必须保留到各自的最后消费者。`title` 无任何消费者时才应从源扫描输出消失。

## 2. 两个独立开关

两者均为会话级可设置的 GUC，当前默认 `off`：

```sql
SET enable_union_all_required_columns_pushdown = on;
SET enable_union_all_diskann_topk_pushdown = off;
```

下表只描述两个特性主动执行的改写；任何一个 `on` 都仍须通过安全门：

| 列裁剪 | Top-K | 改写行为 |
|---|---|---|
| off | off | 不执行这两个特性的改写；保留当前版本的其他原生优化 |
| off | on | 允许分支 WHERE、ORDER BY、LIMIT 改写；Top-K 入口自身不删除外层 WHERE，不进行本特性的列接口压紧 |
| on | off | 允许列压紧、安全 WHERE 下移并删除精确外层重检；允许排序表达式成为叶子输出；不由本特性新增分支 ORDER BY 或 LIMIT |
| on | on | 列压紧与 WHERE → Top-K 判断及改写 → 排序键输出改写 |

尤其要区分“距离表达式出现在分支 Output”和“分支有 ORDER BY + LIMIT”：前者由排序键输出阶段产生，后者必须由 Top-K 入口通过自己的 GUC 和安全门后产生。只开列裁剪不打开 Top-K，仍可能变化为 `Merge Append` 或使用某种索引路径；不能仅凭物理计划中存在排序/有序路径，推断 Top-K GUC 失效。

开关关闭只关闭相应改写，不撤销已经迁入同一版本的公共安全修复，也不是切换回迁移前的旧二进制。

## 3. 入口位置和不可颠倒的三阶段顺序

入口位于优化器 `subquery_planner()` 的 Query 预处理阶段，不是执行器，也不是修改 SQL 文本的 parser hook。它位于现有 `reduce_orderby`、单机 `preprocess_rownum()` 处理之后，`pull_up_subqueries()` 和后续 UNION ALL 展平之前。

当前实际编排可概括为：

```cpp
// 本 Query 的成功状态，不能从父 PlannerInfo 继承。
bool unionAllColumnsRewritten = preprocess_union_all_required_columns(root);

// 即使第一阶段没有执行/失败，仍按自己的开关与条件判断 Top-K。
preprocess_union_all_diskann_topk(root);

if (unionAllColumnsRewritten)
    preprocess_union_all_order_key_projection(root);

// 此后才做普通子查询上拉及 UNION ALL 展平。
```

原因有三点：

1. 第一阶段必须在旧列号仍有效时先映射 WHERE，再重编号。否则删除 `title` 后，旧 `embedding` 列号可能指错位置。
2. 第二阶段需要识别真实距离操作符。若先把外层排序表达式改为普通 `Var`，DiskANN Top-K 识别会失去原表达式。
3. 第三阶段仅改变排序键的输出位置和列生命周期；它不承担增加局部排序或截断行数的职责。

三个阶段分别保证自身提交的完整性，不是一个“后续任一阶段失败就回滚全部前序阶段”的大事务。第一阶段成功而 Top-K 不适用时，列裁剪仍然有效。

## 4. 第一阶段：必需列从源头压紧

`preprocess_union_all_required_columns()` 的实现要点：

1. 检查外层 SELECT、必需 WHERE、单一 UNION 子查询及语义安全边界。
2. 对完整外层 Query 做 `copyObject()`，后续准备在副本上进行。
3. 收集纯 UNION ALL 集合树的所有叶子 RTE，验证容器、全部叶子、位置型输出及类型信息。`leaves` 不是叶子内部所有子查询，`NOT EXISTS` 的 `delete_t` 不单独算一个 UNION 叶子。
4. 按原列号把可安全下推的完整外层 WHERE 映射到每个叶子，与已有 quals 做 AND。全部成功才删除外层的精确重检；做不到则保留外层 WHERE 及所需列。
5. 收集外层 targetlist、残留条件等实际引用，保留叶子中不能随意删除的语义输出。空列集场景保留一个载体列，避免旧规划路径的零列 tuple 假设问题。
6. 构造连续新列号，同步叶子/UNION targetlist、别名列名、所有 SetOperationStmt 的 `colTypes/colTypmods/colCollations`、外层 Var。`resno`、`varattno`、`varlevelsup`、`resjunk` 和 `ressortgroupref` 不能混用。
7. 全部校验通过才安装副本的 `targetList/jointree/rtable`，设置扫描策略标志并返回 `true`。

单阶段内不允许一部分 UNION 叶子安装新接口、另一部分保留旧接口。不同叶子内部本来就可能需要不同的连接/过滤输入，这种物理扫描差异不等于接口改写“只成功一半”。

### 内外列数本来相同会怎样

当前没有 `newNcols == oldNcols` 的提前返回。在满足入口条件时仍会映射 WHERE、压紧/校验接口、安装副本并设置标志。即使原始列数没有减少，删除外层重检、排序键输出和禁止物理 targetlist 回扩，也可能导致计划变化。

`SELECT *` 通常已展开为普通列 Var，不等同于 whole-row Var；不能仅因选择全部列就认为入口必然跳过。`root->union_all_required_columns_pruned=true` 也不能解释成“本 Query 一定删除了一列”。

## 5. 第二阶段：分支 ORDER BY + LIMIT

`preprocess_union_all_diskann_topk()` 首先检查自己的 GUC，然后检查外层、UNION 容器及全部叶子。

- ORDER BY 必须存在，并至少有一个排序表达式通过 `is_diskann_vector_order_expr()` 的 Catalog 校验；不是按 `<->` 字符串或某个历史 vector OID 判断。
- LIMIT 必须符合当前 `diskann_topk_outer_query_is_safe()` 接受的形式。OFFSET、WITH TIES、PERCENT 等不能机械复制成普通分支 LIMIT；参数表达式等细节需按目标分支实现核验。
- 完整排序列表逐项映射，保留比较操作符、方向、NULL 次序和 sortgroupref；不要只复制第一项距离键。能否使用 ANN 有序路径是后续路径选择问题，不是仅凭逻辑改写成功即可保证。
- 外层 WHERE 若仍存在，必须能安全映射到全部叶子；局部 LIMIT 在叶子原有 WHERE、JOIN、NOT EXISTS 之后，不能先从 snapshot 取 K 行再删掉已删除记录。
- 所有叶子副本准备成功才统一提交。外层排序和 LIMIT 的语义保留；Top-K 函数自身不主动删除外层 WHERE。

对于无 OFFSET 的普通 Top-K，在相同有效排序语义下，每个分支取最多 K 个候选，再做全局 Top-K 即可。排序键并列时，未指定 tie-break 的结果行身份可能不唯一；`WITH TIES` 要返回所有边界同分行，不能套用普通局部 LIMIT K。ANN 路径本身的近似召回契约也必须单独验证。

## 6. 第三阶段：排序键输出，不是再次下推 LIMIT

`preprocess_union_all_order_key_projection()` 只有在本 Query 第一阶段成功、列裁剪开关开启、存在排序列表且没有残留外层 quals 等条件下才继续。

它将适合的非 Var 排序表达式映射到各叶子，增加真实计算表达式的输出列，再把外层对应表达式替换为引用该输出位置的 Var。原始输入若还有最终投影、过滤或其他表达式消费者，必须继续保留。

例如外层只取 `row_id` 并按距离排序：叶子通常向 UNION 输出 `row_id + distance`；源扫描仍要提供计算距离需要的 `embedding`。如果外层还要返回 `embedding`，则不能删掉这列，接口可能是 `row_id + embedding + distance`。排序键表达式的类型按实际表达式保存，并非统一强制转换成 float8；向量距离是常见的 float8 例子。

残留外层 WHERE 是重要安全边界：即使排序表达式 immutable，也可能在被过滤掉的行上抛错，例如 `100 / row_id`。不能把“无 volatile”错误等同于“可在更多行上提前计算”。

## 7. 防止源扫描把无用列重新加回来

单改 Query 输出还不够。`createplan.cpp::use_physical_tlist()` 原先可能为了避免投影而使用基表完整物理 targetlist，把已经不需要的 `title` 加回扫描、Join 或 Stream 接口。因此有定向保护：

```cpp
if (root->union_all_required_columns_pruned && rel->rtekind == RTE_RELATION)
    return false;
```

这里的 `false` 是“不采用完整物理 targetlist”，不是“不扫描这张表”。后续仍使用实际需要的扫描输出，也仍遵守连接键、过滤、MVCC 和权限需求。

这个 flag 可以从父 PlannerInfo 继承，让后代扫描也遵守输出策略；第三阶段的资格却必须用本 Query 的局部返回值。混用两者曾导致聚合子查询中的 `Aggref` 被错误移到非聚合节点。

`EXPLAIN VERBOSE Output` 表示计划输出表达式，不能据此保证磁盘不读取包含 title 的页，或 heap tuple 解码绝不遍历相关属性。该特性的直接承诺是减少不必要的表达式输出和层间 tuple 字段，不是列式物理存储改造。

另一个与 Top-K 配套但目的不同的 `createplan.cpp` 修复，是构造有序索引 `indexorderbyops` 时从真实 `indexorderby` 表达式取得类型，避免依赖可能匹配不到的 UNION 子关系 EC member。迁移时不能只搬 `use_physical_tlist()` 的几行而漏查这一历史依赖。

## 8. 安全门及六类已复现问题

当前是受限形状优化，不声称所有被拒绝的 SQL 都在数学上无法优化。CTE、递归、修改型 CTE、RLS、窗口、SRF 等判断中，有的针对已复现错结果，有的是保守范围限制或防御性重复检查；放宽前需要独立证明和原生回归。

| 问题 | 曾出现的现象 | 不可漏掉的保护 |
|---|---|---|
| B1 残留过滤与排序求值顺序 | 对本应过滤掉的零值做除法而报错 | 残留外层 quals 时不做排序键输出下移 |
| B2 继承状态被当成本层资格 | `Aggref found in non-Agg plan node` | 可继承扫描 flag 与本 Query 改写成功 bool 分开 |
| B3 ROWNUM | 下推 WHERE 后叶子编号改变 | 同时检查目标表达式和当前查询的 ROWNUM 条件 |
| B4 UNION 容器已有 LIMIT | 越过内层截断后返回不同记录 | 不能只检查叶子，必须验证 UNION 容器边界 |
| B5 外层 SRF | 最近输入行输出零行，局部 Top-K 导致漏结果 | 直接检查 targetlist SRF，不仅依赖可能未置位的 `hasTargetSRFs` |
| B6 WITH TIES | 应返回全部同分行，实际只剩各叶子的 K 行 | 外层 WITH TIES 回退；PERCENT 也作防御性拒绝 |

完整现象、最小 SQL、修复前后输出见[缺陷修复报告](../测试/UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md)。这些保护必须在只开 Top-K、只开列裁剪和两者组合时分别核验，不能把原来 Top-K 的安全问题归为列裁剪专属问题。

## 9. 最快判断计划变化的归因与合理性

### 9.1 先建立单变量对照

同一 SQL、数据快照、统计信息、索引、参数和实际二进制下，固定 Top-K GUC，直接重新规划执行列裁剪 `off → on → off` 的 `EXPLAIN (VERBOSE ON, COSTS ON)`；不要复用可能保留旧计划的 prepared statement。测试交互作用时，再补完整的两开关四组合。

需要判断是否命中改写时，可在诊断会话临时启用 `client_min_messages=debug1`，查看当前实际日志：

```text
union_all_required_columns: old=3 new=2 where_pushed=true outer_recheck=false
preprocess_union_all_diskann_topk: GUC disabled
union_all_required_columns: order_keys=1 raw_columns=1 materialized=true
```

以上是“只开列裁剪、只返回 row_id、按距离排序”的命中示例，数值不是通用常量。`old=3 new=3` 同样可以是成功改写。Top-K 成功的独立日志为 `successfully pushed ORDER BY + LIMIT to N leaves`，不要拿 `materialized=true` 代替它。

### 9.2 再解释变化链

- `Subquery Scan` 消除：接口压紧或表达式映射可能让投影成为无操作，后续原生清理便能去掉包装层。内外 targetlist 等长只是有关条件，不能代替逐位置表达式/Var、过滤及其他清理条件检查。
- `Sort + Append` 变为 `Merge Append`：更直接的排序键输出可能让后续 planner 识别各分支的有序性。消失的是某个物理 Sort，不是允许删除全局排序语义。
- Join 方向/算法变化：WHERE 重检消除、列宽变化和排序键关系会改变后续候选及成本；特性本身没有写死某种 Join。需比较估算行数、width、构建侧、work_mem、Hash 分批和候选成本。
- Hash 分批是内存放不下时按哈希分区分批处理；估算行数或行宽上升可让批数增多，不固定为 2。计划阶段的分批成本估算不等于执行时已经发生落盘。

已有[Join 变化专项实测](../测试/Join类型变化验证/根因分析.md)通过固定参数的 off/on 对照及“保留宽列”“提高内存”的反事实对照，证明一例 snapshot 行宽下降使 Hash 候选从估算两批变一批，最终改变 Anti Join 构建侧。该证据针对只查询 id 的例子，不足以直接证明另一同源分支中返回全部三列那份计划的根因。

### 9.3 最后判定是否合理

正确性优先于计划好看：输出值及重复行语义、WHERE/反连接/局部 LIMIT 顺序、排序规则、异常与副作用边界、类型和列映射都必须成立。确定性完整结果用双向 `EXCEPT ALL`，有全序 Top-K 时另比较有序结果；没有 ORDER BY 的 LIMIT、同分无 tie-break、volatile 或 ANN 近似检索，需按各自契约设计验证，不能机械要求任意一次返回行身份完全一致。

验证通过只能说明该变化语义合理；成本估计下降不等于实测提速。性能结论需要合适构建、同条件测量，不能由 Debug/O0 计划观察推断生产收益。

## 10. 测试与交付边界

原生框架为 `src/test/regress` 的 SQL + expected + schedule，专用目标为 `union_all_required_columns_check`。既有证据包括主用例、安全用例与 `diskann_vector`、`setop`、`reduce_orderby` 的最终 5/5 联跑；完整主 schedule、多 DN/Stream、分布式默认开启和性能准入不在该证据覆盖范围内。

原生回归中同时看护典型计划及查询结果。跨同源分支时，若合法 CBO 选择导致文本计划改变，应先按上一节证明，再更新对应 expected，不能直接覆盖 golden 消除 diff。

构建/启停遵循工作区 AGENTS.md 的 openGauss 统一脚本；不要照历史文档手工 configure/make 或假设当前实例二进制就是当前 Git HEAD。迁移清单、修复包、测试日志及源码链接均见[设计总入口](README.md)。
