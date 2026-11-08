# DiskANN 距离结果复用：详细开发设计

## 0. 文档状态、阅读顺序与结论

- 状态：首版及 `dc7e06d922` / `d4a5b5617b` 修复之后，又完成 R1（TID 再使用）、R2（候选逆序）、R3（删除回滚）的生命周期补强；2026-09-15 Debug 构建安装、旧索引免重建和旧快照验证通过，原生回归 **3/3 通过**。本轮已提交为 `84f0a21f543da7ffaa9582d44d0ff0772068c9ae`，尚未 push；限定 heap 路径的严格输出见 §10.3，不代替性能及长期并发准入。
- 源码核对日期：2026-09-15。
- 核对工作树：`/home/qiumc/data/code/cpp/openGauss-server`，分支 `integration/multimodal-stacked`；当前 HEAD `84f0a21f54`，本轮修复基线 `d4a5b5617bfc`。早期修复基线为 `e22030731162`。
- 工作树存在未提交的列裁剪安全修复、DiskANN INCLUDE 禁用改动及其他用户修改；HEAD 不等于当前完整源码快照。开发前必须重新记录 dirty 状态，不能覆盖这些改动。
- 本轮为用户授权的开发修复与测试；不初始化现有数据库、不自动 REINDEX 用户索引。
- 适配范围仅为 DiskANN。HNSW 不做适配，保持现有搜索、距离计算和投影行为；共用 ANN 执行器必须按实际索引 AM 限定准入。
- 配套：[开发实施指南](ANN距离结果复用开发实施指南.md)。先阅读本文的距离契约、引用节点、有效性和排序章节，再按实施指南逐阶段开发。
- 实测记录：[缺陷修复与验证报告](../测试/ANN距离复用缺陷修复与验证报告.md)。

核心设计：AM 内部继续保存搜索分数，在完整距离已取得、节点候选排序及 AM 内过滤准入后，物化 TID 前缓存标量转换结果；`gettuple` 发布候选值及状态。可见堆行阶段按 §5.5～§5.6 校验真实向量身份、处理 L2 精算；§10.3 收集并排序完整候选批次后才向父节点发布，投影通过显式引用读取该行的最终缓存。不能把节点代表值或相同物理 TID 冒充当前行的精确距离。

| 类型 | 已有内部完整距离 | 拟导出的值 | 对外承诺 |
|---|---|---|---|
| 普通 vector L2 | 同一对原始向量的 float 平方距离 | `sqrt((double)distance)` | 在本文安全条件内，与当前 SQL `<->` 的计算结果一致 |
| 普通 vector Cosine | 浮点归一化向量的 float 负内积 | `clamp(1.0 + (double)distance, 0.0, 2.0)` | 显式选择的近似 Cosine 值，不承诺与原始 SQL `<=>` 逐位一致 |

不做：改索引页格式、恢复 INCLUDE、把所有 ANN 路径都标为精确有序、把 AM 增加为计划树中的独立子节点、改变 WHERE 的精确计算语义、无上限跨查询距离缓存。

## 1. 已确认的设计决策

| 编号 | 决策 | 原因 |
|---|---|---|
| D01 | 仅适配 DiskANN，先覆盖普通行存 vector | 共用 AnnIndexScan 不代表所有 AM 都支持新协议；只允许实际 DiskANN 索引命中 |
| D02 | L2 使用 double 开方，禁止 sqrtf 后再转 double | 对齐现有 `<->` 的计算顺序与返回精度 |
| D03 | Cosine 只在显式允许近似的模式下复用 | 归一化已经改变输入浮点值，单纯加 1 不能恢复原 SQL 的结果 |
| D04 | 搜索分数和输出距离分开保存 | 续扫、候选堆、阈值、精排等仍依赖原分数尺度 |
| D05 | 使用专用 `AnnOrderByRef` 结果引用节点 | 明确表示“取 AM 返回值”；不滥用物理索引列或普通 Var 编号 |
| D06 | 保留 `indexorderby` / `indexorderbyorig` | 它们仍描述发给 AM 的搜索要求，不是读取返回值的位置 |
| D07 | 只替换经审查的投影距离表达式 | 不把近似距离传播到过滤、连接、分组、去重等精确语义位置 |
| D08 | 独立 GUC，默认关闭，提供 off / l2 / cosine / all 四种正交模式 | L2 与 Cosine 可独立启用，并与 UNION ALL 下推、列裁剪、INCLUDE 分离控制 |
| D09 | AM 专用结果通道携带质量及有效性元数据 | 不能把近似分数冒充通用 IndexScan 所要求的精确值或下界 |
| D10 | 数值可用性和流有序性独立验收 | 得到精确/近似输出值，不代表跨扩批结果全局有序 |
| D11 | 不支持的场景尽可能在计划阶段原路保留 | 避免生成无法执行的新引用节点，避免运行中混合两种距离语义 |
| D12 | 新引用节点必须补全所有基础设施再准入 | 包含 copy、read/write、walker、类型查询、双表达式引擎、EXPLAIN、版本门 |
| D13 | 不改索引格式；L2 合并节点的非原始 TID 显式 heap recheck | 旧索引也可能把不同向量合并；仅判断 TID 数量或给距离开方不能证明逐行精确 |

这些名字为设计命名。实施时可按目标分支规范调整，但不得删除相应职责。

## 2. 当前实现的真实行为

### 2.1 DiskANN 计算的是什么

相关代码：[diskannscan.cpp](../../../src/gausskernel/storage/access/datavec/diskannscan.cpp)。

当前 `diskannsearch()` 大致执行：

```cpp
SearchFixedPoint(so);
for (size_t i = 0; i < queue->size(); i++)
    queue->_data[i].distance = GetDistance(so, queue->_data[i].id, NULL, NULL);
qsort(...);
```

`GetDistance()` 从索引页读取完整索引向量，调用 `so->procinfo`，返回 float。这里“完整距离”是区别于 PQ 估算值的内部距离，并非已经对齐 SQL 返回值：

- L2 支持函数是 `vector_l2_squared_distance`。
- Cosine 支持函数是 `vector_negative_inner_product`，索引向量和查询向量已按对应路径归一化。
- `DiskAnnCandidatesData.distance` 虽为 double，当前来源仍是 float；扩宽类型不能恢复已经不存在的精度。
- 当前返回路径会写入 `xs_orderbyvals`，但写入的仍是上述内部值。注释中的 exact 不能作为 SQL 值已经精确对齐的证据。

非 PQ 搜索已经可能计算完整内部距离，后面的整队列 GetDistance 因而可能重复工作；PQ 搜索则通常仍需要完整距离精排。是否能删一次调用必须根据距离来源及节点有效性判断，不能只看“函数调用了两次”。

### 2.2 DiskANN 搜索候选与返回候选的边界

相关结构见 [diskann.h](../../../src/include/access/datavec/diskann.h)，搜索、物化和返回逻辑见上一节的 diskannscan.cpp。

搜索队列中的 `Neighbor` 保存节点 ID、float distance 及关联 heap TID 信息。后续返回候选使用 `DiskAnnCandidatesData`；其中 distance 为 double，但来自前面的 float 内部距离。两处都不能仅凭字段名 distance 就认定保存的是 SQL 输出值。

新输出缓存应附着在已完成完整距离确认、且能验证身份的候选上。搜索分数保持原值，另存 output_distance、output_ready 和质量/来源标记；候选被复制、替换或重新精排时，必须同步维护这些标记。

同一有效节点的多个 heap TID 可以共享一次标量转换，但每次返回 TID 都是独立的发布事件。扩批还可能重新遇到已有候选，因此转换缓存、候选去重与逐行返回通道是三个不同职责，不能混成一个全局“上次距离”。

首批、扩批、rescan 和 endscan 都必须纳入生命周期审查。这里的缓存只服务当前 DiskANN 扫描，不建立跨查询缓存，也不扩展到其他 AM。

### 2.3 执行器仍重复计算距离

相关代码：[nodeAnnIndexscan.cpp](../../../src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp)、[execScan.cpp](../../../src/gausskernel/runtime/executor/execScan.cpp)、[indexam.cpp](../../../src/gausskernel/storage/access/index/indexam.cpp)。

普通 heap 路径：

```text
AM 搜索候选并返回 TID
    → index_getnext / IndexFetchTuple 取得可见堆元组
    → AnnIndexNext 放入 scan slot
    → ExecScan 执行 Filter
    → ExecProject 计算 Output 中的距离表达式
    → 上层节点读取输出值
```

对于 `Output: row_id, (embedding <-> q)`，输出的是 row_id 和标量距离；embedding 只是表达式输入，不是独立向上传递的输出列。上层 Limit 的 Output 再次显示该表达式，也不代表 Limit 再遍历一次向量。

### 2.4 与既有列裁剪特性的关系

[协同设计说明](UNION_ALL_LIMIT下推与列裁剪协同设计说明.md)中的排序键输出改写，可使距离在 UNION ALL 叶子边界被物化，并由上层引用。这为本特性提供了常见命中形态，但两个功能不互相隐式打开。

本特性不再次下推 WHERE，不增加分支 LIMIT，不重新分配 UNION 列号，也不改变 `use_physical_tlist()` 的既有边界保护。

## 3. 目标 SQL 与首版范围

### 3.1 核心场景

```sql
SELECT row_id
FROM union_all_topk_onecol_test.gs_1w_1bucket
WHERE title IS NOT NULL
ORDER BY embedding <-> '[0.1,0.2,0.3,12.8]'::vector
LIMIT 10;
```

同样覆盖显式输出距离：

```sql
SELECT row_id,
       embedding <-> '[0.1,0.2,0.3,12.8]'::vector AS distance
FROM union_all_topk_onecol_test.gs_1w_1bucket
WHERE title IS NOT NULL
ORDER BY distance
LIMIT 10;
```

Cosine 使用 `<=>`，并显式启用 `cosine` 或 `all`。视图展开后的 snapshot / delete 分支继续原有处理；不是 ANN 的分支不使用 AM 距离引用。

### 3.2 命中条件

- 已选择真正的普通 `AnnIndexScan`，且索引 relation 的实际 AM 为 DiskANN。不能仅凭节点类型、索引名称或 SQL 文本判断；HNSW 等其他 AM 不生成引用。
- 第一版只覆盖 heap 行存、内置 vector、一个距离排序键、ASC，以及常量或可在扫描开始时绑定的查询参数。
- 目标距离表达式实际位于该 ANN 节点的 targetlist 中。若计算仍在更上层且没有可用叶子输出，首版不额外做距离表达式下移。
- 距离操作符、支持函数、opclass、参数表达式、类型/typmod、collation 和左右操作数必须匹配。
- WHERE 可以存在且是本目标场景的必测项；AM 距离复用本身不依赖必须有 WHERE 或 LIMIT。
- 无 ORDER BY 且 AM 没有计算相应距离时，不能仅凭 SELECT 中出现距离函数便生成引用。
- `SELECT embedding` 等其他消费者仍可保留向量；本特性不能擅自删去它们。

### 3.3 首版拒绝或保留原路的场景

| 场景 | 首版策略 |
|---|---|
| 非 DiskANN AM（包括 HNSW） | 不在本特性适配范围；不请求新通道、不生成引用，保持原路径 |
| DiskANN PQ | 未建立“最终距离来源”适配前不生成引用；必要精排不能删除 |
| mmap、UStore、分区切换、bucket、Index Only ANN | 独立适配前回退；不以普通 heap 验证替代 |
| 自定义 opclass、halfvec / sparsevec / bit 等类型 | 回退，不按算子名称或 OID 数值猜测语义 |
| 多个索引距离排序键、DESC | 回退，数组接口可预留但不虚报已支持 |
| LLVM/JIT、ByPass、向量化、Stream/远程/并行路径 | 第一版不植入新节点，普通解释执行的递归和扁平化两条路径必须均支持 |
| 行锁、DML、EPQ 相关路径 | 首版不生成引用；防御性处理见执行器章节 |
| 近似距离被 WHERE、Join Qual、HAVING、DISTINCT、GROUP BY、窗口、集合去重等消费 | 首版保留原表达式，不允许近似值改变精确关系运算 |
| 有无法分析的 SubPlan / CTE 消费链 | 保守回退，不盲目跨 Query/Plan 边界匹配 |

首版范围是准入边界，不是宣称这些机制本来有缺陷；增加支持必须扩展对应测试。

## 4. 距离转换契约

### 4.1 L2：复用同一份平方距离

当前 [vector.cpp](../../../src/common/backend/utils/adt/vector.cpp) 的 `<->` 实现为：

```cpp
sqrt((double)VectorL2SquaredDistance(a->dim, a->x, b->x))
```

底层平方距离为 float。因此同一对原始向量、同一计算内核得到的完整 float 平方距离，可以通过完全相同的 double 开方生成输出值，不引入一次额外向量遍历。

```cpp
// 设计伪代码；需在调用前验证来源和有效性。
double value = sqrt((double)candidate.search_distance);
```

必须满足：

1. query 是当前扫描的 query，不是上一次 rescan 的参数。
2. 候选数据与当前有效索引条目/返回 TID 相对应。
3. 来源为完整平方距离，不是量化估算、归一化后另一种距离或其他变换分数。
4. 不使用 sqrtf，不先把开方结果放进 float 字段。
5. NULL 单独标记；正无穷等值按与 SQL 相同的约定处理。异常负平方距离不能取绝对值掩盖，应视为来源错误。

“一致”是同一输入和同一核函数下的结果一致，不是保证不同编译器、不同 CPU/SIMD 内核产生完全相同的原始平方距离。

#### 4.1.1 为什么当前非 PQ L2 对 `d²` 开平方后基本没有额外误差

当前代码的两条路径可以逐步对齐：

| 阶段 | DiskANN AM 已有路径 | SQL `<->` 路径 | 结论 |
|---|---|---|---|
| 输入 | 当前 query 向量+索引页内的完整 vector | 同一 query+对应表行的完整 vector | 有效索引条目下应为同一组 float 分量 |
| 平方距离 | DiskANN L2 opclass 的 support function 1 是 `vector_l2_squared_distance` | `l2_distance()` 也调用 `VectorL2SquaredDistance()` | 同一平方距离内核 |
| 中间精度 | `VectorL2SquaredDistance()` 返回 float，`GetDistance()` 当前也以 float 接收 | 同一 float 平方距离转 double | AM 没有比 SQL 多一次量化 |
| 输出转换 | 拟新增 `sqrt((double)cand->distance)` | 已有 `sqrt((double)VectorL2SquaredDistance(...))` | 转换顺序和精度一致 |
| SQL 类型 | 通过 `Float8GetDatum()` 发布 | `PG_RETURN_FLOAT8()` | 都是 float8 |

对应源码事实是：

1. DiskANN 的 `vector_l2_ops` 把 `vector_l2_squared_distance(vector, vector)` 注册为距离 support function。
2. [diskannscan.cpp](../../../src/gausskernel/storage/access/datavec/diskannscan.cpp) 中 `GetDistance()` 通过 `so->procinfo` 取得该 float `d²`；候选结构虽用 double 保存，其有效精度仍来自这个 float。
3. [vector.cpp](../../../src/common/backend/utils/adt/vector.cpp) 中 SQL L2 的实现正是对同一 float `d²` 执行 double `sqrt`。
4. 因此可在候选确认后只做一次 O(1) 标量开平方，然后将结果放入专用输出缓存或 `xs_orderbyvals`，不需要再取向量、再做一遍 O(dim) 距离累加。

因而“基本没有误差”的准确含义是：

- **不新增算法误差或量化误差**；原始 float 累加已有的舍入同时存在于 AM 值和 SQL 基准中。
- 对同一份向量字节、同一 `VectorL2SquaredDistance()` 结果和同一 double `sqrt`，预期可与 `<->` 的 float8 结果完全相同；即使跨 CPU/SIMD 实现讨论，也只应有原有浮点舍入级差异。
- `sqrt` 是在非负数域上严格单调的；它不改变 DiskANN 分支内的排序，并把该分支从 `d²` 单位转回与 SeqScan 分支一致的 `d` 单位。

建议的发布逻辑是：

```cpp
// 仅适用于已验证来源的完整、非 PQ L2 平方距离。
double squared = candidate.search_distance;
if (squared < 0.0 || isnan(squared))
    fallback_to_original_expression();
double output_distance = sqrt(squared);  // 必须是 double sqrt，不是 sqrtf
```

当前的逐分量差平方累加不应产生负数，所以不需要常规 clamp。若未来改成 `‖a‖²+‖b‖²-2a·b` 一类快速公式，才可能因消去误差出现微小负数；届时必须在距离来源契约中明确容差和 clamp 范围，不能对任意负数盲目取绝对值或归零。

上述结论不适用于 PQ/量化距离、归一化后的 Cosine 分数、搜索阶段的估算分数，或候选与当前 query/索引条目不再对应的情况。对一个已含近似误差 `ε` 的平方距离开平方，只是改变量纲，不会消除 `ε`；这类值不得借助 `sqrt` 冒充 SQL 精确 L2。

### 4.2 Cosine：定义明确的近似输出

设内部完整负内积为 `s`。首版固定转换规则：

```cpp
double value = 1.0 + (double)s;
if (value < 0.0)
    value = 0.0;
else if (value > 2.0)
    value = 2.0;
```

这个值来自浮点归一化向量，不是原始向量的 SQL 精确 Cosine。禁止把它标为 SQL_EXACT。

明确的语义约定：

- `cosine` / `all` 是用户主动允许“相关 ANN 输出距离可近似”的选择，不是因为 ANN 召回本来近似就自动取得这种授权。
- 相同 SQL 在不同物理路径下可能返回有细微差异的距离，也可能改变近距离候选的顺序和 LIMIT 边界行。
- 不承诺对任意维度、极端幅值、病态输入都有一个固定的绝对误差上界。原始 SQL 的 float 累加还可能产生不同的溢出/下溢行为。
- 数据集验收用的误差阈值必须注明维度和输入范围，不能当成数学保证。
- NULL 查询值与无效缓存不能伪装成 0 或 1。非有限内部值应走定义清楚的拒绝/回退策略，不得临时制造正常距离。
- WHERE 等精确关系条件继续执行原始表达式。首版遇到近似结果可能经列引用影响这些条件时，应整体拒绝该 ANN 节点的近似复用。

### 4.3 不覆盖原搜索距离

```cpp
struct CandidateDistanceCache {       // 设计示意
    float search_distance;            // 现有算法继续使用
    double output_distance;           // 补充转换后的值
    bool output_ready;
    AnnDistanceQuality quality;
};
```

不能把开方或加 1 的值写回原 distance 字段。DiskANN 的搜索队列、候选比较、扩批和阈值逻辑仍可能读取它，必须始终使用原搜索分数尺度。

“补充计算”应为 O(1) 标量工作；不得为所有已访问节点额外取原始向量、解 TOAST 或调用 SQL 距离函数。

## 5. AM 与执行器的数据通道

### 5.1 为什么不能只用 xs_recheckorderby

[relscan.h](../../../src/include/access/relscan.h) 对标准 `xs_orderbyvals` 的约定是 SQL ORDER BY 值；`xs_recheckorderby=true` 时还要求该值是实际值的下界。

普通 [nodeIndexscan.cpp](../../../src/gausskernel/runtime/executor/nodeIndexscan.cpp) 的重排逻辑会使用这个下界假设。任意 PQ 估值和本文近似 Cosine 都不能默认满足它。

首版采用专用 ANN 返回通道，所有新投影引用只读取这个通道；不能让近似值意外流入通用 IndexScan 的有损重排队列。

### 5.2 拟新增专用数据结构

以下是逻辑字段，不要求实施时逐字复制布局：

```cpp
enum AnnDistanceQuality {
    ANN_DISTANCE_UNAVAILABLE,
    ANN_DISTANCE_SQL_EXACT_L2,
    ANN_DISTANCE_APPROX_COSINE
};

struct AnnOrderByValue {
    Datum value;                       // float8 Datum
    bool isnull;
    bool valid;                        // valid 与 isnull 不是同一个状态
    AnnDistanceQuality quality;
    uint64 scan_epoch;
    uint64 row_token;
    ItemPointerData index_heap_tid;    // AM 返回的索引项所指向的堆 TID
};

struct AnnOrderByChannel {
    bool requested;                    // 仅由已通过 DiskANN 能力门的扫描请求
    int policy;                        // 扫描开始时锁定
    int nvalues;                       // 首版只能为 1
    AnnOrderByValue* values;
};
```

建议在 IndexScanDesc 上挂载该通道指针；实际候选缓存仍归 AM 管理，返回值使用按值 float8，不向执行器泄漏候选、索引页、TOAST 或临时内存指针。

`row_token` 用于区分同一个扫描中不同返回事件；不能用一个 valid 布尔值顶替行身份。发布前必须关联 AM 返回的 TID；消费时必须对应当前 scan slot 的同一次索引取行事件。

还要区分 AM 给出的索引根 TID 与 HOT 链中实际可见元组的 TID。当前 heap 取行路径会沿 HOT 链查找；不能机械要求两者的物理 TID 相等。扫描状态应保留 AM 返回事件、根 TID 和可见元组的关联，利用合法 HOT 更新不改变索引键的保证，不能因为 HOT 走到另一偏移就误报缓存串行，也不能因此完全放弃行关联校验。

### 5.3 生命周期与所有权

1. beginscan：通道未请求、无有效值；数组由扫描上下文拥有。
2. ExecInit/运行参数就绪：验证模式、AM 能力、原始 ORDER BY 表达式，锁定本次扫描策略。
3. 每次 AM gettuple 开始：使上一次返回值失效。
4. 找到候选：验证候选身份；在必要精排之后完成标量转换。
5. 返回 TID 同时发布值、质量、epoch、row_token。
6. 索引框架跳过不可见 TID 或再次获取候选时，旧值不得被沿用。
7. 可见堆行进入 slot 后，AnnIndexNext 将对应返回事件绑定到扫描状态；Filter 失败不产生结果投影。
8. 投影读取值；同一行多个相同投影引用读取同一份缓存。
9. rescan：epoch 递增，候选缓存和发布状态失效，重新绑定参数。
10. endscan：释放通道和临时数据；不留下引用旧 MemoryContext 的表达式状态。

同一索引节点关联多个 heap TID 时，可以共用标量转换的工作，但不能据此断言各 TID 的 L2 数值相同；必须执行 §5.5 的原始 TID 校验。每次发布独立绑定本次 TID/row_token。并发 VACUUM、删除、槽位复用检查不能因为缓存命中而跳过。

必须额外证明“距离、索引身份、关联 TID”来自同一有效候选版本。尤其不能把后来重新读取的索引向量距离与早先保存的旧 TID 列表随意拼接。保留一次 GetDistance 并不自动完成这种证明；必要时应在同一受保护的读取过程中取得完整距离及身份/TID，再物化候选。如果目标分支无法建立该关联，连执行器距离复用也不能准入，而不只是暂缓删除重复 GetDistance。

### 5.4 DiskANN 遗留标准字段处理

实施必须同时审计现有 `xs_orderbyvals = cand->distance` 写入：

- L2 只有完成 SQL 开方转换、且确认对应当前 TID 后，才可以按标准精确值发布；需要 heap recheck 的候选不向标准字段发布节点距离。
- Cosine 近似值只走新专用通道，不设置“精确 SQL”标记，也不冒充下界。
- 普通 IndexScan 等其他消费者不得接收仅有近似值的通道；若目标分支存在这样的调用路径，需要能力拒绝或独立适配后再开放。
- 不以改注释代替修复返回值口径，也不能让旧消费者读取未初始化数组。

此项属于接口正确性门，实施者必须给出真实消费者搜索结果和回归证据。

### 5.5 已复现的合并节点问题与不改格式修复

`FindDuplicateNeighbor()` 依据内部距离阈值合并节点，并不逐元素验证向量相同。`ComputeL2DistanceFast()` 的平方范数/内积相减还可能发生抵消。例如 `[10000,20000,30000,40000]` 与 `[10001,20000,30000,40000]` 可被合并；节点只能保存其中一个向量。首版把节点距离开方后复制给全部 TID，因而出现 SQL 应返回 0、1、2，却复用成相同数值的错误。这不是 `sqrt` 精度问题。

`dc7e06d922` 的兼容处理不修改页布局、索引版本或合并算法（后续生命周期修复进一步收紧合并条件，见 §5.6）：

1. 最终 `GetDistance()` 在同一个索引页共享锁内取得距离、有效 TID 列表及 `IndexTuple.t_tid`，存入 `Neighbor.vectorTid`。不再把最终距离与搜索早期 TID 列表拼接。
2. L2 候选的 `heapTid != vectorTid` 时设置 `DiskAnnCandidatesData.heapRecheck`。即使数组只剩一个 TID，也不能据数量宣称向量相同；完全相同的重复向量也保守重算。
3. `gettuple` 发布显式 `xs_ann_orderby.heap_recheck`，仍要求合法 epoch、token、质量和 TID。该状态不是“缓存缺失”；缺失/过期发布仍报错。
4. `AnnIndexBindOrderByValue()` 在可见堆行进入 slot 后校验事件，只有 EXACT_L2 允许这个状态。HOT 比较的是 AM 根 TID 的身份；不要求可见 HOT 成员的 offset 等于原根 offset。
5. 递归和融合两套 `AnnOrderByRef` 执行路径在第一次投影时执行独立 fallback ExprState，得到当前堆行的原 SQL L2，写入本行缓存并清除 recheck 标记；同一行的第二个引用直接复用该精确缓存。下一行、过滤失败、重扫均清理状态。

这是**只针对精确 L2 的显式行级重算**：两种结果都等于 SQL 欧式距离，没有混合 `d²` 与 `d`，也没有混合精确与近似 Cosine。Cosine、无效发布、epoch/token 错误不能借用这个例外。旧索引直接适用，不需要重新初始化或 REINDEX；代价是合并节点的非原始 TID 仍提取堆向量并计算距离，不承诺所有行都消除距离重算。

上述初次修复只处理非 owner TID 的数值，未覆盖 owner 地址再次使用及输出重排。后续 R1/R2 复现表明它不够完整；当前修正方案见 §5.6 和 §10.3，实测状态以测试报告为准。

### 5.6 生命周期补强：VACUUM、TID 再使用和旧索引

2026-09-15 的 R1/R3 复现明确了两项不能省略的前提：物理 TID 不带行代际，generic WAL 也不提供事务回滚撤销。后续修复遵循以下协议，不增加磁盘格式字段。

1. **全部 DiskANN 格式延迟物理删除。** `DiskAnnIndexUsesDeferredDelete()` 对 DiskANN 返回 true；即使表设了 `immediate_delete=true`，DELETE/UPDATE 也不直接删 TID、断边。普通 MVCC 决定可见性，VACUUM 决定何时能清理。只在提交时删除同样不安全，因为旧快照仍可能需要旧行。
2. **真正实现 bulkdelete。** `diskannbulkdelete_internal()` 从 PQ 保留区之后的第一个顶点页开始扫描，包含 frozen point 和未连入图的重复页，不能把 frozen block 当作起始页。逐页持有排他锁，调用本分支四参数 VACUUM callback，压紧存活 `heaptids[]`，清空尾项，并独立判定、失效已死的 `IndexTuple.t_tid` owner；修改通过 generic WAL 写入。空节点保留向量和图边，只作为导航节点，不回收图页。
3. **TID 相等仍要证明向量相同。** 旧版本可能已经漏清理且发生地址复用。`AnnIndexBindOrderByValue()` 在拿到可见堆行后调用 `diskann_orderby_vector_matches()`，比较当前堆向量与本候选索引节点的完整向量字节；归一化 opclass 先按对应口径归一化。L2 不一致时转为显式 heap recheck；近似 Cosine 不允许逐行切成精确值，报错并提示 REINDEX。不能用 token/epoch 合法代替跨事务的行身份校验。
4. **新的合并要求真正相等。** 内部距离阈值只找候选，`AddDuplicateNeighbor()` 必须在目标页排他锁内比较向量维度和全部坐标字节，再修改 TID。源页向量先复制、释放源锁，再锁目标页，规避 A→B/B→A 死锁。VACUUM 失效的 owner 只有在本次相等证明成立后才能被新 TID 接管。
5. **旧错误桶需要按行重算再排序。** 不自动重建用户索引；已存在的非等值合并桶由 L2 recheck 和 §10.3 的最终排序共同兜底。已被旧 immediate delete 破坏而丢失的图边，无法凭本补丁恢复；需要另行确认后 REINDEX，不能承诺补丁能修复所有历史损坏。

代价必须说清：普通 L2 正常行也需要提取堆向量并做字节校验，因此不是“零回表/零向量读取”；它仍省掉正常行再次执行完整距离算术。归一化 Cosine 校验还会产生归一化开销。没有行版本代际元数据的旧格式，不能同时承诺免读取和对历史 TID 再使用的严格证明。

## 6. 显式结果引用与计划入口

### 6.1 选择专用表达式节点

```cpp
typedef struct AnnOrderByRef {          // 拟新增，不是现有类型
    Expr xpr;
    int orderby_index;                  // 通道数组下标，首版为 0
    Oid resulttype;                     // FLOAT8OID
    int32 resulttypmod;                 // -1
    Oid resultcollid;                   // InvalidOid
    int expected_quality;
    Expr* original_expr;                // 原表达式：依赖、展示及受控回退
} AnnOrderByRef;
```

该节点表示 ANN 扫描的一个虚拟返回结果，不是索引物理列。不要给 INDEX_VAR 随意添加一个超过索引列数的属性号，也不要增加一个假的 AM 子计划来满足显示需求。

正常求值时不计算 original_expr；只读取通道。original_expr 必须保留用于依赖追踪、打印，以及执行前选择原路径时的独立 fallback ExprState。

### 6.2 与 ORDER BY 的方向区分

```text
AnnIndexScan.indexorderby / indexorderbyorig
    → 告诉 AM 用哪个索引列、查询向量和距离规则搜索

AM 专用返回通道
    → AnnOrderByRef
    → AnnIndexScan.Output
    → 上层 Limit / Sort / Merge Append 的列引用
```

因此只把已匹配的输出表达式换成结果引用，不能把发送给 AM 的 ORDER BY 要求改成对尚未产生的结果值的引用。

### 6.3 入口位置

推荐在 `standard_planner()` 中：

1. 主计划和相关子计划的 `set_plan_references()` 已完成；
2. 所有 Var 已可按最终 plan targetlist 追踪；
3. 但尚未进入规划内存可能释放前的 copyObject，也尚未封装/序列化最终 PlannedStmt；
4. 增加一次只处理本特性的末端计划遍历，例如 `prepare_ann_orderby_result_refs()`。

当前定位参考：[planner.cpp](../../../src/gausskernel/optimizer/plan/planner.cpp)，主计划 setrefs 在约 747 行，子计划处理完成后、约 916 行的内存复制块之前是需要核实的插入窗口。迁移时按阶段定位，不按行号机械插入。

选择这个位置的理由：本特性不负责产生 ANN 路径，不需要重做 UNION 改写或 EC 推导；晚替换可保持 targetlist 的 resno、resjunk、类型和输出接口不变。替换后不得再让不认识新节点的 pass 遍历它。

首版若存在无法分析的独立 SubPlan，可保守跳过；不能只处理主计划却向未适配子计划发送新节点。

### 6.4 匹配与消费链审查

实施算法：

1. 查找符合范围且实际索引 AM 为 DiskANN 的 AnnIndexScan，记录其父计划链；其他 AM 直接跳过。
2. 从 indexorderbyorig 取得唯一原始距离表达式。
3. 校验内置操作符与 support proc / opclass 的实际语义绑定，不能只匹配文本名称。
4. 在该扫描的 targetlist 中寻找语义完全相同的距离表达式；首版只处理完整 TLE 表达式，不在任意嵌套函数内做替换。
5. 追踪该输出位置向上层的引用，尤其检查 Filter、Join Qual、分组和去重消费者。
6. L2 和 Cosine 均不得移动计算跨越原有过滤边界；近似 Cosine 只允许简单投影、排序、Limit 和可证明的透传链。
7. 保留 resno / resjunk / ressortgroupref 等接口元数据，建立 AnnOrderByRef。
8. 每个扫描以“全部校验成功后一次安装”为原则；失败不留下半改写节点。

相同距离表达式出现两次，可以绑定同一个 orderby_index；不同 q、不同 cast、不同 operator、不同参数编号，不得合并。

当前修复采用保守的透明节点白名单，而非仅排除 Join/Agg 的黑名单：Limit、Sort、Material、Append、MergeAppend、SubqueryScan 及普通 Result 必须没有上层 qual，输出必须是 Var/Const 透传；Result 的 `resconstantqual` 也须为空。扫描自身的其他表达式和过滤仍独立按原 SQL 计算。任何未知节点、上层计算表达式、SubPlan/Param 消费都会阻断近似复用。

尤其 `plan_tree_walker()` 可由 `PARAM_EXEC` 直接进入 InitPlan，绕过调用者的 `T_SubPlan` 分支，不能只拦截 SubPlan 标签。List 是 Append/MergeAppend 子计划的容器，不应误判为不安全消费者。安全上下文退出每条分支后恢复，独立 snapshot Anti Join 不应误禁 delta 分支。

### 6.5 节点基础设施

必须覆盖：

- NodeTag 分配、结构声明；遵循目标分支编号及版本规则，不随意移动现有枚举编号。
- copyObject、equal、out/read；字段复制和序列化顺序严格对称。
- exprType、exprTypmod、exprCollation、expression_tree_walker/mutator 等。
- 依赖、Param、权限相关遍历必须能看到 original_expr；执行器的正常求值路径则不能因此自动计算它。
- EXPLAIN/ruleutils、新节点不支持时的 codegen 检测、计划缓存和版本兼容。
- 调试版和发布版均有明确失败/回退行为，不能只依赖 Assert。

## 7. DiskANN 的实施设计

### 7.1 转换时机

本节把“AM 内做二次距离转换”固定到可实施的位置，不再在 `diskannsearch()` 与 `diskanngettuple_internal()` 之间保留模糊选择。

#### 7.1.1 首版固定插入点

转换放在 DiskANN 搜索过程的“结果候选物化”阶段，语义位置是：

```text
SearchFixedPoint
    → 对需要的节点调用完整距离函数
    → 将距离来源标记为 FULL_INTERNAL_DISTANCE
    → 按原 search_distance 排列搜索候选
    → 执行 DiskANN AM 内过滤准入
    → 生成 output_distance                 ← 二次转换的固定位置
    → 将同一 output_distance 复制到该节点的各 heap TID 候选
    → 加入或更新返回候选集
    → gettuple 使上一行失效，选取候选，发布已转换值
    → AnnIndexNext 将通道发布事件与当前可见堆行绑定
    → ExecProject/AnnOrderByRef 校验并读取值
```

对应当前逻辑，插入点应位于 `diskannsearch()` 已经为 queue 节点取得完整距离、完成 `qsort`、并通过 AM 内过滤之后，进入“遍历节点关联 heap TID 并构造 `DiskAnnCandidatesData`”的循环之前。迁移时按这个语义阶段定位，不依赖固定行号。

这个位置有四个目的：

1. 输入已经是完整精排距离，不会把 PQ/搜索估算值误当成 SQL 值。
2. 原 `search_distance` 已用于本轮搜索候选排序，转换值不会反向影响搜索堆、扩批阈值和剪枝。
3. AM 内过滤已完成，不为明确被 AM 拒绝的节点做无用转换。
4. 同一索引节点可能关联多个 heap TID；在 TID 循环之前只转换一次，再按值复制，不为每个 TID 重复开平方或加 1。

#### 7.1.2 候选字段与原距离的共存关系

为减少对已有比较器的扰动，首版不必强制重命名当前 `DiskAnnCandidatesData.distance`。实施时可明确它始终表示 AM 内部搜索尺度，另外增加输出字段：

```cpp
struct DiskAnnCandidatesData {
    BlockNumber id;
    ItemPointerData heapTid;

    double distance;                 // 已有字段；仍是 search_distance
                                     // L2 下为 d²，不得覆盖为 d

    double outputDistance;           // 对上层发布的 SQL 口径值
    bool outputDistanceReady;        // false 时严禁读取 outputDistance
    bool heapRecheck;                // L2：值未证明属于当前 TID，须先精确重算
    AnnDistanceQuality outputQuality;
    DiskAnnDistanceSource distanceSource;
    uint64 outputEpoch;              // 生成时的 query/rescan epoch
};
```

`distanceSource` 至少要能区分：

```cpp
enum DiskAnnDistanceSource {
    DISKANN_DISTANCE_UNKNOWN,
    DISKANN_DISTANCE_SEARCH_ESTIMATE,
    DISKANN_DISTANCE_PQ_ESTIMATE,
    DISKANN_DISTANCE_FULL_INTERNAL
};
```

字段名可按目标分支风格调整，但下列语义不能省略：

- 搜索分数与输出距离是两个不同字段。
- `ready`、`quality`、`source`、`epoch` 各自解决不同问题，不得用一个布尔值代替全部状态。
- `outputDistance` 是按值的 double，不是指向索引页、向量或临时内存的指针。
- 同一节点的多个 TID 候选可复制转换结果，但各自保留 TID 身份与 heapRecheck 状态；非原始 TID 的 L2 值不得直接消费（§5.5）。

#### 7.1.3 转换函数契约

建议将转换集中在一个 DiskANN 专用函数中，不要在搜索循环、`gettuple`、执行器表达式回调中各写一份：

```cpp
enum DiskAnnOutputPrepareResult {
    DISKANN_OUTPUT_NOT_REQUESTED,
    DISKANN_OUTPUT_READY,
    DISKANN_OUTPUT_UNSUPPORTED_BEFORE_SCAN,
    DISKANN_OUTPUT_BROKEN_INVARIANT
};

static DiskAnnOutputPrepareResult
DiskAnnPrepareOutputDistance(DiskAnnScanOpaque so,
                             double searchDistance,
                             DiskAnnDistanceSource source,
                             double* outputDistance,
                             AnnDistanceQuality* quality);
```

函数输入不包含 vector 指针，也不接收 heap tuple。它只对已计算好的标量进行 O(1) 转换，从接口上阻止该函数意外变成又一次 O(dim) 距离计算。

L2 exact 模式的伪代码：

```cpp
if (!so->orderbyChannel->requested)
    return DISKANN_OUTPUT_NOT_REQUESTED;

if (so->outputMetric != DISKANN_OUTPUT_L2 ||
    source != DISKANN_DISTANCE_FULL_INTERNAL)
    return DISKANN_OUTPUT_BROKEN_INVARIANT;

if (isnan(searchDistance) || searchDistance < 0.0)
    return DISKANN_OUTPUT_BROKEN_INVARIANT;

*outputDistance = sqrt((double)searchDistance);
*quality = ANN_DISTANCE_SQL_EXACT_L2;
return DISKANN_OUTPUT_READY;
```

必须注意：

- 使用 double `sqrt`，不使用 `sqrtf`。`searchDistance` 虽然存在 double 字段中，当前有效精度来自 float 完整平方距离，与 SQL `<->` 的转换顺序对齐。
- 当前完整 L2 距离来自逐分量差平方累加，负值表示来源或内存状态违反契约；首版不对任意负值静默 clamp。
- 正无穷的处理应与 SQL 基线一致；NaN 会破坏排序契约，不得伪造为 0。
- 是否允许 L2 的输入特殊值，以实现时已有 vector SQL 语义为准；新通道不得自行放宽输入集。

Cosine 模式（`cosine` 或 `all`）的伪代码：

```cpp
if (so->outputMetric != DISKANN_OUTPUT_COSINE ||
    source != DISKANN_DISTANCE_FULL_INTERNAL ||
    (so->lockedPolicy & ANN_REUSE_COSINE) == 0)
    return DISKANN_OUTPUT_BROKEN_INVARIANT;

double value = 1.0 + searchDistance;
if (value < 0.0)
    value = 0.0;
else if (value > 2.0)
    value = 2.0;

*outputDistance = value;
*quality = ANN_DISTANCE_APPROX_COSINE;
return DISKANN_OUTPUT_READY;
```

Cosine 返回的仍是明确标记的近似值，不得因为做了 `1 + distance` 就改标为 exact。

#### 7.1.4 `diskannsearch()` 候选物化伪代码

下列伪代码展示转换与节点、TID 的关系；它不是可直接粘贴的最终补丁：

```cpp
SearchFixedPoint(so);

for (each node in queue) {
    node.distance = GetDistance(...);          // 完整内部距离
    node.distanceSource = DISKANN_DISTANCE_FULL_INTERNAL;
}

qsort(queue, CompareBySearchDistance);         // 仍按 d²/原搜索尺度排序

for (each node in sorted queue) {
    if (!PassesDiskAnnInternalFilter(node))
        continue;

    double nodeOutput = 0.0;
    AnnDistanceQuality nodeQuality = ANN_DISTANCE_UNAVAILABLE;
    DiskAnnOutputPrepareResult prepared =
        DiskAnnPrepareOutputDistance(so,
                                     node.distance,
                                     node.distanceSource,
                                     &nodeOutput,
                                     &nodeQuality);

    if (prepared == DISKANN_OUTPUT_BROKEN_INVARIANT)
        ereport(ERROR, ...);

    for (each heapTid associated with node) {
        DiskAnnCandidatesData candidate;
        candidate.id = node.id;
        candidate.heapTid = heapTid;
        candidate.distance = node.distance;    // 原值保留，不写入 nodeOutput
        candidate.distanceSource = node.distanceSource;

        if (prepared == DISKANN_OUTPUT_READY) {
            candidate.outputDistance = nodeOutput;
            candidate.outputDistanceReady = true;
            candidate.outputQuality = nodeQuality;
            candidate.outputEpoch = so->scanEpoch;
        } else {
            candidate.outputDistanceReady = false;
            candidate.outputQuality = ANN_DISTANCE_UNAVAILABLE;
        }

        AddCandidateIfNew(candidate);
    }
}
```

这里的关键约束是：

- `qsort` 和 DiskANN 内部比较器只读 `distance`，不读 `outputDistance`。
- 转换每个索引节点只做一次，多个 heap TID 按值复制结果，不共享易失效指针。
- 只有通道被请求时才准备输出值；GUC 关闭或扫描已在启动前选择 fallback 时，不增加开平方/加 1 工作。
- `AddCandidateIfNew()` 拒绝重复候选时，不能让新建临时值覆盖一个身份或 epoch 不同的旧候选。如需更新，必须同时校验节点、TID、距离来源和 epoch。

#### 7.1.5 `diskanngettuple_internal()` 只发布，不做转换

每次尝试取下一个候选时，先使上一次发布值失效。选中候选后，先关联返回 TID，再按值发布缓存：

```cpp
DiskAnnInvalidateOrderByChannel(scan);

DiskAnnCandidatesData* candidate = GetNextResultCandidate(so);
scan->xs_ctup.t_self = candidate->heapTid;

if (so->orderbyChannel->requested) {
    if (!candidate->outputDistanceReady ||
        candidate->outputEpoch != so->scanEpoch ||
        !QualitySatisfiesPolicy(candidate->outputQuality, so->lockedPolicy))
        ereport(ERROR, ...);

    AnnOrderByValue* value = &so->orderbyChannel->values[0];
    value->value = Float8GetDatum(candidate->outputDistance);
    value->isnull = false;
    value->valid = true;
    value->quality = candidate->outputQuality;
    value->scan_epoch = so->scanEpoch;
    value->row_token = ++so->rowToken;
    value->index_heap_tid = candidate->heapTid;
}
```

L2 exact 模式下，若该扫描仍需向标准 `xs_orderbyvals` 发布 ORDER BY 实值，写入的也必须是 `candidate->outputDistance`，不再是原 `candidate->distance`：

```cpp
scan->xs_orderbyvals[0] = Float8GetDatum(candidate->outputDistance);
scan->xs_orderbynulls[0] = false;
scan->xs_recheckorderby = false;
```

Cosine 近似值不能因为也是 float8 就写入一个会被通用 IndexScan 当成 SQL 精确值或下界的通道。它只能在 `cosine` 或 `all` 契约下走 ANN 专用通道。

`gettuple` 不重新执行 `sqrt`、`1 + distance` 或向量距离函数。这使“转换次数”与“发布次数”成为可分开观测的两类事件。

#### 7.1.6 执行器如何消费已转换值

AM 成功发布 TID 和距离后，堆访问层可能因 MVCC/HOT 选择实际可见版本。`AnnIndexNext` 必须把 AM 返回事件与当前 slot 绑定，不能只检查“通道现在 valid”。

`ExecProject` 求值 `AnnOrderByRef` 时执行：

```text
AnnOrderByRef(orderby_index = 0)
    → 取 AnnIndexScanState 当前绑定的发布事件
    → 校验 channel.requested
    → 校验 valid / isnull
    → 校验 scan_epoch
    → 校验 row_token 与当前 slot 绑定事件
    → 校验索引根 TID 与 HOT 可见行的合法关联
    → 校验 quality 对应的 l2 / cosine 能力位已启用
    → 返回已是 SQL 口径的 float8 Datum
```

执行器不需要知道 L2 内部是 `d²`，也不应根据计划表达式再猜一次如何转换。它只依赖 `value + quality + identity` 通道契约。

#### 7.1.7 为什么不选其他转换位置

| 候选位置 | 问题 | 结论 |
|---|---|---|
| 直接在 `GetDistance()` 中开平方 | 会把 DiskANN 搜索、阈值和候选比较依赖的 `d²` 全局改成 `d` | 禁止 |
| `qsort` 之前覆盖 queue distance | 可能与已有比较器、扩批阈值和去重逻辑混用两种尺度 | 禁止 |
| 每个 heap TID 候选中现算 | 同一索引节点关联多 TID 时重复转换 | 不作为首版主路 |
| `diskanngettuple_internal()` 中现算 | 每次返回事件都需要转换，也容易把“候选质量确认”推迟到发布时 | 首版仅发布 |
| 执行器 `AnnOrderByRef` 中开平方 | 执行器必须理解各 AM 的私有搜索尺度，还可能对同一行多次求值时重复转换 | 禁止 |
| 执行器重新调用 SQL 距离表达式 | 再次读取向量并执行 O(dim) 计算，与特性目标相反 | 只能作为整次扫描的 fallback |

#### 7.1.8 失败、fallback 与“不允许逐行混用”

静态或启动时可判定的不支持情况，必须在输出第一行前选择整次扫描策略：

```text
REUSE：AM 转换和发布，AnnOrderByRef 读取
或
FALLBACK：AM 不请求通道，执行器全部返回原距离表达式
```

不能先向上层返回几行 AM 转换值，再因某个候选的 `ready/source/quality/epoch` 不合法而改用原表达式。这会让同一 Sort/Merge Append 比较域混入不同来源，尤其在近似 Cosine 模式下会改变 LIMIT 边界。

§5.5 的 `EXACT_L2 + heap_recheck` 是明确的兼容例外：发布事件本身必须完全有效，只是原始索引向量不属于该 TID；原表达式与直接复用结果均为精确 SQL L2。不能把这个例外泛化成任意缺失缓存的单行回退。

如果扫描已开始后才发现下列不可能状态，应作为内部契约破坏报错，而不是单行 fallback：

- 通道已请求，但当前候选没有 `outputDistanceReady`。
- `outputEpoch` 与当前 rescan epoch 不一致。
- exact 计划收到 PQ/近似质量值。
- `row_token` 或 TID/HOT 关联与当前 slot 不一致。
- 转换函数在已锁定 exact L2 模式下收到负平方距离或 NaN。

`rescan` 必须执行：

```text
scanEpoch++
清空/作废 candidates 中的 outputDistanceReady
使通道当前 values 全部 valid=false
rowToken 重置或继续单调增长，但不得与旧 epoch 共同命中
按新 Param/query 重新计算和转换
```

#### 7.1.9 转换、发布与读取计数口径

为了证明执行器没有重算向量距离，至少分开以下计数：

| 计数器 | 递增位置 | 不应计入 |
|---|---|---|
| `Output Distance Conversions` | `DiskAnnPrepareOutputDistance()` 成功生成新值时 | 同节点多 TID 的按值复制、通道发布、投影多次读取 |
| `AM Order By Value Publishes` | `gettuple` 为一个返回事件成功发布时 | 后续不可见 TID 未进入 slot 的投影读取 |
| `AM Order By Value Reads` | `AnnOrderByRef` 成功返回 Datum 时 | AM 内候选比较、同行缓存未被投影消费的情况 |
| `Projection Distance Fallback Evaluations` | 整次扫描 fallback 或显式 L2 heap recheck 真正求值原表达式时 | 静态计划未命中、LIMIT 0、未执行分支、同行已缓存 recheck 的后续引用 |

因转换在节点物化阶段进行，`Output Distance Conversions` 可以高于最终输出行数；因同节点多 TID 共用一次转换，它也可以低于发布次数。验收不能简单要求这三个计数必须相等，而应结合唯一索引节点数、TID 数、过滤和 LIMIT 解释。

扩批时已经存在且节点身份、距离来源、query epoch 均仍有效的候选，不重复生成相同 `outputDistance`。如果无法证明旧值仍与当前节点/TID 关联，必须重新取完整距离并重新转换，不能只因数值相同就沿用。

### 7.2 跳过冗余完整距离计算的条件

给候选增加独立的来源标记，例如 UNKNOWN、PQ_ESTIMATE、FULL_INTERNAL_DISTANCE。只有同时满足下列条件才可跳过 GetDistance：

- 当前路径确实使用完整向量执行同一个内部支持函数；
- query epoch 相同；
- 节点身份、版本/回收状态可验证；
- 该距离没有被量化估算或其他变换覆盖；
- 被跳过的 GetDistance 没有承担尚未替代的 TID、页或并发有效性读取职责。

如果能在最终物化时证明距离与 TID 的关联，但无法证明较早搜索缓存仍有效，先保留这一轮 GetDistance 及必要的身份/TID 读取，只交付执行器复用。若连最终返回候选的关联都无法证明，则距离复用也必须拒绝；保留一次计算不能替代身份校验。实际限制必须在报告中明确记录。

### 7.3 禁止的改法

- `if (!enablePQ) continue;` 直接跳过整轮全部逻辑。
- 把所有候选的 distance 改为开方结果，然后继续用原阈值扩批。
- 将 float 扩宽成 double 后宣称精度已经等同 SQL。
- 未确认过滤通过和行身份，就复用上一条返回值。
- 为了复用值，重新开放用户已经要求关闭的 INCLUDE。

## 8. DiskANN 准入与共用代码隔离

### 8.1 用实际 AM 身份判定

AnnIndexScan 是共用计划/执行节点，不能把“是 AnnIndexScan”直接等同于“能读取本特性距离”。规划阶段应从该节点的索引 relation / catalog 元数据解析实际 AM，再校验 DiskANN 的 opclass、支持函数、类型和执行能力。

迁移时使用目标分支实际注册的 DiskANN AM 身份及能力接口，不按索引名字包含 diskann 判断，也不硬抄另一分支的 OID。先通过 AM 身份门，才进入表达式匹配和结果引用安装。

### 8.2 执行开始再次核验

扫描开始时，在当前实际索引描述符上重新核验 DiskANN 身份、输出质量能力及计划请求策略。只有全部满足时才设置专用通道 requested，并启用候选转换/发布。

非 DiskANN 扫描保持未请求状态，执行其原有投影；共用结构新增字段的默认值必须为关闭/无效。不能让打开 GUC 的所有 ANN 扫描都访问新缓存，更不能要求未适配 AM 填写新字段。

若缓存计划与实际能力不再相容，按第 9、11 节的整次扫描回退/重规划规则处理；不得让错误 AM 读取引用，也不得在输出部分结果后混换距离来源。

### 8.3 本次文件与验证边界

AM 专用改动限定在 DiskANN 的候选结构、搜索/精排、物化、返回和清理链路。HNSW 的候选结构、搜索算法、返回接口和配置均不改动，不增加 HNSW 距离复用适配阶段或正向功能用例。

节点协议、计划末端遍历、双表达式引擎和 EXPLAIN 属于 DiskANN 所需的共用基础设施，仍须完整实现。共用模块的兼容性检查应确认其他 AM 不会生成 AnnOrderByRef；这属于准入隔离验证，不代表适配了其他 AM。

## 9. 执行器设计

### 9.1 初始化顺序

当前 ExecInitAnnIndexScan 中表达式初始化早于部分 relation/scan 初始化。新引用的 ExprState 初始化不得假设 IndexScanDesc 已存在。

推荐将引用绑定到 AnnIndexScanState 的稳定地址或专用返回值状态；实际扫描开始后再连接当前 IndexScanDesc。禁止缓存一个可能因分区/bucket 或 rescan 改变的旧描述符指针。

普通 EXPLAIN 不执行扫描，不应触发 AM 搜索，也不应因通道尚未建立而报错。

### 9.2 两套表达式执行路径

| 路径 | 必须实现 |
|---|---|
| 递归表达式引擎 | AnnOrderByRef 的 ExprState 与求值回调；受控 fallback 状态 |
| 扁平化引擎 | 新 opcode，例如 EEOP_ANN_ORDERBY_REF；初始化、解释执行、dispatch table 全覆盖 |
| LLVM / ByPass 等未适配路径 | 在创建新节点之前拒绝本特性，不让未知节点进入执行路径 |

当前文件入口为 execQual.cpp、execExpr.cpp、execExprInterp.cpp、相关 ExprState/ExprEvalStep 头文件。仅修改 nodeAnnIndexscan.cpp 并不能自动让 ExecProject 读取 AM 返回值。

### 9.3 防止 fallback 被正常路径提前求值

扁平化引擎会先经 `get_last_attnums_walker()` 收集属性号，再生成 FETCHSOME。新节点的 original_expr 是 fallback/元数据，不应无条件加入正常路径的属性预取和向量求值。

必须：

- 正常投影初始化将 AnnOrderByRef 当作读取通道的叶节点；
- 原表达式单独编译为 fallback ExprState，且只在已选择 fallback 模式时调用；
- 依赖追踪的通用 walker 仍能遍历 original_expr；不要为优化取值而全局隐藏它。

这可以避免“没有调用距离函数，但仍因错误的预处理去提取原始向量”的实现偏差。其他投影或过滤需要 embedding 时，仍应正常提取。

### 9.4 读取与错误处理

```cpp
// 设计伪代码，不能直接粘贴作为完整实现。
if (scan_state->reuse_mode == FALLBACK_BEFORE_SCAN)
    return EvalOriginalExpression(fallback_state, econtext);

value = GetCurrentAnnOrderByValue(scan_state, ref->orderby_index);
if (!MatchesEpochRowAndQuality(value, scan_state, ref))
    ereport(ERROR, ...);  // 缓存不一致是缺陷，不能伪造 0/NULL 或取上一行

if (value->heap_recheck) {  // 仅允许 EXACT_L2，见 §5.5
    RecheckAndCacheExactL2ForCurrentHeapRow(...);
    value = GetCurrentAnnOrderByValue(scan_state, ref->orderby_index);
}

*isnull = value->isnull;
return value->value;
```

静态不支持应不植入引用；运行参数/能力不支持，应尽可能在首行输出前选择整次扫描的原路径。近似模式一旦开始输出，不允许逐行静默切换为精确 Cosine，造成混合比较和不可解释的结果。

EPQ/行锁首版在计划门拒绝。如果目标分支仍可能将新节点送入 EPQ，必须补足显式原表达式路径及测试，不能读取普通扫描遗留缓存。

## 10. 排序契约：必须独立通过的门

### 10.1 三件事不能混为一谈

1. 某个候选的导出值是否正确转换。
2. AM 已返回的这些候选是否按导出值非递减排列。
3. 候选是否包含全表真实 Top-K。

标量复用主要解决第 1 项；后续 R2 修复另行实现第 2 项的严格候选输出。第 3 项仍属于 ANN 召回契约，不能因 L2 数值精确、候选输出有序就宣称全表精确 Top-K。

### 10.2 修复前的风险与已确认问题

- 原有完整扩批后统一排序分支只在 `so->filter != NULL && scan->limitk > 0` 时生效。用户示例是 Post-Filter，不能假设它也走这条路径。
- “第一批有序”不等于“后来拼接的批次仍有序”。

后续 R2 已实测单批内逆序：节点代表值排序后展开了不同向量的 TID，即使各行投影精算，仍出现 1、0、2、1。修复前后证据及验收范围见测试报告。

### 10.3 修正方案：先完成候选批次，再发布有序行

本次按用户要求修复 R2，新增独立于复用 GUC 的基础排序协议：`off` 也必须避免既有逆序。只覆盖 DiskANN heap、单个 float8 ORDER BY 的执行路径；HNSW、index-only、Ustore 和 bucket 不进入此协议，不能由本轮测试推断这些路径均已解决有序性。

1. `AnnIndexOrderedNext()` 设置 `orderedBatch`，由 `diskann_advance_ordered_batch()` 显式启动首批/扩批。AM 的 gettuple 只消费本批，不在已输出部分结果后自行扩批。
2. 对本批每个候选，先经过原堆可见性及 index qual recheck，再执行扫描 WHERE 一次。可见且合格的行计算最终比较键：有效复用值，或显式 L2 heap recheck 的精确原表达式，或 off/不支持复用时的原表达式。
3. 完整消费当前批次后，才判断合格行数是否达到父 Limit 传入的需求（含 OFFSET）。不足则继续扩批，直至需求满足、搜索前沿耗尽或原有迭代上限。目标有 SRF 时不能用基本行数作为 Limit 满足证明，禁用这一提前终止条件。
4. 将所有已收集行统一放入 `tuplesort`，按最终 float8 比较键排序；**排序完成之后才返回第一行，返回后不再扩批**。因此后批更小值不会出现在已发布较大值之后，分支 Limit 也不会先截断错误次序。
5. 排序器受 `work_mem` 管理，可落临时文件。内部 envelope 为“距离 + 原堆 tuple 字节”，保留 ctid、tableoid、事务基值等系统属性；清空持久化头中的指针和无效成员，不能把 pin 释放后的堆页指针存入 sorter。取行使用本分支 `tableam_tslot_getattr()`。
6. 候选过滤在收集时执行一次，正常 ExecScan 暂时跳过同一 qual，异常路径必须恢复；用户投影、易变 target 和 SRF 延迟到真正输出行时执行。不能在收集候选时提前执行所有用户投影。
7. rescan 销毁 sorter、清理结果 slot、重新计算 runtime keys、清理 AM 候选与 epoch；end 同样释放。EPQ 替代 tuple 保留原有精确 qual 检查。

此方案会增加首行延迟、候选物化及排序成本，不能继续宣称仅增加一次标量开方。它保持 ANN 候选搜索的近似性，不保证全表 Top-K，也未完成性能准入。计划仍可为 `Limit -> Ann Index Scan`：排序是该节点的内部协议，不伪造额外计划节点或恢复距离诊断行。

### 10.4 UNION ALL 分支

所有参加同一次比较的分支必须使用同一数值单位。本文首版通道的契约是：L2 都是开方后的 SQL 距离；Cosine 都是余弦距离尺度，不能一支为负内积、一支为 1-cos。只用于排序的 L2 若以后增加“全比较域统一为精确平方 L2”的独立优化，也能保持顺序；但这不是只替换单个 ANN 分支可以自动获得的性质。

`cosine` / `all` 下，ANN 分支近似、普通分支精确是明确接受的质量差异，不是平方/开方那种单位错误；近距离排名仍可能变化，应按实际导出值验证归并，而不是要求结果与全精确 SQL 的 LIMIT 行集完全一致。

#### 10.4.1 Hint 实测：DiskANN 与 SeqScan 确实可同时进入 Merge Append

2026-09-14 在 Debug 构建 `098d77c8`、端口 `55440`、schema `union_all_topk_onecol_test` 上实测。下列 SQL 只使用 L2 操作符 `<->`，距离只是 `ORDER BY` 隐藏键，最终不返回给客户端。`indexscan` 强制 delta 分支使用 DiskANN，`tablescan` 强制 snapshot 分支使用 SeqScan：

```sql
SET search_path = union_all_topk_onecol_test, public;
SET enable_union_all_diskann_topk_pushdown = on;
SET enable_union_all_required_columns_pushdown = on;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = on;
SET enable_nestloop = off;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM (
    SELECT /*+ indexscan(dt delta_embedding_diskann) */
           dt.row_id, dt.title, dt.embedding
    FROM delta_t dt

    UNION ALL

    SELECT /*+ tablescan(st) */
           st.row_id, st.title, st.embedding
    FROM snapshot_t st
    WHERE NOT EXISTS (
        SELECT 1
        FROM delete_t dd
        WHERE dd.row_id = st.row_id
    )
) u
WHERE title IS NOT NULL
ORDER BY embedding <-> '[0.1,0.2,0.3,12.8]'::vector
LIMIT 4;
```

实际完整计划如下；两个 hint 均已生效，无 unused-hint 告警：

```text
                                                  QUERY PLAN
---------------------------------------------------------------------------------------------------------------
 Limit
   Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
   ->  Result
         Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
         ->  Merge Append
               Sort Key: ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
               ->  Limit
                     Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                     ->  Ann Index Scan using delta_embedding_diskann on union_all_topk_onecol_test.delta_t dt
                           Output: dt.row_id, (dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                           Order By: (dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                           Filter Mode: Post-Filter
                           Filter: (dt.title IS NOT NULL)
               ->  Limit
                     Output: st.row_id, ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                     ->  Sort
                           Output: st.row_id, ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           Sort Key: ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           ->  Merge Anti Join
                                 Output: st.row_id, (st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                                 Merge Cond: (st.row_id = dd.row_id)
                                 ->  Sort
                                       Output: st.row_id, st.embedding
                                       Sort Key: st.row_id
                                       ->  Seq Scan on union_all_topk_onecol_test.snapshot_t st
                                             Output: st.row_id, st.embedding
                                             Filter: (st.title IS NOT NULL)
                                 ->  Sort
                                       Output: dd.row_id
                                       Sort Key: dd.row_id
                                       ->  Seq Scan on union_all_topk_onecol_test.delete_t dd
                                             Output: dd.row_id
(32 rows)
```

该计划证明了需要防守的真实计划形状：

```text
Merge Append
  -> Limit -> Ann Index Scan    -- delta_t
  -> Limit -> Sort -> Seq Scan  -- snapshot_t
```

但这不是当前版本的错误计划。当前 AnnIndexScan 的 `Order By` 由 AM 内部平方 L2 维持顺序，而 `Output` 中的 `<->` 仍从堆元组计算 SQL L2；Snapshot Sort 也计算 SQL L2，所以 Merge Append 当前实际比较的是 `d` 与 `d`。`EXPLAIN` 中顶层和 Limit 显示距离不表示客户端选择了它；它是 Merge Append 必需的 resjunk 排序列。

危险的改法是只将 delta 分支的 Output 替换为 AM 原始平方 L2，却保留 snapshot 分支的 SQL L2：

```text
Merge Append
  Sort Key: hidden_distance
  -> DiskANN branch: hidden_distance = d²
  -> SeqScan branch: hidden_distance = d
```

例如 DiskANN 分支真实距离为 `5`、原始 AM 值为 `25`，SeqScan 分支真实距离为 `6`。Merge Append 若比较 `25` 与 `6`，会在 LIMIT 之前错误选择距离 `6` 的行。DiskANN 自身可能已正确找回距离 `5` 的候选，但该候选仍会被上层错误排除；这是归并键口径错误，不是 SeqScan “召回更高”或 DiskANN 原有召回率的一部分。

因此，对“L2 且距离只用于排序”的额外优化，只有在同一 Merge Append/Sort 比较域内所有行统一为精确 `d²` 时，才可以省略 `sqrt`。若不做全域统一改写，本文首版继续将 DiskANN 的 `d²` 通过一次标量 `sqrt` 转成 SQL L2，再供 Merge Append 消费。禁止用户在 SQL 中直接调用距离支持函数，不能解决此问题，因为分支间的 `d²/d` 差异由物理执行路径产生。

不改内核的完整 SQL 正确性模拟，包含 `delta_t UNION ALL (snapshot_t Anti Join delete_t)`、WHERE、分支 LIMIT 10、hint、实测计划与错误/正确 Top-10 对照，见 [DiskANN 与 SeqScan 混合距离尺度 SQL 模拟复现](../测试/DiskANN与SeqScan混合距离尺度_SQL模拟复现.md)。该用例中混合口径实测造成 5 条正确行遗漏、5 条错误行多出，Recall@10 为 0.50。

## 11. GUC、计划缓存与版本兼容

拟新增：`ann_orderby_distance_reuse`。

GUC 与 AnnOrderByRef 等共用接口保留 ANN 命名，适配能力仅对 DiskANN 开放。任一档位都不会为 HNSW 生成引用或改变其距离输出语义。

| 值 | 行为 |
|---|---|
| off | 默认；不生成引用，维持原距离投影 |
| l2 | 仅对已证明的 DiskANN 普通 L2 使用精确 AM 值 |
| cosine | 仅允许 DiskANN 使用本文定义的近似 Cosine；不复用 L2 |
| all | 同时启用 l2 与 cosine 两个能力位 |

内核使用两位能力掩码实现：`off=0`、`l2=1`、`cosine=2`、`all=3`。所有准入、执行期复核和 AM 发布检查都必须测试对应能力位，禁止再使用 `mode >= l2` 或 `mode == all` 这类依赖枚举顺序的判断，否则 `cosine` 会误开 L2，或 `all` 无法命中单项能力。

`all` 是 SQL 保留字，设置该档位时必须将值写成字符串：`SET ann_orderby_distance_reuse = 'all';`。`off`、`l2`和 `cosine` 也可统一加单引号。

要求：

- 会话级独立控制，不挂到列裁剪、Top-K 或 INCLUDE 开关下。
- mode 写入计划特性元数据；执行时重新验证实际会话/参数环境。
- GUC 变化应进入计划环境兼容性判断。Prepared statement 从 `cosine` / `all` 切到 `off` / `l2` 后，不能继续静默返回旧近似值；从 `l2` / `all` 切到 `off` / `cosine` 后，也不能继续复用 L2。
- 若目标分支无法直接让缓存计划重新规划，至少在整次扫描开始前选定统一 fallback，并验证原路径排序安全；不能在已输出部分行后改模式。
- 不在 GUC assign 回调中直接递归执行 SQL 或重建计划。
- 旧版本节点不能读取新 Expr Node；使用目标分支新分配的版本门，不借用 DISKANN_INCLUDE_VERSION_NUM。版本不足时不产生新节点。
- 首版不跨 Stream/远程发送引用节点；即使暂不支持，copy/read/write 也不能遗漏，因为本地缓存与计划复制同样会使用。

## 12. EXPLAIN 展示

### 12.1 Output 与 Order By 的区别

现有 [explain.cpp](../../../src/gausskernel/optimizer/commands/explain.cpp) 从 indexorderbyorig 打印 ANN 的 Order By，从 targetlist 打印 Output。[ruleutils.cpp](../../../src/common/backend/utils/adt/ruleutils.cpp) 在展开某些 Var 的表达式时会额外加括号。

新节点不是现有 Var。为了与计划中常见的“引用已产生结果”风格保持一致，`AnnIndexScan` 在打印自身 targetlist 时，必须为 `AnnOrderByRef` 展开的 `original_expr` 额外包一层括号。这层括号只是 EXPLAIN 的可读性约定，真正的执行语义仍由 `AnnOrderByRef` 提供。

不应在 `ruleutils.cpp` 的通用 `AnnOrderByRef` 反解析分支中无条件加括号，否则上层 `OUTER_VAR` 展开时还会再加一层，形成三层括号。正确做法是只在 `show_plan_tlist()` 打印产生者 `AnnIndexScan.Output` 时增加展示括号；上层节点仍由原有 `OUTER_VAR` 逻辑自然得到双括号。

修改前的目标分支：

```text
Limit
  Output: delta_t.row_id, ((delta_t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
  -> Ann Index Scan using delta_embedding_diskann on delta_t
       Output: delta_t.row_id, (delta_t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
       Order By: (delta_t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
       Filter: (delta_t.title IS NOT NULL)
```

设计预期，非实测输出：

```text
Limit
  Output: delta_t.row_id, ((delta_t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
  -> Ann Index Scan using delta_embedding_diskann on delta_t
       Output: delta_t.row_id, ((delta_t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
       Order By: (delta_t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
       Filter: (delta_t.title IS NOT NULL)
```

`Order By` 没有变成结果引用；`Output` 才是读取 AM 距离的位置。计划不再追加 `Distance Projection`、`Distance Quality`、`Distance Transform`、`Distance Fallback` 及一组运行时计数器；本特性在 EXPLAIN 中仅使用 `AnnIndexScan.Output` 的双括号展示约定，避免污染常用计划。`Filter Mode` 属于 DiskANN 过滤机制，不属于本距离复用特性。

### 12.2 同一完整 UNION ALL Top-K 计划的修改前后

本节使用第 `10.4.1` 节已经实测的典型结构：

```text
delta_t
UNION ALL
snapshot_t Anti Join delete_t
    ↓
ORDER BY embedding <-> query
LIMIT
```

对比重点不是 Sort、Limit 或 Join 的个数，而是 DiskANN 分支向 `Merge Append` 发布的隐藏排序列来源。

#### 12.2.1 修改前：当前实测计划

下列计划复用第 `10.4.1` 节的当前实测结果：

```text
                                                  QUERY PLAN
---------------------------------------------------------------------------------------------------------------
 Limit
   Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
   ->  Result
         Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
         ->  Merge Append
               Sort Key: ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
               ->  Limit
                     Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                     ->  Ann Index Scan using delta_embedding_diskann on union_all_topk_onecol_test.delta_t dt
                           Output: dt.row_id, (dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                           Order By: (dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                           Filter Mode: Post-Filter
                           Filter: (dt.title IS NOT NULL)
               ->  Limit
                     Output: st.row_id, ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                     ->  Sort
                           Output: st.row_id, ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           Sort Key: ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           ->  Merge Anti Join
                                 Output: st.row_id, (st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                                 Merge Cond: (st.row_id = dd.row_id)
                                 ->  Sort
                                       Output: st.row_id, st.embedding
                                       Sort Key: st.row_id
                                       ->  Seq Scan on union_all_topk_onecol_test.snapshot_t st
                                             Output: st.row_id, st.embedding
                                             Filter: (st.title IS NOT NULL)
                                 ->  Sort
                                       Output: dd.row_id
                                       Sort Key: dd.row_id
                                       ->  Seq Scan on union_all_topk_onecol_test.delete_t dd
                                             Output: dd.row_id
(32 rows)
```

这个计划中 DiskANN AM 内部使用平方 L2 `d²` 搜索和排序，但 `Ann Index Scan` 的 Output 仍由执行器计算原始 `<->` 表达式，生成 SQL L2 `d`。因此 `Merge Append` 当前比较的是：

```text
DiskANN 分支 Output  = 执行器重算 d
Snapshot 分支 Output = SeqScan/Sort 计算 d
```

单位正确，但 DiskANN 分支对已在 AM 中完成的距离又做了一次 O(dim) 向量计算。

#### 12.2.2 修改后：设计预期计划，非已实测输出

修改后的计划拓扑预期保持不变。优化器只把 DiskANN targetlist 中经严格匹配的距离投影表达式换成 `AnnOrderByRef`；`EXPLAIN` 仍用 `original_expr` 展示原 SQL 表达式，并只在产生该引用的 `AnnIndexScan.Output` 上增加一层括号。这里的括号是计划可读性标记，不是新的 SQL 运算：

```text
                                                  QUERY PLAN
---------------------------------------------------------------------------------------------------------------
 Limit
   Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
   ->  Result
         Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
         ->  Merge Append
               Sort Key: ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
               ->  Limit
                     Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                     ->  Ann Index Scan using delta_embedding_diskann on union_all_topk_onecol_test.delta_t dt
                           Output: dt.row_id, ((dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           Order By: (dt.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                           Filter Mode: Post-Filter
                           Filter: (dt.title IS NOT NULL)
               ->  Limit
                     Output: st.row_id, ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                     ->  Sort
                           Output: st.row_id, ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           Sort Key: ((st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector))
                           ->  Merge Anti Join
                                 Output: st.row_id, (st.embedding <-> '[0.1,0.2,0.3,12.8]'::vector)
                                 Merge Cond: (st.row_id = dd.row_id)
                                 ->  Sort
                                       Output: st.row_id, st.embedding
                                       Sort Key: st.row_id
                                       ->  Seq Scan on union_all_topk_onecol_test.snapshot_t st
                                             Output: st.row_id, st.embedding
                                             Filter: (st.title IS NOT NULL)
                                 ->  Sort
                                       Output: dd.row_id
                                       Sort Key: dd.row_id
                                       ->  Seq Scan on union_all_topk_onecol_test.delete_t dd
                                             Output: dd.row_id
(26 rows)
```

这里的 `(26 rows)` 是与当前简化版展示对应的结构示例；不同 openGauss 分支可能因 `EXPLAIN` 的其他属性而有不同的行数，不应把行数本身作为功能契约。

修改后各分支向全局比较域提供：

```text
DiskANN 分支 Output  = sqrt(AM 精排 d²) = d
Snapshot 分支 Output = SeqScan/Sort 计算 d
```

因此不会形成 `d²/d` 混合排序，`Merge Append` 的排序契约保持不变。

#### 12.2.3 关键变化汇总

| 观察项 | 修改前 | 修改后 |
|---|---|---|
| 计划拓扑 | `Limit → Result → Merge Append` | 不变 |
| DiskANN `Order By` | 原 `<->` 搜索要求 | 不变，不替换为结果引用 |
| DiskANN AM 内部分数 | 精排平方 L2 `d²` | 不变，保留搜索所需尺度 |
| DiskANN Output 的值来源 | 执行器重算 `<->` | `AnnOrderByRef` 读取 AM 输出缓存 |
| DiskANN 对外值 | 重算的 `d` | `sqrt((double)d²)` 生成的 `d` |
| Snapshot 分支 | SeqScan/Sort 计算 `d` | 不变 |
| Merge Append 比较单位 | `d` 对 `d` | 仍为 `d` 对 `d` |
| 向量计算开销 | DiskANN 完整距离 + 投影重算 | DiskANN 完整距离 + O(1) 标量开平方 |
| 计划展示 | DiskANN 产生者的 Output 与普通表达式相同 | 产生者 Output 用双括号标记引用；不增加额外诊断字段 |
| 预期 Top-K 结果 | 基线 | exact L2 模式不应改变 |

需特别避免把下面这种形态误写成“修改后计划”：

```text
Merge Append
  Sort Key: hidden_distance
  -> DiskANN branch: hidden_distance = d²
  -> SeqScan branch: hidden_distance = d
```

这不是本设计的目标，而是第 `10.4.1` 节已说明的错误改写。如果实现后 `EXPLAIN ANALYZE` 显示了 AM 距离读取，但 delta/snapshot Top-K 变成了该错误口径的结果，必须判定为正确性失败，不能归因为 ANN 召回波动。

### 12.3 可观测性要求

普通 EXPLAIN 只用 `AnnIndexScan.Output` 的双括号说明计划中存在结果引用，不宣称已经发生缓存命中，也不打印实现细节。`EXPLAIN ANALYZE` 同样保持常用计划简洁；AM 内部仍可维护完整距离计算、标量转换、发布和引用读取等计数，供调试器或专用测试接口使用，但这些计数不再成为公开计划格式的一部分。

结果正确性通过 SQL 结果对照、GUC 模式、rescan、HOT、过滤回退和 UNION ALL 有序性用例看护。没有实际执行、LIMIT 0、从未进入的分支，内部计数仍须为零；这属于执行器状态契约，不要求在 EXPLAIN 文本中逐项展开。

## 13. 回表、列生命周期与性能边界

命中复用后，为这个距离表达式生成值不再需要从堆表取得 embedding。实际查询仍可能回表取得 row_id、检查 title 和 MVCC。

如果 embedding 内联在同一个堆页中，访问该页不等于能避免读取所有向量字节；本特性不保证堆页 I/O 减少。向量为外部 TOAST 存储且没有其他消费者时，可避免仅为该投影产生的解 TOAST/解压及距离计算。

以下情况仍需向量：SELECT embedding、其他向量表达式、距离过滤、受控回退。不能以 AnnOrderByRef 存在为由删除它们的输入依赖。

首版不主动修改 CBO 代价公式和 Join 类型选择。若后续调整投影 CPU 成本，须独立提交、独立归因，不能把计划形状变化和距离复用的收益混在一起。

## 14. 正确性与测试要求

| 类别 | 必测内容 | 通过标准 |
|---|---|---|
| L2 数值 | 小向量、相等向量、多维、不同符号、大/小幅值、同一返回 ID | 与同一行原始 SQL 距离的 float8send 一致；特殊值按基线语义处理 |
| Cosine 近似 | 不同模长、正交、同向/反向、近相等距离、受控随机向量 | 与规定的归一化负内积转换口径一致；记录对原 SQL 的误差，不虚报逐位一致 |
| 表达式绑定 | 两个相同距离输出、不同 q、参数、cast、别名、未输出距离 | 正确复用或明确回退，不误合并、不丢隐藏排序列 |
| WHERE | title 过滤、过滤全部、过滤后需续扫、距离阈值条件 | 不使用近似输出值替代精确过滤，不改变求值边界 |
| 生命周期 | PREPARE/EXECUTE、rescan、更换参数、LIMIT 0、多次游标取数 | 不使用上一行/上一轮/上一参数的值 |
| 数据变化 | INSERT/UPDATE/DELETE/VACUUM、HOT 链、重复向量多个 TID、槽位回收 | 输出值与有效返回行对应，不错误要求根 TID 等于可见 HOT TID，无悬空指针、崩溃、泄漏 |
| 计划 | DiskANN 两距离、两表达式引擎、GUC 四档、回退路径 | DiskANN 引用在自身 Output 显示为双括号；Order By 原要求保留；非 DiskANN 不生成引用 |
| UNION ALL | 纯 ANN 分支、ANN+普通 Sort、Anti Join、列裁剪开关组合 | 单位一致，必要排序和 LIMIT 位置正确，列号不串 |
| 有序性 | DiskANN 首批、多轮扩批、Post-Filter、严格合批条件 | 按实际导出值检查序列；未知有序能力不能通过 Merge Append 集成门 |
| 兼容性 | copy/read/write、旧版本门、codegen/ByPass/远程拒绝 | 没有未知 NodeTag/Opcode，无静默错误解释 |

结果保存不得改变被测计划的准入：直接 `CREATE TABLE AS SELECT`/`INSERT SELECT` 可能引入修改节点，让近似复用根本未执行。当前用例通过 PL/pgSQL `RETURN QUERY EXECUTE` 执行独立原 SELECT 后保存结果，另用同一 SELECT 的 EXPLAIN 看护。Cosine 转换误差断言须为 0 条不匹配，而“与精确 SQL 有差异”在该固定数据中应非零；不能把错误的 9 条转换不匹配写入 golden 并宣称通过。

测试必须接入 openGauss 原有 `src/test/regress` SQL + expected + schedule，另用隔离会话编排补充并发检查。EXPLAIN 的双括号用于计划看护；结果对照、GUC 切换、rescan、HOT 与 fallback 用例共同负责语义正确性看护。

## 15. 代码量、阶段边界与交付口径

以下按仅适配 DiskANN 的范围估算。专用引用节点、计划基础设施、双表达式引擎和通道有效性仍是主要工作，减少 AM 数量不会使代码量按比例减半：

| 内容 | 规划预算，不是实测统计 |
|---|---:|
| DiskANN 普通 L2 / 近似 Cosine、引用节点、双执行路径、GUC、兼容及诊断 | 约 1000～1700 行新增或实质修改内核代码 |
| DiskANN 原生 SQL + expected | 约 700～1300 行 |
| 严格扩批排序的新实现、量化/mmap/UStore/并行扩展 | 不包含在上述预算，需单独设计估算 |

不能为了维持早期行数承诺省略节点协议、缓存失效或错误回退。最终交付必须报告实际 diff，并区分内核、SQL、expected、文档。

准入结论按三个层次分别报告：

1. AM 标量转换和结果引用已实现、距离口径通过；
2. 支持矩阵及生命周期回归通过；
3. 目标 UNION ALL 有序集成和实际性能通过。

未通过第 3 层时，不能仅凭新 Output 形态宣称目标场景已经完整交付。

## 16. 开发前必须回答的检查表

- [ ] DiskANN 的距离来源和现有候选缓存已列清，不依赖注释中的 exact 字样。
- [ ] 规划和执行均核验实际 DiskANN 身份，其他 AM 不请求通道、不生成引用。
- [ ] 近似 Cosine 的用户开关、单位、范围和精确消费者拒绝规则已确认。
- [ ] 新节点建立在 setrefs 后的明确阶段，后续所有消费者均认识它。
- [ ] original_expr 不在正常投影路径被提前求值，但依赖和权限信息完整。
- [ ] 返回值具备 epoch / row / TID / quality 校验，rescan 和续扫不会串值。
- [ ] xs_orderbyvals 的遗留口径和通用消费者已处理，不滥用 lower-bound 契约。
- [ ] DiskANN 首批、扩批、多个 TID、HOT 关联及 rescan 均有测试或明确的能力拒绝看护。
- [ ] 直接进入 Merge Append 的流有序性已有证明或保守拒绝方案。
- [ ] 不重新开放 INCLUDE，不承诺消除整个回表，不改写 WHERE 精确语义。
- [ ] 两套表达式引擎、版本门、计划缓存、GUC 切换均有回归。
- [ ] 使用统一 full 构建脚本验证涉及头文件的新布局；不拿旧二进制验收。
- [ ] 实施与报告入口见[开发实施指南](ANN距离结果复用开发实施指南.md)。
