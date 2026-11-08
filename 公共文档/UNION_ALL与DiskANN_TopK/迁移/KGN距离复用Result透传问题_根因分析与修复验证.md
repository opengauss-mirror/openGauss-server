# KGN 距离复用 Result 透传问题：根因分析、修复与验证

本文针对以下问题：湖表视图的 snapshot 分支已经使用 KGN 索引，距离也已经进入 Index Scan 的 targetlist，但扫描节点的 Output 仍显示单层括号；怀疑上层 Result 的安全检查阻止了 AM 距离复用。

编写日期：2026-10-30。

**交付状态（2026-10-30 实测更新）：已全量编译源仓集成代码，按用户补充的视图/SQL 构造 1024 维小数据复现，并编译验证“无 WHERE 也进入列裁剪”的最小增强。修复前后各 60 组查询矩阵、各 12 项消费者检查完成；修复后原生回归 7/7 通过。目标 KGN 仓不在本机，尚未运行目标二进制，不能将源仓结果表述为目标已经修复。**

**先看第 14 节实测结论：当前源仓的原因不是 Result 节点类型未放行，而是无 WHERE 被列裁剪入口拒绝，导致排序键没有跨 UNION 物化，Result 仍需计算 OpExpr。直接放行 Result 不是本次验证通过的方案。第 3～5 节保留对其他目标实现的条件性诊断方法；以第 14 节事实区分适用分支。**

## 1. 先读结论与使用顺序

### 1.1 当前可以确定什么

1. 源仓 `ann_orderby_plan_is_unsafe()` 返回 **false 表示通过这道检查**，不是阻断原因。
2. 源仓 `ann_orderby_node_allows_approx()` 返回 **false 表示当前节点不允许近似距离透传**。它会令当前子树的 `approximatePlanSafe` 变为 false。
3. 用户提供的计划中，snapshot 扫描已有距离表达式。这只能证明存在局部距离 TLE；不能单凭这一点证明列裁剪入口执行、排序键成为 UNION 可见列或上层引用链完整。源仓实测表明这些阶段必须分别验证。
4. Result 不天然阻断复用。源仓允许满足约束的 `T_BaseResult` 透传；现有回归期望文件中也有 `Result → Merge Append → … → Ann Index Scan` 成功复用的计划。
5. 目标 KGN 的 L2 和 Cosine 均按近似距离契约处理。源仓精确 L2 绕开部分近似消费限制的做法，不能直接照搬。
6. **单层括号不构成目标仓“没有复用”的独立证明。** 源仓额外括号的显示分支限定为 AnnIndexScan；目标普通 IndexScan 如果只迁移了计算逻辑、漏迁显示适配，内部已是 AnnOrderByRef 也可能显示单层。

### 1.2 推荐执行顺序

1. 先看第 14 节源仓实测，再按第 4 节观察目标真实节点和拒绝分支，定位第一次从“允许”变成“不允许”的位置。
2. 如果确认为普通 Result 被误拒绝，再按第 5 节修复；如果已注入引用而只是显示不符，走显示适配分支。
3. 按第 6～9 节分别验证计划、实际取值和查询语义。
4. 按第 10 节进入目标仓原生回归并记录证据。
5. 最后填写第 11 节验收记录，明确“已验证、未覆盖、不适用”，不把未覆盖写成通过。

**修复目标：先保证排序键投影与槽位链完整，再允许已证明透明的 Result 透传。源仓实际修复只扩大列裁剪的无 WHERE 适用范围，未修改 Result/Join 安全检查；不改变距离公式和候选搜索策略。**

### 1.3 核对范围

| 项目 | 本次实际核对内容 |
|---|---|
| 源仓 | `/home/qiumc/data/code/cpp/openGauss-server` |
| 分支 | `integration/multimodal-stacked` |
| 源仓 HEAD | `ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4` |
| 源仓工作区 | 初始无已跟踪修改；本次留下 preptopk.cpp 最小增强、新原生 SQL/expected 及 schedule，未提交；无关未跟踪资产未手工处理 |
| 目标仓 | 根据用户历史信息为 GV/KGN；本次没有目标源码访问证据 |
| 目标源文件线索 | `optimizer_plan_optplan_optplanner.cpp`，具体路径和行号需目标仓重新定位 |
| 目标计划 | 用户粘贴的 zhongyan 湖表查询计划，查询向量被替换为 `[xxx]` |
| SQL 原文 | 用户已补充完整视图定义和 SELECT：无外层 WHERE、重复三次 id、LIMIT 100；向量仍以 [xxx] 脱敏 |
| 实际执行 | 使用新编译源仓 DiskANN 实例 55450、独立测试库；真实 SQL/计划/断点/回归输出见第 14 节，未执行 KGN 目标二进制 |

本文源仓行号主要对应上述基线版本；候选修改会使后续行号略变。迁移按函数职责定位，不按行号粘贴。

## 2. 问题现象与计划证据

### 2.1 用户提供的计划

下面保留用户计划的节点、输出和成本，整理缩进及一处粘连换行。`[xxx]` 是用户脱敏占位，不是可执行向量。

~~~text
Limit  (cost=33.14..35.64 rows=100 width=32)
  Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024)))
  -> Result  (cost=33.14..38.14 rows=100 width=32)
       Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024))), (Expression Flatten Optimized)
       -> Merge Append  (cost=33.14..38.14 rows=100 width=32)
            Sort Key: ((zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024)))
            -> Limit  (cost=32.01..32.26 rows=100 width=56)
                 Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024)))
                 -> Sort  (cost=32.01..33.10 rows=100 width=56)
                      Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024)))
                      Sort Key: ((zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024)))
                      -> Seq Scan on d_lake_zhongyan.zhongyan_10yi_dt1  (cost=0.00..15.43 rows=434 width=56)
                           Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", (zhongyan_10yi_dt1.embedding <-> '[xxx]'::floatvector(1024)), (Expression Flatten Optimized)
            -> Limit  (cost=1.12..1.37 rows=100 width=56)
                 Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <-> '[xxx]'::floatvector(1024)))
                 -> Nested Loop Anti Join  (cost=1.12..2500008.81 rows=100 width=56)
                      Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <-> '[xxx]'::floatvector(1024))), (Expression Flatten Optimized)
                      -> Index Scan using zhongyan_10yi_vsi_001 on d_lake_zhongyan.zhongyan_10yi_snap1 s  (cost=1.12..3.88 rows=100 width=64)
                           Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", (s.embedding <-> '[xxx]'::floatvector(1024)), s.row_id
                           Order By: (s.embedding <-> '[xxx]'::floatvector(1024))
                      -> Index Only Scan using zhongyan_10yi_del1_idx on d_lake_zhongyan.zhongyan_10yi_del1 d  (cost=0.00..0.41 rows=9 width=8)
                           Output: d.row_id
                           Index Cond: (d.row_id = s.row_id)
~~~

### 2.2 这份计划能说明什么

**第一，原始向量已经不作为 snapshot 扫描的独立输出列传给 Anti Join。** 它输出的是 `s.embedding <-> query_vector` 的标量结果，另保留 `s.row_id` 供删除匹配。

**第二，Anti Join 上的双括号通常表示读取子计划已经计算好的槽位。** 这是排序键/表达式结果复用，不等于已经读取 AM 的距离。扫描层完全可能执行一次 SQL 距离函数，再由上层引用它。

**第三，delete 分支按 row_id 做相关查找。** 从可见计划看，没有用距离决定删除匹配。目标代码仍须检查真正的 joinqual、plan.qual、nestParams、其他隐含条件，不能只凭文本排除所有消费者。

**第四，Result 从输出文本上看是透传，但这是待内部节点验证的判断。** 文本没有显示 Filter/One-Time Filter，仍不能据此证明所有内部字段为空。

**第五，混合分支本身不是拒绝理由。** delta Seq Scan 使用 SQL 距离，snapshot KGN 使用经过目标公式转换的近似距离，这是本项目已接受的契约。两者需要同一度量和尺度；不能一边用 L2、一边用平方 L2。

### 2.3 不能从计划文本直接得出的结论

- 不能从 `Result` 字样断言内部一定是 `T_Result` 或 `T_BaseResult`。
- 不能从单层括号断言目标内部一定没有 `AnnOrderByRef`。
- 不能从 `Expression Flatten Optimized` 断言 targetlist 中存在某种包装节点，或断言一定运行了哪套表达式引擎。应定位目标输出该标记的代码及实际执行入口。
- 不能从 cost、width 或 rows 的差异证明是否重复计算了距离。
- 不能把上层 OUTER_VAR 引用与扫描层 AM 引用混为一谈。

源仓 EXPLAIN 在 [explain.cpp](../../../src/gausskernel/optimizer/commands/explain.cpp:3726) 中只对 `AnnIndexScan + AnnOrderByRef` 增加一层展示括号。目标的普通 IndexScan 必须有对应适配；没有这项显示适配时，以真实表达式节点和执行器路径为准。

## 3. 源仓机制与条件性根因分析

### 3.1 两个布尔函数不能混淆

| 检查 | true | false |
|---|---|---|
| `ann_orderby_plan_is_unsafe()` | 检测到不支持的执行域，跳过整个复用改写 | 通过这道检查，继续后续判定 |
| `ann_orderby_node_allows_approx()` | 当前节点允许近似距离透传，但不能推翻祖先的拒绝 | 当前节点及其下层不能依赖这条透传通路使用近似距离 |

源仓入口的关键逻辑：

~~~cpp
// 源仓逻辑节选：is_unsafe 为 true 才退出。
if (g_instance.role != VSINGLENODE ||
    t_thrd.proc->workingVersionNum < ANN_ORDERBY_DISTANCE_REUSE_VERSION_NUM ||
    ann_orderby_plan_is_unsafe(glob, topPlan)) {
    return;
}
~~~

`ann_orderby_plan_is_unsafe()` 调用的 walker 对 Stream、RemoteQuery、LockRows、ModifyTable 及相应列式节点返回 true；没有单独将普通 BaseResult 判为 unsafe。

因此，如果调试记录只有“is_unsafe(Result) 返回 false”，这条记录实际上不能支持“Result 导致阻断”的结论。应继续追踪真正设置 `approximatePlanSafe=false` 的位置。

### 3.2 正确调用顺序

下面是相关阶段顺序，不是把几个函数虚构成直接相互调用：

~~~text
standard_planner / 目标等价规划入口
  ├─ Query 阶段的 Top-K、列裁剪及排序键投影处理
  ├─ 构造物理计划
  ├─ push_down_union_all_source_projections(...)
  │    └─ 安全时把 Anti Join 上的距离投影送到保留侧 Scan
  ├─ set_plan_references(...)
  │    └─ 上层相同表达式变成 OUTER_VAR / INNER_VAR 槽位引用
  ├─ 完成相关子计划的 setrefs 及其他必需后处理
  └─ ann_orderby_distance_reuse != off 时：
       rewrite_ann_orderby_distance_refs(...)
         ├─ 全计划执行域检查
         └─ 逐节点近似值透传检查
              └─ 合格 KGN 扫描内：
                   距离 OpExpr → AnnOrderByRef

随后在执行初始化阶段：
  表达式编译/初始化 → AM 距离发布 → 行身份绑定 → 引用取值
~~~

源仓投影下推调用位于 [planner.cpp:1353](../../../src/gausskernel/optimizer/plan/planner.cpp:1353)，setrefs 位于其后；复用注入入口位于 [planner.cpp:1531](../../../src/gausskernel/optimizer/plan/planner.cpp:1531)。

目标仓可以有不同函数划分，但必须保留这些阶段之间的数据依赖。若目标在规划阶段预先生成了表达式执行状态，必须确保注入发生在对应状态生成前，或者正确重建状态，不能只改 Plan 而沿用旧执行状态。

### 3.3 Result 为什么可能误阻断整个 snapshot 分支

源仓逻辑：

~~~cpp
bool parentApproximatePlanSafe = walkerCxt->approximatePlanSafe;

if (!ann_orderby_node_allows_approx(node)) {
    walkerCxt->approximatePlanSafe = false;
}

// 当前节点的注入判定和对子节点的递归在这个上下文中执行。
// 近似质量的注入必须尊重 approximatePlanSafe。

bool found = plan_tree_walker(node, walker, context);

// 离开子树时恢复，防止影响兄弟分支。
walkerCxt->approximatePlanSafe = parentApproximatePlanSafe;
return found;
~~~

如果 Result 在入口处把状态从 true 改为 false，则后面的 Merge Append、snapshot Limit、Anti Join 和 IndexScan 都继承 false。后代节点即使自身检查通过，也不能主动恢复为 true。

这解释了“Join 透传代码已迁移，但扫描仍不注入”：**Join 检查通过只是必要条件，祖先 Result 的拒绝仍然有效。**

也要区分两种位置：位于 Merge Append 上面的不安全 Result 合理地约束两个分支；只位于某一个分支里的不安全节点，不得污染另一个兄弟分支。

### 3.4 需要逐项证实的根因分支

| 编号 | 可能原因 | 必须看到的证据 | 修复方向 |
|---|---|---|---|
| R0 | 把 false 的含义理解反了 | is_unsafe=false，后续实际仍继续执行 | 不改该谓词；继续找真正退出点 |
| R1 | Result 的实际 NodeTag 未适配 | 真实标签进入 allows_approx 的 default；结构和字段证明它是支持范围内的普通透传节点 | 适配真实标签，保留字段和公共检查 |
| R2 | 检查早于 setrefs | Result 的距离项仍是完整 OpExpr；调用栈证明尚未完成引用转换 | 调整调用顺序；不要把全部 OpExpr 放行 |
| R3 | Result 确有额外表达式或条件 | qual、resconstantqual、额外过滤字段非空，或 targetlist 不是纯透传 | 判断是真实消费者还是已证明无害的包装；本次默认保持保守拒绝 |
| R4 | KGN 使用普通 IndexScan，但扫描准入只认识 AnnIndexScan | 普通 IndexScan 进入未知节点分支，或注入入口根本不匹配它 | 按目标 KGN 扫描类型适配两处入口并保留 AM 身份校验 |
| R5 | 上下文传播或恢复错误 | 当前安全路径的状态受前一个兄弟节点污染，或祖先拒绝被后代重置 | 修复保存/继承/恢复关系；正反排列都验证 |
| R6 | 注入匹配或 AM 资格未满足 | 透传状态始终 true，但质量未获准，或 equal 不命中 | 核对 AM/opclass/操作符/类型/ORDER BY 表达式等，不绕开资格检查 |
| R7 | 已注入，EXPLAIN 显示适配遗漏 | 真实扫描 targetlist 是 AnnOrderByRef，但文本仍单层 | 只修目标 IndexScan 的引用显示，不重复改注入 |
| R8 | 已注入，但执行时没有使用 AM 值 | 引用存在，实际执行进入 fallback 或绑定失败 | 排查运行期资格、发布值、质量和行绑定；不再改 Result 白名单 |
| R9 | 列裁剪入口未通过，跨 UNION 排序键投影未执行 | 源仓无 WHERE 实测：入口 outer_query_shape 拒绝，Result 保留 resjunk OpExpr，扫描没有可替换的距离 TLE | 让该安全查询形态进入完整列裁剪/排序键下沉链，不放宽 Result 白名单 |

**目标仓属于哪一个分支仍需目标代码证据。源仓实测命中下述 R9，Result 的真实标签为 T_BaseResult，既有标签识别正常，setrefs 的调用顺序也正常，不能将 R1/R2 当作本次实测根因。**

### 3.5 Result 结构不能只看名字

核对版本的源仓中存在两种不同结构：

~~~cpp
// 普通行执行路径。
typedef struct BaseResult {
    Plan plan;
    Node* resconstantqual;
} BaseResult;

// USE_SPQ 下的另一种 Result，不等价于简单改名的 BaseResult。
typedef struct Result {
    Plan plan;
    Node* resconstantqual;
    int numHashFilterCols;
    AttrNumber* hashFilterColIdx;
    Oid* hashFilterFuncs;
} Result;
~~~

后一种结构的哈希过滤可能计算分发目标并丢弃不属于当前执行分段的行。**因此不能机械新增 `case T_Result: return true;`，也不能把两种结构直接按同一种布局强转。**

目标仓是否有这些字段尚未确认。如果有，本次最小修复要求未启用这些额外行为；若希望支持，应作为额外能力证明其距离消费、分布式执行和序列化契约，不顺手放开。

源仓 `T_Result` 还受 `USE_SPQ` 条件编译影响。目标适配时需要核对枚举是否存在、是否为别名，避免重复 case 或引用不存在的类型。EXPLAIN 名称相同并不能解决这些问题。

### 3.6 L2 与 Cosine 的目标差异

源仓精确 L2 的注入分支没有使用 `approximatePlanSafe`，Cosine 分支有该限制。目标 KGN 的两个度量都为近似质量，应使用目标的近似消费者约束。

不得为解决“L2 不能复用”而把 KGN L2 标成 `EXACT_L2`。这样虽然可能绕过 Result 拒绝，但也会绕过距离参与匹配、过滤的保护。

同样，不重新设计距离转换：L2 使用目标已有 L2 转换公式，Cosine 使用目标已有 Cosine 转换公式；已转换值不能再转换一次。Cosine 操作符为目标 `<+>`，不是源仓的 `<=>`。

## 4. 根因确认：先观察，再修改

### 4.1 固定环境与输入

先记录目标源码 commit、dirty diff、实际运行二进制路径及构建标识。`version()` 仅辅助确认，不能代替安装和运行文件的一致性核对。

保存以下信息：

~~~sql
SELECT version();
SHOW enable_union_all_diskann_topk_pushdown;
SHOW enable_union_all_required_columns_pushdown;
SHOW ann_orderby_distance_reuse;

SELECT pg_get_indexdef(
    'd_lake_zhongyan.zhongyan_10yi_vsi_001'::regclass);
~~~

如目标保留该参数，再记录：

~~~sql
SHOW enable_expr_fusion;
~~~

同时保存真实原始 SQL、完整查询向量、索引定义、opclass、统计信息和 hint。不要从脱敏 `[xxx]` 推测距离值，也不要只保留截图。

### 4.2 在目标仓按符号定位

以下是只读定位命令，仓库路径按目标实际位置调整：

~~~bash
cd /usr1/GaussVector/server
git rev-parse HEAD
git status --short
rg -n 'ann_orderby_plan_is_unsafe|ann_orderby_node_allows_approx' src
rg -n 'rewrite_ann_orderby_distance|set_plan_references|AnnOrderByRef' src
rg -n 'typedef struct (Result|BaseResult)|numHashFilterCols' src
rg -n 'Expression Flatten.*Optimized|Expression FlattenOptimized' src
~~~

若函数改名，定位职责等价的入口。目标 node copy/equal/walker 接口可与源仓不同，不要求机械保留源函数名。

### 4.3 每层必须记录哪些字段

对 `Limit → Result → Merge Append → snapshot Limit → Anti Join → IndexScan`，记录：

| 内容 | 用途 |
|---|---|
| 节点地址、真实 NodeTag、父节点或访问路径 | 区分同名 Result 和不同分支 |
| 进入时的 approximatePlanSafe | 判断拒绝是不是来自祖先 |
| 当前检查结果与具体拒绝理由 | 定位第一个阻断点 |
| plan.qual 是否为空 | 区分透传与逐行过滤 |
| Result.resconstantqual 是否为空 | 区分纯投影与一次性过滤 |
| Result 其他执行字段 | 排除哈希过滤、路由等隐藏消费者 |
| 每个 TLE 的 resno、resjunk、表达式 NodeTag | 证明距离是不是 resjunk，透传是否已变成 Var |
| Var 的 varno、varattno、类型 | 确认引用哪个子计划的哪个槽位 |
| 扫描进入时的 mode、质量判断与 AM 身份 | 排除 GUC/质量/opclass 原因 |
| 扫描距离项与 ORDER BY 原表达式的 equal 结果 | 确认注入匹配 |
| 退出当前子树后恢复的上下文 | 验证兄弟隔离 |

优先用调试器或临时 DEBUG 日志。记录节点标签和槽位即可，不要在每层无差别打印 1024 维向量，导致证据难以阅读。

GDB 中 `nodeTag` 往往是宏，不能保证可以直接调用。可在检查函数入口观察：

~~~text
break ann_orderby_node_allows_approx
continue
print ((Node*)node)->type
print ((Plan*)node)->qual
~~~

只有确认节点确实是 Plan 后才做 Plan 强转；`T_List` 等容器不是 Plan。确认 Result 的实际类型后，才能读取对应 resconstantqual 和其他字段。

对调用者观察 `walkerCxt->approximatePlanSafe`，对具体 return 行设断点。若优化构建显示 `<optimized out>`，使用目标仓受支持的调试构建或加入定点诊断，不靠猜测补值。

### 4.4 建议的临时诊断记录格式

下面只是日志格式示意，不是本次运行结果：

~~~text
node=<地址> tag=<实际标签> path=<访问路径>
parent_safe=<true/false> node_allowed=<true/false>
reason=<unknown_tag / plan_qual / one_time_qual /
        result_extra_behavior / non_passthrough_tle /
        join_consumes_distance / allowed>
tle_resno=<编号> expr_tag=<实际标签> resjunk=<true/false>

scan=<地址> am=<真实AM> mode=<当前值>
quality_allowed=<true/false> orderby_match=<true/false>
injected=<true/false> expr_before=<标签> expr_after=<标签>
~~~

如果增加拒绝原因枚举，应只用于诊断或遵循目标现有调试机制，不必因此修改用户 EXPLAIN 格式或引入一套长期公共接口。诊断日志不能修改检查结果。

### 4.5 明确的定因判据

确认 R1 必须同时具备：

1. 第一个拒绝点是实际 Result 的未知 NodeTag 分支。
2. 它的条件字段为空，输出是纯子槽位透传或常量，没有额外执行行为。
3. 它属于目标已支持的本地执行路径。
4. 其他下游条件、AM 资格及表达式匹配通过。
5. 修复后同一 SQL 的该节点通过，扫描成功注入；修复前对照确实失败。

确认 R2 必须同时证明“检查时不是 Var”和“检查发生在 setrefs 前或输出引用转换未完成”。仅看到一个 OpExpr，不足以断言调用顺序错误；它也可能本来就是 Result 自己需要计算的表达式。

如果扫描已经有 AnnOrderByRef，立即转查 R7/R8，不再扩大 Result 白名单。

## 5. 修复方案与代码约束

### 5.1 推荐修改范围

按已证明的根因选择修改范围：R1 才修改 Result 识别，R2 才调整阶段顺序；R9 则修复列裁剪/排序键投影适用范围。源仓本次采用 R9 方案，详见第 14 节。不要为了当前问题整体覆盖源 planner 文件。

已有 Join 保护、Hash 透传、共享 targetlist 修复、AM 发布和执行器绑定均应保留。没有证据时，不改搜索算法、距离公式、列裁剪逻辑和 Top-K 下推规则。

### 5.2 若根因是 R1：补充真实 Result 标签

以下是**目标适配示例，不是可直接应用的目标补丁**。假定目标的普通节点确实叫 Result，且其布局已经审核。

~~~cpp
switch (nodeTag(node)) {
    // 原有 List、Scan、Join 等分支保留。

    case T_Result: {
        Result* result = (Result*)node;

        // 本次只放行无一次性过滤条件的普通透传 Result。
        if (result->resconstantqual != NULL) {
            return false;
        }

        // 如果目标 Result 还有哈希过滤、路由或其他执行字段，
        // 必须在这里检查本次路径没有启用这些额外行为。
        // 不得因为这份示例未列出目标字段，就默认它们安全。

        // 不能直接 return true：
        // 还要经过原有 plan.qual 和 targetlist 公共检查。
        break;
    }

    // 原有其他分支保持。
    default:
        return false;
}
~~~

如果目标确实具有源仓 SPQ 的字段，本次保守检查至少包含：

~~~cpp
// 只在目标结构真实存在此字段时编写；不是通用可复制代码。
if (result->numHashFilterCols != 0) {
    return false;
}
~~~

这个检查不等于支持 SPQ/分布式执行。全计划执行域限制仍然有效。

如果目标使用 BaseResult，就在正确的 `T_BaseResult` 分支检查，不额外添加虚假的 `T_Result`。如果两种类型都存在，分别用正确结构读取各自字段，不依赖偶然相同的结构前缀。

### 5.3 保留公共检查，尤其不能误改 Join

源仓公共检查节选：

~~~cpp
Plan* plan = (Plan*)node;

// Join 已经由专用函数检查其 qual 是否消费距离。
// Result 等其他透明节点仍要求 plan.qual 为空。
if (!IsA(node, NestLoop) &&
    !IsA(node, MergeJoin) &&
    !IsA(node, HashJoin) &&
    plan->qual != NIL) {
    return false;
}

ListCell* lc = NULL;
foreach (lc, plan->targetlist) {
    Expr* expr = ((TargetEntry*)lfirst(lc))->expr;

    // setrefs 后只接受直接引用和常量。
    // 此检查覆盖全部 TLE，包括 resjunk，不只用户显示列。
    if (!IsA(expr, Var) && !IsA(expr, Const)) {
        return false;
    }
}
return true;
~~~

迁移时按目标 `IS_A`、List 遍历等接口调整。不要把 Result 的“qual 必须为空”无差别套到所有 Join，否则会重新禁用之前已支持的与距离无关的 Join 过滤。

这是一种保守准入策略：它可能拒绝实际与距离无关的函数投影，也可能拒绝逻辑上恒真的条件。本次修复不承诺消除所有保守拒绝，只修复已证明普通透传 Result 的误拒绝。

### 5.4 若根因是 R2：修调用阶段，不放开任意表达式

应确保源头投影下推在 setrefs 前完成，近似消费检查与扫描引用注入在所有相关计划完成 setrefs 后进行。

~~~cpp
// 顺序示意，函数参数和子计划处理使用目标已有实现。
if (required_columns_projection_is_needed) {
    push_down_union_all_source_projections(...);
}

topPlan = set_plan_references(...);
// 此处还须完成目标仓已有的相关 subplan 引用设置。

if (reuse_mode != OFF) {
    rewrite_ann_orderby_distance_refs(...);
}
~~~

不要为命中检查而执行第二次 setrefs，也不要把已经形成 OUTER_VAR/INNER_VAR 的计划交给只认识源表 Var 的投影下推函数。

若目标表达式展平引入包装，先识别该包装究竟发生在 Plan 表达式层还是执行状态层。只有证明它纯粹透传同类型值、不做算术/转换/过滤/集合返回时，才讨论专门适配；不能用“展平优化”作为跳过一切表达式检查的理由。

### 5.5 核对普通 IndexScan 的两层适配

目标 KGN 使用普通 IndexScan，需要同时满足：

1. **遍历/准入层认识它。** 不能在达到注入代码之前，就因只有 `T_AnnIndexScan` 白名单而将当前扫描判为未知消费者。
2. **注入层验证它确实是合格 KGN 扫描。** 校验目标 AM、opclass/支持函数、操作符、返回类型、查询向量表达式、扫描方向和已经约定的执行限制。

两层职责不同。识别普通 IndexScan 不表示所有 B-tree、HNSW 或其他 AM 都可复用 KGN 通道。

扫描自己的 qual 和其他投影必须继续按既有协议执行；只能替换与其 ORDER BY 对应的完整距离 TLE，不能递归替换 WHERE 里的距离，也不能把所有长得像距离的 OpExpr 都换成引用。

### 5.6 保存并恢复父上下文

保留“保存进入状态 → 当前节点只收紧 → 递归 → 恢复进入状态”的模式：

~~~cpp
bool saved = context->approximatePlanSafe;
context->approximatePlanSafe =
    saved && ann_orderby_node_allows_approx(node);

// 当前扫描资格检查与注入。
// 遍历孩子；每个递归调用负责恢复自己收到的状态。
walk_children(...);

context->approximatePlanSafe = saved;
~~~

这是逻辑示意；源实现对 false 的赋值写法与此等价。注意：

- 不要写成 `context->approximatePlanSafe = node_allowed;`，否则会覆盖祖先的拒绝。
- 引入提前 return 时也须恢复状态；正常返回路径不能遗漏。
- List 容器不能当作 Plan 读取字段，Merge Append 的孩子通常通过 List 遍历。
- 不要通过恢复 true 强行让某一条业务 SQL 命中。

### 5.7 若根因是 R7：只修显示适配

先证明目标 IndexScan 的匹配项已经是 AnnOrderByRef，再在目标 EXPLAIN 的扫描输出路径上增加对应显示包装。

判定应基于“合格扫描节点 + 真实引用节点”，不是基于操作符文本。普通 Seq Scan 的距离 OpExpr 不得也显示为 AM 引用。

Order By 仍显示原始 SQL 操作符表达式；上层 Var 继续按正常反解析规则展示，避免连续套三层、四层括号。这项改动仅改善可观察性，不是计算复用的实现。

### 5.8 禁止采用的快捷修法

- 将 `ann_orderby_plan_is_unsafe()` 永远改成 false。
- 将所有 Result 永远判为允许。
- 将所有 OpExpr、FuncExpr 或带展平标记的表达式放行。
- 将 KGN 近似 L2 伪装成精确 L2。
- 在子节点重新把祖先拒绝状态改成 true。
- 仅为了双括号修改 EXPLAIN，就声称已经减少计算。
- 为绕过未知行为直接关闭表达式展平，并将其作为最终修复。

## 6. 小数据验证准备

### 6.1 先使用原始业务 SQL

第一轮必须在目标测试环境用原始 SQL 复现。保持相同索引、opclass、hint、统计信息、查询向量和数据快照，仅切换距离复用开关。不要重建业务湖表或十亿行索引。

获取以下四份材料：修复前 off/on、修复后 off/on 的完整 EXPLAIN VERBOSE，以及 on 模式的内部拒绝/注入证据。通过计划成本变化或开关变化不能替代内部证据。

### 6.2 辅助小数据 fixture

下面的辅助 fixture 沿用 delta/snapshot/delete 结构，只有 snapshot 建 KGN 索引。用 4 维向量简化阅读；**若目标 KGN 不支持 4 维，按其支持维度扩充数据和查询向量，不能把建索引失败当成复用失败。**

使用专用测试数据库。脚本不自动 DROP 已有 schema；同名对象存在时先核对归属。

~~~sql
CREATE SCHEMA kgn_result_passthrough_test;
SET search_path = kgn_result_passthrough_test, public;

CREATE TABLE delta_t (
    id integer,
    row_id bigint,
    num_1 integer,
    num_2 integer,
    num_3 integer,
    num_4 integer,
    title text,
    embedding floatvector(4)
);

CREATE TABLE snapshot_t (
    id integer,
    row_id bigint,
    num_1 integer,
    num_2 integer,
    num_3 integer,
    num_4 integer,
    title text,
    embedding floatvector(4)
);

CREATE TABLE delete_t (row_id bigint);
CREATE INDEX delete_t_row_id_idx ON delete_t(row_id);

INSERT INTO snapshot_t
SELECT g, 1000 + g, g, g + 1, g + 2, g + 3,
       CASE WHEN g = 4 THEN NULL ELSE 'snapshot-' || g END,
       ('[' || (11 + g) || ',' || (23 + 2*g) || ',' ||
               (35 + 3*g) || ',' || (47 + 4*g) || ']')::floatvector(4)
FROM generate_series(1, 20) g;

INSERT INTO delta_t
SELECT 100 + g, 2000 + g, g, g + 1, g + 2, g + 3,
       CASE WHEN g = 4 THEN NULL ELSE 'delta-' || g END,
       ('[' || (11 + g) || ',' || (24 + 2*g) || ',' ||
               (35 + 3*g) || ',' || (47 + 4*g) || ']')::floatvector(4)
FROM generate_series(1, 12) g;

INSERT INTO delete_t VALUES (1002), (1005), (1010);

CREATE VIEW union_view AS
SELECT id, row_id, num_1, num_2, num_3, num_4, title, embedding
FROM delta_t
UNION ALL
SELECT s.id, s.row_id, s.num_1, s.num_2, s.num_3, s.num_4,
       s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS (
    SELECT 1 FROM delete_t d WHERE d.row_id = s.row_id
);

ANALYZE delta_t;
ANALYZE snapshot_t;
ANALYZE delete_t;
~~~

**下一步必须按目标真实 DDL 建 KGN 索引。本文未掌握其 AM/opclass/参数，不能编造可执行的建索引语句。**

将业务索引的 `pg_get_indexdef()` 输出适配到 snapshot_t，分别建立支持 L2 与 Cosine 的索引。下面仅是需要填充的 DDL 模板，不能原样执行：

~~~text
CREATE INDEX snapshot_l2_kgn ON snapshot_t
USING <目标实际KGN_AM> (embedding <目标L2_opclass>)
<目标要求的索引参数>;

CREATE INDEX snapshot_cosine_kgn ON snapshot_t
USING <目标实际KGN_AM> (embedding <目标Cosine_opclass>)
<目标要求的索引参数>;
~~~

不为 delta_t 创建向量索引；不为 snapshot_t 添加可能抢占测试路径的主键索引。保留 delete_t 的 B-tree，以便覆盖用户的相关 Index Only Scan 路径。

使用 1024 维进行补充时，前三十二条数据仍足够小。可在原四个分量后补 1020 个零，数据与查询向量同时变为 floatvector(1024)。Cosine 查询前四维非零，因此不是零向量；实际合法性仍按目标类型与索引要求验证。

### 6.3 查询与路径控制

核心测试必须保持“距离只用于 ORDER BY，外层不显示距离”：

~~~sql
SET search_path = kgn_result_passthrough_test, public;

-- 仅用于定向路径验证；GUC 不是必然产生该路径的保证。
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_nestloop = on;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT id, num_1, num_2, num_3, num_4
FROM union_view
WHERE id > 2 AND title IS NOT NULL
ORDER BY embedding <-> '[11,23,35,47]'::floatvector(4)
LIMIT 10;
~~~

Cosine 用例只将排序表达式改为：

~~~sql
ORDER BY embedding <+> '[11,23,35,47]'::floatvector(4)
~~~

目标 GUC、hint 名称按实际实现确认。必要时使用目标已支持的 hint 固定 KGN 和 Nested Loop Anti Join。**必须从真实计划确认出现目标索引、Anti Join，以及本次要检查的 Result。** 如果 CBO 消除了 Result，这条用例只能证明其他路径正常，不能算 Result 修复覆盖。

业务计划是最终复现依据；辅助 fixture 的计划不保证逐节点等同于业务计划。

## 7. 计划验证矩阵与预期变化

### 7.1 四阶段开关矩阵

L2 和 Cosine 各运行四阶段。每阶段均执行第 6.3 节完整 EXPLAIN，并保存实际输出。

| 阶段 | Top-K 下推 | 列裁剪 | 距离复用 | 关注点 |
|---|---|---|---|---|
| A | off | off | off | 保留原始基线，无 AM 引用注入 |
| B | on | off | off | 观察实际 Top-K 下推；仍无 AM 引用 |
| C | on | on | off | 距离可能已下沉到 Scan，但仍是 SQL 表达式 |
| D | on | on | L2 用 l2，Cosine 用 cosine | 满足条件的 snapshot KGN 扫描注入 AM 引用 |

L2 的设置顺序：

~~~sql
SET enable_union_all_diskann_topk_pushdown = off;
SET enable_union_all_required_columns_pushdown = off;
SET ann_orderby_distance_reuse = off;
-- 执行 L2 EXPLAIN：阶段 A。

SET enable_union_all_diskann_topk_pushdown = on;
-- 同一条 L2 EXPLAIN：阶段 B。

SET enable_union_all_required_columns_pushdown = on;
-- 同一条 L2 EXPLAIN：阶段 C。

SET ann_orderby_distance_reuse = l2;
-- 同一条 L2 EXPLAIN：阶段 D。
~~~

Cosine 必须重新从三个开关全关闭开始，按相同顺序运行，最后设为 `cosine`。源仓枚举拼写是 `cosine`，不是 `consine`；目标可接受值应通过自身 GUC 定义核对。

再补充：`all` 能处理两个度量；`l2` 对 Cosine、`cosine` 对 L2 不应误注入。每次执行都确保实际重规划，不用旧缓存计划冒充新开关行为。

阶段 A～C 不要求计划形态完全相同；核心是各自行为符合开关。Result 修复的因果比较应固定 Top-K 和列裁剪，仅比较 C/D。

### 7.2 修复前后的最小预期差异

以下是预期差异示意，**不是本次实际执行结果**：

~~~diff
 Index Scan using zhongyan_10yi_vsi_001 on ... s
-  Output: s.id, ..., (s.embedding <-> query_vector), s.row_id
+  Output: s.id, ..., ((s.embedding <-> query_vector)), s.row_id
   Order By: (s.embedding <-> query_vector)
~~~

其内部变化应为：

~~~text
修复前（若确实没有注入）：
  scan.targetlist[distance].expr = OpExpr
  扫描投影调用 SQL 距离函数

修复后：
  scan.targetlist[distance].expr = AnnOrderByRef
  扫描投影读取与本行绑定的 AM 距离
~~~

上层 Result、Limit、Anti Join 中的距离通常仍是 Var 引用；不应为了显示差异，把 AnnOrderByRef 放到不拥有 AM 扫描状态的上层节点。

### 7.3 正向验证清单

| 编号 | 场景 | 验收要求 |
|---|---|---|
| P01 | 业务原始 SQL，L2，普通透传 Result | 找到修复前拒绝点；修复后 snapshot 注入并实际读取 AM 值 |
| P02 | 同结构 Cosine | 使用目标 <+>，近似质量正确，不能套源 <=> |
| P03 | 辅助 fixture 的四阶段矩阵 | 不显示距离的 resjunk 场景在阶段 D 命中 |
| P04 | 显式 SELECT distance 的辅助查询 | 也能命中，但不能代替 P03 |
| P05 | Result 输出额外常量 | 仍为 Var/Const 透传时不误拒绝 |
| P06 | delta SeqScan + snapshot KGN | 两分支同尺度合并；不要求所有分支同时复用 |
| P07 | 距离复用 off | 不引入 AM 引用；列裁剪/Top-K 仍按各自开关工作 |
| P08 | 支持的两套表达式执行路径 | 对同样的实际 Plan 验证引用读取，无残留旧表达式执行状态 |
| P09 | 原有 MergeJoin、HashJoin 透传 | 不因修改公共检查回退；包括 Hash 构建侧与探测侧 |
| P10 | 普通非 KGN IndexScan | 不因扩展节点类型误使用 KGN 发布通道 |

P05 的 SQL 可在核心 SELECT 中加 `7 AS marker`；若优化器将常量放在别处，须核对实际 Result targetlist，不能据 SQL 文本宣布覆盖。

### 7.4 保守拒绝与语义负向用例

以下 SQL 是目标辅助用例；前提仍是实际选择 snapshot KGN 索引。先用 L2 运行，再按目标 Cosine 操作符和匹配 GUC 运行。

**N01：上层投影加工距离，不是直接透传。**

~~~sql
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT q.id, q.distance + 1.0 AS adjusted_distance
FROM (
    SELECT id, embedding <-> '[11,23,35,47]'::floatvector(4) AS distance
    FROM snapshot_t
    ORDER BY embedding <-> '[11,23,35,47]'::floatvector(4)
    LIMIT 40
) q
ORDER BY q.distance;
~~~

预期：保持既有保守策略；不能因为放行 Result 就连带放行非 Var/Const 投影。这里的 `+1` 不意味着数学上必然破坏排序，而是超出本次“纯透传”的准入范围。

**N02：上层过滤直接消费子查询距离。**

~~~sql
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT q.id, q.distance
FROM (
    SELECT id, embedding <-> '[11,23,35,47]'::floatvector(4) AS distance
    FROM snapshot_t
    ORDER BY embedding <-> '[11,23,35,47]'::floatvector(4)
    LIMIT 40
) q
WHERE q.distance < 30
ORDER BY q.distance;
~~~

预期：不能把被阈值过滤消费的距离替换成近似值。检查实际消费者可能是 Result 或 SubqueryScan，不限定 EXPLAIN 名称。

**N03：Join 条件直接消费距离。**

~~~sql
CREATE TABLE distance_gate (max_distance double precision);
INSERT INTO distance_gate VALUES (30);

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT q.id, q.distance
FROM (
    SELECT id, embedding <-> '[11,23,35,47]'::floatvector(4) AS distance
    FROM snapshot_t
    ORDER BY embedding <-> '[11,23,35,47]'::floatvector(4)
    LIMIT 40
) q
JOIN distance_gate g ON q.distance < g.max_distance
ORDER BY q.distance;
~~~

预期：继续被 Join 消费检查拒绝。`double precision` 应按目录中真实距离返回类型适配，不能借隐式类型转换掩盖消费识别遗漏。

**N04：一次性条件或额外哈希过滤。**

用目标真实 One-Time Filter 计划，或按目标单元测试框架构造相应 Result。确认字段非空/过滤已启用时仍拒绝。常量 true 往往会被优化掉；写出 SQL 条件但实际没有该字段不算覆盖。

**N05：整行、复合类型和参数隐藏距离。**

重跑已迁移的 P1 whole-row/RECORD/domain、Nested Loop 参数、LATERAL 回归。不能因放开 Result 绕过其中的距离依赖。

**N06：兄弟分支隔离。**

在 UNION ALL 中一支是安全透传 KGN，另一支是 N02/N03 的距离消费者，交换两个分支的顺序各执行一次。必须实际保留两条 KGN 路径，且共同祖先是安全透传；预期安全分支可复用，不安全分支不能复用。若共同祖先本身不安全，则两支都被禁用是正确行为，不能用这种计划检验兄弟恢复。

**N07：不支持的执行域。**

对已有 Stream、远程或其他未支持执行域的负向回归，继续保持既有拒绝。新增普通 Result 支持不能顺带放开 SPQ/分布式能力。

负向用例先验证“确实进入了相关 AM 与消费者路径”，再检查拒绝。没有向量索引扫描时得出的“不复用”可能是假通过。

阈值 30 主要用于产生消费形态，不能保证每个 KGN 实现都会出现数值差异。若要证明错误放行会改变行集，应先观测某行 SQL 值与 AM 值确实不同，再在二者之间选择可表示阈值；近似值偶然相等时，不能把数值测试称为已触发旧缺陷。

## 8. 证明没有重复计算，而不只看括号

### 8.1 三层证据

1. **计划节点证据**：匹配 TLE 确实从 OpExpr 改成 AnnOrderByRef。
2. **运行路径证据**：执行器实际进入引用取值分支，读当前行绑定的缓存值。
3. **数值与身份一致性证据**：AM 转换后的发布值、绑定值、扫描输出值与上层透传值属于同一行和同一次扫描，数值一致。

只具备第一层，最多说明“计划请求复用”；只有显示双括号，甚至可能连第一层都未直接验证。

### 8.2 源仓可参考的执行位置

| 职责 | 源仓入口 |
|---|---|
| AM 发布 | `diskannscan.cpp` 中 `xs_ann_orderby` 的赋值及 `publications++` |
| 当前行绑定 | `nodeAnnIndexscan.cpp` 中 `AnnIndexBindOrderByValue()` |
| 有序候选物化读取 | `nodeAnnIndexscan.cpp` 中读取 `iss_AnnOrderByValue` 的分支 |
| 传统表达式执行 | `execQual.cpp` 的 `ExecEvalAnnOrderByRef()` |
| 展平表达式执行 | `execExprInterp.cpp` 的 `EEOP_ANN_ORDERBY_REF` |
| 计划显示 | `explain.cpp` 的扫描 targetlist 反解析 |

这些是源仓定位参照，目标普通 IndexScan 的执行入口可能不同。必须建立目标对应关系，不能在目标中找不到同名函数就认定没有实现。

源仓的直接读取核心：

~~~cpp
// 前提：扫描状态有效、质量匹配；具体检查见源函数。
scanstate->iss_AnnOrderByReads++;
*isNull = scanstate->iss_AnnOrderByIsNull;
return scanstate->iss_AnnOrderByValue;
~~~

目标验证应看到等价行为，而不是反复执行保存的 original_expr。原表达式保留用于反解析/受控回退，不意味着每次都会执行。

### 8.3 临时计数与断点怎么设置

针对一个确定的 snapshot 扫描，观察：

- AM 已转换值的发布次数。
- 执行器成功绑定次数及当前扫描代际、行标识。
- AM 引用读取次数。
- 原始距离投影的回退求值次数。
- 旧 SQL 距离函数是否仍从该扫描的投影调用栈进入。

源仓可参考 `publications`、`iss_AnnOrderByReads`、`iss_AnnOrderByFallbacks`；目标有等价计数就复用，没有则使用临时断点/诊断。

对于正常、稳定、没有失配的正向用例，期望引用读取发生，相关投影回退为 0。不能把“回退次数大于 0”直接归因为 Result：应记录具体回退原因，并按目标既定运行期协议核对。

**不要求全系统距离函数调用数为 0。** AM 原有候选搜索/rerank 可能需要距离计算，delta SeqScan 仍计算 SQL 距离，其他独立过滤表达式也可能计算距离。本次要消除的是同一已发布候选在扫描排序/投影上的无必要重复求值。

也不要求 `publications = reads = LIMIT 10`：预读、过滤、Anti Join 删除匹配、Sort 物化和多个引用都会改变这些计数。应按实际扫描与行身份核对，不能用错误的总数等式判断失败。

### 8.4 两套表达式执行路径

若目标保留 `enable_expr_fusion`：

~~~sql
SET enable_expr_fusion = off;
-- 重新规划并执行正向、负向用例，记录真实入口。

SET enable_expr_fusion = on;
-- 再次重新规划并执行同组用例，记录真实入口。
~~~

开关是请求，实际是否选择该引擎还需断点/执行入口证据。没有走到对应引擎时标记未覆盖，不把两次 SQL 成功视为两套引擎都通过。

不要为验证临时增加一大串用户 EXPLAIN 属性；遵循已有双括号显示约定，把内部诊断保存到测试证据。

## 9. 查询结果正确性怎么判定

### 9.1 不要求近似 on/off 的 Top-10 无条件完全相同

目标已接受 KGN 近似距离和 SQL 距离存在误差，且混合分支也允许如此。因此距离复用 on/off 的排序乃至 Top-10 成员可能不同。不能把所有差异当成缺陷，也不能用“近似允许”掩盖身份错绑、尺度不一致、过滤语义变化。

需要分别检查三件事：

1. 非距离谓词、删除匹配、payload 和行多重集关系正确。
2. 每个候选的距离严格来自其自身的约定计算路径。
3. 在这组实际候选及其最终标量距离上，合并与 LIMIT 正确。

### 9.2 同一候选的数值基准

对于 snapshot 候选，基准是 **KGN 已有公式转换后的当前候选值**，不是强制要求等于 SQL 原始向量精算值。

对于 delta 候选，基准仍为该行 SQL 操作符结果。

对同一行、同一查询向量、同一扫描代际，比较：

~~~text
KGN 按既有公式生成最终距离
  → AM 发布值
  → 执行器绑定值
  → IndexScan 输出槽位
  → Anti Join 输出槽位
  → Merge Append/Result 上层引用值
~~~

纯透传且类型不变时，应保留相同数值表示。使用目标类型的精确取值/序列化方式，避免客户端小数打印截断；源仓 float8 可参考 `float8send`，目标返回类型不同须适配。

诊断时可以增加显式 distance 输出的辅助 SQL，但原本只排序、不显示距离的用例仍须独立验证，防止显式投影改变准入而掩盖 resjunk 路径问题。

### 9.3 独立构造混合分支的 Top-10 参考

最可靠的方法是对同一次执行记录两个分支交给 Merge Append 的实际行、distance、分支身份及相同距离时的处理信息，再在查询外按同样比较规则排序、截取前 10。

但外层 LIMIT 可能提前停止拉取：**只记录 Merge Append 已读取的行，只能验证已读候选的比较，不能证明尚未读取的分支尾部没有更小距离。** 完整验收还须证明各分支按最终发布的距离有序，并检查足以确定前 10 的分支候选窗口。可记录 AM 已生成的候选及对应过滤结果，或在保持同一候选输入的专用诊断运行中补齐各分支窗口；不能把完整顺序验证简化成只对最终 10 行排序。

如果用独立分支查询采集，则必须证明索引参数、query vector、MVCC 快照和候选集合一致；否则只能作辅助观察，不能把不同召回集合造成的差异归为 Result 修复错误。

对每条实际返回行检查：

- snapshot 行不属于 delete_t 中被匹配的 row_id。
- `id > 2` 与 `title IS NOT NULL` 等原谓词成立。
- id、row_id、其他列仍来自同一行，没有距离列替换造成槽位错配。
- 输出按最终被比较的 distance 非递减。
- 至多 10 行；只有证明实际合格候选不少于 10 时，才要求恰好 10 行。
- UNION ALL 的重复行语义保留，不擅自去重。

存在相同距离时，原 SQL 没有第二排序键，不能要求任意固定的同分顺序。为了检查稳定性临时加第二排序键，可能改变索引路径，因此不能代替原 SQL 验证。

### 9.4 如何避免验证查询反过来改变被测计划

不要直接把被测 SQL 包进外层 `array_agg`、`DISTINCT`、`GROUP BY` 来做结果断言；这些节点可能触发消费者安全检查，使本来要测的复用被禁用。

推荐两种方式：

1. 客户端按原 SQL 获取完整结果后，再在查询外比较。
2. 使用目标已有的动态 SQL 捕获辅助函数，让被测 SELECT 独立规划，再在外层对捕获数据比较。

源仓 `union_all_ann_feature_matrix.sql` 的 `join_guard_capture()` 已使用第二种方式，可按目标类型适配。动态 SQL 的作用是隔离被测计划，不是跳过安全检查。

### 9.5 负向用例的结果要求

对于被正确拒绝复用的阈值过滤、距离 Join key、whole-row/参数消费，仍应使用原表达式约定的值决定匹配和过滤。

固定同一候选输入、使用足够大的候选上限排除 Top-K 截断影响后，应保持开关前后的匹配行及 payload 一致。必须同时有“相关扫描未注入”的计划证据，避免近似值碰巧相等导致数值测试假通过。

## 10. 编译、原生回归与交付

### 10.1 按目标仓规则构建

只有 .cpp 调整时，使用目标仓已有增量构建流程；若增加节点字段、改公共头文件或表达式 ABI，按目标构建规则全量重建依赖。

不要为了修 Result 准入而重新初始化数据库或重建业务索引。只有目标真实缺少测试索引时，才在专用 fixture 上创建。

核验安装二进制与当前构建一致，再启动/连接目标测试实例。本文不提供未经确认的 GV 启停命令。

### 10.2 测试进入已有框架

优先把 Result 正反场景补入目标已迁移的：

~~~text
src/test/regress/sql/union_all_ann_feature_matrix.sql
src/test/regress/expected/union_all_ann_feature_matrix.out
~~~

若目标路径不同，使用其等价目录。新拆文件时必须登记 schedule/测试入口，不能只复制 SQL/expected。

源仓现有相关 schedule 是：

~~~text
parallel_schedule.union_all_required_columns
  union_all_required_columns
  union_all_required_columns_safety
  union_all_ann_feature_matrix

parallel_schedule.ann_orderby_distance_reuse
  ann_orderby_distance_reuse
  ann_orderby_distance_reuse_safety
  ann_orderby_distance_reuse_lifecycle
~~~

目标应覆盖以上职责等价的回归，不要求照抄源 Makefile 入口。

Golden 输出由目标真实运行生成并人工审查；不能手写预期双括号再声称测试通过。固定计划用 `EXPLAIN (VERBOSE ON, COSTS OFF)`；运行期证据另存 ANALYZE 输出或调试记录，避免将不稳定计时写入 expected。

### 10.3 本次改动最低验收集合

1. 业务计划修复前/后、off/on 对照。
2. 小数据 L2/Cosine 四阶段矩阵，含 ORDER BY resjunk 场景。
3. 普通 Result 和实际目标 KGN IndexScan 的内部节点证据。
4. 两套实际表达式执行路径的引用读取证明。
5. Result 非透传投影/过滤、距离 Join 消费、整行/参数消费负向测试。
6. 兄弟分支隔离及原有 Join/Hash 透传回归。
7. 同一候选 AM→执行器→上层的值/身份一致性；混合分支合并正确性。
8. GUC off 不注入；模式错配不注入；非 KGN 不误注入。
9. 现有预编译、rescan、行绑定与生命周期回归，确认准入扩大后没有暴露新的执行路径遗漏。

对仅修改 Result 准入的补丁，在相同数据、统计信息和其他参数下，还应比较修复前后 GUC off 的计划结构与结果。入口应继续跳过复用改写，不能出现由本次补丁额外引入的引用或行为变化。列裁剪与 Top-K 自身在 off 模式下仍可能生效，这是各自开关的行为，不属于本次 Result 修复造成的变化。

该列表用于本次优化器修复的验收，不要求无差别重跑所有历史压力测试。新增修改或失败证据再决定是否扩大测试。

### 10.4 建议保存的证据目录

目标修复工作完成后，在目标项目约定的测试证据目录保存：

~~~text
环境与版本.txt
原始业务SQL.sql
小数据建表与真实KGN索引DDL.sql
正向与负向用例.sql
修复前_off计划.out
修复前_on计划.out
修复后_off计划.out
修复后_on计划.out
根因断点或诊断记录.txt
AM发布与引用取值证据.txt
结果核对记录.txt
原生回归.log
目标代码修改.patch
验收结论.md
~~~

这是目标实施时的交付清单；本次已生成源仓证据目录，见第 14 节。源仓证据不能代替目标 KGN 的执行输出。

## 11. 验收记录模板

| 项目 | 目标实测填写 |
|---|---|
| 修复前/后源码 commit 与 diff | 待填写 |
| 实际运行二进制路径、构建标识 | 待填写 |
| 原始业务 SQL 与向量保存位置 | 待填写 |
| 真实 Result 类型和完整字段检查 | 待填写 |
| 第一个拒绝点、原因及状态变化 | 待填写 |
| 根因分类 R0～R8 | 待填写，可多项同时存在 |
| 最小修改内容 | 待填写 |
| 修复前目标路径确实覆盖 | 待填写 |
| 修复后实际 AnnOrderByRef 注入 | 待填写 |
| 执行器读取 AM 值与回退情况 | 待填写 |
| L2/Cosine 与两套引擎覆盖情况 | 待填写 |
| 负向测试与兄弟隔离 | 待填写 |
| 混合分支结果与删除语义 | 待填写 |
| 原生回归结果及日志位置 | 待填写 |
| 未覆盖场景与原因 | 待填写 |

可以报告“修复完成”的条件是：根因有实际证据，修改针对该根因，正向命中、负向拒绝、执行取值和结果核对均通过，并完成相关原生回归。

如果仅证明内部引用存在而修复了显示，应明确报告“EXPLAIN 显示适配修复”；如果没有目标 Result 或 KGN 路径，报告“用例未覆盖”，不能改写为已通过。

## 12. 给目标仓大模型的执行约定

~~~text
请先阅读本文，再检查目标仓实际代码和运行计划。

任务是诊断并修复湖表 UNION ALL 中 Result 可能误阻断 KGN
距离复用的问题，不是无条件让所有 Result 或 IndexScan 放行。

先定位第一个拒绝点，区别：
1. ann_orderby_plan_is_unsafe=false 是通过检查；
2. ann_orderby_node_allows_approx=false 才会收紧近似值上下文；
3. 目标普通 IndexScan 可能只漏了 AnnOrderByRef 的 EXPLAIN 显示。

以真实 NodeTag、字段、setrefs 时序、TLE 表达式、AM 资格和执行器
路径作为证据。不得只凭单层/双层括号定因。

目标 KGN 的 L2、Cosine 都按既定近似质量和既有转换公式处理；
操作符分别为 <->、<+>。不重新设计公式，不伪装成精确距离。

只放行证明透明的 Result，保留 qual、一次性条件、额外执行字段、
距离消费、整行/参数依赖、执行域、兄弟状态恢复等保护。
真实执行结构与源仓不同时，按职责适配，不能整文件覆盖。

先复现原始业务 SQL，再用少量数据覆盖正反场景。
SQL 产生目标路径与实际运行 AM 引用必须可观察。
未覆盖和未执行的项目必须明确记录，不能伪造计划、结果或计数。

编译、安装、原生回归、实际取值及结果核对完成后，提供最小 patch、
完整 SQL、实际输出和验收记录；保留用户数据及无关本地修改。
~~~

## 13. 相关依据与文档

- [ANN 距离复用 Join 透明透传迁移指南](ANN距离复用Join透明透传迁移指南.md)：既有 Join、P1/P2 及迁移前置条件。
- [DiskANN 到 KGN 距离复用跨仓迁移约定](DiskANN距离复用跨代码仓迁移约定与验收指南.md)：目标度量、质量、转换与运行期协议。
- [UNION ALL 列裁剪与 ANN 距离复用联合调用栈](../源码解读/UNION_ALL列裁剪与ANN距离复用联合调用栈.md)：投影下沉、setrefs 与内部引用的关系。
- [ANN 距离复用源码深度解读](../源码解读/ANN距离复用源码深度解读.md)：AM 发布、绑定和两套表达式执行。
- [源仓安全检查与近似透传准入](../../../src/gausskernel/optimizer/plan/planner.cpp:834)：源码入口，不代表目标实现完全相同。
- [源仓 BaseResult 定义](../../../src/include/nodes/plannodes.h:417)及[另一个 Result 定义](../../../src/include/nodes/plannodes.h:1816)：解释不能按名字直接放行的原因。
- [源仓已有成功计划](../../../src/test/regress/expected/union_all_ann_feature_matrix.out:400)：包含 Result 祖先和 Cosine AM 复用；是已有回归期望，不是本次重跑结果。
- [源仓实际引用取值](../../../src/gausskernel/runtime/executor/execQual.cpp:4308)及[展平引擎取值](../../../src/gausskernel/runtime/executor/execExprInterp.cpp:1378)：运行期验证参照。

## 14. 2026-10-30 编译与实测：列裁剪入口才是源仓的关键缺口

### 14.1 测试忠实保留了哪些条件

用户补充的外层 SQL 是：

~~~sql
SELECT id,id,id,"num_1_2B","num_2_2B","num_3_2B","num_4_2B"
FROM d_lake_zhongyan.zhongyan_10yi
ORDER BY embedding <-> '[xxx]'
LIMIT 100;
~~~

源仓复现保留：无外层 WHERE、三个重复 id、完整 14 列视图、delta UNION ALL snapshot NOT EXISTS delete、只有 snapshot 具有向量索引、delete 的 B-tree、1024 维向量和 LIMIT 100。

源仓与目标的明确差异：

- floatvector(1024) 改为源仓 vector(1024)；KGN 普通 IndexScan 改为 DiskANN Ann Index Scan。
- L2 保持 <->；Cosine 用源仓 <=>，目标仍为 <+>。
- 数据为 snapshot 40 行、delta 24 行，delete 删除 snapshot 的 5、10、20，共 61 个可见结果；无需大数据。
- 使用非零查询向量，前四维为 11、23、35、47，其余 1020 维为 0，不使用用户未提供的真实向量。
- 小数据计划中 delete 使用 Materialize 包装的 Index Only Scan，与用户相关 Index Cond 的物理细节不同；都保留 Nested Loop Anti Join 和同一个 row_id 删除语义。

三种查询形态是原始无 WHERE、仅加 WHERE true、加 WHERE id > 2；每种再测试 L2/Cosine、fusion off/on、五个阶段（全关→Top-K→列裁剪→复用→去掉重复 id 对照），共 60 组。

### 14.2 编译与环境记录

先使用工作区统一脚本 full 清理、配置 Debug/O0、编译。会话中断后继续同一次干净构建；清除了中断留下的 4 个零字节 .o 后成功链接、安装。候选仅修改一个 .cpp，随后用统一脚本 incremental 重新编译、安装、启动。

- 源仓基线：ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4，包含 Top-K、列裁剪、距离复用和 Join 透传。
- 运行实例：已有独立测试数据目录 `.runtime/diskann-vector-filter-p12/data`，端口 55450；未重新 initdb，未启动 55440。
- 本次新建数据库：ann_result_validation_20261030；业务同名 schema/table 只创建在此测试库。
- 当前安装及源码目录 gaussdb SHA256 均为 `12d31422aafe1410121d8a5ac703b5520dfbd0bb90f96ff2ce92a3a879010ed8`，运行进程对应该安装。
- 旧 template 遗留 DiskANN INCLUDE 的 diskanncanreturn 目录项；仅在新测试库将 amcanreturn 对齐源码为 0，并重启清理共享目录缓存。它是环境兼容步骤，不是本次优化器修复。
- 原生回归的 PL/pgSQL 自治连接使用 /tmp socket，测试时创建了指向现有 55450 socket 的临时别名；收尾删除该别名。编译脚本默认连通性检查没使用自定义 socket，所以有告警；实际 gsql/psycopg2 连接及回归成功。

构建日志保存了上述过程。测试完成后恢复实例初始停止状态，测试库数据保留以便复查。

### 14.3 修复前的实际调用链

~~~text
原 SQL 没有外层 WHERE
  → required_columns_outer_query_is_safe()
       quals == NULL，返回 false
  → preprocess_union_all_required_columns() 未执行裁剪
  → 本查询 unionAllColumnsRewritten == false
  → 跳过 preprocess_union_all_order_key_projection()
  → root->union_all_required_columns_pruned 不被本查询设置
  → 跳过相应源头距离投影下沉

与此同时，Top-K 独立下推成功
  → 叶子追加局部 resjunk 距离，用于叶子排序
  → 不等于把距离变成 UNION 的可见输出
  → 上层 Result 仍保留完整距离 OpExpr
  → ann_orderby_node_allows_approx(Result) 合理地返回 false
  → snapshot 扫描还没有可注入的完整距离 TLE
  → L2/Cosine 均未使用 AM 投影引用
~~~

叶子 resjunk 不自动成为集合操作接口。关键的排序键复用由 `rewrite_outer_order_projection()` 将外层排序项改为合成输出列 Var，配合 UNION/叶子 targetlist 元数据改写完成；不是仅靠 setrefs 看到某个深层扫描曾经算过距离就能自动跨层复用。

**源仓这次的 Result 是 T_BaseResult，不是未认识的 T_Result。检查发生在 setrefs 之后，仍有 OpExpr 的原因是下层接口缺少可引用的完整排序键，不是 setrefs 时序错误。**

断点实际记录（去掉地址等无关内容）：

~~~text
无 WHERE：
  ann_orderby_plan_is_unsafe → false
  Result: T_BaseResult
  targetlist: 7 个 Var + 第8项 OpExpr(resjunk=true)
  plan.qual为空，resconstantqual为空
  ann_orderby_node_allows_approx → false
  snapshot AnnIndexScan targetlist: 15个 Var，无完整距离 OpExpr
  SCAN_END quality=UNAVAILABLE reads=0 fallbacks=0

仅增加 WHERE true：
  Result: 7 个 Var + 第8项 Var(resjunk=true)
  ann_orderby_node_allows_approx → true
  snapshot Scan 改写前：第6项距离 OpExpr，另保留 row_id
  SCAN_END quality=APPROX_COSINE reads=40 fallbacks=0
~~~

此处的 `fallbacks=0` 在无复用模式不表示避免了原始计算；未注入引用时原表达式正常计算，不计入引用的回退计数。

### 14.4 验证通过的最小修改

修改 `src/gausskernel/optimizer/prep/preptopk.cpp`：

~~~cpp
// 修复前：第一期以语法要求排除所有无 WHERE 查询。
if (query->jointree == NULL || query->jointree->quals == NULL)
    return false;

// 本次增强：无谓词无需下推，继续检查其余查询安全条件。
if (query->jointree == NULL)
    return false;
~~~

保留聚合、窗口、DISTINCT、SRF、行锁、安全屏障、集合操作形态、列号映射等全部检查。无 WHERE 在谓词下推、必要列收集和 Var 改写中已有 NULL 处理；已支持的 WHERE true 是语义等价对照。

还将 DEBUG 的 outer_recheck 从 `!wherePushed` 改为检查实际剩余 quals，避免无 WHERE 时误报存在外层 recheck。

**没有修改 ann_orderby_plan_is_unsafe、ann_orderby_node_allows_approx 或 Join 白名单，也没有改变 AM/执行器的计算逻辑。** 内核 diff 为 +4/-3 行（包含注释和日志）。这是扩大原先“WHERE 必需”的列裁剪适用范围，不是证明原第一期实现违反了旧契约。

新增原生 SQL `union_all_required_columns_no_where.sql`、对应 expected，并登记原列裁剪 schedule。代码未提交；可审核的完整 patch 保存在证据目录。

**开关影响：这个增强归属于列裁剪。只要列裁剪开启，无 WHERE 计划就可能改变，即使 ann_orderby_distance_reuse=off；因此不能声称它只影响距离复用开关打开的查询。列裁剪关闭时，本次 24 组实际计划文本与改动前完全相同。**

### 14.5 真实结果汇总

| 检查 | 修复前 | 修复后 |
|---|---|---|
| 原始无 WHERE，三个特性全开 | Top-K 成功；列裁剪/排序键物化未执行；snapshot 不复用 | 三步均执行，snapshot 复用 |
| 仅加 WHERE true | 已能复用，61 行 | 保持复用，61 行 |
| WHERE id > 2 | 已能复用，59 行 | 保持复用，59 行 |
| 三个重复 id | 真正返回7列，前三列相同 | 同样正确，payload无错位 |
| 60组矩阵 | 行集、列数、payload检查通过，复用差异按上述记录 | 同样通过，原无WHERE复用缺口消除 |
| 12项消费者检查 | 12/12通过 | 12/12通过 |
| 列裁剪关闭的计划 | 比较基线 | 24/24完全一致 |
| 新增无WHERE原生用例 | 尚未纳入 | L2/Cosine计划与七列结果、裸SELECT、LIMIT-only、ORDER-only通过 |
| 原生回归 | 本轮未用旧版重跑全部schedule | 7/7通过，无回归差异；空diff在回归完成时由工具清理 |

7 项为：union_all_required_columns、union_all_required_columns_safety、union_all_ann_feature_matrix、union_all_required_columns_no_where、ann_orderby_distance_reuse、ann_orderby_distance_reuse_safety、ann_orderby_distance_reuse_lifecycle。

修复后重新观察原始无 WHERE SQL：扫描第6项真实节点为 T_AnnOrderByRef；Cosine `reads=40`、`fallbacks=0`。40 是被扫描的 snapshot 候选数，删除匹配在 Anti Join，最终总结果为37条 snapshot加24条delta，不应要求reads等于最终61行。

**表达式引擎覆盖限制：两种 enable_expr_fusion 设置均验证了计划和结果，但本次断点实际捕获的扫描距离取值均走传统 ExecEvalAnnOrderByRef（每次40次），EEOP_ANN_ORDERBY_REF 的断点为0次。因此不得将本轮说成“两套底层取值引擎都已实际命中”；目标若走展平取值入口仍需补其实际证据。**

### 14.6 修复前后的实际计划

以下来自本次输出，**只把 1024 维常量缩略显示，节点和输出结构没有手工构造**；完整向量版本另存。选 Cosine 展示近似消费者检查，L2 完整计划也已保存。

修复前，无 WHERE，全开：

~~~text
Limit
  Output: "*SELECT* 1".id, "*SELECT* 1".id, "*SELECT* 1".id, "*SELECT* 1"."num_1_2B", "*SELECT* 1"."num_2_2B", "*SELECT* 1"."num_3_2B", "*SELECT* 1"."num_4_2B", (("*SELECT* 1".embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
  ->  Sort
        Output: "*SELECT* 1".id, "*SELECT* 1".id, "*SELECT* 1".id, "*SELECT* 1"."num_1_2B", "*SELECT* 1"."num_2_2B", "*SELECT* 1"."num_3_2B", "*SELECT* 1"."num_4_2B", (("*SELECT* 1".embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
        Sort Key: (("*SELECT* 1".embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
        ->  Result
              Output: "*SELECT* 1".id, "*SELECT* 1".id, "*SELECT* 1".id, "*SELECT* 1"."num_1_2B", "*SELECT* 1"."num_2_2B", "*SELECT* 1"."num_3_2B", "*SELECT* 1"."num_4_2B", ("*SELECT* 1".embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
              ->  Append
                    ->  Subquery Scan on "*SELECT* 1"
                          Output: "*SELECT* 1".id, "*SELECT* 1"."num_1_2B", "*SELECT* 1"."num_2_2B", "*SELECT* 1"."num_3_2B", "*SELECT* 1"."num_4_2B", "*SELECT* 1".embedding
                          ->  Limit
                                Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.embedding, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", zhongyan_10yi_dt1."num_5_2B", zhongyan_10yi_dt1.num_scalar1_10000, zhongyan_10yi_dt1.num_scalar2_10000, zhongyan_10yi_dt1.num_scalar3_10000, zhongyan_10yi_dt1.num_filter1_100, zhongyan_10yi_dt1.num_filter2_100, zhongyan_10yi_dt1.cat_single, zhongyan_10yi_dt1.cat_pair, ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                                ->  Sort
                                      Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.embedding, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", zhongyan_10yi_dt1."num_5_2B", zhongyan_10yi_dt1.num_scalar1_10000, zhongyan_10yi_dt1.num_scalar2_10000, zhongyan_10yi_dt1.num_scalar3_10000, zhongyan_10yi_dt1.num_filter1_100, zhongyan_10yi_dt1.num_filter2_100, zhongyan_10yi_dt1.cat_single, zhongyan_10yi_dt1.cat_pair, ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                                      Sort Key: ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                                      ->  Seq Scan on d_lake_zhongyan.zhongyan_10yi_dt1
                                            Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.embedding, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", zhongyan_10yi_dt1."num_5_2B", zhongyan_10yi_dt1.num_scalar1_10000, zhongyan_10yi_dt1.num_scalar2_10000, zhongyan_10yi_dt1.num_scalar3_10000, zhongyan_10yi_dt1.num_filter1_100, zhongyan_10yi_dt1.num_filter2_100, zhongyan_10yi_dt1.cat_single, zhongyan_10yi_dt1.cat_pair, (zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
                    ->  Subquery Scan on "*SELECT* 2"
                          Output: "*SELECT* 2".id, "*SELECT* 2"."num_1_2B", "*SELECT* 2"."num_2_2B", "*SELECT* 2"."num_3_2B", "*SELECT* 2"."num_4_2B", "*SELECT* 2".embedding
                          ->  Limit
                                Output: s.id, s.embedding, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", s."num_5_2B", s.num_scalar1_10000, s.num_scalar2_10000, s.num_scalar3_10000, s.num_filter1_100, s.num_filter2_100, s.cat_single, s.cat_pair, ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                                ->  Nested Loop Anti Join
                                      Output: s.id, s.embedding, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", s."num_5_2B", s.num_scalar1_10000, s.num_scalar2_10000, s.num_scalar3_10000, s.num_filter1_100, s.num_filter2_100, s.cat_single, s.cat_pair, (s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
                                      Join Filter: (d.row_id = s.row_id)
                                      ->  Ann Index Scan using zhongyan_10yi_vsi_cos on d_lake_zhongyan.zhongyan_10yi_snap1 s
                                            Output: s.id, s.row_id, s.embedding, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", s."num_5_2B", s.num_scalar1_10000, s.num_scalar2_10000, s.num_scalar3_10000, s.num_filter1_100, s.num_filter2_100, s.cat_single, s.cat_pair
                                            Order By: (s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
                                      ->  Materialize
                                            Output: d.row_id
                                            ->  Index Only Scan using zhongyan_10yi_del1_idx on d_lake_zhongyan.zhongyan_10yi_del1 d
                                                  Output: d.row_id
~~~

修复后，同一 SQL 和参数：

~~~text
Limit
  Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
  ->  Result
        Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
        ->  Merge Append
              Sort Key: ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
              ->  Limit
                    Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                    ->  Sort
                          Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                          Sort Key: ((zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                          ->  Seq Scan on d_lake_zhongyan.zhongyan_10yi_dt1
                                Output: zhongyan_10yi_dt1.id, zhongyan_10yi_dt1."num_1_2B", zhongyan_10yi_dt1."num_2_2B", zhongyan_10yi_dt1."num_3_2B", zhongyan_10yi_dt1."num_4_2B", (zhongyan_10yi_dt1.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
              ->  Limit
                    Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                    ->  Nested Loop Anti Join
                          Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024)))
                          Join Filter: (d.row_id = s.row_id)
                          ->  Ann Index Scan using zhongyan_10yi_vsi_cos on d_lake_zhongyan.zhongyan_10yi_snap1 s
                                Output: s.id, s."num_1_2B", s."num_2_2B", s."num_3_2B", s."num_4_2B", ((s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))), s.row_id
                                Order By: (s.embedding <=> '[11,23,35,47,...1020个0]'::vector(1024))
                          ->  Materialize
                                Output: d.row_id
                                ->  Index Only Scan using zhongyan_10yi_del1_idx on d_lake_zhongyan.zhongyan_10yi_del1 d
                                      Output: d.row_id
~~~

### 14.7 这对目标 KGN 的结论是什么

可以确认：列裁剪与排序键复用链不完整，确实会使 Result 保留计算表达式、拒绝近似透传；本次在源仓用编译后的真实代码复现并解决了这条链路。

**不能直接确认目标也是同一个入口问题。** 用户目标计划已经在 snapshot 扫描输出标量距离、上层列也更窄；本次源仓修复前扫描则仍输出宽行且没有可注入距离 TLE，两者不是完全相同的物理计划。目标可能已经扩大无 WHERE 范围，或有其他投影优化。

目标建议按以下顺序处理：

1. 查当前查询是否真的执行了列裁剪及排序键可见列改写，不能只看GUC为on。
2. 查 Result 的真实距离项是 Var 还是 OpExpr，以及中间 UNION 接口是否保留对应距离槽位。
3. 如果仍保留 OpExpr，先补完整投影/槽位链；如果列裁剪只因无 WHERE 被拒绝，可迁移本次最小增强及回归。
4. 如果已是纯 Var/Const，才按前文 R1/R3 检查真实 Result 类型或其他字段。
5. 如果引用已注入而括号仍不符，走 R7 显示适配；如果执行器没读AM值，走 R8。

直接放开所有 Result 的原建议没有被本次测试证明安全或必要；本文将它修正为“先查列裁剪/排序键链，再按实际节点决定”。

### 14.8 可直接复查的文件

所有证据集中在 [Result透传与列裁剪联动验证](../测试/Result透传与列裁剪联动验证/)：

- [完整小数据初始化](../测试/Result透传与列裁剪联动验证/01_小数据初始化.sql)、[用户SQL源仓等价版本](../测试/Result透传与列裁剪联动验证/02_用户SQL源仓等价.sql)。
- [矩阵执行脚本](../测试/Result透传与列裁剪联动验证/run_matrix.py)、[全部矩阵SQL](../测试/Result透传与列裁剪联动验证/03_完整开关矩阵.sql)、[实际完整输出](../测试/Result透传与列裁剪联动验证/03_完整开关矩阵.out)、[摘要](../测试/Result透传与列裁剪联动验证/03_矩阵摘要.json)。修复前版本有“修复前_”前缀。
- [修复前Cosine完整计划](../测试/Result透传与列裁剪联动验证/修复前_cosine_完整计划.out)、[修复后Cosine完整计划](../测试/Result透传与列裁剪联动验证/修复后_cosine_完整计划.out)、[修复后L2完整计划](../测试/Result透传与列裁剪联动验证/修复后_l2_完整计划.out)。
- [消费者验证脚本](../测试/Result透传与列裁剪联动验证/run_negative.py)、[正反计划及结果](../测试/Result透传与列裁剪联动验证/04_消费者正反验证.out)。
- [修复前内部节点与取值证据](../测试/Result透传与列裁剪联动验证/05_修复前内部节点与取值.log)、[修复后证据](../测试/Result透传与列裁剪联动验证/05_修复后内部节点与取值.log)。
- [新增用例实际输出](../测试/Result透传与列裁剪联动验证/06_新增原生用例实际输出.out)、[原生回归日志](../测试/Result透传与列裁剪联动验证/原生回归.log)。日志开头存在回归工具源码统计变量未初始化的shell告警，不涉及SQL断言；最终7项通过、没有回归差异。
- [最小内核增强与原生测试patch](../测试/Result透传与列裁剪联动验证/无WHERE列裁剪与距离复用联动修复.patch)、[构建日志目录](../测试/Result透传与列裁剪联动验证/构建日志/)。
