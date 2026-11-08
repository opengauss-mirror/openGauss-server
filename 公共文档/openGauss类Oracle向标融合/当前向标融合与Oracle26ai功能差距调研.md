# 当前向标融合与 Oracle 26ai 功能差距调研

核验日期：2026-10-29。按用户最新要求，只比较功能、SQL 适用范围和执行行为，不比较时延、吞吐、代价估算、回表次数或计划选择收益。

## 1. 结论及比较口径

**当前前两阶段已经实现单表与 Join 的 PRE／IN／POST 主体功能，包含非唯一匹配、业务列投影、最终结果行计数和 Nested Loop 重扫。主要还差三类核心能力：搜索完成控制、查询质量接口、SQL 接入范围。** 如果把 Oracle 26ai 整个向量检索产品的范围也纳入，还存在分区／分布执行、更多索引形态、分组 Top-K 等扩展差距；这些并不都属于当前第三阶段。

必须区分两个问题：

- **SQL 能否执行并保持关系语义？** 多数下述受限 SQL 在 AUTO 下仍可选择普通精确计划；不能说这些 SQL 整体不支持。
- **SQL 能否使用本次开发的向标融合？** 新过滤路线有明确准入门槛；不满足时不生成该路线，强制指定模式会报不支持。本报告中的“接入差距”指这一层。

不能据此给出“已覆盖 Oracle 的百分之多少”：基本过滤模式、SQL 边界、质量接口、分区能力不是等权功能，Oracle 各 RU 的能力也不同。更准确的定位是：**核心关系过滤功能已有；搜索质量控制和通用 SQL 接入尚未完整；大型部署及新型检索形态还未适配。**

### 1.1 本次比较对象

| 项目 | 实际对象与证据 |
|---|---|
| 我们的当前实现 | `openGauss-vector-filter-p12`，分支 `feature/diskann-vector-filter-p12`，HEAD `72778fe2aae139cfade1abe7396e3e085869ee22`；P1 commit `415b78e6c92627e38b5c2823b7257b284a95e415` |
| 当前运行产物 | `.install/diskann-vector-filter-no-include-p12-release/bin/gaussdb`，SHA256 `bf484902a3ab0fdd53a87a8f884b4797847de1339db4a98758aef0a49e83fb0e`；隔离实例 55450，库 `vf_p12_baseline` |
| 阶段状态 | 执行包 25 项完成、10 项 pending、无活动任务；第三阶段尚未实施。以当前 state／receipt 为准，早期阶段报告中的历史 HEAD 不作为当前版本 |
| Oracle 本机实测 | Oracle AI Database 26ai Free **23.26.2.0.0**，复用已有 HNSW 表和索引 |
| Oracle 官方资料 | 26ai 在线文档已包含 **23.26.4** 的能力；新版文档能力不冒充本机实测能力 |
| 新增验证 | Oracle 12 条只读能力探测、当前实现 9 个只读探测项，保存 SQL、返回行和计划／错误；不是性能测试，也不是同数据跨库结果对照 |

本次没有修改内核、设计红线、阶段状态或业务数据。Oracle 和隔离 openGauss 均已恢复停止状态，已跟踪源码仍干净。见[当前二进制身份](验证/Oracle26ai功能差距核验_20261029/OG二进制身份.json)、[收尾核验](验证/Oracle26ai功能差距核验_20261029/服务恢复与源码核验.json)。

## 2. 核心功能差距

| 类别 | Oracle 能力 | 我们当前能力 | 功能结论 |
|---|---|---|---|
| **A. 搜索完成控制** | 本机 POST 已观察到同一 HNSW 算子内多轮近似搜索，随后转索引内精确搜索 | 新 PRE／IN／POST 路线每个扫描周期执行一次预算限定的搜索；第三阶段的补搜反馈、POST 第二 AM 精确入口尚未实现 | **明确未完成**。缺少执行中继续搜索／切换搜索方式的能力，不是单纯速度差距 |
| **B. 查询质量接口** | SQL 可指定目标 accuracy；有索引精度评估 API | 通过 `diskann_probes` 等工作量参数及外部测试脚本调参；没有等价的目标召回接口与内置精度报告 | **明确缺少产品接口**。目标精度本身也不是每条查询的硬保证 |
| **C1. 参数化查询接入** | 本机绑定查询向量、绑定 K、同时绑定二者均生成 HNSW 过滤计划 | custom plan 绑定后可进入新路线；generic plan 的查询向量仍为 Param 时被拒绝，规划期未知 K 也被拒绝 | **已实证的接入限制**。不是所有参数化查询都不支持 |
| **C2. 复杂资格 SQL 接入** | 本机部分相关子查询、INLINE 后的 CTE 可生成 PRE | 当前相关子查询样例被拒绝；保留在当前查询块的 CTE 等形态受限制；InitPlan 标量条件可用 | **部分形态有差距**。不能概括为“全部子查询不支持”或“Oracle 全部支持” |
| **C3. 向量列的准入条件** | 本机向量列允许 NULL、查询未写非空条件，仍生成 PRE | 新路线要求能证明目标向量非空，COSINE 还需要非零域证明 | **已实证的接入限制**。不代表已测清 Oracle 所有 NULL／零向量结果行为 |

### 2.1 A：执行期间的继续搜索和 POST 精确入口

当前 `DiskAnnQuerySearch()` 增加一次 `search_rounds`，完成本轮导航后设 `state->done=true`；后续交付这次搜索保存的结果。尚无完整的“普通父计划反馈最终合格行数 → 继续搜索 → 判断停止”的第三阶段控制协议，也没有 POST 同一扫描内的精确搜索入口。代码见 [diskannscan.cpp](/home/qiumc/data/code/cpp/openGauss-vector-filter-p12/src/gausskernel/storage/access/datavec/diskannscan.cpp:314)。

这里限定为**本次新增过滤路线**。同文件中原生 DiskANN 扫描的内部排序和补批逻辑仍存在，不能写成“整个 DiskANN 都没有补批”；原生逻辑也不能直接视为已经完成新路线的第三阶段协议。

既有本机 Oracle 证据中，合格行有 20 行、请求 Top-10，POST 在同一 `HNSW SCAN POST-FILTER` 内进行了 5 次近似和 1 次穷尽搜索；节点 `Starts=1`，没有启动第二棵普通关系计划。该结论有函数调用顺序和官方统计语义交叉支持，见[原实测报告与原始证据](验证/Oracle运行时Exact回退核验_20261010_100228/Oracle运行时精确搜索切换实测报告.md)。官方将 exhaustive 计数定义为 HNSW 索引内的精确搜索次数。[官方统计说明](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/oracle-ai-vector-search-dictionary-statistics.html)

我们已有 AUTO 的不足 K 记录，因此这一缺项会影响返回行数和召回，而不仅是运行速度。例如既有 `join768_50` 的 500 查询记录中，预算 192 选择 POST，255 条不足 K，平均召回 85.18%；预算 256 选择 POST，94 条不足 K，平均召回 95.90%。这些是既有本库质量记录，不是此次两端对照，也不据此推导 Oracle 在同数据下必然返回更多。见[原始质量记录](测试附件/前两阶段最终测试/PRE_IN路线优势验证/Join_PRE五预算500召回/records.json)。

需要保留两条边界：

1. Oracle 的近似检索公开说明允许不足 K，目标 accuracy 也不是必达保证。不能把差距表述为“Oracle 永远返回 K，我们不能”。[近似检索说明](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/understand-approximate-similarity-search-using-vector-indexes.html)
2. 既有 52 条 Oracle PRE／IN 测试没有触发 exhaustive，但这不能证明所有输入都不触发。我们已确定的项目红线仍是 **PRE／IN 不做精确搜索；POST 在第三阶段通过同计划第二 AM 入口完成，不保存备用普通计划**。见[PRE／IN 原实测](验证/Oracle_PRE_IN穷尽搜索核验_20261010_141443/Oracle前中过滤搜索行为实测报告.md)。本次不修改该红线。

### 2.2 B：按目标质量查询及评估的接口

Oracle 提供 `WITH TARGET ACCURACY`，也允许 HNSW 显式指定 `EFSEARCH`。目标 accuracy 会参与搜索工作量设定，但指定 100 仍不能等同于强制精确检索。[HNSW 目标精度说明](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/overview-hierarchical-navigable-small-world-indexes.html)

我们目前能通过人工调参和外部基准测量平均召回 92%／95%／99%，但产品尚不能直接接受“本次查询按目标精度 95 执行”并据此决定搜索工作量。**测试得到某组参数达到 95%，与具备目标精度查询接口，是两件事。**

Oracle 另有 `DBMS_VECTOR.INDEX_ACCURACY_QUERY`／`INDEX_ACCURACY_REPORT`，可以评估查询向量或已捕获工作负载的索引精度。我们已有 EXPLAIN 搜索／资格探测／内存等计数，但没有等价的内置精度评估接口。Oracle 这些接口也不能直接当成“任意业务 Join 过滤后的平均召回报告”；需按接口范围使用。[官方精度报告](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/index-accuracy-report.html)

### 2.3 C：哪些 SQL 还能进入新过滤路线

本次同时检查源码和实际执行，得到如下更精确的边界。

| SQL／计划形态 | Oracle 23.26.2 本次结果 | 我们当前结果 | 判断 |
|---|---|---|---|
| 常量向量＋单表标量条件 | PRE，10 行 | PRE，10 行 | 两端主体链路均可用 |
| 绑定查询向量 | PRE，10 行 | custom plan PRE，10 行；generic plan 强制 PRE 报错，AUTO 为普通扫描＋Sort，10 行 | 我们要求规划时绑定；Oracle 绑定变量实现不等同于 PG generic plan，未推断其内部缓存机制 |
| 绑定 K | PRE，10 行 | 源码要求规划时已知正整数 K | 运行期 K 的新路线接入尚未完成 |
| 查询向量与 K 同时绑定 | IN，10 行 | 受上述两项门槛限制 | Oracle 本机确有该形态 |
| 无相关标量子查询 | PRE，10 行 | 转成 InitPlan 后 PRE，10 行 | 不应列作整体缺项 |
| 相关子查询作为资格条件 | 非空样例 PRE，10 行；另一个空资格样例 0 行 | 本次对应类别样例强制 PRE 报“无完整安全资格路线” | 证明具体 SQL 形态接入范围有差异；未证明任意相关 SubPlan 都可用，Oracle 也可能改写子查询 |
| CTE 作为资格来源 | INLINE 样例 PRE；MATERIALIZE 样例普通计划 | 本次保留 CTE 样例被新路线拒绝 | 是否合并／内联很重要，不能把所有 WITH 一概判断 |
| 可空向量列，没有显式非空条件 | PRE，10 行 | 本次无域证明表被拒绝 | 我们的域证明前提更严格 |
| 距离后增加 `id` 排序键 | 本机没有 HNSW，Hint Report 明示多排序表达式不适用 | 强制 PRE 被拒绝 | **不是本机版本已经证实的差距**；最新在线语法另有说明 |
| 非零 OFFSET | 普通计划，无 HNSW | 新路线要求 OFFSET 为零 | **不能算我们独有的缺项** |

完整 SQL／错误／返回结果见 [Oracle 第一批 8 项](验证/Oracle26ai功能差距核验_20261029/SQL边界结果.json)、[补充 3 项](验证/Oracle26ai功能差距核验_20261029/补充SQL边界结果.json)、[非空相关子查询](验证/Oracle26ai功能差距核验_20261029/CORRELATED_NONEMPTY.json)、[本库 9 项](验证/Oracle26ai功能差距核验_20261029/OG边界结果.json)。每条 Oracle SQL 的同名 `.log` 保存实际游标计划和 Hint Report；本库保存 EXPLAIN 及实际查询结果／错误。

本次通过显式模式／Hint 检查能力边界，不据此宣称 AUTO 必然选择这些路线。两端使用已有不同数据集、部分不同距离类型，不用于比较结果 ID 相等或召回差异。

当前准入检查集中在 [diskannFilterPath.cpp](/home/qiumc/data/code/cpp/openGauss-vector-filter-p12/src/gausskernel/optimizer/path/diskannFilterPath.cpp:210)：查询形态、规划期 K、查询向量 Param、向量域、存储类型都在此检查。目标列或过滤条件中的 Param 与“查询向量 Param”不能混为一谈；本次 InitPlan 用例已经证明标量 `PARAM_EXEC` 可以使用。

## 3. 若比较整个 Oracle 产品，还存在的扩展功能差距

这些是功能范围扩展，不按性能优劣计分，也不自动纳入当前 P3。

### 3.1 分区及分布执行

当前新路线限定单节点、行执行器、非分区 AStore，拒绝 Stream／并行查询等形态。Oracle 有本地分区 HNSW 和分区搜索能力，也有 RAC 分布 HNSW。故分区／分布形态是明确尚未适配的范围。[本地分区 HNSW](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/local-hnsw-indexes.html)、[RAC 分布 HNSW](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/hnsw-distribution-oracle-rac.html)

Oracle 的这些功能也有组合限制，例如本地分区 HNSW 的 DML、INCLUDE、在线构建等限制，不能把各页面功能随意组合成“任意分区、任意 Join、任意并发模式都支持”。本次没有启动 RAC 或新增分区索引实测。

UStore 属于我们同源仓迁移时的存储适配问题，不能说 Oracle 具有“UStore 支持”。既有迁移红线继续有效：先核实目标仓扫描行为和存储类型，再制定方案。

### 3.2 索引、距离表达式及数据形态

当前这套新过滤能力只适配单列、非 PQ 的 DiskANN，目标必须为基础向量列，距离限于已登记的 L2／COSINE。表达式索引、带其他列的索引、部分索引、PQ 以及其他 AM 均不能直接套用。代码见 [ann_preserve_filter_candidate](/home/qiumc/data/code/cpp/openGauss-vector-filter-p12/src/gausskernel/optimizer/path/diskannFilterPath.cpp:345)。

Oracle 的 HNSW 与 IVF 都有向标过滤能力，但两种 AM 的过滤方案并不完全相同；不能写成每种 AM 都提供同样的三模式。Oracle 23.26.2 还加入函数／表达式 HNSW，23.26.4 加入 HNSW 乘积量化。[HNSW 计划](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/optimizer-plans-hnsw-vector-indexes.html)、[IVF 计划](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/optimizer-plans-ivf-vector-indexes.html)、[23.26.2 发布说明](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/april-2026-release-update-23.26.2.html)、[23.26.4 发布说明](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/October-2026-release-update-23.26.4.html)

这里是**新特性适配范围**的差异，不是说 openGauss 没有 HNSW／IVF／PQ 实现，也不意味着必须把 DiskANN 换成 HNSW。若业务只需要当前 AStore＋DiskANN＋L2／COSINE，这些可列为扩展范围，而不是现有功能未完成。

覆盖索引 INCLUDE 在 Oracle 有公开功能，而用户此前明确将其从本项目开发／测试基线剔除。本次仅记录范围差异，不恢复相关提交，不因其减少回表而给核心功能扣分。[Oracle INCLUDE](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/included-columns-hnsw-indexes.html)

### 3.3 分组 Top-K／多向量检索形态

Oracle 文档中的 multi-vector search 包括按文档／段落等层级分组取结果；IVF 在 23.26.2、HNSW 在 23.26.4 提供相应能力。HNSW 使用 `PARTITIONS BY`、包含分组列的索引和 `VECTOR INDEX HNSW PART-SCAN` 等机制。我们的当前路线是一条查询向量、一个目标关系、普通最终 LIMIT，没有这种分组 ANN Top-K 能力。[HNSW multi-vector](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/multi-vector-search-using-hnsw-indexes.html)、[IVF multi-vector](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/multi-vector-search-ivf-index.html)

这不是“多个距离表达式任意加权排序”的同义词，也不是另一个项目的 BM25＋向量 RRF。Oracle 该功能仍可能不足每组请求行数，且有索引／分组列限制。HNSW 新版能力此次仅由官方文档确认，未在本机旧 RU 验证。

## 4. 已有功能和不能算成差距的事项

| 事项 | 当前判断 |
|---|---|
| 单表 PRE／IN／POST | 已实现，不是只完成后过滤 |
| Join PRE／IN／POST | 已实现完整资格关系与普通输出 Join 的组合，不是只能单表 |
| INNER／SEMI／ANTI／保留向量目标侧的 OUTER | 已有相应验收；是否可用仍受化简后的关系语义限制，不意味着全部 OUTER 形态都可用 |
| 非唯一匹配、业务列、重复结果 | 已处理；候选 TID 去重不等于结果 DISTINCT，最终 LIMIT 按结果行计数 |
| Nested Loop 内侧 | 已有重扫、参数／扫描周期处理及既有 Oracle 对照；不存在“内侧结果必须唯一”的总限制 |
| 普通精确计划 | 规划时仍可选择，AUTO 保留普通路线；缺的是执行中 POST 的第二 AM 搜索形式 |
| AStore MVCC、更新／删除后的读取 | 已有身份核验和并发测试；不能笼统称为不支持更新／删除。不同快照／存储适配仍需各自验证 |
| PRE 资格集、IN 候选资格回调 | 已实现；不能以没有独立 VectorFilterScan 节点推断功能缺失 |
| no-join-back、候选行列值复用、成本选择更精细 | 主要是访问方式和优化能力；按用户最新要求，不计入本报告核心功能缺项 |
| OFFSET、WITH TIES 等 | Oracle 近似检索也有相应限制，不能直接列为我们单方面差距 |
| FULL OUTER／可生成空向量目标的 Join | 当前普通计划保底；既有特定样例两端都未走 ANN，尚不能据此断言 Oracle 全面支持而我们不支持 |

关系语义证据见 [Join 资格与固定结果对照](执行包/evidence/J-04/Join资格语义与Oracle对照验收.md)、[非唯一匹配及最终行数](执行包/evidence/J-05/非唯一匹配与最终行数验收.md)。当前阶段状态与验收引用以 [task_state.json](执行包/state/task_state.json) 的 receipt 为准；当前源码的后续限定回归见[已有回归记录](测试附件/前两阶段最终测试/PRE_IN路线优势验证/行数估算修复/限定完整Release回归/summary.json)。这些是既有验证，不冒充本次重新全量验收。

**行为与结果一致的确认范围仍然是已冻结的 SQL、数据与比较规则。** 不能由有限用例推出任意跨引擎 ANN 返回 ID 相同，尤其是候选集合不同或距离并列而没有稳定次排序键时。Oracle 内部搜索、并列截断的全部私有实现仍未取得公开设计证明。

## 5. 版本差异和未确认事项

最新在线 `SELECT` 文档允许距离作为首排序键、后面附加其他键；本机 23.26.2 的实测却明确因多排序表达式不使用 HNSW。本报告将其记为**在线文档能力与本机版本有差异**，不指定未经证实的引入 RU，也不列入本机已证实的差距。[当前 SELECT 文档](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/SELECT.html)、[本机原始 Hint Report](验证/Oracle26ai功能差距核验_20261029/SECOND_SORT.log)

Oracle 通用 guidelines 页面与较新的函数索引／multi-vector 专题页面存在范围或更新时间不一致。本次优先采用明确 RU 的发布说明、功能专题和实际执行证据，没有把通用旧限制同时套到新版功能上。全部官方来源见[资料索引](验证/Oracle26ai功能差距核验_20261029/官方资料索引.json)。

以下只能确认本库边界，尚未形成 Oracle 对等能力证据，不能列作确定的竞争差距：任意 RLS／VPD 组合、Serializable 下的全部 ANN 形态、任意大 PRE 集合的磁盘溢写、任意多表局部 Top-K 与剩余 Join 的安全改写。当前本库资格哈希超出 `diskann_filter_work_mem` 会报资源限制；普通 Sort／Hash 能溢写，不代表资格哈希已具备溢写，也没有证据证明 Oracle 的对应内部集合可无限溢写。

## 6. 按功能补齐的建议顺序

1. **先完成已设计的第三阶段搜索控制。** 保持现有计划红线，补最终行数反馈、有界继续搜索、去重／停止状态，POST 增加同一 AM 内精确搜索入口；PRE／IN 仍不做精确搜索。这解决已确认的执行行为缺项。
2. **扩展常见 SQL 的接入。** 先处理 generic plan 查询向量／运行期 K 的安全校验，再细分相关子查询、可合并 CTE、可空列／COSINE 非零域的支持边界。需要新增语义证明及 Oracle 对照，不能直接删除 guard。
3. **补产品质量接口。** 明确目标 accuracy 的含义、参数映射与评估 API；针对业务过滤／Join 的平均召回另行定义样本和 Exact 参考，不能把 Oracle 的索引精度接口直接当业务结果保证。
4. **按业务需求决定扩展范围。** 分区、其他 AM／存储、表达式索引、量化、分组 Top-K 分别立项。不能宣称做完 P3 就覆盖这些能力，也不因它们缺失而否定当前基本向标融合功能。

上述是功能差距建议，不是本次实施授权或设计变更。本次交付为源码核验、官方资料核验及只读功能探测；完整新证据保存在[本次核验目录](验证/Oracle26ai功能差距核验_20261029)。
