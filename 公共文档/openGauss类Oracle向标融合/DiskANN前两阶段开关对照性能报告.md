# DiskANN 前两阶段开关对照性能报告

**状态：最新 55c8fc61ae 的正式性能测试已完成，最终版本审计 50/50 通过；阶段签收以 taskctl 的凭据为准。** 此报告针对移除覆盖索引后的新基线；历史包含 INCLUDE 的数据不进入本次结论。部分典型 SQL 已达到 10 倍，不能据此宣称所有场景达到目标。

## 对比与质量口径

同一个 Release/O2 安装、同一个静态数据目录、相同索引和相同 SQL，对比 `enable_diskann_filter=off` 与 `on`。开启侧 AUTO 是主要自动选路结果；显式 PRE/IN/POST 单独用于分析。搜索预算允许分别校准，必须先满足可比的平均召回率，再测量时延。Exact 仅用于生成质量参考。

根据用户最新要求，先核对 OFF/ON 的完整实际计划；计划相同的场景，两侧都独立生成并复用 gplan，再比较性能。保留直接 SQL 的完整请求对照，并单列首次准备、生成计划的开销。不能让两侧共用 OFF 时生成的计划来掩盖开启侧规划，也不能只为一侧缓存计划。计划核对保留节点、索引、过滤模式、表达式及子计划，忽略估算成本数值；`EXPLAIN (COSTS OFF, VERBOSE)` 直接执行与双方 gplan 的文本须完全一致。

固定 D2 前 500 个查询向量，最终 Top-10。每个查询计算 `Recall@10 = 实际结果与该 SQL 的 Exact 参考结果交集行数 / 10`，再对全部 500 个查询取算术平均。返回不足 10 行仍以 10 为分母，不跳过困难查询。三档目标为平均召回率 92%、95%、99%；参数选择依据质量，不依据时延。正式配对时两侧均值差不得超过 0.5 个百分点，并同时记录各自实测值、逐查询召回率、返回不足次数和搜索预算。

主 SQL 保持自然代价选择。若关闭特性后自然计划是 Exact、平均召回率固定 100%，不能把它与开启后的 92%/95%/99% 直接称作等质量提升。用于比较两侧 ANN 实现的额外试验可在两侧相同 SQL 中使用同一个原生 ANN hint，须明确标成控制试验，不能混入无 hint 的 AUTO 结论。

## 数据与环境

- 源码：`/home/qiumc/data/code/cpp/openGauss-vector-filter-p12`，AStore；覆盖索引移除基线 `ba1e4ad2f9`，阶段一 `415b78e6c9`，阶段二当前 `55c8fc61ae`，历史提交 `3fb6d6466f`、`b5cac7b01a` 由备份分支保留。当前编译二进制 SHA-256 为 `0743303a36e4f7eac9383d1a0012476be14127f8893a018b8d62925a98dbef0c`；源码冻结与真实运行映像分别核对，版本 banner 的旧 build 字段不能用于推定实际源码。下文第一轮 b5 数字属于旧二进制，不与本轮数字拼接。
- 独立 Release 安装：`/home/qiumc/data/code/cpp/.install/diskann-vector-filter-no-include-p12-release`；独立实例端口 55450，数据目录 `.runtime/diskann-vector-filter-no-include/data`。
- 平均召回数据：`vf_switch_recall.documents`，100,000 行、768 维归一化 float32 向量；单向量 key 的 DiskANN cosine 索引，`index_size=16`。复用既有 D2 源向量文件，重新装载并构建无覆盖索引，全部数据库向量逐位核对通过。D2 是主题词词袋投影生成的合成数据，不能外推成真实业务负载。
- 本报告正式 500 查询主矩阵的单表与 Join 均采用约 50% 的独立业务资格；3.125% 的额外诊断记录不作为主矩阵收益。bucket 为 `CRC32(id 的小端 uint32 编码)%32`；Join 通过独立业务表完成，不使用索引 payload。原始九条典型 SQL 另覆盖重复向量、非唯一业务匹配和 NL 内侧；它们的原 SQL、投影及 LIMIT 保持冻结。
- 本轮未启动 Oracle 或 Milvus；Oracle 行为正确性引用此前保存的原始输出，不产生新的 Oracle 性能结论。

数据核验见 [数据来源及逐位核验](执行包/evidence/no-include-performance-20261011/recall-data-verified/manifest.json)，精确答案见 [固定查询集的 Exact 参考](执行包/evidence/no-include-performance-20261011/recall-truth-500/truth.json)。准备过程首轮 ANALYZE 多命令调用失败已单独保留；后续只修正调用并继续核验，没有重新装载或重建索引。

## 已发现的问题与修正范围

1. 开启 AUTO 时，旧原生 ANN 路径仅保存为新路线的派生模板，没有独立参与最终成本竞争。修正为继续交给原生 `add_path` 构造普通 Scan/Join，同时独立持有派生模板，防止剪枝后悬空。完整精确资格源仍拒绝 ANN 输入。
2. 原图工作估计始终按完整索引成员计费，明显保守。优化前一条 10 万行查询实际访问约 8,166 个图节点，仍按 10 万成员计价。新估计使用最多 32 个节点页的拓扑样本与实际 beam 配置；未知、小索引或不可用样本使用全成员回退。密度必须从完整索引统计取得，不能使用局部工作量替代。该估计仍须通过实际计数和选路结果验收。
3. 图展开重复复制向量并计算了未消费的距离，候选队列用线性比较定位插入位置。修正仅涉及查询读路径中的计算和查找；MVCC、HOT、key 核验、SQL 资格和最终普通 Sort 保留。

上述修改已用统一 Release 脚本编译，当前 1439 项核心功能、原生 8 项、规划 42 项、AUTO 数量门 8 项通过。未新增计划节点、未修改持久格式、写入或 WAL；没有实现第三阶段补搜或精确搜索。

新增修正：AUTO 旧单表 ANN 按实际有界批次/内部完整 Sort 计价，Join 下面的旧 ANN 不误套 LIMIT 早停。前两阶段的新固定预算路线预计完整输出不足 K 时，AUTO 保留其他路线；显式模式仍如实交付实际候选。POST 不再对全部保留的导航组重复计算和排序图距离，快照/key 核验与最终普通 Sort 保留。

旧版本一次有序 ANN 扫描在 Join 下面可能因未收到 LIMIT 而走完全部批次；这一点不能当作关闭开关时所有 SQL 都强制走 ANN 的证据。自然计划仍可能选择更快的普通 Exact。

平均召回的整数预算阶跃：116/204/1024 校准到 92.56%/95.38%/99.04%，两侧均值及逐查询结果一致。三档作为目标下限并公开实际值；两侧均值差仍要求不超过 0.5 个百分点。该单表控制试验的当前 AUTO 保留旧 ANN 路线，不预设会达到 10 倍。

强制 PRE/IN 在某些场景诊断较慢：完整资格构建可能扫描全部目标，IN 对搜索过程中访问的成员执行核验/业务 Probe，而普通关系路线可能只处理少量合格行。显式模式用于说明实现行为，不能把其劣化混作自动选路的收益。当前完整拓扑样本仍有明显查询相关误差，不是运行时上界；物理页跨度诊断也不能据此推导向量图的实际搜索跳数，未把这个假设写入成本代码。

## 正式测量要求与完成范围

每个正式窗口至少预热 10 秒、计时 30 秒，并在完整 500 查询周期结束；按 OFF→ON→ON→OFF 运行。保存逐查询时延、结果核对、完整执行计划及图访问/回表/资格 Probe 计数。正式配对先声明取样规则：按时间顺序分别取首个干净 OFF→ON 配对与首个干净 ON→OFF 配对，两者可来自不同尝试。每个配对的两个窗口都必须零换页、零 major fault；任一受污染则丢弃该整个配对，不能留下它的干净一侧，也不按时延挑选。旧版本的整批作废尝试仍保留为诊断，不重新纳入新协议。结果仅在整批结束后写盘，避免工作区索引器在计时期间被证据写入唤起。

当前完成九组自然 AUTO、四组独立 backend 的同计划 gplan、四组同 backend 的同计划 gplan、十二组 Scan 和四组 Join 的正式对照。九条典型 SQL 的单向量重复测量与 500 查询平均召回矩阵分别报告；每组完整 P50/P95/P99/QPS、预算、资源和接受窗口均在原始 JSON 中。Join 的 92%/95% 共八组质量无法匹配，明确排除，不能写成已经完成等召回性能比较。

## 最新优化版本复测状态（55c8fc61ae）

当前内核32个C++／头文件新增2923、删除17、净增2906行；不包含测试和文档。当前Release的16组功能、42项规划、20项优化器及8项数量门均通过；九条冻结SQL的232配置复核完成，20组共同完整结果预算保持可用。

单表固定500查询已完成15组质量核验：OFF/AUTO/PRE/IN/POST的平均Recall@10分别为92.56%、95.38%、99.04%，无不足Top-10。原联合校准在极慢的带hint旧Join处中止，保留INCOMPLETE和中止原因，只提取已经完整执行的15组单表记录，不把整个中止运行称为通过。

Join 使用两侧相同、不带 ANN hint 的 SQL。关闭侧自然普通计划的 500 查询平均召回为 100%；开启侧 116/204 预算为 92.56%/95.38%，因此八组 92%/95% 档不满足 0.5 个百分点容差，不报告等质量收益。AUTO 的 1024 预算选择普通精确计划、召回 100%；显式 PRE/IN/POST 的 6144 预算平均召回均为 99.64%，与关闭侧相差 0.36 个百分点，完成四组正式对照。6144 是已经测试的最低合格预算，未证明它是所有整数中的最小值；选择依据质量与预算，不依据时延。原 Q00～Q08 SQL、投影和 LIMIT 均未改动。

正式复测已经结束。质量证据：[典型SQL质量复核](执行包/evidence/no-include-performance-20261011/early-prune-nine-case-quality-profile/summary.json)、[完整单表质量记录及中止范围](执行包/evidence/no-include-performance-20261011/early-prune-mean-recall-scan-join/interruption-reason.json)、[自然Join质量核验](执行包/evidence/no-include-performance-20261011/early-prune-join-natural-recall/summary.json)。质量校准期间的时延属于诊断，不进入正式性能结论。

### 最新九组自然 AUTO 的直接 SQL 对照

每个场景一条冻结 SQL、一个固定查询向量，反复执行 OFF→ON→ON→OFF。所有查询的完整投影及重复次数均与冻结 Exact 参考相同；这是九条典型 SQL 的完整结果验证，不是 500 查询的平均召回率测试。每个接受窗口的 swap-in、swap-out、major fault 增量为零。按照时间顺序保留首个干净的正向及反向配对；Q02、Q05 的两对来自不同补跑批次，不按耗时挑选。

| 场景 | OFF P50 (ms) | ON AUTO P50 (ms) | ON/OFF QPS |
|---|---:|---:|---:|
| Q00 无过滤 | 198.559 | 8.986 | 21.753× |
| Q01 单表保留50%，正相关 | 119.817 | 9.033 | 13.175× |
| Q02 单表保留1%，正相关 | 1.057 | 1.110 | 0.952× |
| Q03 单表保留1%，独立过滤 | 175.379 | 66.494 | 2.637× |
| Q04 Join保留1% | 2.491 | 2.753 | 0.910× |
| Q05 Join全部允许 | 274.957 | 11.646 | 23.453× |
| Q06 重复向量及多匹配，K32 | 15.552 | 15.816 | 0.9995× |
| Q07 NL内侧非唯一，原Oracle SQL | 0.763 | 1.138 | 0.673× |
| Q08 10万行768维、每目标4匹配，K32 | 497.942 | 12.287 | 40.141× |

Q00、Q01、Q05、Q08 达到 10 倍；Q02、Q04、Q07 仍有完整请求劣化，Q06 本轮 QPS 差约0.05%。不能将本轮与 b5 不同时段的差额全部归因为代码优化。原始证据：[九组正式汇总](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/summary.json)、[完整时延及接受窗口](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json)。

### 相同计划的 cplan / gplan 验证及正式对照

全部九组检查后，Q02、Q04、Q06、Q07 的直接执行和 OFF/ON 独立 gplan 均有相同的完整计划文本。两侧分别建立会话、保持自己的开关及预算，设置 `plan_cache_mode=force_generic_plan`，以原始字节不变的 SQL `PREPARE`；首次及重复 `EXECUTE` 均实际收到 `Generic plan is used` 日志。计时前关闭这些诊断日志，持续复用各自计划。其他五组计划不同，保留在直接 SQL 对照中，不强行改成相同计划。

| 场景 | OFF生成计划的诊断中位耗时 (ms) | ON生成计划的诊断中位耗时 (ms) | 增量 (ms) |
|---|---:|---:|---:|
| Q02 | 0.422 | 0.468 | 0.046 |
| Q04 | 0.683 | 0.796 | 0.114 |
| Q06 | 0.388 | 0.539 | 0.151 |
| Q07 | 0.635 | 0.996 | 0.361 |

上表来自交替执行40次普通 `EXPLAIN (COSTS OFF, VERBOSE)`，包含解析、分析、优化与计划输出，**不是纯优化器时间，也不是正式查询时延**。Q07 的0.361ms增量与直接请求P50增加0.376ms接近，支持额外规划准备开销这一方向；待正式 gplan 对照进一步验证。首次 EXECUTE 的诊断耗时单独保存，包含首次规划、执行及传输，不冒充纯规划时间。质量/计划证据：[gplan核验](执行包/evidence/no-include-performance-20261011/early-prune-gplan-profile/summary.json)、[全部计划及首次/重复Generic日志](执行包/evidence/no-include-performance-20261011/early-prune-gplan-profile/records.json)。四组正式 gplan 计时已完成，接受窗口均零换页、零 major fault，完整结果多重集与参考一致。倍数仍为 **ON AUTO / OFF**，两侧都使用 gplan，不是 gplan 与 cplan 的性能比较。

| 场景 | OFF gplan P50 (ms) | ON gplan P50 (ms) | ON/OFF QPS |
|---|---:|---:|---:|
| Q02 | 0.832 | 0.847 | 0.9771× |
| Q04 | 2.044 | 2.093 | 0.9800× |
| Q06 | 14.783 | 14.861 | 0.9943× |
| Q07 | 0.321 | 0.331 | 0.9781× |

Q07 完整请求的32.65% QPS下降在双方复用 gplan 后缩小到2.19%，与规划准备开销的诊断相符，但剩余差异尚不能断言为噪声或零回退。四组采用各自独立会话；进一步用同一 backend 顺序生成双方计划，排查会话差异，结果全部保留。原始证据：[四组gplan正式汇总](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-same-plans/summary.json)、[四组时延及接受窗口](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-same-plans/measurements.json)。

已另行验证实际 cplan：四组各两侧共8项，完整 `COSTS OFF VERBOSE` 文本与原直接执行及双方 gplan 一致。使用一个声明但未使用的整数参数，仅为了让 `boundParams` 非空；SELECT正文、过滤、向量、排序和LIMIT均未改变。该代码仓无参数时在检查 `force_custom_plan` 前就选择 gplan，不能仅凭 GUC 声称测试了 cplan。真实 Custom/Generic 日志均已保存；此审计不产生性能数字。[cplan/gplan完整形式审计](执行包/evidence/no-include-performance-20261011/early-prune-actual-cplan-gplan-audit-v2/summary.json)。首轮审计因复用的 catalog 查询范围不同而失败，保留原始失败，修正为原对照相同 catalog 后通过。

同一 backend 的补测四组全部完成，均在首个ABBA中得到干净对称窗口，完整投影多重集一致。结果为Q02 **1.0029×**、Q04 **0.9952×**、Q06 **0.9968×**、Q07 **0.9959×**。仍报告实际差异，不写成零下降；独立会话约2%的下降在此测法中未复现，普通请求与缓存请求的差额支持规划准备是主要因素。Q06的Hash Join存在为兼容观察到的业务输入次序而反转桶链的实际执行开销，不能仅凭节点相同就断言执行代码完全相同。Q02/Q04/Q07没有ANN或Hash Join执行节点；当前证据未显示固定2%的算法退化，无需据这个差额改内核。保留所有测法，不能仅选择最好数字。[同backend正式汇总](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-one-backend-v2/summary.json)、[完整测量](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-one-backend-v2/measurements.json)。首次补测因采样器读取旧实例数据目录而在正式采样前中止，失败归档保留；v2仅修正测试脚本环境，内核与SQL不变。

500查询计时脚本也已按此规则更新：先逐查询核对双方及真实cplan/gplan完整形式；所有查询相同才双方复用gplan，所有准备在预热前完成；有不同计划则保留直接SQL比较。两条冻结查询、四种模式的实际生命周期预验已通过，不能当作整个500查询矩阵通过。[预验](执行包/evidence/no-include-performance-20261011/early-prune-mean-cache-preflight/summary.json)。

## 最新500查询 Scan 等召回率正式结果

完整12组均通过。每档固定500查询及相同SQL，双方平均召回率与逐查询结果已复现；所有接受窗口零swap/major fault。AUTO的全部500条直接、真实cplan、gplan完整形式双方相同，因此双方预热前生成各自gplan，同一backend计时。PRE/IN/POST与OFF计划不同，保留直接SQL测法。此Scan质量控制双方使用相同ANN hint，不能当作自然AUTO在任意SQL上的收益。

| 实际平均召回率 | AUTO（双方gplan） | PRE | IN | POST |
|---|---:|---:|---:|---:|
| 92.56% | 1.0921× | 0.0224× | 0.4321× | 0.8228× |
| 95.38% | 1.0109× | 0.0283× | 0.4568× | 0.7838× |
| 99.04% | 1.0030× | 0.0965× | 0.4454× | 0.7854× |

倍数为ON/OFF QPS。三个AUTO档接近持平或小幅提升，**此500查询矩阵未达到10×**；四条典型SQL的13～40×不能外推为各档平均收益。强制模式均更慢，但AUTO没有选取这些方案。

慢的原因分为两类：AUTO仍选原生ANN，执行路线无新增收益；强制模式有实际额外工作。92.56%档实际计划中，PRE资格源全表扫描得到50,001行，资格源50,000次buffer访问，整条路线共102,195次访问；IN遍历1,251节点并进行1,251次资格Tid Scan；POST仅116次候选成员身份/可见性检查，仍增加查询过滤协议及外层Sort等工作。OFF原生ANN本身已有候选排序、过滤和补批，此Scan场景不宜强制替换。以上为EXPLAIN实际工作量，不把buffer次数直接换算为墙钟占比，也不将POST全部下降归因于某一个尚未单独计时的函数。

[12组正式汇总](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/summary.json)、[逐组时延与接受窗口](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/measurements.json)、[全部500查询真实cplan/gplan审计](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/plan-cache-audits.json)。有污染的原始批次全部保留；IN99第一轮2次、第二轮57次major fault的窗口未采用，不按性能选取窗口。

## 最新500查询 Join 等质量正式结果

四组正式对照全部通过，每组两侧各计时 1000 次查询（两个完整 500 查询周期），错误与不足 Top-10 均为零。AUTO 的全部 500 条直接、真实 cplan、gplan 完整形式相同，双方使用同一 backend 的各自 gplan；显式模式的计划不同，双方保留直接 SQL。

| 模式 | OFF / ON 平均召回 | 计时形式 | OFF P50 ms | ON P50 ms | ON/OFF QPS |
|---|---|---|---|---|---|
| AUTO | 100.00% / 100.00% | gplan | 139.186 | 139.305 | 0.9981× |
| PRE | 100.00% / 99.64% | direct SQL; different plans | 139.441 | 577.892 | 0.2423× |
| IN | 100.00% / 99.64% | direct SQL; different plans | 142.494 | 491.352 | 0.2902× |
| POST | 100.00% / 99.64% | direct SQL; different plans | 142.637 | 184.955 | 0.7782× |

AUTO 为普通 `Limit → Sort → Hash Join → Seq Scan`，保留关闭侧计划，基本持平。显式模式均比关闭侧普通计划慢，没有达到 10×。99 档 POST 的较低预算 1024/1536/2048/4096 平均召回分别为 99.04%/99.20%/99.24%/99.46%，均未匹配关闭侧 100%；8192 为 99.70%，6144 为 99.64%，以较低的已测合格预算 6144 完成 PRE/IN/POST 对照。所有质量配置和失败记录保留，不从快的窗口反选预算。

代表查询的实际工作量解释如下，计数用于定位工作，不直接等同于 CPU 时间占比：

| 模式 | 图访问节点 | 回表身份核验 | 资格工作 | 进入最终 Sort 的行 | 查询过滤状态峰值 |
|---|---:|---:|---|---:|---:|
| OFF / AUTO | 不使用 ANN | 普通全表扫描 | 100,000 目标行与 16 条业务行 Hash Join | 50,001 | 无查询过滤状态 |
| PRE | 16,244 | 8,207 | 先完整构建 50,001 条资格，之后图搜索；整条路线 136,742 次 buffer 访问 | 6,144 | 40,536,184 B |
| IN | 16,244 | 16,244 | 16,244 次 Tid Scan 与业务主键 Probe，8,207 次成功；整条路线 93,509 次 buffer 访问 | 6,144 | 39,489,120 B |
| POST | 16,244 | 6,144 | 图输出 6,144 候选，普通 Hash Join 得到 3,064 行；整条路线 40,822 次 buffer 访问 | 3,064 | 38,447,224 B |

PRE/IN 的资格检查允许省略输出 Join，仅限业务键唯一且只投影目标列、已证明不丢失重复和业务列的分支。本场景符合该证明；非唯一匹配或业务列投影仍保留真实输出 Join。POST 始终由普通关系节点执行这条 SQL 的业务连接。尽管 POST 的 buffer 访问少于普通计划，随机图访问、距离计算与 6144 次成员核验仍有成本；当前计数不能将 0.7782× 全部归因于某个单函数。

接受窗口依时间顺序选择：AUTO/IN 首轮全部干净；PRE 正向取 attempt2、反向取 attempt1；POST 取 attempt1。PRE attempt0/1/2 和 POST attempt0 的受污染窗口及完整原始批次均保留，各接受窗口的 swap-in/swap-out/major fault 增量均为零。PRE 的正反两对来自不同批次是预先规定的取样规则，不是按时延挑选。

[四组正式汇总](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/summary.json)、[全部时延与接受窗口](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/measurements.json)、[500查询完整计划与缓存审计](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/plan-cache-audits.json)、[质量配对与八组排除原因](执行包/evidence/no-include-performance-20261011/early-prune-join-mean-selection.json)。

## 资源、行为验证和交付边界

正式 Scan/Join 共 16 组、64 个接受窗口保存返回数、全服务 VmRSS、查询过滤状态峰值、Sort 和临时 I/O。全服务 RSS 包含共享缓冲、其他已分配状态，不能当作单查询私有内存。各路线代表查询的最终 Sort 为 top-N heapsort，计划中未记录临时 I/O；这只适用于本轮负载，Hash spill、取消和 ReScan 由另外的功能回归验证。

当前源码 40 个输入文件与实际 Release 二进制逐项哈希一致，32 个内核 C++/头文件 +2923/-17、净增 2906 行。1439 项核心功能（含 626 项并发）、原生 8 项、规划 42 项、优化器 20 项、AUTO 数量门 8 项均通过；无覆盖索引实际基线的持久边界 14 项通过，没有修改页格式、WAL、AStore DML/VACUUM 或引入新计划节点。

13 项 Oracle 行为/最终结果硬门使用不可变 Oracle 原始 SQL 与输出，在当前 Release 重新验证。PRE 在首次图搜索前完整 EOF/Seal，IN 在图搜索期间同步执行真实 SubPlan，POST 由普通父节点过滤；非唯一匹配与 NL 六次重扫的 epoch、同语句快照和输出重复均有真实运行观察。当前独立 DWARF 类型对象严格采用实际 C++11/USE_SPQ 配置，只供 GDB 读取，不链接到服务；此前缺类型或 ABI 偏移的脚本失败均保留。IN next/rescan 重入错误与连接复用也重新通过。不同 ANN 引擎任意输入的近邻 ID、并列截断成员恒等仍未证明，不能从 13 项有限对照推广。

[最终50项审计](执行包/evidence/no-include-performance-20261011/early-prune-final-delivery-audit/summary.json)汇总完整来源、正式配对、资源和范围；[当前Oracle对照](执行包/evidence/no-include-performance-20261011/early-prune-oracle-compatibility-v4/compatibility-actual-symbol-audit/summary.json)、[最终工作区与服务](执行包/evidence/no-include-performance-20261011/early-prune-final-workspace/summary.json)保留原始证据。自有 55450 已 fast 停止、数据保留，Oracle/Milvus 未启动，swappiness 恢复 60；源码仅两个本地特性提交，未 push，用户无关资产保留。

已确认四条典型 SQL 的自然 AUTO 达到 13～40×；其余自然场景可能接近持平或仅约 2.64×。500 查询 Scan/Join 未达到 10×，强制模式有明显劣化，AUTO 避开这些方案。收益大小取决于代价选择、过滤率、图搜索预算和数据分布；没有证明普遍零退化或全场景 10×。当前成本拓扑采样的查询误差、真正业务数据和更大并发的收益仍是后续优化方向。第三阶段补搜、反馈控制及仅 POST 的同计划第二 AM 精确入口未实施，PRE/IN 不做精确搜索。

## 优化前第一轮正式结果（阶段二 b5cac7b01a）

本轮是同一个无覆盖索引 Release 二进制的开关对照。九条冻结 SQL 共 20 组，每次查询都校验完整投影结果的多重集合，与对应 Exact 参考一致；这九条样例的结果召回为 100%，不能将其称为 500 查询集的平均召回试验。表中的倍数为开启侧 QPS / 关闭侧 QPS，小于 1 表示退化。AUTO 为主要收益，显式模式为诊断。

| SQL / 模式 | OFF P50 (ms) | ON P50 (ms) | QPS 倍数 |
|---|---:|---:|---:|
| Q00_auto | 222.901 | 9.159 | 23.990× |
| Q01_auto | 133.695 | 9.216 | 14.318× |
| Q01_post | 133.492 | 10.486 | 12.610× |
| Q02_auto | 1.073 | 1.180 | 0.917× |
| Q02_pre | 1.075 | 15.459 | 0.077× |
| Q03_auto | 199.171 | 67.924 | 2.908× |
| Q03_in | 199.434 | 81.405 | 2.461× |
| Q04_auto | 2.605 | 3.552 | 0.744× |
| Q04_pre | 2.615 | 236.554 | 0.011× |
| Q04_in | 2.580 | 50.337 | 0.053× |
| Q05_auto | 299.023 | 11.497 | 25.844× |
| Q05_post | 299.307 | 10.940 | 27.153× |
| Q06_auto | 15.499 | 16.432 | 0.943× |
| Q06_post | 15.557 | 2.226 | 6.722× |
| Q07_auto | 0.744 | 2.172 | 0.352× |
| Q07_in | 0.742 | 157.134 | 0.005× |
| Q08_auto | 521.095 | 11.879 | 43.412× |
| Q08_pre | 520.644 | 1750.973 | 0.298× |
| Q08_in | 513.851 | 52.566 | 9.575× |
| Q08_post | 517.448 | 11.323 | 44.890× |

Q00 为无过滤，Q01 为 50% 单表资格，Q02 为 1% 强相关单表资格，Q03 为 1% 独立单表资格，Q04 为 1% Join，Q05 为宽范围 Join，Q06 为重复向量和四次业务匹配，Q07 为向量目标在 NL 内侧，Q08 为 10 万行 / 768 维向量与 40 万行非唯一业务匹配。最终 Top-10，Q06/Q08 为 Top-32。

九类 AUTO 中五类提升，四类退化。Q00/Q01/Q05/Q08 超过 10 倍；Q03 约 2.9 倍。共核验 492,053 次正式查询。P99 在慢的显式 PRE 等低样本窗口只能作为本批样本分位数，不能用来承诺 SLA。完整 P50/P95/P99/QPS、预算、计划及原始窗口见 [20 组正式对照](执行包/evidence/no-include-performance-20261011/formal-twenty-pairs-isolated/measurements.json)。

平均召回 500 查询控制试验使用两侧相同 ANN hint，三档两侧分别都是 92.56% / 95.38% / 99.04%，预算 116 / 204 / 1024，逐查询结果一致且无不足 Top-10。当前 AUTO 在这组控制试验保留原生 ANN；P50 分别慢约 2.13% / 3.34% / 0.73%，没有 10 倍收益。它只验证等平均召回的开关开销，不能代替 PRE/IN/POST 各自三档的性能矩阵。原始证据见 [三档控制试验](执行包/evidence/no-include-performance-20261011/formal-control-three-tiers-isolated/summary.json)。

第一轮结果对应 b5cac7b01a，不能直接归属于后续优化提交。新增优化只提前执行完整输出行数门和可证明的 startup 下界剪枝；不改代价价格、过滤语义或显式模式的执行路径。下一轮使用新二进制重新核验全部功能，并复测 AUTO 及平均召回控制试验。
