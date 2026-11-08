# DiskANN 前两阶段最终测试报告

本报告按测试环境、测试数据、测试SQL、测试模型、测试结果五部分整理无覆盖索引基线的前两阶段实测。**§5.1只展示OFF与自然AUTO的主对照，分为收益至少2×和性能劣化两张表；强制模式、hint及历史测法控制移出展示，完整51条记录保留在附件中。** 后续补测统一追加到§5.1；表内明确每轮版本、实际计划、召回、时延与QPS，其他章节保留详细分析和证据。

测量于2026-10-11～2026-10-12完成，涉及55c8fc61ae、670d0c5e6a、72778fe2aa三个P2版本，历史结果不改写。原G-P1/G-P2凭据保留，第三阶段未实施。展示表的收益筛选不改变完整测试范围，也不代表所有场景都提升。

## 1. 测试环境

### 1.1 主机与版本

| 项目 | 配置 |
|---|---|
| 运行环境 | WSL2 / x86_64，openEuler 22.03 LTS-SP4 |
| CPU | Intel Core i9-12900H；WSL 可见20个逻辑CPU、1个NUMA节点，属于虚拟拓扑 |
| 内存 / Swap | 约7.8 GiB / 4.0 GiB |
| 磁盘 | /dev/sdd，ext4；当前卷总容量约1007 GiB，未记录底层物理盘型号 |
| 系统补录 | Linux 6.18.33.2-microsoft-standard-WSL2 x86_64 GNU/Linux；这是报告生成时的静态信息，测试期间资源以窗口记录为准 |
| 数据库 | openGauss 7.0.0-RC3，单机 AStore |
| 编译 | 统一 Release/O2，enable_debug=no、enable_cassert=no，GCC 10.3.0 |
| 源码 | /home/qiumc/data/code/cpp/openGauss-vector-filter-p12 |
| 无覆盖索引基线 | ba1e4ad2f99295b1b0f65e3880a05cb75a1121d4 |
| 原始测量提交 | P1：415b78e6c9；P2：55c8fc61ae9291fe4471194d6e6f9ad90ff178d1 |
| 原始测量二进制SHA-256 | 0743303a36e4f7eac9383d1a0012476be14127f8893a018b8d62925a98dbef0c |
| §5.4.2测量两个提交 | P1：415b78e6c9；P2：670d0c5e6a9a7da22c3c6eb20ce121035c06fa11 |
| §5.4.2二进制SHA-256 | c00e6e25ac383c7fdee97c614e89fadd4d14b0b2f0eab2c1b033b95c93df825a |
| 当前两个提交 | P1：415b78e6c9；P2：72778fe2aae139cfade1abe7396e3e085869ee22 |
| 当前二进制SHA-256 | bf484902a3ab0fdd53a87a8f884b4797847de1339db4a98758aef0a49e83fb0e |
| 安装 | /home/qiumc/data/code/cpp/.install/diskann-vector-filter-no-include-p12-release |
| 连接 | Unix socket：.runtime/diskann-vector-filter-no-include/socket；55450；库 vf_p12_baseline |
| 数据目录 | /home/qiumc/data/code/cpp/.runtime/diskann-vector-filter-no-include/data |

版本 banner 的旧 build 字段不能代替源码/映像核验；最终40个源码输入文件哈希与实际Release对应。静态主机信息见[环境补录](测试附件/前两阶段最终测试/环境补录.json)，运行身份见 [九组运行身份](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/identity.json)。正式窗口内没有编译、GDB或其他数据库测试，Oracle/Milvus停止。

### 1.2 会话与索引参数

| 参数 | 设置 |
|---|---|
| shared_buffers | 128 MB |
| work_mem | 128 MB |
| diskann_filter_work_mem | 131072 KiB，即128 MiB查询过滤状态预算 |
| query_dop | 1 |
| enable_opfusion | off |
| statement_timeout | 180 s，单语句限制 |
| 向量索引 | DiskANN一列向量key，index_size=16；无INCLUDE payload |
| 数据/事务 | 静态数据；只读REPEATABLE READ |
| 对比 | 同一个Release二进制，enable_diskann_filter=off 与 on |
| ON模式 | auto为主要自动选路；pre/in/post为强制模式诊断 |

索引、数据、SQL正文、投影、过滤与LIMIT在每个配对中相同。搜索预算先按质量校准，具体见§3.3。Exact用于质量参考；关闭开关时由原优化器自然选择原生ANN或普通计划，并非一律强制Exact。

## 2. 测试数据

### 2.1 九条典型 SQL

| 场景 | 目标行数 | 维度/距离 | 业务关系 | 资格与最终K |
|---|---|---|---|---|
| Q00 | 100,000 | 768/L2 | 无 | 无过滤，K10 |
| Q01 | 100,000 | 768/L2 | 无 | id≤50000，50%，K10 |
| Q02 | 100,000 | 768/L2 | 无 | id≤1000，1%，K10 |
| Q03 | 100,000 | 768/L2 | 无 | mod(id×7919,100000)<1000，1%，K10 |
| Q04 | 100,000 | 768/L2 | 100,000业务行，每目标1匹配 | bucket_pos<10，1%，K10 |
| Q05 | 100,000 | 768/L2 | 100,000业务行，每目标1匹配 | bucket_pos<1000，全部，K10 |
| Q06 | 8,192 | 8/L2，每向量8目标 | 32,768业务行，每目标4匹配 | 全部，K32 |
| Q07 | 50,000 | 8/cosine | 1,550业务行，非唯一bucket键 | tenant=7、bucket IN(626,286)，K10 |
| Q08 | 100,000 | 768/L2 | 400,000业务行，每目标4匹配 | 全部，K32 |

Q00～Q05/Q08共用`vf_p1_quality_20261011.docs_100000`；每行有8/768维向量和交替16/2048字符的PLAIN文本。向量第一坐标为`1+id/100000`，其余为0.1；查询向量第一坐标为1，因此id区间与近邻正相关。Q03的乘法取模过滤降低这种相关性。这是合成测试分布，不是生产数据统计。

业务表位于`vf_p12_perf_v2`，`bucket_pos=floor((target_id-1)×1000/目标行数)`，范围0～999，有普通target_id/bucket索引。Q06的`duplicates`有1024个不同向量、每向量8个目标；输出保留每目标4条业务匹配。Q07使用`vf_oracle_join_p2`中已保存的Oracle原静态输入及原SQL，目标50000行、业务1550行；性能阶段没有启动Oracle。

### 2.2 500 查询平均召回矩阵

| 项目 | 数据 |
|---|---|
| 目标 | vf_switch_recall.documents，100,000行、768维归一化float32 |
| 来源 | 合成D2主题词词袋向量投影到768维并归一化，不是生产业务数据 |
| 查询 | 源文件1000查询，正式固定query_id=0～499，共500个，所有查询等权 |
| 资格 | bucket_id=CRC32(id小端uint32编码)%32，bucket_id<16，共50,001目标 |
| 业务表 | allowed_buckets共32行，bucket_id主键；0～15 enabled=true，共16行 |
| Scan/Join资格 | 均约50%，本矩阵无多匹配；多匹配由Q06/Q08覆盖 |
| 索引 | vf_switch_docs_cos一列DiskANN cosine；目标id/bucket普通索引与业务主键 |
| 数据核验 | 新装载全部float32逐位、bucket逐行核验；新建无覆盖索引，不复用旧INCLUDE catalog |
| 质量参考 | 逐SQL/逐查询向量冻结普通Exact Top-10 |

向量文件SHA-256：`2d6da658ec199512b84612efb08958db69bb2e85a41036c95d8401df9695f437`；查询文件SHA-256：`b1eac770648ad3a3f2de2e772106699333197e720d93395d10427877316b8642`。见 [逐位核验](执行包/evidence/no-include-performance-20261011/recall-data-verified/manifest.json)、[500查询Exact参考](执行包/evidence/no-include-performance-20261011/recall-truth-500/truth.json)。

## 3. 测试 SQL

### 3.1 Q00～Q08 原查询

每个场景只有一条冻结SQL和一个固定查询向量，重复执行。下文仅把完整向量字面量替换为排版符号：`q768_l2=[1,0.1,…,0.1]`（768维）、`q8_l2=[1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]`、`q8_cos=[1,3,5,7,11,13,17,19]`。这些符号不是测试绑定参数；正式直接SQL和gplan都保留原完整字面量SELECT，没有将查询向量参数化。

[完整原始字面量SQL](测试附件/前两阶段最终测试/Q00-Q08原始测试SQL.sql)可阅读/复跑；原投影、Join、WHERE、hint、ORDER BY、LIMIT保持不变。模式和计时由原正式工具配置。

#### Q00：无过滤控制：仅比较前后既有扫描路径

```sql
SELECT d.id,d.body,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 10;
```

#### Q01：单表弱过滤：保留50%，近邻与过滤正相关

```sql
SELECT d.id,d.body,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
WHERE d.id <= 50000
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 10;
```

#### Q02：单表强过滤：保留1%，近邻与过滤正相关

```sql
SELECT d.id,d.body,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
WHERE d.id <= 1000
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 10;
```

#### Q03：单表强过滤：保留1%，过滤与近邻独立

```sql
SELECT d.id,d.body,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
WHERE mod(d.id*7919,100000) < 1000
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 10;
```

#### Q04：业务Join强过滤：存储列等值连接，保留1%

```sql
SELECT d.id,a.business_id,d.body,a.payload,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
JOIN vf_p12_perf_v2.business_n100000_m1 a ON d.id=a.target_id
WHERE a.bucket_pos < 10
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 10;
```

#### Q05：业务Join弱过滤：存储列等值连接，全部允许

```sql
SELECT d.id,a.business_id,d.body,a.payload,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
JOIN vf_p12_perf_v2.business_n100000_m1 a ON d.id=a.target_id
WHERE a.bucket_pos < 1000
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 10;
```

#### Q06：重复向量与业务多匹配：每向量8目标、每目标4业务行，原K32

```sql
SELECT d.id,a.business_id,d.body,a.payload,d.v8 <-> :q8_l2::vector AS distance
FROM vf_p12_perf_v2.duplicates d
JOIN vf_p12_perf_v2.business_n8192_m4 a ON d.id=a.target_id
WHERE a.bucket_pos < 1000
ORDER BY d.v8 <-> :q8_l2::vector
LIMIT 32;
```

#### Q07：NL内侧非唯一：原Oracle兼容SQL、全局Top10完整业务行

```sql
SELECT /*+ leading((a d)) nestloop(a d) */ d.id,d.bucket_id,a.acl_group
FROM vf_oracle_join_p2.documents d
JOIN vf_oracle_join_p2.allowed_buckets a ON a.bucket_id=d.bucket_id
WHERE d.tenant_id=7 AND a.bucket_id IN(626,286)
ORDER BY d.embedding <=> :q8_cos::vector
LIMIT 10;
```

#### Q08：高维多匹配Join：10万目标、768维、每目标4业务行，原K32

```sql
SELECT d.id,a.business_id,d.body,a.payload,d.v768 <-> :q768_l2::vector AS distance
FROM vf_p1_quality_20261011.docs_100000 d
JOIN vf_p12_perf_v2.business_n100000_m4 a ON d.id=a.target_id
WHERE a.bucket_pos < 1000
ORDER BY d.v768 <-> :q768_l2::vector
LIMIT 32;
```

### 3.2 500 查询矩阵 SQL

`q768_cos_i`代表第i个固定查询向量，i=0～499；正式工具逐条提交完整字面量SQL，同计划场景每条SQL也各自准备gplan。

Scan控制试验，两侧共同原生ANN hint：

```sql
SELECT /*+ IndexScan(d vf_switch_docs_cos) */ d.id
FROM vf_switch_recall.documents d
WHERE d.bucket_id < 16
ORDER BY d.embedding <=> :q768_cos_i::vector
LIMIT 10;
```

自然Join对照，两侧均无ANN hint：

```sql
SELECT d.id
FROM vf_switch_recall.documents d
JOIN vf_switch_recall.allowed_buckets a ON d.bucket_id = a.bucket_id
WHERE a.enabled
ORDER BY d.embedding <=> :q768_cos_i::vector
LIMIT 10;
```

Scan共同hint使关闭侧保留原生ANN，用于比较等平均召回的实现开销，不代表无hint自然AUTO的普遍收益。Join保留自然成本选择，关闭侧可选择普通精确计划；不能在一侧改变hint或SQL配平质量。

[Scan完整500条冻结SQL](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/input-sql-freeze.json)、[Join完整500条冻结SQL](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/input-sql-freeze.json)。

### 3.3 预算与质量配置

| 组别 | OFF probes | ON probes | 质量 |
|---|---|---|---|
| Q00～Q06、Q08 | 32 | 32 | 该固定查询完整投影多重集等于Exact |
| Q07 | 2048 | 2048 | 原SQL原K完整投影多重集等于Exact |
| Scan目标92% | 116 | 116 | 两侧92.56% |
| Scan目标95% | 204 | 204 | 两侧95.38% |
| Scan目标99% | 1024 | 1024 | 两侧99.04% |
| Join99档AUTO | 116（普通计划不消费） | 1024（普通计划不消费） | 两侧100% |
| Join99档PRE/IN/POST | 116（普通计划不消费） | 6144 | OFF100%、ON99.64%，差0.36个百分点 |

预算先依据质量选择，不看校准时延；6144是已测试的最低合格预算，未证明它是所有整数中的最小值。每个配对SQL/索引不变，允许搜索预算分别校准；实际差值与排除原因见§5.4。

## 4. 测试模型：预热、并发与计时

### 4.1 负载与缓存协议

| 项目 | 协议 |
|---|---|
| 并发 | 单连接、单请求在途，并发=1；没有多客户端性能或并发写入压测 |
| 执行并行 | query_dop=1，SQL串行；资源采样线程不是查询并发 |
| 缓存 | 预热后的重复负载；不清空OS page cache，不宣称冷态随机I/O性能 |
| 每窗口预热 | 至少10 s，启用结果核验，预热时延不入正式统计 |
| 每窗口计时 | 至少30 s；单SQL完成当前请求后结束 |
| 500周期 | 固定顺序完整遍历500查询，周期边界才结束，实际窗口可显著超过30 s |
| 顺序 | OFF → ON → ON → OFF，正反各一对 |
| 完整计划相同 | 双方独立生成各自gplan，全部准备在预热前完成；Q02/Q04/Q06/Q07及矩阵AUTO适用 |
| 完整计划不同 | 双方直接SQL；其余五条典型SQL及显式PRE/IN/POST矩阵 |
| 实际形式核验 | COSTS OFF VERBOSE保留节点、索引、模式、表达式、子计划；直接/真实cplan/gplan一致 |
| 快照 | 静态数据、只读REPEATABLE READ，资格/回表遵守同快照 |
| 会话 | 同会话gplan为最终同计划补测；独立会话gplan另存，两个会话轮流计时，不是并发2 |

无参数PREPARE在本仓检查force_custom_plan之前选择generic。真实cplan审计声明一个未使用的整数参数使boundParams非空，保留真实Custom/Generic日志；SELECT、向量、过滤与LIMIT未变。此审计不产生性能数字，见 [真实cplan/gplan审计](执行包/evidence/no-include-performance-20261011/early-prune-actual-cplan-gplan-audit-v2/summary.json)。

### 4.2 窗口污染与统计

正式期间swappiness暂设0，批次前宿主用户缓存页prefault，结束恢复60；零swappiness不能替代监控。接受窗口全局pswpin、pswpout、pgmajfault增量必须全部为零。

预先声明：按时间顺序取首个干净OFF→ON配对和首个干净ON→OFF配对，两对可来自不同尝试。一个窗口污染则整对不采用，不留下干净的一半、不按时延挑选。原始污染、异常、未完成运行保留。正式期只写临时目录，整批结束后发布工作区证据，降低索引器被唤起的干扰。

单次时延从客户端execute前到完整fetchall后，包含客户端驱动、Unix socket和数据库请求，不是EXPLAIN节点时间。该次时延计点后核验结果；QPS为完成数/窗口墙钟，因此包含请求间校验和采样开销，不能用1/P50替代。P50/P95/P99汇总同一侧两个接受窗口的全部时延，按原工具排序插值；不平均不同场景的QPS作为整体收益。

```text
QPS倍数 = ON QPS / OFF QPS
QPS变化 = (QPS倍数 - 1) × 100%
```

### 4.3 正确性与召回口径

九组典型SQL逐次核验原完整投影多重集与重复次数，等于固定Exact参考。固定一个向量的结果相同，不能声称500查询平均召回为100%。

500查询分别计算Recall@10=实际与对应Exact Top-10交集行数/10，再对全部500查询取算术平均。少返回仍以10为分母，不删除低召回查询。原矩阵按两侧均值差≤0.5个百分点配对；2026-10-12按用户新口径补充：OFF自然精确保持100%，ON ANN达到平均92%/95%/99%目标即可，不强迫ANN达到100%，并明确标为“精确基线对比目标召回”。双方均为ANN时仍按相近平均召回配对。每个计时请求还必须重现各自冻结质量结果。返回不足次数、预算、质量、性能分别公开。

### 4.4 正式测量覆盖

| 正式试验 | 组数 | 接受窗口 | 完成查询数 |
|---|---|---|---|
| 九组直接SQL | 9 | 36 | 301,620 |
| 四组独立会话gplan | 4 | 16 | 504,759 |
| 四组同会话gplan | 4 | 16 | 494,574 |
| Scan三档×四模式 | 12 | 48 | 206,000 |
| Join等质量四模式 | 4 | 16 | 8,000 |
| Q00共同hint直接SQL/gplan | 2 | 8 | 29,886 |
| 55版本显式PRE/IN补测 | 6 | 14 | 8,000 |
| 670版本AUTO补测 | 3 | 8 | 8,000 |
| 727版本AUTO PRE/IN及高召回补测 | 6 | 18 | 34,000 |
| 727版本AUTO PRE超过10×场景 | 1 | 4 | 5,000 |

合计51条正式比较、184个接受窗口/逻辑窗口、1,599,839次接受请求，完整数据保存在附件；§5.1按用户要求筛选展示，不改变本节统计范围。后续分段测量中的共享OFF按实际两窗计数，不按处理数重复累计；污染重试不计入接受请求。整窗与分段计时口径、历史版本仍逐行区分，不能将上述请求拼成一项吞吐结论。

## 5. 测试结果

### 5.1 OFF与自然AUTO对比（收益与劣化）

**按用户要求，本节只比较关闭特性OFF与开启后自然AUTO的主对照。** 显式指定PRE/IN/POST、共同hint及历史测法控制均不进入展示表。共20条自然AUTO主对照，其中收益表展示QPS倍数≥2×的12条，劣化表展示QPS倍数<1×的5条；其余3条主对照未达到收益展示阈值且无劣化。完整51条正式结果及原始证据仍保留在附件中。后续补测统一按此口径追加到这两张表，其他章节只保留分析和证据。

每行均为同一二进制的OFF/ON配对；“P2版本”区分55、670、727三轮，P1均为415b78e6c9。历史数据保留原值，**不能跨行、跨版本拼接OFF/ON，也不能把全表计算成一个平均加速比**。后续用同一数据/SQL复测时追加版本和来源，不能静默覆盖旧结果。

时延统一为P50/P95/P99（ms），QPS倍数=AUTO/OFF，变化=(倍数−1)×100%；2×相当于QPS增加100%。两侧完整计划相同时采用已核验的同会话双方gplan，其余使用直接SQL；不再将同场景的历史测法控制重复列入。展示筛选不改变原计时和质量。分段计时遵循固定50查询段的首次干净采纳规则，不能与整窗计时跨行合并。

“单向量”是一个固定向量的Recall@K，Q06/Q08按完整业务结果行及重复次数计算、K=32，其余K=10；不能称为500查询平均召回。“500均值”是固定500查询等权平均Recall@10。OFF自然精确100%而ON达到目标均值的行，不称等召回对比；ANN双方质量匹配仍按原规则。所有正式行均无返回不足和错误，质量筛选失败/未计时项保留在详细章节。

表内编号：Q为原九组主对照，J为原Join AUTO，A为670版本AUTO，N-E/N-P为727版本外置/内联向量AUTO，P-A为新增自动前过滤Scan场景。92/95/99后缀表示目标档位，实际均值见召回列。场景链接直接指向对应原始测量，实际计划的old/new表示原有/新增能力，不表示两个二进制。附件中的S/E/H/C等诊断或历史控制仍保留，未混入本节主对照。

#### 5.1.1 收益至少2×

| 场景/记录 | P2版本 | ON模式/用途 | 预算OFF/ON | 测法 | OFF实际计划 | ON实际计划 | 召回OFF/ON | 返回OFF/ON（K） | OFF时延ms | ON时延ms | OFF/ON QPS | QPS倍数（变化） | 详情 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Q00](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | 直接SQL | 普通精确（全表扫描 + Sort） | old ANN（无过滤） | 单向量 100.00%/100.00% | 10/10（10） | 198.559/210.047/217.440 | 8.986/10.614/11.423 | 5.022/109.251 | 21.7531×（+2075.31%） | §5.2 |
| [Q01](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | 直接SQL | 普通精确（主键扫描 + Sort） | old 后过滤（原生ANN） | 单向量 100.00%/100.00% | 10/10（10） | 119.817/128.048/132.135 | 9.033/10.467/11.177 | 8.288/109.198 | 13.1748×（+1217.48%） | §5.2 |
| [Q03](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | 直接SQL | 普通精确（全表过滤 + Sort） | old 后过滤（原生ANN） | 单向量 100.00%/100.00% | 10/10（10） | 175.379/187.285/196.564 | 66.494/72.815/74.960 | 5.654/14.908 | 2.6366×（+163.66%） | §5.2 |
| [Q05](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | 直接SQL | 普通精确（Hash Join + Sort） | new POST（ANN + NL Join） | 单向量 100.00%/100.00% | 10/10（10） | 274.957/287.773/294.139 | 11.646/12.920/13.670 | 3.623/84.970 | 23.4527×（+2245.27%） | §5.2 |
| [Q08](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | 直接SQL | 普通精确（Hash Join + Sort） | new POST（ANN + NL Join） | 单向量 100.00%/100.00% | 32/32（32） | 497.942/511.877/519.382 | 12.287/13.585/14.353 | 2.003/80.410 | 40.1406×（+3914.06%） | §5.2 |
| [A92-AUTO（外置/Join50%）](测试附件/前两阶段最终测试/AUTO代价修复/正式AUTO对照/measurements.json) | 670d0c5e6a | AUTO／主对照 | 512/512 | 直接SQL/分段 | 普通精确（Hash Join + Sort） | new POST（ANN + NL Join） | 500均值 100.00%/92.36% | 10/10（10） | 262.752/290.533/316.344 | 12.449/33.948/41.441 | 3.766/63.241 | 16.7938×（+1579.38%） | §5.4.2 |
| [A95-AUTO（外置/Join50%）](测试附件/前两阶段最终测试/AUTO代价修复/正式AUTO对照/measurements.json) | 670d0c5e6a | AUTO／主对照 | 512/2048 | 直接SQL/分段 | 普通精确（Hash Join + Sort） | new POST（ANN + NL Join） | 500均值 100.00%/95.90% | 10/10（10） | 262.752/290.533/316.344 | 42.342/80.564/86.369 | 3.766/20.745 | 5.5089×（+450.89%） | §5.4.2 |
| [A99-AUTO（外置/Join50%）](测试附件/前两阶段最终测试/AUTO代价修复/正式AUTO对照/measurements.json) | 670d0c5e6a | AUTO／主对照 | 512/6144 | 直接SQL/分段 | 普通精确（Hash Join + Sort） | new POST（ANN + NL Join） | 500均值 100.00%/99.54% | 10/10（10） | 262.752/290.533/316.344 | 120.192/184.455/196.020 | 3.766/7.589 | 2.0152×（+101.52%） | §5.4.2 |
| [N-P92（内联/Join10%）](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_IN/measurements.json) | 72778fe2aa | AUTO／主对照 | 48/48 | 直接SQL/分段 | 普通精确（Hash Join + Sort） | new IN | 500均值 100.00%/92.94% | 10/10（10） | 122.618/136.099/151.109 | 8.714/10.833/11.966 | 8.077/113.229 | 14.0186×（+1301.86%） | §5.4.3 |
| [N-P95（内联/Join10%）](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_IN/measurements.json) | 72778fe2aa | AUTO／主对照 | 48/60 | 直接SQL/分段 | 普通精确（Hash Join + Sort） | new IN | 500均值 100.00%/95.48% | 10/10（10） | 122.618/136.099/151.109 | 10.008/12.311/13.305 | 8.077/99.023 | 12.2598×（+1125.98%） | §5.4.3 |
| [N-P99（内联/Join10%）](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_IN/measurements.json) | 72778fe2aa | AUTO／主对照 | 48/256 | 直接SQL/分段 | 普通精确（Hash Join + Sort） | new POST | 500均值 100.00%/99.96% | 10/10（10） | 122.618/136.099/151.109 | 13.749/16.065/17.369 | 8.077/72.076 | 8.9235×（+792.35%） | §5.4.3 |
| [P-A92（外置/Scan12.5%）](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式计时_PRE/measurements.json) | 72778fe2aa | AUTO／主对照 | 40/40 | 直接SQL/分段 | 普通精确（Bitmap Heap Scan + Sort） | new PRE（Bitmap资格源） | 500均值 100.00%/92.46% | 10/10（10） | 202.982/281.644/298.583 | 18.135/20.737/22.164 | 4.742/54.847 | 11.5673×（+1056.73%） | §5.4.4 |

#### 5.1.2 性能劣化（QPS小于1×）

以下仅列自然AUTO主对照，按QPS倍数从低到高排列，保留版本、召回和实际计划。纳入条件使用未四舍五入的实际倍数，包括小幅下降。当前五条两侧最终计划相同，均使用双方同会话gplan，QPS下降约0.19%～0.48%；尚未证明这些微小差额具有统计显著性或会稳定复现。强制模式、hint和其他历史测法控制不计入本表。

| 场景/记录 | P2版本 | ON模式/用途 | 预算OFF/ON | 测法 | OFF实际计划 | ON实际计划 | 召回OFF/ON | 返回OFF/ON（K） | OFF时延ms | ON时延ms | OFF/ON QPS | QPS倍数（变化） | 详情 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| [Q04](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-one-backend-v2/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | gplan（同一会话） | 普通精确（NL Join + Sort） | 普通精确（NL Join + Sort） | 单向量 100.00%/100.00% | 10/10（10） | 2.102/2.939/3.407 | 2.119/2.946/3.415 | 451.905/449.755 | 0.9952×（-0.48%） | §5.2 |
| [Q07](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-one-backend-v2/measurements.json) | 55c8fc61ae | AUTO／主对照 | 2048/2048 | gplan（同一会话） | 普通精确（NL Join + Sort） | 普通精确（NL Join + Sort） | 单向量 100.00%/100.00% | 10/10（10） | 0.334/0.586/0.758 | 0.335/0.590/0.758 | 2549.534/2539.201 | 0.9959×（-0.41%） | §5.2 |
| [Q06](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-one-backend-v2/measurements.json) | 55c8fc61ae | AUTO／主对照 | 32/32 | gplan（同一会话） | 普通精确（Hash Join + Sort） | 普通精确（Hash Join + Sort） | 单向量 100.00%/100.00% | 32/32（32） | 15.050/16.477/17.308 | 15.091/16.482/17.219 | 65.651/65.440 | 0.9968×（-0.32%） | §5.2 |
| [N-E99（外置/Join5%）](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_PRE99同计划/measurements.json) | 72778fe2aa | AUTO／主对照 | 512/512 | gplan（同会话）/分段 | 普通精确（Hash Join + Sort） | 普通精确（Hash Join + Sort） | 500均值 100.00%/100.00% | 10/10（10） | 26.107/28.583/30.081 | 26.222/28.575/30.125 | 37.985/37.902 | 0.9978×（-0.22%） | §5.4.3 |
| [J99-AUTO（Join/50%）](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/measurements.json) | 55c8fc61ae | AUTO／主对照 | 116/1024 | gplan（同会话） | 普通精确（Hash Join + Sort） | 普通精确（Hash Join + Sort） | 500均值 100.00%/100.00% | 10/10（10） | 139.186/147.987/153.068 | 139.305/148.097/151.882 | 7.142/7.128 | 0.9981×（-0.19%） | §5.4 |

原九组中Q00/Q01/Q05/Q08超过10×，Q03约2.64×。Q00的21.7531×来自普通精确改选old ANN，不能理解为同一个ANN执行计划加速；相关共同hint控制保留在附件，不计入AUTO主对照。最新N-P92/N-P95自然选择new IN，分别约14.02×/12.26×。收益不足2×且没有劣化的记录仍保留在完整附件中。该合成样本不证明所有场景10×，也未完成新布局上调优旧ANN/POST的相近召回全量PK。

两表由[统一汇总数据](测试附件/前两阶段最终测试/全量结果表.json)逐条生成并与原始measurements.json核对；[结果与来源](测试附件/前两阶段最终测试/最终结果与来源.json)保留每轮版本和证据哈希。

### 5.2 保留原完整请求和独立会话对照

直接SQL包含每次解析/规划与执行。原九组的最终取值和历史控制均保存在完整附件中，§5.1仅展示自然AUTO主对照中收益至少2×或出现劣化的记录，不能用缓存后的结果覆盖完整请求劣化。

同计划三种测法见完整附件的Q主对照与C历史控制记录，保留各自QPS变化。

Q07完整请求下降32.65%，缓存后缩小到独立会话2.19%、同会话0.41%，支持规划准备是主要因素，但不是纯优化器计时或零回退证明。Q06为兼容观察到的业务输入次序而反转Hash桶链，有实际执行工作；同节点形式不代表执行代码完全相同。Q02/Q04/Q07没有ANN或Hash Join执行节点。全部独立会话结果见 [独立会话gplan](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-same-plans/measurements.json)。

#### 5.2.1 Q00强制原生ANN：验证选路收益

2026-10-12补测，仅在原Q00的SELECT中增加 `/*+ IndexScan(d docs_100000_768_l2) */`，两侧使用相同hint、相同SQL主体、相同当前Release二进制、相同 `diskann_probes=32`。OFF和ON AUTO均生成 `Limit→Ann Index Scan`，没有新Filter Mode。直接计划、实际cplan、实际gplan完整VERBOSE形式一致；gplan在同一会话中分别生成，两侧都在预热前完成准备。

每组ABBA四窗口，每窗口预热10s、测量30s，合计8个接受窗口、29,886次正式查询；每次原完整结果多重集均与冻结Exact参考相等，均返回10行，错误为零，所有接受窗口的pswpin/pswpout/pgmajfault均为零。同计划gplan下ON QPS本轮低1.04%，不能写成零劣化。

该补测证明关闭功能后强制原生ANN也能达到相近性能，Q00的21.75×应归因于自然选路改善，不能归因于相同ANN执行计划的大幅加速。补测改变了SQL hint，属于收益归因对照；它不替换§5.1原自然选路结果，也不把本次和原试验的跨日绝对时延直接相除计算收益。来源：[直接SQL原始测量](测试附件/前两阶段最终测试/Q00索引hint对照/正式ABBA/measurements.json)、[同会话gplan原始测量](测试附件/前两阶段最终测试/Q00索引hint对照/同会话gplan正式ABBA/measurements.json)、[实际缓存策略与计划形式](测试附件/前两阶段最终测试/Q00索引hint对照/同会话gplan正式ABBA/plan-cache-audits.json)。

#### 5.2.2 统计信息、行数估计与代价估计分别核对

Q00的实际表行数100,000，`pg_class.reltuples=99,942`，误差0.058%；本次核对的其他八张业务/目标表行数统计与实际COUNT一致。Q00的主要问题是旧ANN成本估计：关闭时 `diskanncostestimate_internal`把完整索引的tuple数量交给通用索引成本计算，并把总索引成本作为启动成本；普通LIMIT只能折算后续成本，不能消除这部分过高的启动成本。实测OFF强制ANN启动成本400,761.82，高于自然全表扫描加排序的78,413.99，因此OFF自然选了全表扫描。开启后单表原生ANN成本按分批搜索、必要补批和内部排序重估，原生ANN计划启动成本为802.62。成本单位不是毫秒。

| 核对项 | 实际证据 | 判断 |
|---|---|---|
| Q00/Q01/Q03自然选路 | OFF普通精确；ON原生ANN。Q01/Q03在OFF加索引hint也生成原生ANN，完整结果与参考相同 | 三组都有原生ANN选路收益；Q01/Q03本次只补核计划和结果，没有补跑正式hint性能窗口 |
| Q02/Q04/Q06/Q07最终路线 | 两侧均为普通精确，完整节点和表达式形式相同 | 不能把这些组的小幅变化计为新增过滤路线收益 |
| Q05/Q08最终路线 | OFF全量Hash Join；ON明确使用新POST及候选驱动的Nested Loop | 使用新增路线；不属于Q00那种单表原生ANN成本重估 |
| Q03普通过滤行数 | `mod(id*7919,100000)<1000`估计33,314行，实际1,000行 | 该表达式没有可用统计时，原标量不等式选择率函数回退到默认1/3；不是表行数统计错了。新成本也复用这个选择率，估计仍有偏差 |
| Q07 Join输出行数 | 两侧Join均估计1行，实际300行；内表估计每次5行，实际300行/6次=50行 | 强相关的tenant/bucket条件按独立条件组合，再叠加未正确反映过滤后连接键分布的Join选择率，开关两侧都低估 |

Q07进一步拆解：`tenant_id=7`单独估计2,502行、实际2,500行；两个bucket单独估计100行、实际100行。两个条件合用估计5行、实际100行，因为这两个bucket的50+50行全部属于tenant 7，条件选择率不能直接相乘。优化器日志显示其按tenant条件过滤后仍估计918.05个bucket，实际只有50个；再限定这两个bucket后，连接键分布更集中。Join成本日志两侧输入各估计5行、连接选择率0.0010825781，得到约0.027行，按原生最小行数规则显示为1行；实际输入6条业务行匹配100条目标行，同两bucket匹配，共300行。该结论依据单条件/组合条件实测、原生DEBUG2日志及`calc_joinrel_size_estimate`的乘积公式，未修改统计或内核代码。详见[Q07选择率拆解](测试附件/前两阶段最终测试/代价与统计核验/Q07选择率拆解.json)。

注意本openGauss TEXT计划的 `actual rows`是跨loops累计值：Q07内表300行、6次执行，平均每次50行；Q08业务内表128行、32次执行，平均每次4行。不能照搬PostgreSQL按每次执行显示实际行数的解释，也不能把受LIMIT提前停止的实际输出行数直接当完整扫描行数统计误差。

此前从接受窗口原始数据复算了完整附件的Q系列九条最终记录及S/J系列十六条矩阵记录：P50/P95/P99、完成数/墙钟QPS、ON/OFF倍数均一致；500查询平均召回按每个查询等权复算，返回K与逐次质量检查也一致。指标数值没有发现计算错误，单固定向量100%和500查询平均召回仍分别表述；VmRSS仍是整个服务进程内存。证据：[计划和统计核验](测试附件/前两阶段最终测试/代价与统计核验/核验.json)、[25条结果指标复算](测试附件/前两阶段最终测试/代价与统计核验/指标复算.json)。

### 5.3 500查询Scan：92%/95%/99%等平均召回

12组完成，两侧平均召回及逐查询结果重现，所有正式请求返回10行且无错误。共同原生ANN hint属于实现开销控制，不能当自然AUTO收益。AUTO全部500计划形式相同，双方gplan；显式模式形式不同，双方直接SQL。时延列为P50/P95/P99。

AUTO三档1.0921×/1.0109×/1.0030×，未达到10×；强制模式均更慢。92.56%代表查询：PRE完整资格源50,001行、整条路线102,195次buffer访问；IN图访问和资格Tid Scan均1,251次；POST116次成员核验，另有过滤协议/外层Sort。旧原生ANN已有排序、普通过滤、补批，AUTO保留它避开新增工作。计数不直接等同CPU/墙钟占比。来源：[Scan十二组](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/measurements.json)、[全部500计划审计](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/plan-cache-audits.json)。

### 5.4 500查询Join：自然选路与质量匹配

本节为原先相近质量口径的历史正式对照：关闭侧普通自然计划平均召回100%；开启侧116/204预算92.56%/95.38%，不能在0.5个百分点内匹配，92/95两档×四模式共八组明确排除，未用较低质量计算收益。排除是当时配对质量条件不成立，不代表SQL失败或模式未实现。新口径允许精确OFF对比目标召回ANN，见§4.3/§5.4.1；不把未计时的历史八组改称已测。

四组每侧1000正式查询（2×500周期），无不足K/错误。AUTO普通Limit→Sort→Hash Join→Seq Scan，两侧100%，0.9981×基本持平。PRE/IN/POST6144均99.64%，差0.36个百分点；速度0.2423×/0.2902×/0.7782×，强制均明显劣化。POST1024/1536/2048/4096质量99.04/99.20/99.24/99.46%均未匹配；6144=99.64%、8192=99.70%，依据最低已测合格预算6144对照，不依诊断时延。来源：[Join四组](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/measurements.json)、[配对与八组排除](执行包/evidence/no-include-performance-20261011/early-prune-join-mean-selection.json)。

#### 5.4.1 PRE/IN优势补测：默认TOAST存储、目标召回对照

2026-10-12按用户要求新增实测，内核仍为55c8fc61ae、同一Release/O2二进制。补测回答“PRE/IN有没有优势场景”，分别报告显式模式性能与自然AUTO选路。原正式结果数值没有重写，现已统一列入§5.1；本轮新增正式场景为Join，Scan初筛不计入收益表。

**测试环境与数据。** 会话、并发及实例沿用§1/§4，新增自有schema `vf_prein_adv_20261012`。目标表100,000行、768维L2，向量从原`docs_100000.v768`逐位复制，全部float32核验通过；第一坐标`float32(1+id/100000)`，其余767坐标均为`float32(0.1)`。业务表100,000行，`target_id`主键，`bucket_ind=mod(target_id*7919,1000)`，资格`bucket_ind<500`恰好50,000行，每目标至多一条业务匹配。仅新增表/索引及其ANALYZE，未改变旧表/旧统计。

向量使用类型默认**EXTERNAL**存储（`attstorage=e`，TOAST外置、不压缩）；不是PLAIN，也不是EXTENDED。目标主heap5,259,264字节、pg_table_size（含TOAST与辅助fork）419,528,704字节，业务heap3,629,056字节；DiskANN索引819,224,576字节，单向量key、`index_size=16`、无INCLUDE。资格路径只取id/TID，可扫描约5MB主heap，而不为全部资格行读取向量。此数据虽然声明768维，但有效变化仅一个坐标，是受控低内在维度样例；不能外推为真实768维embedding普遍收益。[数据和全部float32核验](测试附件/前两阶段最终测试/PRE_IN优势场景补测/默认存储数据/manifest.json)、[存储、大小和自然计划核验](测试附件/前两阶段最终测试/PRE_IN优势场景补测/选路与存储核验.json)。

**测试SQL。** 500条SQL仅查询向量不同，固定query_id=0～499；`pos_i=(i*7919+31253)%100000`，向量第一坐标按九位有效数字输出`1+(pos_i+0.271)/100000`，其他坐标0.1。该序列在质量校准前固定，不按结果筛查询。代表SQL如下；`:q_i`仅为排版缩写，实际提交完整768维字面量，两侧均无索引hint：

```sql
SELECT d.id
FROM vf_prein_adv_20261012.documents d
JOIN vf_prein_adv_20261012.business a ON a.target_id=d.id
WHERE a.bucket_ind<500
ORDER BY d.v768 <-> :q_i::vector
LIMIT 10;
```

[完整500条冻结SQL](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式对照_分段重测/input-sql-freeze.json)、[代表完整字面量SQL](测试附件/前两阶段最终测试/PRE_IN优势场景补测/补测代表SQL.sql)。Exact参考利用逐位已核验的数据计算精确float32 L2，Top10/11边界最小正间隙约7.70e-10；正式OFF遍历全部500查询并逐条与参考多重集比对通过。没有删除返回10行但召回为0的困难查询；92档两模式均有34/500个查询召回为0、459/500个查询为100%，因此92.36%只是平均值，并不保证每个查询达标。92/95/99档分别选择最低**已测合格**预算512/2048/6144，对应平均92.36/95.90/99.54%，不声称所有整数预算中的最小值；5120只有98.90%，未四舍五入作为99档。[精确参考](测试附件/前两阶段最终测试/PRE_IN优势场景补测/解析精确参考500.json)、[冻结质量与预算](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式输入.json)。

**测试模型。** OFF自然选择精确计划，保持100%召回；ON显式PRE/IN达到各档目标，称“精确基线对比目标召回”，不称相同召回。每窗预热至少10秒、计时至少30秒并结束于完整500查询周期；串行单连接、query_dop=1，同一只读REPEATABLE READ事务。完整计划不同，双方均直接SQL；全部500 SQL的形式核验已保存。为减少暴搜重复，六处理共享首尾两窗OFF，执行顺序如下，每个模式投影为OFF→ON→ON→OFF，但两次OFF**不是六份独立基线**：

```text
OFF, PRE92, IN92, PRE95, IN95, PRE99, IN99,
IN99, PRE99, IN95, PRE95, IN92, PRE92, OFF
```

此前整窗尝试出现宿主IDE/WSL后台换入或主缺页，保留原污染/中断证据。本次在计时前临时暂停两个已核对身份的后台IDE索引器，结束后自动恢复，并预触碰宿主已换出的映射页；两侧处于相同环境。计时按预先固定的50查询段检查污染，每段接受时间顺序首个pswpin/pswpout/pgmajfault均零的尝试；污染段完整保留、整体重跑，不按时延挑选，也不删除任何查询。每个逻辑窗口仍遍历完整500查询周期，至少30秒**已接受段的累计活动墙钟**；QPS分母为这些段的活动墙钟之和，段间保存记录和污染重试不计入。总表明确标记分段测法，不跨测法拼时延。每个计时请求重现各自冻结结果，所有返回K10，错误和不足均为0。共8000次请求、14个逻辑接受窗口（各由固定50查询段组成），OFF共1000次，IN92共2000次，其余五种模式各1000次；不得把共享OFF重复计为12个窗口或汇总成六个独立实验。本次160个固定段全部首次即干净，14个完整逻辑窗口的全程污染计数也均为零；实际没有采纳任何重试段。原33组/132窗统计保留，新补测统一并入§4.4覆盖统计和§5.1总表。[正式计时和实际计划](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式对照_分段重测/measurements.json)、[完整窗口](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式对照_分段重测/blocks.json)、[独立复算](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式结果独立复核.json)。

**结果说明（§5.1的E系列）。** 时延为P50/P95/P99；倍数为ON/OFF墙钟QPS。OFF最终路线均为普通精确，ON均为明确的新PRE/IN；总表E前缀为显式PRE/IN处理；这三个预算的自然AUTO另行核验均为普通精确，未把AUTO当作显式处理的性能测量。

**实际完整计划拓扑。** 以下来自正式92档代表查询，仅删除cost/actual统计及展开的向量字面量；原完整TEXT计划在正式窗口目录。该SQL只投影d.id，业务target_id唯一，因此最终输出Join可经已验证的消除规则省去；资格Join仍保留。多匹配场景不能套用此消除，既有非唯一红线不变。

OFF：

```text
Limit
  -> Sort
        Sort Key: ((d.v768 <-> query_vector))
        -> Hash Join
              Hash Cond: (d.id = a.target_id)
              -> Seq Scan on documents d
              -> Hash
                    -> Seq Scan on business a
                          Filter: (bucket_ind < 500)
```

PRE：

```text
Limit
  -> Sort
        Sort Key: ((d.v768 <-> query_vector))
        -> Ann Index Scan using vf_prein_documents_l2 on documents d
              Order By: (v768 <-> query_vector)
              Filter Mode: PRE
              -> Hash Join
                    Hash Cond: (d.id = a.target_id)
                    -> Seq Scan on documents d
                    -> Hash
                          -> Seq Scan on business a
                                Filter: (bucket_ind < 500)
```

IN：

```text
Limit
  -> Sort
        Sort Key: ((d.v768 <-> query_vector))
        -> Ann Index Scan using vf_prein_documents_l2 on documents d
              Order By: (v768 <-> query_vector)
              Filter Mode: IN
              SubPlan 1
                -> Nested Loop
                      -> Tid Scan on documents d
                            TID Cond: (ctid = $0)
                      -> Index Scan using business_pkey on business a
                            Index Cond: (target_id = d.id)
                            Filter: (bucket_ind < 500)
```

**收益与限制。** 普通OFF先Join取得5万合格目标，对这些目标读取外置向量并算距离，再做Top-N；PRE先从窄主heap/业务表产生TID集合，IN则在图搜索过程中按候选TID查目标与业务主键，二者只对实际访问/接受的候选执行必要工作。相较旧PLAIN宽表的资格读取，窄heap显著降低PRE建立资格集合的成本；IN仍有逐候选SubPlan代价，预算越大收益越小。这里的倍数是相对**关闭后自然精确计划**，不能当成相对已调优旧ANN的同计划执行收益。

修复前55版本的自然AUTO存在成本排序不足：预算512/2048/6144均选普通精确；最高已测仍走新POST的预算256，500查询平均只有90.26%，未达到92档。DEBUG2日志中普通完整路线成本上界6453.73；预算512时，日志中PRE/IN/POST的启动成本分别可见14215.67/19642.52/12415.70，均超出上界，因此按当时模型剪枝（AM模式枚举1=IN、2=POST、3=PRE，不能按枚举顺序误标名称）。这是估计未反映实测排序，不能仅凭此认定剪枝逻辑错误；此处记录历史状态，后续修复见§5.4.2。[AUTO边界](测试附件/前两阶段最终测试/PRE_IN优势场景补测/AUTO边界核验.json)、[AUTO256完整质量](测试附件/前两阶段最终测试/PRE_IN优势场景补测/自然AUTO召回500_p256/records.json)、[成本日志](测试附件/前两阶段最终测试/PRE_IN优势场景补测/AUTO成本日志.json)。

另外同SQL两侧共同加原生ANN hint的质量诊断显示，OFF旧ANN初始预算1/4/16已能达到92.36/95.90/100%（旧扫描会内部补批，初始参数数值不能直接视作与新扫描相同的最终搜索工作量）；其路线为Limit→Nested Loop→原生Ann Index Scan+业务主键扫描。该诊断不属于正式性能窗口，不拿其诊断时延计算倍数，也不证明新PRE/IN胜过调优旧ANN。[旧ANN质量与计划诊断](测试附件/前两阶段最终测试/PRE_IN优势场景补测/旧ANN_HINT召回500/records.json)。本轮只补充性能证据，未改内核、未启动第三阶段、未改变Oracle行为及计划红线。

#### 5.4.2 AUTO代价修复与三档正式复测

**修复结果。** 原先在512/2048/6144预算下误选普通暴搜的问题已修复；同一SQL现在由AUTO自然选择新POST，外层普通Nested Loop执行业务资格。未使用hint或强制POST。修复归入第二阶段提交`670d0c5e6a`，第一阶段`415b78e6c9`保持不变，仍是两个阶段、两个提交；原55提交保存在`backup/diskann-vector-filter-p12-pre-toast-cost-20261012`。本次仅修改优化器`diskannFilterPath.cpp`，相对55为+88/-4行，不改执行器、AM接口、数据或索引格式。[源码输入及二进制](测试附件/前两阶段最终测试/AUTO代价修复/修复提交与二进制.json)、[本次修改](测试附件/前两阶段最终测试/AUTO代价修复/最终内核修改.patch)。

**根因与处理。** 本例`pg_stats.avg_width=18`正确表示外置指针，实际每个向量约3KB。原普通扫描和距离表达式成本没有计入约5万合格向量的TOAST解引用，而ANN已计入索引页访问，成本比较因此偏向暴搜。修复按照存储策略、维度和已有宽度统计估计外置读取：普通输出投影、新ANN的key核验和最终投影采用同一口径；PRE/IN/POST分别按实际机制估计核验数量，旧原生单表ANN保留自己的补批/排序计价。普通资格路径只取TID/连接键时不被额外收取向量读取费用；小向量短头、PLAIN、可压缩存储和未知统计分别处理。没有篡改统计、任意降低ANN成本或移除普通/旧ANN竞争者。设计细节已补充至设计文档§12.1.2。[修复前目录统计](测试附件/前两阶段最终测试/AUTO代价修复/修复前目录.json)、[修复后成本与完整计划](测试附件/前两阶段最终测试/AUTO代价修复/三档500召回/plans.json)。

**环境、数据和SQL。** 沿用§1与§5.4.1的Release/O2实例、100000目标/100000业务行、768维L2、50%资格和固定500查询；不重建索引、不重采样查询、不改变SQL或业务语义。仍是只有一个坐标变化的受控合成数据，不外推生产embedding。数据/索引filenode、大小、行数统计与准备时一致；40个编译输入文件哈希与本次提交一致。三档预算沿用512/2048/6144，重新执行全部500查询并逐条审查AUTO计划，三档全部选择POST、均无不足K。[三档质量](测试附件/前两阶段最终测试/AUTO代价修复/三档500召回/summary.json)、[正式输入](测试附件/前两阶段最终测试/AUTO代价修复/正式输入.json)、[结束目录核验](测试附件/前两阶段最终测试/AUTO代价修复/结束目录核验.json)。

**测试模型。** OFF保持自然精确100%，ON使用AUTO并达到目标平均召回，不称等召回对比。双方计划不同，均直接SQL；每窗预热至少10秒，计时至少30秒且完成500查询的整数周期。单连接、query_dop=1、同一只读REPEATABLE READ事务。顺序为`OFF→AUTO92→AUTO95→AUTO99→AUTO99→AUTO95→AUTO92→OFF`，三个处理共享首尾两次OFF。继续采用§5.4.1预先约定的50查询分段、首次干净采纳规则：正式接受8000请求、160段、8个逻辑窗口，其中OFF1000、AUTO92为4000、AUTO95为2000、AUTO99为1000；各侧所有查询等权。27个含主缺页的分段（1350请求）原样保留并重跑，未计入接受结果；所有接受段pswpin/pswpout/pgmajfault均为0。**完整逻辑窗口包含被剔除的重试，不能声称本轮整窗零主缺页。** QPS分母为接受段累计活动墙钟，段间写记录、预热及重试不计入。后台索引器已恢复，swappiness恢复60，测试实例fast停止。

**结果说明（§5.1的A系列）。** 时延为P50/P95/P99；倍数为本轮AUTO/OFF的QPS比，关闭侧最终路线均为普通精确，开启侧均为AUTO选择的新POST。

本轮OFF的绝对时延也明显低于§5.4.1；其计划与结果未变，不能将这种跨轮差额归因为此次开启侧成本修复。表内只用同期两侧数据计算，不能拿上轮约1047ms的OFF搭配本轮AUTO夸大收益。三档均提升，但仅92档超过10×，95/99档没有达到10×；也没有证明本例新POST在所有参数、硬件或存储方式下均最优。[正式原始结果](测试附件/前两阶段最终测试/AUTO代价修复/正式AUTO对照/measurements.json)、[全部窗口和被拒绝分段](测试附件/前两阶段最终测试/AUTO代价修复/正式AUTO对照/blocks.json)、[独立逐请求及指标复算](测试附件/前两阶段最终测试/AUTO代价修复/正式结果独立复核.json)。

三档代表计划的节点形式一致，仅搜索预算与估计/实际工作量不同：

```text
Limit 10
  -> Sort
       Sort Key: d.v768 <-> query_vector
       -> Nested Loop
            -> Ann Index Scan using vf_prein_documents_l2 on documents d
                 Order By: v768 <-> query_vector
                 Filter Mode: POST
                 Initial Search Nodes: 512 / 2048 / 6144
            -> Index Scan using business_pkey on business a
                 Index Cond: target_id = d.id
                 Filter: bucket_ind < 500
```

99档代表查询实际Visited Nodes=8372、候选/回表核验6144、外层业务Index Scan loops=6144、Join输出3071、最终10行；Search Rounds=1，Qualification Probes=0，未启用第三阶段精确入口。[完整EXPLAIN ANALYZE](测试附件/前两阶段最终测试/AUTO代价修复/正式AUTO对照/a0_w3_auto_99.plan.txt)。

**回归范围。** 本次Release重新通过1439项核心检查（包含626项并发/资源及既有Oracle静态数据/结果对照）、8组原生回归与取消复用；另通过59项成本、42项规划、20项优化器、8项AUTO数量检查。原九组的18个开关侧结果和计划形式不变，其中四组同形计划重新核验实际cplan/gplan完整形式相同；这次没有重复发布原九组整套吞吐数据。初次九组核验脚本漏加VERBOSE导致显示差异，失败原样保留，补齐相同EXPLAIN选项后通过；未因该脚本问题修改内核。原阶段门凭据不改写，新修复证据单列，P3仍未开始。[完整Release回归](测试附件/前两阶段最终测试/AUTO代价修复/完整Release回归/summary.json)、[九组回归与gplan核验](测试附件/前两阶段最终测试/AUTO代价修复/nine-cases-v2/summary.json)。

#### 5.4.3 AUTO自然选择PRE/IN：新布局、完整500查询补测

**本次补齐的内容。** 之前§5.4.1只证明显式PRE/IN相对OFF精确计划有收益，§5.4.2的AUTO则选择POST；不足以回答“AUTO何时会选中新PRE或新IN”。本节使用同一新Release二进制、同一SQL与数据分别关闭/开启特性，所有开启侧均使用AUTO，未用hint或强制模式改变正式结果。两种布局的92%和95%档确实自然选中PRE、IN；99%档分别选择普通精确和POST，按实际情况保留。

**代码修复与边界。** P1仍为`415b78e6c9`，P2归并为`72778fe2aae139cfade1abe7396e3e085869ee22`，未push。相对670仅`costsize.cpp`新增14行、删除1行：PRE/IN在资格检查后向候选队列收集结果，不能与POST一样简单用`C×s`估计输出。对单表及已证明可省略输出Join的路线，改为`min(C, M×s)`，C是候选容量、M是预计检查成员数、s是资格比例；未降低图访问或资格检查成本，也未改变执行器/AM。

首次对所有路线应用修正时，Oracle既有静态样例的非唯一LEFT JOIN从NL变为Merge，并列第10行的业务成员改变，触发结果红线。失败证据保留，未放宽比较器。当前只修正`!ann_output_join_required`的PRE/IN；仍需真实输出Join的路线暂保留原估计，**该部分估算偏差尚未解决**。限定修复已通过完整16组Release回归、1439核心检查、61成本、42规划、20优化器、8数量检查，以及原九组18侧结果/计划和四组真实cplan/gplan形式核验。Oracle检查是对已冻结原SQL/输入/输出的重放，不是本轮重新启动Oracle实测。第三阶段未开始。[提交与40文件输入核验](测试附件/前两阶段最终测试/PRE_IN路线优势验证/行数估算修复/提交与二进制.json)、[当前完整回归](测试附件/前两阶段最终测试/PRE_IN路线优势验证/行数估算修复/限定完整Release回归/summary.json)、[保留的首次红线失败](测试附件/前两阶段最终测试/PRE_IN路线优势验证/行数估算修复/完整Release回归/summary.json)。

**测试环境和数据。** 沿用§1的AStore、Release/O2、端口55450、单连接、query_dop=1、128MiB work_mem；当前二进制SHA-256为`bf484902a3ab0fdd53a87a8f884b4797847de1339db4a98758aef0a49e83fb0e`。新schema为`vf_prein_narrow_20261012`：100000目标行、100000业务行，每目标恰好1条业务记录，最终K10。向量先生成8维独立Gaussian float32（seed=20261013），再重复96次得到768维，因此**内在维度为8，这是合成验证数据，不能外推真实768维embedding**。500条固定独立查询全部等权，未挑选有利查询；每条参考都实际执行SQL Exact核验。

两张目标表的id、bucket和向量值全量核验相同。`documents`采用EXTERNAL外置向量，主heap窄、avg_width=18；`documents_plain`采用PLAIN内联，avg_width=3080。业务资格为`bucket=(id×7919)%1000`，与向量生成独立；分别取`bucket<50`（5000行，5%）和`bucket<100`（10000行，10%）。每张目标表都有主键和单向量key DiskANN，无INCLUDE；外置表另有bucket B-tree，内联表没有该索引，但这两条SQL过滤的都是业务表bucket，均使用业务表同一个bucket索引。两例用于展示各自路线，不将布局和选择率不同的两例互作性能基线。[数据逐位与双表核验](测试附件/前两阶段最终测试/PRE_IN路线优势验证/窄表宽向量数据/manifest.json)、[PRE全部500计划/结果核验](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式输入_PRE/summary.json)、[IN全部500计划/结果核验](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式输入_IN/summary.json)。

**测试SQL。** 每例只替换500个冻结的查询向量，OFF/ON的SQL正文完全相同：

```sql
-- PRE布局：EXTERNAL向量，业务资格5%
SELECT d.id
FROM vf_prein_narrow_20261012.documents d
JOIN vf_prein_narrow_20261012.business a ON a.target_id = d.id
WHERE a.bucket < 50
ORDER BY d.v768 <-> query_vector
LIMIT 10;

-- IN布局：PLAIN向量，业务资格10%
SELECT d.id
FROM vf_prein_narrow_20261012.documents_plain d
JOIN vf_prein_narrow_20261012.business a ON a.target_id = d.id
WHERE a.bucket < 100
ORDER BY d.v768 <-> query_vector
LIMIT 10;
```

**测试模型。** OFF自然选择普通精确，保持100%召回；ON达到目标平均召回即可，不称等召回比较。预算按全部500查询先校准，所有正式处理均无返回不足。计时前再次检查每条查询的真实计划和结果；不同计划采用直接SQL。外置99%档两侧计划相同，500条查询的OFF/ON以及实际cplan/gplan完整VERBOSE形式全部核验相同（2000个真实策略检查），正式两侧都用gplan。

三块依次串行执行：PRE92/95共享首尾OFF的6窗对称块；外置99%的4窗gplan块；IN92/95及POST99共享首尾OFF的8窗对称块。各窗预热≥10秒、接受活动计时≥30秒，并完成500查询整数周期；一次只读REPEATABLE READ事务覆盖同块。采用已有固定50查询段、首次干净尝试的规则，QPS分母为接受段活动墙钟，预热、重试、段间写记录不计入。共接受34000请求、680段、18个逻辑窗口；另保留并排除14个污染重试段。接受段的pswpin/pswpout/pgmajfault均为0；完整逻辑窗口是否全部干净：**否，包含被排除的污染重试，不能声称整窗零主缺页**。没有按快慢挑段，所有查询均等保留；共享OFF只计两次，不冒充独立多轮。

**结果说明（完整附件N-E/N-P系列，达到阈值者展示于§5.1）。** 时延为P50/P95/P99；倍数为同块AUTO/OFF QPS。所有OFF最终路线均为普通精确。

PRE两档QPS为关闭侧的1.28×/1.23×，IN两档为14.02×/12.26×；四个实际PRE/IN处理有2个达到10×，不声称全部达成。外置99%同计划gplan的QPS差额为-0.22%，该小差额本轮未给出统计显著性证明，不能解释成结构性优化或保证严格零下降。

**如何理解收益。** PRE先从窄heap和业务关系生成5000条资格TID，图搜索只收集合格候选，避免精确计划对全部5000个外置向量计算距离。IN布局的普通精确计划扫描宽heap并对10000条资格行计算距离，IN在图搜索时按候选TID和业务主键检查资格，降低需要访问的目标范围。业务键唯一、仅投影目标id，这两例通过证明省略了资格检查后的第二次输出Join；并不代表非唯一Join也可省略。

这是“关闭特性自然选路”对“开启特性AUTO”的收益证明。**尚未完成这两个新布局上，调优旧ANN与调优POST在相近平均召回下的正式全量计时对照，不能声称PRE/IN跨所有预算全局最快。** 当前AUTO比较给定预算下的路线，不会自动为每个模式分别搜索最佳预算。同预算下POST返回不足的筛选结果不能充当等质量性能基线；99%改选POST或普通精确也不能算作PRE/IN收益。完整负面筛选和校准结果均保留。

实际92%代表计划如下（仅省略Output列及schema前缀，向量字面量改写为query_vector；完整原计划见链接）：

```text
Limit
  ->  Sort
        Sort Key: ((d.v768 <-> query_vector))
        ->  Ann Index Scan using vf_narrow_l2 on documents d
              Order By: (d.v768 <-> query_vector)
              Storage: AStore
              Filter Mode: PRE
              Initial Search Nodes: 80
              ->  Hash Join
                    Hash Cond: (d.id = a.target_id)
                    ->  Seq Scan on documents d
                    ->  Hash
                          ->  Bitmap Heap Scan on business a
                                Recheck Cond: (a.bucket < 50)
                                ->  Bitmap Index Scan on vf_narrow_business_bucket
                                      Index Cond: (a.bucket < 50)
```

```text
Limit
  ->  Sort
        Sort Key: ((d.v768 <-> query_vector))
        ->  Ann Index Scan using vf_narrow_plain_l2 on documents_plain d
              Order By: (d.v768 <-> query_vector)
              Storage: AStore
              Filter Mode: IN
              Initial Search Nodes: 48
              SubPlan 1
                ->  Nested Loop
                      ->  Tid Scan on documents_plain d
                            TID Cond: (d.ctid = $0)
                      ->  Index Scan using business_pkey on business a
                            Index Cond: (a.target_id = d.id)
                            Filter: (a.bucket < 100)
```

[PRE正式原始结果](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_PRE/measurements.json)、[外置99%同计划对照](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_PRE99同计划/measurements.json)、[IN/POST正式原始结果](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式计时_IN/measurements.json)、[六组逐请求、召回、分位数、QPS独立复核](测试附件/前两阶段最终测试/PRE_IN路线优势验证/正式结果独立复核.json)。历史55/670结果保持原值，不与本轮基线交叉计算倍数。

#### 5.4.4 AUTO前过滤超过10×：普通索引资格源与外置向量

**本场景达到用户要求的“一个自然AUTO前过滤提升超过10×”。** 正式结果统一列在§5.1的P-A92行。沿用P2 `72778fe2aa`及Release二进制`bf484902a3ab…`，本轮只构造查询与补测，未改内核、数据、统计或索引。复用§5.4.3的`vf_prein_narrow_20261012.documents`：100000行、AStore、EXTERNAL向量、单键`vf_narrow_l2`和普通B-tree `vf_narrow_bucket`。768维由固定Gaussian8维重复96次构造，属于受控合成数据，不能外推生产全维embedding。

**查询。** `bucket < 125`准确筛出12500行（12.5%）；查询向量复用预先冻结的500条独立查询，每条权重相同。下面的`:query_vector`只是正文占位，实际压测使用[冻结文件中的500条完整字面量SQL](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式输入_PRE/sql-freeze.json)。

```sql
SELECT d.id
FROM vf_prein_narrow_20261012.documents d
WHERE d.bucket < 125
ORDER BY d.v768 <-> :query_vector
LIMIT 10;
```

两侧均固定`diskann_probes=40`、`diskann_filter_mode=auto`、`enable_opfusion=off`、`query_dop=1`、`work_mem=128MB`、`diskann_filter_work_mem=131072`。只切换`enable_diskann_filter=off/on`，没有hint或显式PRE。全部500条SQL Exact结果与离线坐标计算结果一致；AUTO平均Recall@10为92.46%、全部返回10条。OFF自然精确为100%，这是满足92%目标的开关对照，**不是等召回100%的对照**。本轮正式验收一个92%场景；预算48/56/64的500条质量筛选及未入选短样本均保留，不冒充其性能已正式验收。

**实际计划。** 全部500条OFF计划均为普通Bitmap扫描加Sort，全部500条AUTO计划均为PRE；计划不同，双方使用直接SQL计时。以下只缩写查询向量、省略成本与计数，节点来自原始EXPLAIN。

```text
-- OFF
Limit
  -> Sort
       Sort Key: d.v768 <-> query_vector
       -> Bitmap Heap Scan on documents d
            Recheck Cond: bucket < 125
            -> Bitmap Index Scan on vf_narrow_bucket
                 Index Cond: bucket < 125

-- ON / AUTO
Limit
  -> Sort
       Sort Key: d.v768 <-> query_vector
       -> Ann Index Scan using vf_narrow_l2 on documents d
            Order By: d.v768 <-> query_vector
            Storage: AStore
            Filter Mode: PRE
            Initial Search Nodes: 40
            -> Bitmap Heap Scan on documents d
                 Recheck Cond: bucket < 125
                 -> Bitmap Index Scan on vf_narrow_bucket
                      Index Cond: bucket < 125
```

收益来自减少外置向量的读取和距离计算：OFF对12500个合格向量逐一计算距离再取Top-10；PRE先用普通索引取得资格TID，然后执行受资格限制的ANN搜索。代表查询实测资格源12500行、构建和Seal各1次，图访问651个节点、接受89个成员、回表89次，输出40个候选给Sort；没有IN资格Probe，也没有精确回退。这些是该代表查询的工作计数，不是500查询平均值。[OFF完整计划](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式计时_PRE/a0_w0_off.plan.txt)、[AUTO PRE完整计划](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式计时_PRE/a0_w1_auto_pre_92.plan.txt)。

**正式测量。** 同一二进制、同一只读REPEATABLE READ事务、单连接，顺序为OFF→AUTO→AUTO→OFF；每窗预热至少10秒、接受活动时间至少30秒，完成500查询的整数周期。两个OFF窗口各500次，两个AUTO窗口各2000次，合计5000次请求；各侧500个向量等权。100个固定50查询段均首次干净，没有污染重试，四个完整窗口的pswpin/pswpout/pgmajfault也全部为0。每次返回结果均与冻结结果核对，无错误、无不足K。独立复核重新计算召回、分位数、QPS、结果哈希和段选择；正式指标以§5.1为准，不采用筛选或EXPLAIN的单次时延。

这证明本合成场景的OFF自然普通精确→AUTO自然PRE选路收益，不证明PRE优于跨预算调优后的原生ANN，也不证明其他召回档或生产数据都能达到10×。本轮没有改变P1/P2提交或启动第三阶段。测试结束已停止自有55450实例，保留数据；IDE索引器和swappiness恢复初态，Oracle/Milvus没有启动。

[完整500查询质量](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/单表125_500召回/records.json)、[计划冻结核验](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式输入_PRE/summary.json)、[原始正式测量](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式计时_PRE/measurements.json)、[独立复核](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/正式结果独立复核.json)、[源码/二进制/目录核验](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/结束数据与源码核验.json)、[服务恢复](测试附件/前两阶段最终测试/AUTO_PRE显著收益验证/收尾服务恢复.json)。

### 5.5 额外工作、资源与劣化

下列Join99代表查询来自原§5.4的PLAIN矩阵，不是§5.4.2的EXTERNAL补测；实际计数不是CPU时间归因：

| 模式 | 图访问 | 成员回表核验 | 资格工作 | Sort输入 | 过滤状态峰值B |
|---|---|---|---|---|---|
| AUTO | 无ANN | 普通全表扫描 | 10万目标与16业务行Hash Join | 50,001 | 无 |
| PRE | 16,244 | 8,207 | 完整源50,001行；全路线136,742次buffer访问 | 6,144 | 40,536,184 |
| IN | 16,244 | 16,244 | 16,244次Tid/业务主键Probe；全路线93,509次buffer访问 | 6,144 | 39,489,120 |
| POST | 16,244 | 6,144 | 普通Join从6144候选得到3064行；全路线40,822次buffer访问 | 3,064 | 38,447,224 |

PRE资格构建、IN图内同步Probe明确增加工作；POST虽较少buffer访问，仍有图距离计算与6144次核验，未证明某单函数独占其下降。AUTO比较完整路线后保留普通计划，避开明显较慢方案。

本Join矩阵业务键唯一且只投影目标列，PRE/IN证明不丢失匹配/业务列后省略输出Join；非唯一或需要业务列的Q06/Q08仍保留真实输出Join。LIMIT按完整SQL行计数，不按去重目标数。

Scan/Join64接受窗口全服务峰值VmRSS约998.06～1038.09 MiB，10ms采样；包括共享缓冲与其他分配，不能称单查询私有内存。过滤状态峰值来自代表EXPLAIN。最终Sort为25/26KiB top-N heapsort，计划未记录temp read/written；只对该负载成立，不证明其他负载无spill。Hash spill/重扫由功能矩阵另测。

132接受窗口零swap/major fault。原受污染尝试保留；例如Join PRE正向attempt2、反向attempt1，POST attempt1，按首个干净配对，不按快慢选窗。

### 5.6 功能与交付范围

此表保留55版本的原阶段验收范围；670/727版本成本修复的重新验证范围见§5.4.2/§5.4.3，未重写原凭据或宣称全部历史性能/时序审计再次执行。

| 检查 | 结果 |
|---|---|
| 当前核心功能 | 1439/1439，包含626并发/资源检查，不重复相加 |
| 原生回归 | 8/8及普通取消/连接复用通过 |
| 规划/优化器/AUTO数量 | 42/42、20/20、8/8 |
| 持久边界 | 14/14，实际ba1基线，未改页格式/WAL/AStore DML/VACUUM |
| Oracle | 13适用行为/最终结果硬门；75/108/NL158原查询检查已计入核心矩阵 |
| 真实时序 | PRE EOF/Seal、IN图内ExecSubPlan、POST普通父过滤、NL六epoch/候选去重通过 |
| 冻结/阶段门 | 50/50最终审计、9/9阶段二审查；G-P1/G-P2完成，25/35、无活动任务 |
| 第三阶段 | 10项pending；反馈/补搜/仅POST同计划第二AM精确入口未实现，PRE/IN不做Exact |

Oracle使用不可变原SQL/静态输入/输出，在当前Release复验；有限硬门不保证任意跨ANN引擎ID和并列截断成员恒等。本报告是合成/静态数据、热态、并发1，未完成生产数据、冷态、多客户端性能和更大规模收益证明。典型四组达到13～40×，500矩阵未普遍10×，强制劣化和同计划小差异如实保留；成本拓扑样本的查询误差及真实负载收益仍是后续方向。

### 5.7 原始证据与复核

| 材料 | 链接 |
|---|---|
| PRE/IN六组补测 | [打开](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式对照_分段重测/measurements.json) |
| 补测独立复算 | [打开](测试附件/前两阶段最终测试/PRE_IN优势场景补测/正式结果独立复核.json) |
| 精确参考独立复算 | [打开](测试附件/前两阶段最终测试/PRE_IN优势场景补测/精确参考独立复算.json) |
| 最终数值与来源SHA | [打开](测试附件/前两阶段最终测试/最终结果与来源.json) |
| 原字面量SQL | [打开](测试附件/前两阶段最终测试/Q00-Q08原始测试SQL.sql) |
| 九组直接SQL | [打开](执行包/evidence/no-include-performance-20261011/early-prune-formal-nine-auto/measurements.json) |
| 独立会话gplan | [打开](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-same-plans/measurements.json) |
| 同会话gplan | [打开](执行包/evidence/no-include-performance-20261011/early-prune-formal-gplan-one-backend-v2/measurements.json) |
| Scan十二组 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-formal-scan-three-tiers/measurements.json) |
| Join四组 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-formal-join-matched-tiers/measurements.json) |
| 核心功能 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-full-release-regression-v3/summary.json) |
| 50项最终审计 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-final-delivery-audit/summary.json) |
| Oracle硬门 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-oracle-compatibility-v4/compatibility-actual-symbol-audit/summary.json) |
| 9项阶段门 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-current-p2-gate/summary.json) |
| 工作区/服务 | [打开](执行包/evidence/no-include-performance-20261011/early-prune-final-workspace/summary.json) |

优化过程与历史失败保留在[开关对照性能报告](DiskANN前两阶段开关对照性能报告.md)，其b5/旧覆盖索引版本数字不进入本最终表。测试结束55450fast停止、数据保留，Oracle/Milvus停着、swappiness恢复60。迁移其他同源仓仍先核验DiskANN排序/补批及AStore/UStore。

