# ANN 距离复用缺陷修复与验证报告

## 范围与状态

工作树：`/home/qiumc/data/code/cpp/openGauss-server`，分支 `integration/multimodal-stacked`。首轮修复基线为 `e22030731162`，本轮深度审计基线为 `dc7e06d9222b`。仅适配 DiskANN，不恢复 INCLUDE、不适配 HNSW、不初始化现有数据库、不自动 REINDEX 用户索引。

首轮确认并处理 5 项问题：2 项内核正确性问题、3 项测试问题，提交为 `dc7e06d922`。本轮又完整复现并修复 3 项 DiskANN 内核问题，合计记录 8 项，提交为 `d4a5b5617b`。两轮都已完成 Debug 构建、实测复现前后对比和 openGauss 原生回归；均为本地提交，未 push。

**2026-09-15 后续更新：**在 `d4a5b5617b` 上复审确认的 R1～R3 已按用户要求修复，三类定向复现、旧索引和旧快照验证通过，最终原生回归 **3/3 通过**。验收后已提交为 `84f0a21f543da7ffaa9582d44d0ff0772068c9ae`（未 push）；详情见“R1/R2/R3 修复与本轮验收”。下方原始复审章节保留修复前事实，不代表当前修复状态。

| 编号 | 现象 | 根因 | 修复 |
|---|---|---|---|
| K1 | L2 输出与返回行的真实距离不同，整数坐标也能复现 | 距离阈值合并不保证向量相等，却将节点距离复制给全部 TID | 最终距离与行身份同锁读取；非原始 TID 显式 L2 heap recheck，按当前行缓存 |
| K2 | Cosine 开关改变外层 WHERE/比较/InitPlan 的真假 | 只列举 Join/Agg 等不安全节点，遗漏普通节点中的语义消费者及 PARAM_EXEC 通路 | 透明节点白名单、qual/投影检查、Param/SubPlan 拒绝、分支上下文恢复 |
| T1 | 主回归 SQL 执行成功，输出比较仍失败 | SQL 回显注释与 expected 未同步 | 同步注释并重新执行原生比较 |
| T2 | 预编译计划在切换 GUC 后只比较 EXPLAIN，无法发现结果错误 | 去掉诊断计数后，没有独立数值断言 | 两套表达式引擎实际 EXECUTE，验证近似确实发生且降级后逐位精确 |
| T3 | 转换检查出现 9 条不匹配，却被原 golden 接受 | 直接 CTAS 包装使近似复用未命中，保存的是精确原表达式结果 | 动态执行独立 SELECT 后保存结果，要求转换不匹配为 0 |
| K3 | 自定义“L2 支持函数 + 归一化支持函数”opclass 在 `l2` 开关下返回错误距离 | 优化器、执行器和 AM 只检查 L2 support-1，没有排除 support-2 对存储向量/距离尺度的改变 | 三层都要求 norm proc 不存在才允许 exact L2 复用，否则保留原 SQL 表达式 |
| K4 | 超过 `DISKANN_HEAPTIDS=10` 个相同向量时静默丢行；初版修复后又会重复返回某个 TID | 容量满时 `AddDuplicateNeighbor` 失败却被上层当成合并成功；溢出节点保留后，旧 frozen point 可与新节点重复保存同一 TID，扫描却按“节点 + TID”去重 | 合并函数返回是否真正接收；满桶继续尝试其他桶，都满则链接新节点；扫描按 heap TID 去重；无修改 WAL 状态显式 abort |
| K5 | `immediate_delete=true` 删除合并节点的代表 TID 后，同节点其他存活行全部无法召回 | 代码看到 IndexTuple 自身 TID 匹配就直接将整个节点选为删除目标，没有先检查合并 TID 数组的剩余成员 | 先仅移除目标 TID；只有 `heaptidsLength` 降为 0 才断边并标死节点 |

## K1：完整复现与修复边界

正式小数据用例位于 `src/test/regress/sql/ann_orderby_distance_reuse_safety.sql`，检索 `ann_merged_l2_t`。9 行包含两个合并风险组：

```sql
'[10000,20000,30000,40000]'
'[10001,20000,30000,40000]'
'[10002,20000,30000,40000]'
'[10000,20001,30000,40000]'

'[10,20,30,40]'
'[10.00001,20,30,40]'
'[10.00002,20,30,40]'
'[10,20.00001,30,40]'
```

大整数揭示 `su + sv - 2 * dot` 抵消；微小差异揭示合并阈值本身。为每条返回行同时取 ORDER BY 投影值及 `l2_distance(embedding, q)`，用 `float8send` 比较二进制值。保存结果通过 `ann_safety_capture` 执行原 SELECT，不给被测 SELECT 增加修改父节点。

用例覆盖递归/融合表达式、相同距离双引用、非键 HOT 更新及带不同查询向量的相关子计划 rescan；既要断言返回 9 行，也要断言 0 条不匹配，防止空结果伪通过。

本轮另外保留一份由旧二进制创建的 5 行索引，安装后直接查询；验证前后 `relfilenode` 相同。它用于证明旧索引也可修复，不依赖重建后“碰巧不合并”。

修复不会改变合并算法或遗留 ANN 召回/排列顺序。完全相同向量的非原始 TID 也保守重算；这是不改页格式的代价。L2 重算结果与直接复用结果都是 SQL 欧式距离，不混合平方距离，不把规则推广到近似 Cosine。

## K2：完整复现与语义保护

正式用例检索 `ann_threshold_t`：第 1 行向量 `[2,4,6,8]`，其余 39 行为 `[-g,g,0,1]`，查询向量 `[11,23,35,47]`。

```sql
SELECT id, distance FROM (
    SELECT /*+ indexscan(ann_threshold_t ann_threshold_cos_idx) */
           id, embedding <=> '[11,23,35,47]'::vector AS distance
    FROM ann_threshold_t ORDER BY distance LIMIT 10
) s WHERE distance < 0.00008161::float8;
```

精确距离约为 `0.00008162265858591145`，近似转换值约为 `0.00008159875869750977`。阈值夹在二者之间。修复前 off 返回 0 行，cosine 返回 1 行；修复后两者都应返回 0 行。外层比较表达式和 InitPlan 对同一阈值的比较也必须返回 false。

该检查保留原始 SELECT 形状，不在它外面套 count、聚合或直接 CTAS，否则可能恰好命中旧的拒绝条件，掩盖缺陷。

消费链白名单允许可验证的 Var/Const 透传，不做无限制列血缘推断。存在无关的复杂上层表达式也可能保守不命中，这是正确性优先的覆盖边界。独立 UNION ALL 分支的上下文必须恢复，不能把 snapshot 分支的 Anti Join 误传给 delta 分支。

## K3：带归一化函数的自定义 L2 opclass

复现 opclass 同时声明 `<->`、`vector_l2_squared_distance` 和 `vector_norm`。表中 4 个向量为 `[6,8,0,0]`、`[0,10,0,0]`、`[10,0,0,0]`、`[-3,-4,0,0]`，查询向量为 `[3,4,0,0]`。

修复前，`off` 的 SQL 欧式距离是 `5 / 6.70820393249937 / 8.06225774829855 / 10`，`l2` 却返回 `0 / 0.632455536745837 / 0.894427197663918 / 2`。这不是普通 L2 的平方值简单开方问题，而是 support-2 已把向量归一化，AM 距离与原始向量 SQL `<->` 根本不在同一语义尺度。

修复后要同时满足以下三层门禁：

1. 优化器只有在 support-1 为 L2 且 support-2 norm proc 无效时才生成 `AnnOrderByRef`。
2. 执行器在打开索引后重新检查同一条件，防止计划缓存或元数据变化绕过优化器。
3. DiskANN AM 转换函数在发布 `sqrt(rawDistance)` 前再做一次防御性检查。

实测修复后 `l2` 计划的 Ann Index Scan `Output` 保持单层括号，说明未改写为 AM 引用；`l2` 与 `off` 的 4 条 float8 二进制值完全一致，不匹配数为 0。内置、无 norm proc 的 `vector_l2_ops` 仍然命中复用。

## K4：相同向量合并桶满后的丢行、重行与锁顺序

每个旧格式 DiskANN 节点最多保存 `DISKANN_HEAPTIDS=10` 个 heap TID。用 12 行完全相同的 `[10,20,30,40]` 建索引，修复前会输出两次 `Cannot add more neighbors, heaptids array is full`，但索引扫描只返回 id 1～10。根因是下层添加函数没有返回值，上层无条件地把“满了、没写入”当成合并成功，因此不再为当前行建立图连接。

修复后，添加函数只在“TID 原已存在”或“本次真正写入”时返回 true。容量满返回 false，上层会继续尝试其他零距离桶；所有候选都满时才把当前节点正常链入图。在非 build 路径，“已存在”和“容量满”都不发生页修改，必须 `GenericXLogUnregister + GenericXLogAbort`，不能泄漏一个未完成的 generic WAL 状态。

初版修复使 12 行不再丢失，但实测返回了 13 行，id 12 出现两次。这揭示了第二层根因：旧 frozen point 可带有抽样行 TID，溢出新节点又可保存同一 TID；原扫描用 `(node block, heap TID)` 判重，而 SQL 逻辑行身份只是 heap TID。扫描现改为只按 heap TID 去重，不依赖节点物理布局。

最后，容量满会让 `FindDuplicateNeighbor` 尝试多个候选页。若持有当前页共享锁再申请候选页排他锁，并行 worker 可形成 A→B/B→A 锁环。实现现先在当前页锁内复制 `ItemPointerData`，立即解锁，之后才修改候选页，消除嵌套页锁。

完整验证同时覆盖建索引和在线插入：建索引后 heap 12 行，`off/l2` 都返回 12 个唯一 id；再插入 id 13、14 后，heap、`off`、`l2` 均为 14 行，各自 distinct id 为 14，开关间差异为 0。

## K5：immediate delete 只能删空合并节点

复现表有两组向量：A 组 6 行 `[10,20,30,40]`，B 组 6 行 `[40,30,20,10]`，使用旧格式、不带 INCLUDE 的 DiskANN 索引，并将表设为 `immediate_delete=true`。两组各删除索引当前首个返回 id，heap 中各剩 5 行。修复前，由 frozen point 代表的组仍返回 5 行，另一组却返回 0 行。

原实现先比较当前页 IndexTuple 的 `t_tid`。一旦删除的恰好是该代表 TID，就将整个节点作为断边/标死目标，跳过了 `heaptids[]` 剩余成员的生存检查。修复后无论删除的是否为 IndexTuple 代表 TID，都先紧缩 TID 数组；只有新长度为 0 才把该页返回给断边和标死逻辑。剩余 TID 与页内代表 TID 不同时，距离复用已有的 row-identity heap recheck 会保守重算，不会把前一行结果错传给幸存行。

修复后实测 heap A/B 各 5 行，强制 DiskANN 的索引扫描 A/B 也各返回 5 行，不依赖 frozen point 随机选中哪一组。

## 构建与原生框架验收

三个修改的头文件都是运行时状态，不修改磁盘格式；由于本工作树未启用可靠头文件依赖，必须全量构建：

```bash
env CODE_BASE=/home/qiumc/data/code/cpp/openGauss-server \
  GAUSSHOME=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install \
  PGDATA=/home/qiumc/data/code/cpp/openGauss-server/topk_test_data \
  PGPORT=55440 JOBS=4 \
  bash /home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh full
```

回归仍使用原有 SQL/expected/schedule/pg_regress 框架：

```bash
ANN_INSTALL=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install
ANN_GCCLIB=/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3/gcc/lib64
env GAUSSHOME="$ANN_INSTALL" SSL_LIB_PATH="$ANN_INSTALL/lib" \
  LD_LIBRARY_PATH="$ANN_INSTALL/lib:$ANN_GCCLIB" \
  make -C /home/qiumc/data/code/cpp/openGauss-server/src/test/regress ann_orderby_distance_reuse_check
```

框架自己的临时测试库与现有 55440 数据目录分离。两个用例是 `ann_orderby_distance_reuse` 和 `ann_orderby_distance_reuse_safety`，不能只凭 gsql 返回码成功就宣称 golden 比较通过。

### 验收结果

| 检查 | 实测结果 |
|---|---|
| 全量 Debug 编译、安装、原实例重启 | 成功；2026-09-14 23:48:29 开始，2026-09-15 00:01:02 完成 |
| 原生 `ann_orderby_distance_reuse_check` | **2/2 通过**，测试框架退出码 0；测试库端口 25632，完成后自动停止 |
| L2 合并向量，递归表达式 | 9 行全部返回；不匹配从 3 变为 0 |
| L2 合并向量，融合表达式 | 9 行全部返回；不匹配从 5 变为 0 |
| HOT 更新后的合并向量 | 9 行全部返回；不匹配从 3 变为 0 |
| 不同查询向量的 rescan | 两套引擎均由 false/false 变为 true/true |
| 旧二进制创建的索引 | 两套引擎逐行与堆距离一致；索引 OID、relfilenode 前后均为 439961，无 REINDEX |
| Cosine 外层阈值过滤 | off 与 cosine 均返回 0 行；修复前 cosine 错误返回 1 行 |
| Cosine 外层比较、InitPlan 比较 | 均由 true 恢复为 false |
| 预编译查询 GUC 降级 | 两套引擎近似开启时确实与精确值不同，切换 l2 后不匹配均为 0；允许重新规划或执行器回退 |
| 主回归的 Cosine 转换验证 | 不匹配为 0；与精确 SQL 的差异行数为 9，证明未误测成 off 路径 |
| 典型 delta/snapshot/delete Top-10 | L2 开关前后结果差异为 0；另有 Cosine 计划验证 delta 引用未被 sibling Anti Join 误禁 |
| 带 norm proc 的自定义 L2 opclass | 计划不生成 AM 距离引用；`l2/off` 的 4 条 float8 值逐位一致，差异 0 |
| 合并容量越界，建索引路径 | heap 12，`off=12`，`l2=12`，无重复 id，开关差异 0 |
| 合并容量越界，在线插入/WAL 路径 | 插入至 14 行后，heap/off/l2/distinct 均为 14，开关差异 0 |
| 合并节点 immediate delete | 两组 heap 幸存数均为 5，强制 DiskANN 返回数也均为 5 |
| 本轮最终原生 `ann_orderby_distance_reuse_check` | **2/2 通过**，2026-09-15 01:20，包含锁顺序最终修正后二进制 |

首轮构建时 Git HEAD 为 `e22030731162`，二进制 `version()` 报告 build e2203073、debug on，包含当时尚未提交的修复；通过验收后才提交为 `dc7e06d922`。本轮构建时 Git HEAD 为 `dc7e06d9222b`，同样先构建并实测工作树修复，再创建新提交；不能仅用版本字符串是否等于新提交号判断代码是否生效。构建包含工作树原有改动，提交仅纳入本特性的相关文件；列裁剪、INCLUDE 等原有 dirty 修改未被提交或丢弃。

首轮提交行数按 Git diff 统计：内核 8 文件 `+106/-42`；SQL 2 文件 `+212/-7`；expected 2 文件 `+338/-11`；合计 12 文件 `+656/-60`。本轮提交 `d4a5b5617b` 为 8 文件 `+443/-59`，其中内核 6 文件、回归 SQL 和 expected 各 1 个文件。公共文档在指定目录更新，不额外复制进源码树。

证据均保存于 [ANN距离复用修复证据](ANN距离复用修复证据/)：

- [原生回归日志](ANN距离复用修复证据/原生回归.log)、[全量构建安装重启日志](ANN距离复用修复证据/全量构建安装重启.log)。
- [安全回归修复前](ANN距离复用修复证据/安全回归_修复前.out)、[修复后](ANN距离复用修复证据/安全回归_修复后.out)、[主回归修复后](ANN距离复用修复证据/主回归_修复后.out)，均含回显 SQL。
- 旧索引：[旧版本建索引 SQL](ANN距离复用修复证据/旧版本建索引.sql)、[检查 SQL](ANN距离复用修复证据/旧索引兼容检查.sql)、[修复前](ANN距离复用修复证据/旧索引_修复前.out)、[修复后](ANN距离复用修复证据/旧索引_修复后.out)、[原文件标识](ANN距离复用修复证据/旧索引_原文件标识.out)。
- [测试对象清理记录](ANN距离复用修复证据/测试对象清理.out)：仅删除本轮测试 schema、5 行测试表和测试索引；已有用户数据未删除。需重新生成测试数据时可使用上述 SQL。原有 55440 实例恢复运行，原生测试临时实例已停止。

## d4a5b5617b 后续复审：修复前的 3 项复现记录

### 运行口径与证据

本次在现有 `postgres` 库、`/tmp:55440` 运行，仅使用 9 行、12 行和先后各 1 行的小数据。实际服务进程为 PID 477738，二进制为 `/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/bin/gaussdb`，数据目录为原有 `topk_test_data`。没有重新初始化、重启或 REINDEX 用户索引。

Git HEAD 为 `d4a5b5617b`；二进制在提交前编译，版本字符串仍为 `dc7e06d9`，与上节记录的“先构建验证、后提交”一致。本次结论针对当前工作树和该已安装二进制，不声称做了新旧提交的二进制 AB 测试。`off/l2` 比较指同一二进制、同一索引上仅切换距离复用 GUC。

| 编号 | 优先级 | 实测问题 | 与距离复用的关系 | 状态 |
|---|---|---|---|---|
| R1 | P1 | 回收后的 TID 被新行复用，`l2` 返回旧向量距离 0，正确值为 492.95030175465 | `off` 数值正确，`l2` 错误；直接涉及复用的行身份判定 | 已复现，未修复 |
| R2 | P1 | 单批结果出现距离 1、0、2、1，违反 ASC | `off/l2` 都逆序，属于底层 AM 排序问题；逐行精确重算不能修好排列顺序 | 已复现，未修复 |
| R3 | P1 | 删除后回滚，堆表恢复 12 行，索引扫描只返回 6 行 | `off/l2` 都漏行，属于 immediate delete 的事务可见性问题 | 已复现，未修复 |

三组完整 SQL 和实际输出位于 [ANN距离复用复审证据](ANN距离复用复审证据/)。输出保留执行时的临时脚本路径，归档文件没有把失败结果改成期望结果。

### R1：TID 复用使 exact L2 引用旧向量距离

复现：[完整 SQL](ANN距离复用复审证据/03_tid_recycle.sql)、[实际输出](ANN距离复用复审证据/03_tid_recycle.out)。

1. 插入 id=1、向量 `[10,20,30,40]`，其 `ctid=(0,1)`，创建普通、不带 INCLUDE 的 DiskANN 索引。
2. 删除该行并提交；推进事务和两代延迟清理快照后，执行 `VACUUM FREEZE VERBOSE`。实测日志明确显示旧行已清理，不能把“VACUUM 命令执行成功”直接当成已回收。
3. 插入 id=2、向量 `[100,200,300,400]`，实测其 `ctid` 再次为 `(0,1)`。
4. 查询向量为 `[10,20,30,40]`，强制 DiskANN，同一行同时输出 `<->` 和独立 `l2_distance()`。

| 开关/执行引擎 | 返回 id | ORDER BY 表达式投影值 | 独立堆距离 |
|---|---:|---:|---:|
| off，递归表达式 | 2 | 492.95030175465 | 492.95030175465 |
| l2，递归表达式 | 2 | 0 | 492.95030175465 |
| l2，融合表达式 | 2 | 0 | 492.95030175465 |

根因是两层缺陷叠加：

- `src/gausskernel/storage/access/datavec/diskann.cpp:431` 的 `diskannbulkdelete_internal()` 仍直接返回 NULL，没有根据 VACUUM 回调移除死行 TID。堆槽位已经回收，旧索引节点还保存旧向量及旧 TID。
- `diskannscan.cpp:560` 仅用 `vectorTid == heapTid` 判断是否免于 L2 heap recheck。槽位重新分配后，两者数值相等，却已不再代表同一代逻辑行。`GetDistance()` 读的是旧节点向量，执行器读到的是新可见堆行；扫描 epoch、row token 都是本次合法发布，无法识别这种跨事务的物理位置复用。

这不是开方精度误差，也不是 ANN 召回不足，而是同一返回行的 SQL 值错误。仅把执行器的块号比较加强为完整 TID 比较仍修不好，本例连 offset 都完全相同。

修复方向：在可证明的行版本身份建立前，不能把“同 TID”标为 exact；临时保守方案是对无法证明身份的行按堆值计算。长期需补齐 VACUUM 的死 TID 清理及代表向量 owner 身份失效，并证明旧 owner 的地址再次使用时不会误认，同时兼顾 HOT 链。仅清理 heaptids 数组但留下仍可被新行误认的 owner TID，也不构成完整方案。临时关闭距离复用可以规避本例的错误数值，但不会修复 AM 遗留的图结构或排序问题。

测试补充要求：实际断言发生 TID 回收复用；两套表达式引擎逐位比较距离；覆盖普通表、HOT、DELETE/VACUUM/INSERT、旧索引及并发 VACUUM。本次实际复现使用会话临时表，未把普通表及并发覆盖声称为已测。

### R2：单批内逐行值正确，但输出顺序错误

复现：[完整 SQL](ANN距离复用复审证据/01_merged_order.sql)、[实际输出](ANN距离复用复审证据/01_merged_order.out)。复用了原安全回归的 9 行大整数/近似向量数据，`ORDER BY distance ASC LIMIT 10`，WHERE 必须存在；计划为 `Limit -> Ann Index Scan`，没有额外 Sort。

返回前 4 行的 `(id, distance)` 为 `(2,1)、(1,0)、(3,2)、(4,1)`。全部 9 行均返回，与独立堆距离比较的错误值为 0，但相邻逆序对为 2。`off` 输出同样的顺序。因此不能把这次逆序解释为“没有召回到更近的点”：更近的点已经返回，只是排列错了。

根因链路：`diskannutils.cpp:852` 仍按内部距离阈值合并不一定相同的向量；`diskannscan.cpp:510` 排的是节点代表向量的距离，再按 `heaptids[]` 顺序展开；`execQual.cpp:4327` 以及融合引擎只在当前行投影时重算精确值，没有重新排序；`diskannscan.cpp:408` 仍设置 `xs_recheckorderby=false`。因此 K1 修复保证的“逐行数值正确”不蕴含“输入已按这些值有序”。

影响：这不仅是之前报告保留的跨批风险，**单批内已实测逆序**。上层 LIMIT 或信任有序输入的 Merge Append 存在错误截断/合并风险；本轮直接复现的是单支路输出逆序，未把完整 UNION ALL Merge Append 结果差异声称为已实测。

修复方向：新节点合并须验证真正的向量相等，不能仅凭阈值；已有合并节点需在按行精算后对完整候选集重排，或用显式排序/计划门禁阻止消费方信任不成立的有序性。仅对每批排序还不能解决跨批顺序；只在错误的分支 LIMIT 之后加 Sort 也补不回已经截断的候选。

测试补充要求：在动态执行的独立查询循环中捕获实际发出次序，断言相邻距离单调；同时看护最终 LIMIT 和典型 delta/snapshot/delete Merge Append。不能只对结果按 id 重排后比较距离或比较集合。

### R3：immediate delete 的物理改动没有随事务回滚

复现：[完整 SQL](ANN距离复用复审证据/02_delete_rollback.sql)、[实际输出](ANN距离复用复审证据/02_delete_rollback.out)。普通表中 A/B 两组各 6 行，使用不带 INCLUDE 的旧格式 DiskANN，并显式设置 `immediate_delete=true`。

索引原先返回 12 行。执行 `SAVEPOINT; DELETE FROM t; ROLLBACK TO SAVEPOINT;` 后，强制堆扫描显示 A/B 各 6 行，而 `off/l2` 强制 DiskANN 均只返回 B 组 6 行。frozen point 的抽样组可能变化，复跑不应把“必须剩 B 组”当成断言；关键是已回滚的另一组仍无法召回。

根因：`diskanndelete.cpp:79` 从节点的 heaptids 数组直接删去 TID，并立即 `GenericXLogFinish()`；数组变空后，`DiskAnnMarkDead()` 断边并将节点标死。generic WAL 提供重做，不提供 SQL 事务回滚的索引逻辑撤销。回滚恢复了堆行的可见性，却没有恢复索引 TID 和图边。K5 的“只删空节点”修复没有解决这一层事务问题。

修复方向：索引条目的物理删除应推迟到 VACUUM 判定所有相关快照都不再需要后执行，或建立完整的带事务版本的删除协议。不能只移到 commit hook，因为已经存在的旧快照仍可能需要这些行。在协议完善前，应禁止该旧格式使用不安全的 immediate delete 路径；是否改变该模式的产品行为需在修复任务中明确。

测试补充要求：完整事务回滚、savepoint 回滚、旧快照读者、并发插入同向量与删除末个 TID。本次只有回滚场景实测确认，不宣称已完成并发验证。静态审查还发现从“计数为 0、释放锁”到“重新加锁断边”之间的竞态窗口，尚未进行确定性交错复现，不额外计入本轮 3 项已复现问题。

### 复跑与环境恢复

```bash
ANN_AUDIT_INSTALL=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install
ANN_AUDIT_EVIDENCE=/home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/测试/ANN距离复用复审证据
env LD_LIBRARY_PATH="$ANN_AUDIT_INSTALL/lib" "$ANN_AUDIT_INSTALL/bin/gsql" \
  -X -h /tmp -p 55440 -d postgres -f "$ANN_AUDIT_EVIDENCE/03_tid_recycle.sql"
```

另外两例分别将文件名换为 `01_merged_order.sql`、`02_delete_rollback.sql`。必须使用启用了本特性的正确二进制并核对 EXPLAIN 确实走 DiskANN。脚本的 `ON_ERROR_STOP` 只保证 SQL 错误退出；这些缺陷属于返回结果错误，需要检查上述数值/次序/行数，不能根据 gsql 退出码 0 判断用例通过。

前两例在完整事务内创建专用 schema，最后 ROLLBACK；第三例使用会话临时表，断开后自动清理，不触及用户已有对象。第三例的两个 6 秒等待匹配本实例 5 秒延迟清理设置；其他环境须以 VACUUM 确认旧行已清理且新旧 ctid 相同为前提，否则只能记为“没有触发”，不能误记为问题已修复。

本次没有修改内核、原生 SQL/expected 或重新编译，没有停止原本运行的服务；上述 3 项均仍待修复。

## R1/R2/R3 修复与本轮验收（2026-09-15）

### 修复内容与边界

| 编号 | 本轮实现 | 实测修复后结果 |
|---|---|---|
| R1 | VACUUM 回调清理死 TID 和失效 owner；可见堆向量与索引候选向量做字节校验，L2 不一致则精确重算 | 普通表真实发生 ctid 复用；两套引擎均返回新行正确距离。升级前已错误的旧索引，不 REINDEX 也从 0 恢复为 492.95030175465 |
| R2 | 新合并先证明向量真正相等；显式收集完整批次，对可见且满足 WHERE 的行按最终距离统一排序，首行发布后不再扩批 | off/l2/融合三路均返回 Top-10，逆序数与逐位距离错误数均为 0；旧合并索引逆序数为 0；delta/snapshot/delete Top-10 集合差异和逆序数均为 0 |
| R3 | 所有 DiskANN 格式禁止立即物理删除，推迟到 VACUUM 的全局可见性判断；保留图边及导航节点 | savepoint 删除回滚、UPDATE 事务回滚后均 A/B 各 6 行；off 返回 12 行；并发删除并 VACUUM 后，旧快照 12 行、新快照 0 行 |

修复同时纠正了两个适配遗漏：`tablecmds.cpp` 的 INCLUDE DDL 判定不能使用已经扩大为全部 DiskANN 的 deferred-delete 判定；新增执行器代码要使用实际可链接的 `tableam_tslot_getattr()`，且不能增加原生回归禁止的新 PGXC 宏。上述均已重新编译安装。

排序使用 work_mem 受控的内部 tuplesort，并保存原堆元组系统属性；WHERE 只执行一次，用户投影与 SRF 延迟到输出。80 行、每行 6000 字节 PLAIN 文本及 64kB work_mem 用例验证了超内存排序的返回完整性。另有真实 runtime-key 重扫、游标和预编译策略测试沿用原主/安全回归；其最终结果见下方原生验收。

明确的代价与不承诺：

- 正常 L2 仍须提取堆向量并进行身份字节校验，不是零回表；失配/旧合并行仍会精算。Cosine 校验还需要归一化，未验收性能收益。
- 严格输出收集完整批次，增加首行延迟及物化/排序成本；`off` 也应用基础有序性修复，不能再将它当作恢复旧错误行为的开关。
- 此排序协议只覆盖合格的 DiskANN heap 单距离路径，不扩大到 HNSW、index-only、Ustore、bucket；近似 Cosine 失配报错提示 REINDEX，不逐行切换成精确值。
- 补丁不会自动修复旧 immediate delete 已经损坏、丢失的图连接，也未证明全表精确 Top-K；历史损坏索引需另外评估 REINDEX。

### 构建与定向验收记录

- 使用统一 Debug 脚本从 full 清理重新编译；遇到取列接口链接错误后仅修改 cpp，增量完成全量对象构建、安装及原实例重启；最终适配修正于 **02:27:42** 安装并启动成功。没有初始化原有 17GB 数据目录。
- 最终服务 PID **542403**，原端口 **55440**，二进制来自 `openGauss-server/mppdb_temp_install/bin/gaussdb`，版本 build `d4a5b561`、debug。遵循先构建测试、后提交的顺序，版本串仍表示编译时的旧 HEAD，不能据此认定缺少 `84f0a21f54` 的修复。
- `ann_orderby_distance_reuse_lifecycle.sql` 独立执行约 12.5 秒，返回码 0；核验结果包括：Top-10 各模式逆序/错值均 0、系统列错误 0、qual 一次为 true、投影只处理发出行 true、SRF 返回 id=5、OFFSET 返回 2 行、联合 Top-10 差异/逆序均 0、排序落盘返回 id 1～10 且文本长度 6000、回滚后 12 行、ctid 实际复用 true、两套引擎的复用后错值均 0。
- 两个升级前的旧索引 `merged_idx/recycled_idx` 的 OID 和 relfilenode 分别保持 **440634/440641**，在原文件上验证，不重建索引。
- 双会话旧快照测试：读者在 REPEATABLE READ 中读到 12 行后等待；写者 DELETE 提交、VACUUM、读新快照为 0；读者第二次 ANN 查询仍为 12。它是确定性交错的定向测试，不是长期并发压测。
- 原生框架第一次准备被新增旧宏门禁拦截，另一次因和主安装并行争用构建文件失败，均**不计为测试通过**；后续采用串行构建/安装与回归。新增用例的 nextval 返回类型也已按本分支显式转为 bigint。
- **原生最终验收：3/3 通过**，依次为 `ann_orderby_distance_reuse`（0.704 秒）、`ann_orderby_distance_reuse_safety`（0.504 秒）、`ann_orderby_distance_reuse_lifecycle`（12.641 秒），框架总测试时间 13.883 秒，不含隔离安装/启动准备时间。主用例仅更新 5 处 `Rows Removed by Filter: 1` 及对应计划行数：完整消费候选批次后确实遇到被 WHERE 拒绝的那行，不是放宽结果断言。新 expected 保留典型计划、完整 Top-10 和各错误计数的精确看护。
- 仅清理本轮创建的两个旧索引/快照 schema，生命周期脚本自行清理专用 schema；原生隔离实例已停止。原有 55440 实例保持运行，用户表和索引不删除，测试对象可由归档 SQL 重建。

### 本轮证据

提交记录：`84f0a21f54 fix(diskann): preserve row identity, MVCC and candidate order`，位于 `openGauss-server` 的 `integration/multimodal-stacked` 分支，包含 9 个内核文件和 4 个原生测试文件，共新增 983 行、删除 31 行。只提交 R1/R2/R3 相关差异；列裁剪、INCLUDE 禁用、既有其他测试和文档的 dirty 改动仍原样保留。公共文档位于源码仓之外，不包含在该 Git commit 内，已在原路径回填提交号；本次未 push。

证据集中在 [ANN距离复用生命周期修复证据](ANN距离复用生命周期修复证据/)。修复前日志完整保留原来的失败值，不改写成期望结果。新加入的 SRF/OFFSET/易变 target 用例是修复后新增边界看护，不宣称这些边界在修复前都存在缺陷。

- [生命周期完整 SQL](ANN距离复用生命周期修复证据/生命周期完整用例.sql)、[修复前输出](ANN距离复用生命周期修复证据/生命周期_修复前.out)、[修复后输出](ANN距离复用生命周期修复证据/生命周期_修复后.out)。
- [旧索引修复前](ANN距离复用生命周期修复证据/旧索引_修复前.out)、[旧索引修复后](ANN距离复用生命周期修复证据/旧索引_修复后.out)、[旧索引检查 SQL](ANN距离复用生命周期修复证据/旧索引检查.sql)。
- [旧快照读者输出](ANN距离复用生命周期修复证据/旧快照_读者.out)、[写者输出](ANN距离复用生命周期修复证据/旧快照_写者.out)、[测试对象清理](ANN距离复用生命周期修复证据/测试对象清理.out)。
- [最终编译安装日志](ANN距离复用生命周期修复证据/最终编译安装.log)、[原生回归最终日志](ANN距离复用生命周期修复证据/原生回归_最终.log)；原生实际输出保存在同目录 `原生_主回归.out`、`原生_安全回归.out`、`原生_生命周期回归.out`。

旧快照用例依次运行 `旧快照_准备.sql`；在会话 A 运行 `旧快照_读者.sql`，打印第一次数量 12 后的 15 秒等待内，在会话 B 运行 `旧快照_写者.sql`。必须观察读者前后均 12、写者 0，不能只看脚本退出码。

## 未包含的结论

- 本轮只对限定 heap 路径的已收集候选实施严格输出；未覆盖路径及全表召回仍须独立审查，不能用数值正确和已召回候选有序证明全表精确 Top-K。详见设计第 10 节。
- AM 内整队列 `GetDistance()` 仍保留；本次将其距离与 TID 读取绑定，并未宣称消除该次内部完整距离计算。
- 没有进行性能基准、长期并发 DML/VACUUM 压测或全量内核回归；Debug 构建数字不能作为性能结论。

配套：[详细设计](../设计/ANN距离结果复用详细开发设计.md)、[实施指南](../设计/ANN距离结果复用开发实施指南.md)。
