# DiskANN 距离结果复用：开发实施指南

## 0. 使用说明与当前断点

本指南配套[详细开发设计](ANN距离结果复用详细开发设计.md)，面向后续开发者及迁移分支中的编码代理。设计规定语义和边界，本指南规定修改顺序、文件职责、测试和交付方法。

适配范围仅为 DiskANN。HNSW 不做适配，不修改其候选结构、搜索/返回代码或配置，也不新增 HNSW 距离复用功能用例。AnnOrderByRef、ANN 通道和共用执行器仍需实现，但规划与执行都必须按实际 DiskANN AM 身份限定准入。

当前断点：`d4a5b5617bfc` 之后的 R1/R2/R3 生命周期补强已完成，并提交为 `84f0a21f543da7ffaa9582d44d0ff0772068c9ae`（未 push）。2026-09-15 已完成 Debug 构建安装、原生回归 **3/3**、旧索引免 REINDEX、删除回滚及双会话旧快照验证。迁移必须包含 §0.3；详见[缺陷修复与验证报告](../测试/ANN距离复用缺陷修复与验证报告.md)。长期并发、未覆盖访问路径与性能结论仍须独立验收。

基线核对日期 2026-09-12，工作树 `/home/qiumc/data/code/cpp/openGauss-server`，分支 `integration/multimodal-stacked`，HEAD `098d77c8b76d56eace4af8fddaf8d94b394b996c`。源码存在用户 dirty 修改，开发前必须重新盘点。

本文不授权修改其他项目、删除测试之外的数据、重新开启 DiskANN INCLUDE、提交或推送代码。后续只有在用户明确要求实施后，才执行这里的写代码、构建和建表步骤。

### 0.1 本次只写文档时的环境核对

- openGauss 当时在运行，PID 2977361，二进制来自 `openGauss-server/mppdb_temp_install/bin/gaussdb`，数据目录 `topk_test_data`，SQL 端口 55440。
- `version()` 报告 7.0.0-RC3、build 098d77c8、debug on；这不是新特性的构建证明，也不能根据版本字符串认定 dirty 改动是否已安装。
- 只读 SHOW 当时得到：enable_expr_fusion=off、enable_codegen=off、enable_opfusion=on。
- PG19 图项目另有进程在运行，本任务没有操作它；podman 当时没有运行中的容器。
- 本次没有建表、改 GUC、编译、重启、停止服务或运行本特性回归。

上述为时点记录，未来必须重新核对，不能直接复用 PID 或状态。

### 0.2 本轮修复与迁移必带项

- 不改索引页格式，不自动重建索引。最终 `GetDistance` 同锁读取完整距离、TID 列表和 `IndexTuple.t_tid`；新增 `Neighbor.vectorTid` 是内存字段，不是磁盘字段。
- L2 非原始 TID 经 candidate/channel/AnnIndexScanState 的显式 recheck 状态，在双表达式引擎中对当前堆行求原表达式，并按行缓存。不要漏掉任一引擎、逐行 reset、重扫失效或 HOT 根 TID 的区别。
- Cosine 采用透明消费者白名单；必须覆盖 SubqueryScan.qual、Result 计算/常量条件和 PARAM_EXEC→InitPlan；List 不算消费者，分支退出恢复上下文。
- `sql/ann_orderby_distance_reuse.sql` 与 `sql/ann_orderby_distance_reuse_safety.sql` 使用独立动态 SELECT 保存真实命中结果，不能改回会禁用改写的 CTAS 直接包装。
- 新增断言包括大整数/微小差异合并向量、双引用、双表达式引擎、HOT、预编译 GUC 降级后的逐位精确结果、Cosine 阈值过滤和 InitPlan 比较。EXPLAIN 继续仅保留 Output 双括号，不恢复诊断字段。
- 此轮修改了头文件，按工作区规范必须用统一 Debug 脚本 `full`，再运行 `make -C src/test/regress ann_orderby_distance_reuse_check`。已有数据目录保留。

### 0.3 后续 R1/R2/R3 生命周期修复迁移清单

本轮在 `d4a5b5617bfc` 上继续修复，提交为 `84f0a21f54`，具体实测状态以测试报告的新一轮验收为准。不能仅搬运初版的距离转换和引用节点，以下改动互相依赖：

- `diskann.cpp`：真实 bulkdelete、清理 dead TID、失效死 owner；保留空图节点和边；所有格式延迟物理删除。
- `execUtils.cpp` / `diskanndelete.cpp`：执行器分派与直接调用两处均不得进入不安全的 DiskANN immediate delete。
- `tablecmds.cpp`：DDL 的 INCLUDE 限制必须判断索引是否确实包含非键列，不能再把“需要 deferred delete”当作“索引包含 INCLUDE”。普通 v1 索引仍可存在于 immediate_delete=true 的表上，但它自身的物理删除被推迟到 VACUUM。
- `diskannutils.cpp`：新重复向量合并先做锁内的完整字节相等证明；复制源向量后先解锁，再持目标锁；桶满、owner 失效和 WAL 退出路径都不能遗漏。
- `diskannscan.cpp` / `diskann.h`：显式候选批次接口、禁止发布后扩批、空队列保护，以及当前可见向量与候选节点向量的身份核验。
- `nodeAnnIndexscan.cpp` / `execnodes.h`：受 work_mem 管理的完整候选排序，raw heap tuple/system columns 保护，过滤一次且延后投影，SRF 不使用基本行数提前停止，rescan/end 清理；仅 DiskANN 合格 heap 路径启用。
- 新增 `ann_orderby_distance_reuse_lifecycle.sql`、同名 expected 和原有专用 schedule 条目。用例必须检查真实输出顺序及实际 ctid 复用，不能只按 id 比较集合。

构建注意：使用本分支 `tableam_tslot_getattr()`，`slot_getattr()` 只有声明而无可链接实现；条件编译使用项目允许的 `ENABLE_MULTIPLE_NODES`，不要新增会触发原生回归静态门禁的旧宏。主构建/安装与 pg_regress 的临时安装不能并行访问同一源码输出树，应串行进行。

本轮增加了向量身份读取和节点内排序，不能沿用初版“只多一次标量转换”的性能描述。L2 正常行仍省完整距离算术，但不是零向量读取；近似 Cosine 的失配必须报错而不是逐行混用精确值。性能、长期并发以及不在上述 heap 路径内的排序能力不由这份小数据回归背书。

## 1. 实施总顺序与完成标准

| 阶段 | 主要工作 | 阶段产物 | 放行条件 |
|---|---|---|---|
| P0 | 冻结源码基线、距离来源和排序能力审计 | 文件清单、现状计划、风险记录 | 不依赖注释猜测距离/有序语义 |
| P1 | 节点、协议、GUC、版本门 | 可复制/序列化的新节点和通道 | 全量构建通过；off 不生成复用引用，基础正确性修复仍生效 |
| P2 | DiskANN 标量转换与返回 | L2 / 近似 Cosine 候选输出缓存 | 同一候选转换一次，TID/epoch 对应 |
| P3 | DiskANN 扩批与缓存生命周期 | 首批、扩批、多 TID、rescan/endscan 全部接入 | 身份及来源可靠，不污染原搜索分数，不读取失效缓存 |
| P4 | 双表达式引擎、计划末端改写、EXPLAIN | Output 引用 AM 值 | 计数证明匹配投影不再调用原距离函数 |
| P5 | 普通回归与安全回退 | SQL / expected / schedule / 运行日志 | 数值、结果、绑定、GUC、缓存计划均通过 |
| P6 | UNION ALL、有序性、数据变化与性能验证 | 严格排序证据、并发检查、性能原始数据 | 通过设计中的独立有序集成门 |
| P7 | 审查、文档回填、特性补丁 | 分项行数、patch、未覆盖范围 | 补丁仅含本特性；所有证据可复跑 |

P2 的标量转换与 P3 的生命周期处理可在接口冻结后分步形成独立改动，但最终必须共同经过 P4～P6；只有得到明确授权才进行 Git 提交。首批转换正确不代表扩批、重扫和并发关联已经通过，不得跳过 P3。

## 2. P0：环境与基线准备

### 2.1 只读检查

```bash
cd /home/qiumc/data/code/cpp/openGauss-server
git branch --show-current
git rev-parse HEAD
git status --short
git diff --stat
ps -eo pid,comm,args | rg '[g]aussdb|[p]ostgres -D'
ss -ltnp | rg ':55440|:55441|:55450'
podman ps --format '{{.Names}} {{.Status}}'
```

只记录任务相关变量和版本，不把完整环境、凭据、服务器原始内存等写进公共报告。

本机客户端示例：

```bash
ANN_INSTALL=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install
env LD_LIBRARY_PATH="$ANN_INSTALL/lib" "$ANN_INSTALL/bin/gsql" -X -h /tmp -p 55440 -d postgres -c 'SELECT version(); SHOW enable_expr_fusion; SHOW enable_opfusion; SHOW enable_codegen;'
```

如果实际进程来自其他安装树，先更正 ANN_INSTALL。不能拿另一个工作树的二进制测试本源码。

### 2.2 保存基线和保护 dirty 修改

- 明确区分用户原有改动和本特性改动。特别注意 planner.cpp、preptopk.cpp、现有回归 SQL/expected、diskann.cpp 等已经可能 dirty。
- 不执行 reset、checkout --、git clean；不使用 git add -A。
- 对重叠文件先阅读 diff，在已有修改上进行小范围补丁。
- 建立本特性文件白名单和功能分块记录；最终从白名单提取补丁，不能把用户原有的整个 dirty diff 一起打包。
- 若无法可靠分离重叠改动，先说明情况并请用户确认基线；不得靠丢弃已有改动获得“干净环境”。

文档和长期测试报告归档在 `公共文档/UNION_ALL与DiskANN_TopK/`。构建中间产物、隔离数据库目录可放安全临时目录。

### 2.3 必须先回答的代码问题

```bash
rg -n 'GetDistance|SearchFixedPoint|xs_orderbyvals|curIterNum' src/gausskernel/storage/access/datavec/diskannscan.cpp
rg -n 'Neighbor|DiskAnnCandidatesData|distance|heaptids' src/include/access/datavec/diskann.h
rg -n 'rescan|endscan|xs_ctup|candidates|enablePQ' src/gausskernel/storage/access/datavec/diskannscan.cpp
rg -n 'xs_orderbyvals|xs_recheckorderby' src/gausskernel/runtime/executor src/gausskernel/storage/access
rg -n 'set_plan_references|build the PlannedStmt' src/gausskernel/optimizer/plan/planner.cpp
```

逐项输出核对结论：

1. DiskANN 内部当前计算的是平方距离还是负内积？哪些值经过量化或变换？
2. 何时得到完整内部距离？会不会后来再次覆盖候选 distance？
3. 哪些消费者会读取标准 xs_orderbyvals？是否全部经过 AnnIndexScan？
4. DiskANN 首批、扩批和多 TID 有哪些物化/最终返回出口？哪个上下文拥有候选内存？
5. DiskANN Post-Filter 多轮输出是否有序？严格合批分支的实际进入条件是什么？
6. 当前 EXPLAIN 是否真的走 ANN，而不是因为测试包装多了一个 Sort 或退成 Seq Scan？
7. 距离和 TID 是否属于同一个有效索引版本？尤其审计 DiskANN 先保存 TID 列表、后来 GetDistance 重新读取向量的组合，以及 HOT 根 TID 到可见元组 TID 的映射。
8. 如何从索引 relation / catalog 解析目标分支的实际 DiskANN AM 身份？共用 AnnIndexScan 的其他 AM 是否始终保持未请求状态？

P0 的排序审计是前置门。若直接 Merge Append 的输入无法证明有序，按设计保守拒绝相应集成命中，或另行确认严格输出改造的范围；不能把这个缺口藏进距离转换函数。

## 3. 文件与职责清单

以下相对路径均以 openGauss 源码根目录为基准；现有文件应检索具体函数，拟新增文件的命名可按分支规范调整。

| 文件/区域 | 必改或必审内容 |
|---|---|
| src/include/nodes/nodes.h | 新 NodeTag，检查编号/兼容策略 |
| src/include/nodes/primnodes.h | AnnOrderByRef 表达式结构 |
| src/include/nodes/plannodes.h | 必要的计划策略/绑定元数据，不放运行时指针 |
| src/include/nodes/execnodes.h | 扫描绑定状态、epoch/row 关联、fallback 与计数 |
| src/include/access/relscan.h | 专用 ANN 结果通道挂接及接口注释 |
| RelationGetIndexScan 的实现 | 新指针及状态初始化；其他 AM 默认不请求、不导出 |
| src/include/access/datavec/diskann.h | DiskANN 候选输出缓存、来源、构造及复制路径 |
| src/gausskernel/storage/access/datavec/diskannscan.cpp | 精排之后的转换、TID 绑定、扩批、rescan/endscan、标准字段口径修正 |
| src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp | 实际 DiskANN AM 身份核验、运行能力协商、当前返回事件、投影取值绑定 |
| src/gausskernel/runtime/executor/execQual.cpp | 递归表达式引擎 |
| src/gausskernel/runtime/executor/execExpr.cpp | 扁平化初始化、属性预取 walker |
| src/gausskernel/runtime/executor/execExprInterp.cpp | 新 opcode 解释执行及 dispatch |
| 对应 executor/ExprEvalStep 头文件 | opcode、step 布局及回调声明 |
| src/gausskernel/optimizer/plan/planner.cpp | 最终 setrefs 后、计划复制/封装前的入口 |
| src/gausskernel/optimizer/plan/setrefs.cpp | 审查 Var/依赖阶段，不随意修改 INDEX_VAR 含义 |
| 新建的 optimizer 计划辅助模块或既有合适模块 | 能力门、TLE 匹配、消费链追踪、原子安装 |
| src/common/backend/nodes/copyfuncs.cpp、equalfuncs.cpp、outfuncs.cpp、readfuncs.cpp、nodeFuncs.cpp | 新节点及新增计划字段全覆盖 |
| src/gausskernel/optimizer/util/clauses.cpp 等表达式分类入口 | 明确新引用的类型、依赖与不应重新计算的语义 |
| src/common/backend/utils/adt/ruleutils.cpp | 引用节点展示；不能让展示要求反向决定错误语义 |
| src/gausskernel/optimizer/commands/explain.cpp、ruleutils.cpp | Output 中新引用的双括号展示；Order By 保持原搜索表达式；不增加距离诊断字段 |
| src/common/backend/utils/misc/guc/guc_sql.cpp、对应会话属性头 | 四档 GUC，默认 off，缓存计划兼容性检查 |
| src/include/miscadmin.h 及目标分支版本机制 | 独立的新节点版本门，不复用 INCLUDE 版本号 |
| codegen / ByPass / 远程计划判断 | 不支持时在新节点产生前拒绝 |
| src/test/regress/sql、expected、专用 schedule、GNUmakefile | 原生测试及专用入口 |
| 新增源文件所在目录 Makefile / CMakeLists.txt | 两套构建清单同步，不能只让本地 make 偶然通过 |

检索优先使用 rg，不能依赖本文行号。节点基础设施即使“暂不跨节点”也不可遗漏。

文件白名单不包含 HNSW 源文件/头文件。新增共用字段必须默认关闭；支持边界由实际索引 AM 决定，不能让打开 GUC 的所有 AnnIndexScan 一起进入新路径。

## 4. P1：接口、GUC 与节点先落地

### 4.1 建立协议，不先改距离函数

先新增三种质量：不可用、SQL_EXACT_L2、APPROX_COSINE；建立带 requested、epoch、row_token、TID、isnull、valid 的专用返回通道。

requested 只有在当前扫描通过 DiskANN 身份、opclass/支持函数、执行模式和质量能力检查后才能设为 true。其他 AM 不创建有效输出缓存，也不需要新增发布动作。

所有初始化、重扫、结束路径都要显式设置状态。valid 与 isnull 独立：valid=true/isnull=true 才表示一个有效 SQL NULL；valid=false 表示根本没有可读取的值。

返回值使用 float8 Datum 按值传递，候选内存只归 DiskANN 扫描所有。不得把候选对象、候选向量、索引页地址或 tmpCtx 中的指针塞到上层结果 tuple。

### 4.2 四档控制

拟新增语法：

```sql
SET ann_orderby_distance_reuse = 'off';
SET ann_orderby_distance_reuse = 'l2';
SET ann_orderby_distance_reuse = 'cosine';
SET ann_orderby_distance_reuse = 'all';
RESET ann_orderby_distance_reuse;
```

要求 off 默认完全不植入新表达式节点；l2 仅启用合格的 DiskANN L2；cosine 仅启用近似 Cosine；all 同时启用两者。四档均不为 HNSW 生成引用，也不自动打开 UNION 下推、列裁剪或 INCLUDE。后续 R2 基础排序修复独立于复用开关，off 也受保护，不能再把 off 解释成恢复所有旧 AM 缺陷行为。

Prepared plan 必须在模式变化后重新验证策略。增加计划环境签名/兼容性判定，并在执行开始再次防守；不把 GUC 变化留到逐行执行时处理。

### 4.3 新节点测试先行

- 构造含原表达式、参数、类型和策略的 AnnOrderByRef。
- 验证 copyObject 不共享可变运行状态。
- 验证 out/read 往返字段一致、equal 一致。
- exprType=FLOAT8OID、typmod=-1、collation=InvalidOid。
- walker 能发现原参数、列依赖；正常执行初始化不因此运行 original_expr。
- 老版本门关闭时，计划不得含新 NodeTag。
- EXPLAIN_ONLY 状态能初始化和展示，不能要求已存在 AM 候选。

该阶段可暂不让优化入口生成新节点；先保证系统能安全理解它。

## 5. P2/P3：转换函数与 AM 出口

### 5.1 公共转换辅助函数

建议一个无副作用、无向量读取、只做标量转换的 helper。以下伪代码中的类型和错误码由实施者按分支规范补齐：

```cpp
bool PrepareAnnOutputDistance(const InputDistance& input,
                              ReusePolicy policy,
                              uint64 expected_epoch,
                              OutputDistance* out)
{
    out->ready = false;
    out->quality = UNAVAILABLE;
    out->isnull = false;
    if (!input.valid || input.epoch != expected_epoch)
        return false;

    if (input.query_is_null) {
        // 首版不从内部占位分数构造 NULL 距离。
        // 调用层必须选择经过验证的回退或明确拒绝。
        return false;
    }

    if (input.kind == FULL_L2_SQUARED && policy >= EXACT) {
        if (input.value < 0)
            return false;
        out->value = sqrt((double)input.value);
        out->quality = SQL_EXACT_L2;
    } else if (input.kind == FULL_NORMALIZED_NEG_IP && policy == ALLOW_APPROX) {
        if (!isfinite(input.value))
            return false;
        out->value = 1.0 + (double)input.value;
        if (out->value < 0.0) out->value = 0.0;
        else if (out->value > 2.0) out->value = 2.0;
        out->quality = APPROX_COSINE;
    } else {
        return false;
    }

    out->isnull = false;
    out->ready = true;
    return true;
}
```

本伪代码不是完整的特殊值/NULL 实现。对 L2 的 NaN/Inf、NULL 查询向量及 AM 原有的零向量排除行为，应先建立 SQL/索引基线，再决定能力拒绝或合法结果标记；绝不因为 input 为 NULL 就把 AM 中的占位 0 开方后当 SQL 结果。

helper 返回 false 后的行为必须在调用层明确：已知不支持应在开始输出前回退；按承诺本应有效却失败，须报错或按定义跳过陈旧候选，不得逐行偷换近似/精确语义。

### 5.2 DiskANN 具体落点

1. SearchFixedPoint 的每个距离来源都打标：初始/frozen 完整值、非 PQ 完整值、PQ 估算值不能混为一类。
2. 保留必要的完整距离精排；经来源和版本审计确认安全时，才跳过已完成的同一计算。
3. 候选完整内部距离固定后，执行一次 PrepareAnnOutputDistance，保存输出缓存。
4. 同节点多个 TID 复用转换结果，但分别发布 TID/row_token。
5. 新一轮扩批不能把旧候选的缓存当成新 query 的结果；epoch 贯穿候选和通道。
6. gettuple 从候选缓存复制标量值，不在这个位置再次调用 GetDistance 来“拿输出值”。
7. 处理原 xs_orderbyvals 写入。专用近似通道与标准精确/下界通道分离，审查普通 IndexScan 等所有消费者。

建议把“执行器复用”和“跳过 DiskANN 现有非 PQ 整轮重复计算”作为两个可独立验收的小提交。第二项未证明安全时，可以先保留原精排并交付第一项，但报告必须明确尚未消除哪些 AM 内部调用。

上述分步以候选身份已经证明为前提。保留 GetDistance 不等于距离/TID 关联自然安全；若重新读取的距离可能属于被复用后的另一条索引记录，必须先修正同一受保护读取下的身份关联，否则第一项也不能交付。不得依赖执行器原先回表重算距离来掩盖这个缺口，因为本特性正要移除那次计算。

### 5.3 P3：DiskANN 扩批与候选生命周期

1. 盘点首批、普通扩批和严格合批路径，所有最终物化/返回出口都经过同一组转换与发布规则；只改首次 diskannsearch 不足以覆盖全程。
2. 已有候选再次进入队列时，先验证 query epoch、节点身份和距离来源，再决定复用缓存；不能仅凭节点 ID 相同就认定是同一有效版本。
3. 任何精排、候选替换或重新计算导致 search_distance 改变，都必须清除旧 output_ready；在最终距离确认后再转换。
4. 构造/复制 Neighbor 或返回候选时审计新增字段的初始化和传递。共用结构若也用于构建期，构建阶段不得误读扫描输出缓存。
5. 一个节点的多个 heap TID 共用转换值，但每次 gettuple 独立更新 row_token 与返回 TID。过滤、MVCC 跳过候选后不得保留为下一行可读的旧发布值。
6. 同一距离与 TID 的关联校验保留必要的 pin、锁和有效性读取；缓存不能替代它们。只能证明最终物化身份时，保留必要的重复精排及关联读取；连最终候选关联也无法证明时，拒绝该复用。
7. rescan 增加 epoch，清空候选输出就绪状态和发布状态；endscan 按内存所有权释放一次。LIMIT 提前结束、查询取消与错误退出同样需要清理。

### 5.4 发布动作示意

以下片段仅用于已协商成功的 DiskANN 复用分支。requested=false 时执行原有 gettuple 路径，不要求其他 AM 生成 output_ready 或调用转换 helper。

```cpp
InvalidatePublishedValue(scan);

candidate = GetNextValidatedCandidate(...);
if (!candidate->output_ready)
    PrepareOrRejectOutputDistance(candidate, ...);

ItemPointerData tid = SelectNextHeapTid(candidate);
scan->xs_ctup.t_self = tid;
PublishAnnValue(scan, candidate->output_value, candidate->quality,
                CurrentEpoch(scan), NextRowToken(scan), tid);
return true;
```

“validated” 只表示 AM 能验证的候选状态，不能取代后续堆表 MVCC 检查。若后续框架跳过该 TID 并再次调用 AM，通道必须更新到新的返回事件。

HOT 特别要求：AM 根 TID 和 heap_hot_search_buffer 找到的可见元组 TID 可能不同。应保留取行事件及 HOT 映射，检查索引键未变和快照可见性，不能用简单的两个 t_self 相等断言替代，也不能把合法 HOT 链一律判成陈旧候选。正式并发用例应包含只更新 title 的 HOT 场景和更新 embedding 的非 HOT 场景。

## 6. P4：计划替换与执行器接入

### 6.1 先扫描、后安装

```cpp
Plan* PrepareAnnOrderByRefs(PlannerInfo* root, Plan* top)
{
    if (!FeatureAndVersionAllowed(root, top))
        return top;

    List* proposals = CollectEligibleAnnBindings(top);
    foreach (binding, proposals) {
        if (!BindingUsesActualDiskAnnAM(binding)) continue;
        if (!ExactExpressionAndCapabilityMatch(binding)) continue;
        if (!ConsumersAllowQuality(binding)) continue;
        if (!OrderingIntegrationAllowed(binding)) continue;
        InstallOutputReferencesAtomically(binding);
    }
    return top;
}
```

入口放在最终 setrefs 完成后、计划复制与 PlannedStmt 封装之前。替换不改变 TLE 位置、类型、resjunk 和上层列号，不增加 LIMIT，不触碰 WHERE，不把 indexorderbyorig 换成引用。

BindingUsesActualDiskAnnAM 是设计 helper，须读取目标分支的索引 AM 元数据；禁止按节点类型或索引名称猜测。执行器在设置 requested 前再次核验当前 relation，其他 AM 始终保留普通表达式投影。

首版只处理 ANN targetlist 中完整匹配的距离 TLE。复杂嵌套表达式或跨计划计算下移是后续扩展，不要为了命中所有 SQL 无限制递归替换。

### 6.2 消费链审查要求

对近似输出，沿该 TLE 位置向上检查：

- Limit、简单投影、Sort、可验证的 Append/Merge Append 透传：可以继续检查。
- Filter、Join Qual、Hash Key、Group Key、Distinct、Window、SetOp 去重：拒绝该近似绑定。
- 中间存在重新计算原距离而不是引用本输出列：不能宣称已消除该次计算；首版不擅自替换。
- 别名相同不等于表达式相同；相同文本也不等于同一参数或 opclass。

普通 SQL 中未参与本次绑定的 `<=>` 函数仍保持原语义；不全局替换函数实现，不在函数内部读取隐式“当前 ANN 缓存”。

建议按列位置建立消费工作队列，而不是对整棵计划做一次无上下文的 equal 搜索：

```text
工作项 = (产生该值的 Plan, targetlist.resno)
起点   = (已匹配的 AnnIndexScan, 距离 TLE.resno)

1. 建立 Plan 到父节点及输入侧的映射。
2. 取一个工作项，检查所有父节点对该输出位置的使用。
3. 对普通二叉节点，按 OUTER_VAR / INNER_VAR 和 varattno 识别输入侧。
4. 对 Append / Merge Append，验证子输出位置、类型和布局对应关系，
   不把“列数相等”当成位置已经匹配。
5. 若父 targetlist 只是该列的纯引用，把父 TLE.resno 加入队列。
6. 若进入排序键或最终简单输出，登记允许的消费者。
7. 若进入过滤、连接键、分组、去重、复杂表达式或未知节点，
   按首版边界拒绝近似绑定，不猜测该表达式也许单调。
8. 同一 (Plan, resno) 使用 visited 去重，审查完全部消费者才安装引用。
```

metadata 中用于打印的 original_expr 不是一个新的实际消费者，不应递归追踪出假循环。消费审查依据替换前的计划；替换后对新节点做结构校验，不重复把回退表达式当作正常取值依赖。

判断 codegen/ByPass 是否受支持时要结合实际路径；全局 enable_opfusion=on 并不证明某个复杂 UNION 计划真的进入 ByPass。无法可靠判断时保守不生成新节点并记录原因，不静默修改用户的会话开关。

### 6.3 递归引擎

- 增加新节点的初始化和求值入口。
- ExprState 绑定稳定的 AnnIndexScanState，而不是初始化时尚不存在或以后会更换的 scan descriptor。
- 正常模式读取当前通道；不要先求值 original_expr 再覆盖结果。
- fallback 原表达式独立编译；扫描开始前统一选择使用它。
- 同一行重复引用可多次读取标量，但不能再次转换或遍历向量。

### 6.4 扁平化引擎

- 增加对应 opcode、step data、初始化和解释执行入口。
- 检查 computed-goto / switch 两种 dispatch 构建方式，不能只改当前编译命中的分支。
- `get_last_attnums_walker()` 对新引用的正常路径不遍历 original_expr；fallback 单独拥有自己的属性读取初始化。
- 原表达式的依赖/参数 walker 仍要遍历 original_expr；这两种 walker 的职责不同。
- 禁止让 opcode 持有已经释放的候选或 tmpCtx 地址。

两种引擎必须调用相同的通道读取及 epoch/row/quality 检查 helper，避免两套错误处理逐渐分叉。

### 6.5 计划缓存、GUC 与失效

实际启用模式应在扫描第一条输出前锁定：

1. 比较计划策略与当前 GUC。
2. 绑定运行参数，得到本次 query epoch。
3. 检查实际索引、类型、特殊模式和返回通道能力。
4. 能复用则整个扫描遵循该契约；不能复用则在输出前选择完整原路径。
5. 已开始输出后遇到错误质量/行身份，是可观测错误，不是随意返回原表达式的借口。

Prepare/Execute 改参数不会重建所有计划对象，因此候选有效性不能只依赖计划地址。游标 fetch 不等于 rescan，不能每次 fetch 都清空正确缓存，也不能把下一次执行当成同一 epoch。

### 6.6 EXPLAIN 与计数

按用户确定的展示约定，只用 Output 双括号表示 `AnnOrderByRef`，Order By 仍描述搜索表达式：

```text
Ann Index Scan using delta_idx on delta_t
  Output: row_id, ((embedding <-> '[11,23,35,47]'::vector))
  Order By: (delta_t.embedding <-> '[11,23,35,47]'::vector)
```

不再增加 `Distance Projection/Quality/Transform` 或 `Order By Value Source` 等诊断行。内部计数保留用于调试，不混入普通计划。

普通 EXPLAIN 的双括号由新节点反解析产生，表示计划中的内部引用；GUC 降级及 L2 行身份校验仍可能触发受控回退，所以不能仅凭括号证明每一行都消费了 AM 值。必须结合实际结果及内部调试证据，不恢复已取消的诊断字段。

至少维护完整向量调用、输出转换、引用读取、对应投影 fallback 调用四组计数。计数按 ANN 节点归属，所有累加点说明清楚；AM 先返回再被 WHERE 过滤的候选可发生一次廉价转换，不能拿最终 LIMIT 行数直接要求所有计数等于 K。

## 7. 全量构建与二进制核验

本特性会修改头文件和结构布局。工作区没有可靠的自动头文件依赖重编，因此第一次及头文件修改后必须使用统一 full 脚本。

以下命令会安装并重启指定实例，只在后续获准开发、完成环境核对后执行：

```bash
ANN_SRC=/home/qiumc/data/code/cpp/openGauss-server
ANN_INSTALL=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install
ANN_DATA=/home/qiumc/data/code/cpp/openGauss-server/topk_test_data
ANN_BUILD=/home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh
CODE_BASE="$ANN_SRC" GAUSSHOME="$ANN_INSTALL" PGDATA="$ANN_DATA" PGPORT=55440 JOBS=4 bash "$ANN_BUILD" full
```

仅改 .cpp 且配置未变时，后续才可选择 incremental。不得手工另造 configure 参数，不得把 Debug/O0 时延作为性能结论。

构建后核对：

```bash
git -C "$ANN_SRC" rev-parse HEAD
git -C "$ANN_SRC" status --short
sha256sum "$ANN_INSTALL/bin/gaussdb"
ps -eo pid,comm,args | rg '[g]aussdb'
env LD_LIBRARY_PATH="$ANN_INSTALL/lib" "$ANN_INSTALL/bin/gsql" -X -h /tmp -p 55440 -d postgres -c "SELECT version(); SHOW ann_orderby_distance_reuse;"
```

同时记录构建日志路径、运行二进制路径/哈希、dirty 特性 diff 和 SHOW 结果。version() 的 commit 文本相同不代表二进制相同。

如果实例原本运行，后续测试结束应按用户工作需要恢复；如果专门启动了隔离实例，结束后停止它但保留需要的证据。不能停止无关 PG19/Oracle/Milvus 服务来做这个功能测试。

## 8. 原生回归框架接入

拟新增的基础文件：

```text
src/test/regress/sql/ann_orderby_distance_reuse.sql
src/test/regress/expected/ann_orderby_distance_reuse.out
src/test/regress/sql/ann_orderby_distance_reuse_safety.sql
src/test/regress/expected/ann_orderby_distance_reuse_safety.out
src/test/regress/parallel_schedule.ann_orderby_distance_reuse
```

schedule 示意：

```text
test: ann_orderby_distance_reuse
test: ann_orderby_distance_reuse_safety
```

在 GNUmakefile 增加 `ann_orderby_distance_reuse_check`，参照已有 `union_all_required_columns_check` 使用原生 pg_regress_check、single_node、现有测试配置和表空间设置，不另造一套只有 Python 断言的替代框架。

测试要求：

- 新用例应在隔离测试库建立自己的 schema；不复用或 DROP 用户手动测试 schema。
- 本文提供的是用例骨架；正式 SQL 必须补齐第 10 节矩阵并由真实 pg_regress 生成/核对 expected。
- 不手写一份“看起来正确”的计划输出作为已通过 golden；不把首次执行结果不经审查直接覆盖 expected。
- 加入主 schedule 的位置需遵循分支规范，避免已有用例创建的 test 数据库和新用例互相污染。
- 联跑现有 diskann_vector、union_all_required_columns、union_all_required_columns_safety；必要时增加 setop、reduce_orderby 等受影响用例。

专用目标运行示例，目标和 schedule 在开发完成后才存在：

```bash
ANN_TMP=$(mktemp -d /tmp/ann-distance-reuse-regress.XXXXXX)
ANN_GCCLIB=/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3/gcc/lib64
env GAUSSHOME="$ANN_INSTALL" SSL_LIB_PATH="$ANN_INSTALL/lib" LD_LIBRARY_PATH="$ANN_INSTALL/lib:$ANN_GCCLIB" make -C "$ANN_SRC/src/test/regress" ann_orderby_distance_reuse_check p=55450 dir="$ANN_TMP/runtime" EXTRA_REGRESS_OPTS="--temp-install=$ANN_TMP/install --outputdir=$ANN_TMP/output"
```

执行前先用 make -n 检查最终展开命令，确认临时目录和端口没有被默认参数覆盖；目标分支的 pg_regress 参数若不同，按实际 --help 调整，不盲目复制。

55440 附近有内部端口占用，隔离测试使用 55450 并先检查相邻监听。不要把临时测试的 temp-install 或 dir 指向真实数据目录、源码根目录或安装根目录。

保留退出码、回归日志、results 和 diff。命令通过 tee 保存日志时必须启用 pipefail，不能只取得 tee 的成功状态。

## 9. 小数据 SQL 骨架

以下 SQL 是未来原生用例和人工验证的起点，当前由于 GUC 尚未开发不能直接全部执行。只在空的隔离测试数据库使用；若 schema 已存在，应停止检查，不能自动 DROP CASCADE 重建。

### 9.1 初始化：每表 8 行、4 维、两个 DiskANN 距离索引

```sql
CREATE SCHEMA ann_distance_reuse_test;
SET search_path = ann_distance_reuse_test, public;

CREATE TABLE d_l2 (row_id int PRIMARY KEY, title text, embedding vector(4));
CREATE TABLE d_cos (row_id int PRIMARY KEY, title text, embedding vector(4));

INSERT INTO d_l2 VALUES
 (1, 'r1', '[0.1,0.2,0.3,12.8]'),
 (2, NULL, '[0.2,0.2,0.3,12.8]'),
 (3, 'r3', '[0.3,0.2,0.3,12.8]'),
 (4, 'r4', '[0.4,0.2,0.3,12.8]'),
 (5, 'r5', '[0.5,0.2,0.3,12.8]'),
 (6, 'r6', '[0.6,0.2,0.3,12.8]'),
 (7, 'r7', '[0.7,0.2,0.3,12.8]'),
 (8, 'r8', '[0.8,0.2,0.3,12.8]');
INSERT INTO d_cos SELECT * FROM d_l2;

CREATE INDEX d_l2_ann ON d_l2 USING diskann (embedding vector_l2_ops);
CREATE INDEX d_cos_ann ON d_cos USING diskann (embedding vector_cosine_ops);

ANALYZE d_l2;
ANALYZE d_cos;

SET enable_seqscan = off;
SET enable_codegen = off;
SET enable_opfusion = off;
```

每表只有一个距离索引，减少误选同表其他距离 opclass 的风险。即使设置 enable_seqscan=off，也必须核对真实计划，不把 GUC 当成已选 ANN 的证明。

### 9.2 L2 计划与同一返回行的位级校验

```sql
SET ann_orderby_distance_reuse = 'off';
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id, embedding <-> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_l2
WHERE title IS NOT NULL
ORDER BY d LIMIT 4;

CREATE TEMP TABLE d_l2_off AS
SELECT row_id, embedding <-> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_l2
WHERE title IS NOT NULL
ORDER BY d LIMIT 4;

SET ann_orderby_distance_reuse = 'l2';
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id, embedding <-> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_l2
WHERE title IS NOT NULL
ORDER BY d LIMIT 4;

CREATE TEMP TABLE d_l2_on AS
SELECT row_id, embedding <-> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_l2
WHERE title IS NOT NULL
ORDER BY d LIMIT 4;

-- 校验查询不使用 AM 结果引用，直接计算原始 SQL 表达式。
SET ann_orderby_distance_reuse = 'off';
SELECT count(*) AS returned_rows FROM d_l2_on;
SELECT count(*) AS wrong_l2_values
FROM d_l2_on r JOIN d_l2 t ON t.row_id = r.row_id
WHERE float8send(r.d) IS DISTINCT FROM
      float8send(t.embedding <-> '[0.1,0.2,0.3,12.8]'::vector);

SELECT count(*) AS off_on_differences
FROM (
    (SELECT row_id, d FROM d_l2_off EXCEPT ALL SELECT row_id, d FROM d_l2_on)
    UNION ALL
    (SELECT row_id, d FROM d_l2_on EXCEPT ALL SELECT row_id, d FROM d_l2_off)
) s;
```

这组数据应返回 4 行，数值差异为 0。正式验收还必须确认 on 查询实际命中新引用、投影 fallback 计数为 0；否则“结果相同”可能只是根本没有开启优化。

还要在 d_l2 覆盖只输出 row_id、距离作为隐藏排序列的形态，确认 resjunk 距离同样读取 DiskANN 返回值；每种形态均检查实际计划和运行计数。

CTAS 用于固化返回行和值，避免校验查询重写原 ANN 路径；它不记录 AM 的实际输出时序。有序性仍需原始 SELECT 输出和对应计划/运行诊断证据，不能仅靠临时表里再 ORDER BY 得出结论。

### 9.3 Cosine 近似转换口径和误差校验

```sql
SET ann_orderby_distance_reuse = 'l2';
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id, embedding <=> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_cos WHERE title IS NOT NULL ORDER BY d LIMIT 4;

SET ann_orderby_distance_reuse = 'all';
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id, embedding <=> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_cos WHERE title IS NOT NULL ORDER BY d LIMIT 4;

CREATE TEMP TABLE d_cos_approx AS
SELECT row_id, embedding <=> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_cos WHERE title IS NOT NULL ORDER BY d LIMIT 4;

SET ann_orderby_distance_reuse = 'off';
SELECT count(*) AS returned_rows FROM d_cos_approx;

-- 当前普通 vector 的归一化/负内积函数仅作该路径的转换参考。
SELECT count(*) AS wrong_conversion
FROM d_cos_approx r JOIN d_cos t ON t.row_id = r.row_id
WHERE float8send(r.d) IS DISTINCT FROM float8send(
    least(2.0::float8, greatest(0.0::float8,
        1.0::float8 + vector_negative_inner_product(
            l2_normalize(t.embedding),
            l2_normalize('[0.1,0.2,0.3,12.8]'::vector)))));

SELECT bool_and(r.d >= 0 AND r.d <= 2) AS in_cosine_range,
       bool_and(abs(r.d - (t.embedding <=> '[0.1,0.2,0.3,12.8]'::vector))
                <= 0.00001::float8) AS this_fixture_error_ok
FROM d_cos_approx r JOIN d_cos t ON t.row_id = r.row_id;
```

`0.00001` 仅是本组 4 维、有限幅值数据的回归阈值，不是全局误差承诺。wrong_conversion 应验证 AM 实际实现的归一化/点积核；如目标分支 SIMD 或归一化核不同，应先说明差异，不能为了通过测试随意放宽口径。

补充 d_cos 正常数据中 off 与 all 距离位模式确实不同的例子，并配合计数证明没有回退成 SQL 精确计算。不要要求近似 Cosine 的 LIMIT 行集在所有近距离案例下都与 off 相同。

### 9.4 精确 WHERE 不被替换

```sql
SET ann_orderby_distance_reuse = 'all';
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id, embedding <=> '[0.1,0.2,0.3,12.8]'::vector AS d
FROM d_cos
WHERE title IS NOT NULL
  AND embedding <=> '[0.1,0.2,0.3,12.8]'::vector < 0.001
ORDER BY d LIMIT 4;
```

按首版保守门，这种距离同时承担精确过滤的形态应拒绝近似绑定或保持可证明的精确过滤路径；正式 golden 固定所选策略，必须确认 Filter 中没有 AnnOrderByRef。

边界测试应选择距离非常靠近阈值的行，验证没有因近似误差跨过 WHERE 边界。仅测试全表都通过的宽松阈值不足以看护。

### 9.5 参数、重扫、模式切换

```sql
SET ann_orderby_distance_reuse = 'l2';
PREPARE ann_l2_p(vector) AS
SELECT row_id, embedding <-> $1 AS d
FROM d_l2 WHERE title IS NOT NULL ORDER BY d LIMIT 4;
EXECUTE ann_l2_p('[0.1,0.2,0.3,12.8]'::vector);
EXECUTE ann_l2_p('[0.8,0.2,0.3,12.8]'::vector);
SET ann_orderby_distance_reuse = 'off';
EXECUTE ann_l2_p('[0.1,0.2,0.3,12.8]'::vector);
DEALLOCATE ann_l2_p;

SET ann_orderby_distance_reuse = 'all';
PREPARE ann_cos_p(vector) AS
SELECT row_id, embedding <=> $1 AS d
FROM d_cos WHERE title IS NOT NULL ORDER BY d LIMIT 4;
EXECUTE ann_cos_p('[0.1,0.2,0.3,12.8]'::vector);
SET ann_orderby_distance_reuse = 'l2';
EXECUTE ann_cos_p('[0.1,0.2,0.3,12.8]'::vector);
SET ann_orderby_distance_reuse = 'off';
EXECUTE ann_cos_p('[0.1,0.2,0.3,12.8]'::vector);
DEALLOCATE ann_cos_p;
```

还需覆盖真正触发 executor rescan 的查询形态；如果首版能力门拒绝该形态，expected 应明确看护拒绝，而不是假装 PREPARE 两次执行等于所有 rescan 场景已经覆盖。

NULL 参数、LIMIT 0、相同向量多个 TID 单独建用例。先确认基线 AM 对 NULL/零向量的既有行为，不能把旧索引覆盖边界误算成新特性承诺。

### 9.6 UNION ALL + Anti Join 小数据场景

```sql
CREATE TABLE snapshot_t (row_id int PRIMARY KEY, title text, embedding vector(4));
CREATE TABLE delete_t (row_id int PRIMARY KEY);
INSERT INTO snapshot_t VALUES
 (101, 's1', '[0.15,0.2,0.3,12.8]'),
 (102, 's2', '[0.25,0.2,0.3,12.8]'),
 (103, 's3', '[0.35,0.2,0.3,12.8]'),
 (104, NULL, '[0.45,0.2,0.3,12.8]');
INSERT INTO delete_t VALUES (102);
ANALYZE snapshot_t;
ANALYZE delete_t;

CREATE VIEW union_view AS
SELECT row_id, title, embedding FROM d_l2
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS (SELECT 1 FROM delete_t d WHERE d.row_id = s.row_id);

SET enable_union_all_diskann_topk_pushdown = on;
SET enable_union_all_required_columns_pushdown = on;
SET ann_orderby_distance_reuse = 'l2';
EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM union_view
WHERE title IS NOT NULL
ORDER BY embedding <-> '[0.1,0.2,0.3,12.8]'::vector
LIMIT 4;
```

需验证：只 ANN 分支的投影命中引用；snapshot 的普通扫描/排序仍计算自身距离；删除行和 NULL title 不出现；两分支比较的都是开方后 L2，不混入平方距离。

若实际未出现预期 ANN 或 Merge Append，先查能力拒绝原因、过滤下推与原始 CBO 选择，不直接编辑 expected 把缺失覆盖当成成功。

此小数据只能验证单批基本链路。另建几十到几百行、较强 Post-Filter 的独立用例迫使续搜，不能让“数据小”变成漏测扩批。

### 9.7 双引擎与清理

对上述核心场景分别设置 enable_expr_fusion=off/on，并核对真实执行采用的引擎。若目标分支该配置不可会话修改，使用两个隔离配置实例，不以 SHOW 值替代实际执行证明。

结束正式用例时 RESET 所有本用例设置的 GUC；测试对象只在隔离 schema 内清理。pg_regress 临时实例由框架收尾；不要复制本段到用户业务库后直接 DROP schema。

## 10. 完整回归矩阵与验收断言

| 编号 | 场景 | 必须观察到的事实 |
|---|---|---|
| V01 | DiskANN + L2，off/exact | 返回值位级一致；exact 实际使用新引用 |
| V02 | DiskANN + L2，相等向量、不同符号及大/小幅值 | 同一返回行位级核对，覆盖零距离和 double 开方精度；特殊值按基线语义处理 |
| V03 | DiskANN + Cosine，exact | 不启用近似返回 |
| V04 | DiskANN + Cosine，allow_approx | 转换符合指定口径；模式显示近似 |
| V05 | SELECT 只取 row_id，距离为 resjunk | 隐藏排序列仍正确，投影不额外取向量求距离 |
| V06 | SELECT 同时取 embedding | 向量仍输出，不能错误裁剪 |
| V07 | 相同距离两个输出、不同查询向量两个表达式 | 同一绑定共享；不同表达式不串值 |
| V08 | title 过滤、过滤全空、过滤引发续扫 | 未通过 WHERE 的行不输出，缓存未跨行污染 |
| V09 | 精确距离阈值过滤、Join/Group/Distinct 消费 | 不把近似值用于精确关系运算 |
| V10 | LIMIT 0、无 LIMIT、有 OFFSET 的可支持计划 | 未执行时零计数；不借本特性增加 Top-K 下推 |
| V11 | PREPARE、参数改变、真实 rescan、游标 | epoch/row 正确，旧值不复用 |
| V12 | allow_approx→exact/off 的缓存计划 | 不能继续静默执行旧近似策略 |
| V13 | UPDATE/DELETE/VACUUM、HOT 链、重复向量多个 TID | 值与可见返回行关联正确，不混淆根 TID 与可见 HOT TID |
| V14 | DiskANN 首批、扩批重复候选、同节点多 TID | 所有支持出口走转换/发布；同一有效候选不重复转换，每个返回事件独立更新 |
| V15 | DiskANN 多轮、强过滤 | 记录真实扩批；不把首批有序冒充全流有序 |
| V16 | UNION ALL ANN+普通分支、Anti Join | 单位一致、ID 正确、隐藏列和删除语义正确 |
| V17 | 两种表达式引擎 | 功能、fallback、计数一致 |
| V18 | copy/read/write、旧版本 | 无未知节点或丢失策略字段 |
| V19 | DiskANN PQ/mmap/UStore/Index Only/远程等未支持路径 | 能力门拒绝，不读取错误来源的值 |
| V20 | 新开关 off 的现有用例联跑、共用模块 AM 隔离 | 不出现无关计划/语义回归；打开开关时非 DiskANN 也不生成引用 |

不能把“返回 0 个差异”单独当通过：同时检查返回行数、实际路径、复用计数，避免空结果或静默回退带来的假通过。

## 11. 有序性和结果正确性如何判定

### 11.1 L2

在不改变搜索算法、候选预算、过滤和其他环境的前提下，on/off 应保持候选及输出值；小数据无 tie 时比较有序 ID。同一返回 ID 的距离以 float8send 作精确参考。

ANN 与 Seq Scan 的全表精确 Top-K 对比属于召回检查，不应在所有数据上要求完全相同。检查距离值时，以 AM 实际返回的同一行原始向量作为参照。

### 11.2 Cosine

分别验证：

1. 导出值符合指定的近似公式。
2. 对原始 SQL Cosine 的误差在所选数据集要求内。
3. 结果序列按实际导出的距离有序，或由计划中正确位置的 Sort 保证。
4. 过滤条件、删除判断等精确逻辑不受近似替换影响。

不能把 Cosine on/off 的微小值差异都当 bug，也不能把所有行集变化都解释为“允许近似”。列号错位、旧参数值、平方/负内积混入、漏过滤、无序输入被 Merge Append 消费，都不是合理误差。

### 11.3 扩批证据

使用原始查询输出顺序和运行计数/调试日志验证 AM 流。不要包一层额外 ORDER BY 再断言有序；不要在测试包装中引入 Window/CTE 导致新引用回退后，还声称测到了它。

源码已知 DiskANN 严格合批有特定 in-filter 条件，Post-Filter 不应直接套用该有序性结论。若 P0/P6 发现问题，记录最小 SQL、前后原始输出及计划，明确属于既有边界还是新改动，再决定是否申请扩展修复范围。

## 12. 并发与资源检查

在隔离数据库用两会话测试，不在现有业务对象上执行：

1. A 在只读一致快照中分批读取候选。
2. B 更新向量/标题、删除部分行、插入重复向量并提交。
3. 按场景执行 VACUUM，再让 A 继续取数。
4. 检查输出对应 A 的可见版本，缓存没有指向已释放或复用节点。
5. A 新开一轮查询/参数后，只能使用新 epoch 的结果。

MVCC/游标行为因隔离级别而异，必须记录隔离级别和基线，不把不同快照的结果硬作相等比较。普通 VACUUM 不保证一定复用指定索引槽位；需结合已有故障注入或调试观察确认真正覆盖了回收路径。

资源看护包括：scan/rescan 多次循环内存稳定、endscan 不 double-free、LIMIT 提前结束释放、查询取消、错误退出后下一次查询正常。不要求无限缓存所有访问节点；内存上界必须随当前候选和扫描预算受控。

## 13. 性能验证与允许的结论

先证明省掉的是什么：

- AM 完整内部距离评估次数是否减少，减少发生在哪个已审计分支。
- 输出转换是否为每个有效候选一次 O(1) 工作，而非一次完整向量计算。
- 对应 ANN 投影的原距离函数调用是否为零。
- 若声称避免 TOAST，是否确认该向量确实外部存储、且没有其他消费者。

再测性能：选择有代表性的维度，先小数据正确性，性能需要时才扩到合适数据量。使用 Release/O2 和同一实例的 off/on 交替测量，记录查询计划、参数、返回行/误差、缓存状态、CPU 和 I/O。

本特性不是 HybridRecall/Milvus 对标，不主动启动其他服务。现有 Release 脚本可作为统一构建入口，但必须显式指定本工作树/安装树/数据目录，不能意外切到纯 HybridRecall 分支。

不承诺整体免回表：row_id、title 和 MVCC 仍可能要求堆访问；四维小向量的总时延收益也可能很小。只减少向量投影 CPU 时，应如实报告，不把它写成磁盘读取下降。

## 14. 常见失败与定位顺序

| 现象 | 优先检查 |
|---|---|
| 计划出现新引用，但距离函数计数不降 | original_expr 被提前初始化/求值；另一份相同投影未绑定；实际回退 |
| L2 与 SQL 相差明显 | 把平方距离直接当值、sqrtf 精度、错误来源/节点/参数 |
| Cosine 差约 1 或出现负值 | 忘记加 1、混用 IP 与 Cosine、绕过转换/范围规则 |
| 首次正常，第二次错 | rescan epoch、prepared 参数、候选 output_ready 未失效 |
| 单次取数正常，过滤/续扫后错 | row_token/TID 未更新、遗漏 DiskANN 扩批/候选替换/多 TID 发布出口 |
| 递归通过，扁平化失败 | 新 opcode / dispatch / LastAttnum walker 未覆盖 |
| 查询执行正常，EXPLAIN 报错 | Explain-only 初始化错误、新节点反解析/类型查询缺失 |
| 计划缓存或拷贝后崩溃 | copy/read/write/版本门缺失、序列化了运行态指针 |
| Merge Append 错序 | 分支流并非严格有序；不能用 extra 括号解释 |
| 禁用 GUC 后仍显示/返回近似 | 计划环境未失效或执行开始没有重新验证 |
| SELECT embedding 消失或 WHERE 失效 | 把原表达式依赖全局隐藏、过度裁剪或近似值传播越界 |

排查过程中保留原始 SQL、GUC、版本、计划和结果，不先改 expected 掩盖问题。

## 15. 交付、补丁与报告

最终交付应包含：

- 内核改动和所有新增源文件/头文件/构建清单。
- SQL、expected、schedule、GNUmakefile 专用入口。
- 更新后的两份设计/实施文档，明确已实现、已验证、未支持、遗留风险。
- 基线和新二进制标识、构建日志、原生回归结果、排序和性能证据。
- 特性专用 git patch 及实际分项行数；用户未要求时不自动 commit/push。

文档中的源码链接和测试链接必须在实际路径上存在。当前用户 dirty 改动与本特性混合时，必须分离对应 hunks 或使用用户认可的清晰基线，不能导出整个工作树 diff 冒充本特性补丁。

建议最后报告采用以下结构：

```text
实现范围：仅 DiskANN / L2 与近似 Cosine / 支持的执行、扩批和过滤模式
语义：L2 exact；Cosine 显式 approximate；不影响精确 WHERE
构建：脚本、模式、日志、二进制哈希
测试：原生用例数量、通过/失败、关键结果与计数
排序：哪些路径已证明有序，哪些保守拒绝
性能：测试条件、实际收益或未测
代码量：内核 / SQL / expected / 文档，按实际 diff
未完成项：明确列出，不以设计预期替代
文件：实际源码、报告、patch 路径
```

### 15.1 停用与回滚

1. 首先将新请求的 ann_orderby_distance_reuse 设为 off，验证新计划不含引用节点；已开始的扫描保持其锁定策略，不在中途改变结果来源。
2. 处理现存 prepared/cached plans：按实现的兼容机制重新规划或在新执行开始使用受控原路径，必要时结束相关测试会话，不把旧引用交给不认识它的执行器。
3. 若要回退二进制，先确认旧版本不含新 NodeTag，并通过正常重启清除新版本的内存计划；不能热换成结构布局不兼容的旧对象文件。
4. 头文件/结构改动的源码回滚同样需完整一致的构建。选定用户认可的恢复点并使用统一脚本，不执行破坏性 reset 来抹掉无关修改。
5. 如果新 GUC 曾写进持久配置，旧二进制启动前需按正常配置管理流程移除或恢复对应配置；仅会话 SET 的测试不会留下这个配置问题。
6. 本设计不改索引页格式，因此仅回滚该功能不要求重建或删除索引。现有用户数据、关闭 INCLUDE 的状态以及其他特性改动均保留。

回滚后的验收仍包含旧用例、连接/版本核验及服务状态记录，不以“进程启动了”代替 SQL 正常工作。

## 16. 最终自检清单

- [ ] 已按详细设计的三层准入分别给出结论。
- [ ] L2 与原 SQL 位级校验通过，Cosine 按近似口径验收。
- [ ] 支持路径里距离投影不再回表取 embedding 来重复计算。
- [ ] 没有错误宣称整个 SQL 不回表或自动消除所有 I/O。
- [ ] 搜索分数未被输出转换覆盖，必要量化精排未被误删。
- [ ] DiskANN 首批、扩批、多 TID、重扫及两套表达式引擎均有看护。
- [ ] 规划和执行按实际 DiskANN AM 准入，未改动 HNSW 适配代码或配置。
- [ ] WHERE、Anti Join、上层列号、resjunk 和精确消费者未被破坏。
- [ ] GUC、prepared plan、rescan、并发变化、版本门正确。
- [ ] 没有把近似值送入标准 lower-bound 重排接口。
- [ ] 没有靠双括号变化、手写 golden 或空结果断言证明成功。
- [ ] 构建使用正确工作树且头文件变更做过 full。
- [ ] 本特性 patch 未混入用户原有 dirty 内容。
- [ ] 服务状态、临时资源和用户数据均按任务边界收尾。

回到[详细开发设计](ANN距离结果复用详细开发设计.md)或[设计总入口](README.md)。
