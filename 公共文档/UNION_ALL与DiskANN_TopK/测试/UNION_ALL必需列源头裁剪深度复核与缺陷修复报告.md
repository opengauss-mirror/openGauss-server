# UNION ALL 必需列源头裁剪深度复核与缺陷修复报告

## 1. 范围与状态

复核对象是 `/home/qiumc/data/code/cpp/openGauss-server`、`integration/multimodal-stacked` 分支，初始提交为 `098d77c8b76d56eace4af8fddaf8d94b394b996c`。本轮同时检查新列裁剪改写与其调用链上的既有 DiskANN Top-K 下推。

已在修复前实例复现并修正六个缺陷：三个来自新列裁剪/排序键阶段，三个来自既有 Top-K 的安全检查。复现主体仅用两张表共 5 行，无需大规模数据、ANN 索引或性能测试。Debug/O0 + assertion 重建和原生回归均已完成；最终联跑 5/5 通过，详见 §5。本轮修复尚未 git commit，不能把 HEAD 的版本号视为已包含这些工作树改动。

修复前实际二进制来自 `mppdb_temp_install/bin/gaussdb`，端口 `55440`。它能识别并运行新 GUC，但版本字符串为 `build cd2f005e`、不含 debug，`config.log` 也显示 `-O2`、`enable_debug=no`、`enable_cassert=no`；不能把旧增量构建记录解释成 Debug 验证。本轮按工作区脚本从 clean/configure 开始重建 Debug/O0 + assertion。

原有 `contrib/shark/smartmatch.pl` 和 `docs/diskann_include/evidence/P2-4/LG.md` dirty 修改不属于本次修复。

## 2. 六个确认的缺陷

| 编号 | 来源 | 触发条件 | 修复前实际结果 | 正确结果 |
|---|---|---|---|---|
| B1 | 排序键物化 | 外层有未下推 volatile Filter，叶子有 volatile 输出，ORDER 表达式对被过滤行不合法 | `division by zero` | 返回 `4,3,2,1` |
| B2 | planner 状态继承 | 已裁剪父查询的叶子含排序聚合标量子查询 | `Aggref found in non-Agg plan node` | `(1,3),(2,0)` |
| B3 | WHERE 下推 | UNION 叶子输出 ROWNUM，外层 WHERE 排除前序输入 | branch_a 的编号 `2,3` 被改成 `1,2` | 编号保持 `2,3` |
| B4 | 既有 Top-K | UNION 容器自身 `ORDER BY row_id LIMIT 2`，外层另按距离 Top-1 | 返回 `row_id=3` | 返回 `row_id=1` |
| B5 | 既有 Top-K | 外层 SRF 对最近输入行输出零行，再做 LIMIT | 返回 0 行 | 返回 `(row_id=1,n=1)` |
| B6 | 既有 Top-K | `FETCH FIRST 1 ROWS WITH TIES`，两叶共 5 行等距 | 只返回 2 行 | 返回全部 5 行 |

这些都属于正确性问题；B1/B2 是正常查询报错，B3～B6 是静默错结果。未把未复现的推测列为已确认缺陷。

### 2.1 B1：immutable 排序表达式也不能随意越过 Filter

叶子形状为：

```sql
SELECT row_id, title, embedding, random() AS side_effect FROM branch_a
UNION ALL
SELECT row_id, title, embedding, random() AS side_effect FROM branch_b;
```

`nonzero(int)` 是返回 `$1 <> 0` 的 PL/pgSQL VOLATILE 函数。查询为：

```sql
SELECT row_id FROM volatile_rows
WHERE nonzero(row_id)
ORDER BY 100 / row_id;
```

关闭新 GUC 时，外层过滤先排除 0，再计算排序键，返回四行。开启时，阶段一保留残留 WHERE，但阶段三仍把 `100 / row_id` 放进叶子 targetlist；叶子有 volatile 输出，后续通用 planner 无法将外层 volatile Filter 移入叶子。结果是在过滤之前对 0 做除法。

修复：阶段三发现外层 `jointree->quals` 非空就跳过。阶段一仍可裁掉完全无关的 title/vector，保留过滤列；只是排序表达式必须在原来的过滤之后计算。

规避要点：`contain_volatile_functions(expr)==false` 只约束副作用/稳定性，不证明表达式对所有输入都无异常。不能据此任意扩大其求值行集。

### 2.2 B2：可继承的扫描策略不等于当前 Query 已通过安全门

最小形状：

```sql
SELECT id, total
FROM
(
  SELECT 1 AS id,
         (SELECT sum(n)
          FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) q
          ORDER BY sum(n) LIMIT 1) AS total,
         'unused'::text AS title
  UNION ALL
  SELECT 2, 0::bigint, 'unused'::text
) u
WHERE id > 0 ORDER BY id;
```

阶段一正确拒绝了有聚合的标量子查询，但它继承父层的 `union_all_required_columns_pruned=true`。原阶段三仅依赖该 flag，误以为当前 Query 也已经通过安全检查，将 `sum(n)` 映射进 UNION 叶子，破坏 Aggref 所属查询层。

修复使用两个不同含义的状态：

```cpp
// 保持可继承：后代源扫描不能恢复宽 physical targetlist。
root->union_all_required_columns_pruned =
    parent_root != NULL && parent_root->union_all_required_columns_pruned;

// 仅本次 subquery_planner 调用有效：本层是否成功改写。
bool unionAllColumnsRewritten = preprocess_union_all_required_columns(root);
preprocess_union_all_diskann_topk(root);
if (unionAllColumnsRewritten)
    preprocess_union_all_order_key_projection(root);
```

`preprocess_union_all_required_columns()` 从 `void` 改为 `bool`：全部失败路径返回 false，成功安装 Query 字段和设置扫描策略后返回 true。没有增加 Plan/Executor 字段，也没有新增需要序列化的状态。

规避要点：父查询授予的策略标志不能充当子查询自身的安全资格。聚合只是最小复现，同一漏洞也会波及其他本应被外层结构门拒绝的子 Query。

### 2.3 B3：ROWNUM 不会被普通 volatile 检查完整覆盖

```sql
SELECT rn, row_id
FROM
(
  SELECT rownum AS rn, row_id, title, embedding FROM branch_a
  UNION ALL
  SELECT rownum AS rn, row_id, title, embedding FROM branch_b
) u
WHERE row_id > 0 ORDER BY row_id;
```

branch_a 的物理输入顺序为 0、1、3。关闭 GUC 时，row_id 1、3 的 ROWNUM 是 2、3；开启后 WHERE 被放入叶子，编号变成 1、2。双向 `EXCEPT ALL` 有 4 条差异。

修复：外层和叶子安全检查增加 `expression_contains_rownum(targetList)` 与 `ContainRownumQual(query)`。包含 ROWNUM 的查询块整体跳过该项改写。既有 Top-K 入口也使用相同的保护。

规避要点：本分支 ROWNUM 有独立节点和求值语义；只保留其输出列、或只检查 volatile 函数，并不能保护编号。

### 2.4 B4：必须检查 UNION 容器自身的 LIMIT

branch_a 的有效候选是 `(id=1,distance=10)`、`(id=3,distance=1)`；branch_b 是 `(id=2,distance=20)`、`(id=4,distance=2)`。

```sql
SELECT row_id
FROM
(
  SELECT * FROM branch_a WHERE row_id > 0
  UNION ALL
  SELECT * FROM branch_b
  ORDER BY row_id LIMIT 2
) u
WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
```

正确执行先在 UNION 容器中取 id 1、2，外层选出 id 1。原 Top-K 仅检查叶子没有 LIMIT，直接让叶子各取距离 Top-1，候选变成 3、4，最终返回 3。

修复：Top-K 收集叶子之后、创建改写副本之前，复用容器和叶子的 shape 校验。UNION 容器已有 LIMIT/OFFSET、排序等边界时保守回退；同步检查 UNION RTE 和叶子 RTE 的 barrier/lateral 条件。

规避要点：只检查 outer 与 leaf 两端会漏掉中间 UNION Query。每个 Query 层都可能拥有独立的截断语义。

### 2.5 B5：SRF 的输入 Top-K 不等于输出 Top-K

```sql
SELECT row_id,
       generate_series(1, CASE WHEN row_id < 3 THEN 1 ELSE 0 END) AS n
FROM all_rows WHERE row_id > 0
ORDER BY embedding <-> '[0,0,0,0]' LIMIT 1;
```

最近的 id 3、4 均不产生输出行；正确查询继续读取并返回 id 1。原 Top-K 每叶只保留一个输入，外层 SRF 处理后剩 0 行。

修复：Top-K 外层安全门检查 `hasTargetSRFs` 并直接执行 `expression_returns_set(targetList)`，同时补齐 recursive/modifying CTE/RLS 等已有查询语义标志。required-column 外层门和共用叶子门也直接扫描 targetlist。

第一轮仅补 `hasTargetSRFs` 后仍实测失败，进一步定位到 `parse_func.cpp`：`check_srf_call_placement()` 只在 `retset && pstate && pstate->p_is_flt_frame` 时调用。因此 `enable_expr_fusion=off` 的旧表达式框架下，该 flag 可能为 false，但 targetlist 确实存在 SRF。通用 `is_simple_subquery()` 同样使用直接 `expression_returns_set()` 检查，本修复遵循该分支的现有安全做法。

规避要点：证明分支 Top-K 等价时必须保证跨越的投影保持行数。SRF 能扩行也能消行，不能只认为它“每行产生更多行”。

### 2.6 B6：WITH TIES 不能下推成普通 LIMIT

把两张表共五行的 embedding 都设成同一距离：

```sql
SELECT row_id FROM tied_rows WHERE row_id >= 0
ORDER BY embedding <-> '[0,0,0,0]'
FETCH FIRST 1 ROWS WITH TIES;
```

正确返回 5 行。原外层资格检查虽有注释说拒绝 WITH TIES，但未读取该字段；随后 `prepare_leaf_diskann_topk()` 将每叶设为普通 LIMIT 1，且主动设 `limitWithTies=false`。全局 WITH TIES 无法找回被局部丢弃的三行。

修复：外层 `limitWithTies` 或 `limitIsPercent` 为真时跳过 Top-K。WITH TIES 有实际错结果复现；PERCENT 是同类语义保护，本轮 102 行探针没有产生 on/off 差异，不将其单独计为确认 bug。

## 3. 修复范围

内核变更：

- `src/gausskernel/optimizer/prep/preptopk.cpp`：阶段一 bool 返回、残留 Filter 求值保护、ROWNUM 检查、Top-K 容器/外层安全门；
- `src/gausskernel/optimizer/plan/planner.cpp`：本层 bool 控制阶段三，与继承扫描策略分离；
- `src/include/optimizer/prep.h`：更新返回类型。

回归变更：

- 新增 `sql/union_all_required_columns_safety.sql` 及对应 expected；
- 纳入专用 schedule 和 `parallel_schedule0`；
- 保留原 `union_all_required_columns.sql` 的正常列裁剪、反删除、ANN 和四类目标计划看护；显式固定 Merge Join 开关，并补独立 Hash Anti Join 计划看护，更新经审阅的 expected。

文档同时修正设计、源码解读和迁移指南中的状态含义、残留过滤条件、ROWNUM 与 Top-K 边界说明。

相对 `098d77c8b76d`，内核三个文件为 **+61/-23 行**；测试及 schedule 为 **+852/-58 行**，其中新安全 SQL 268 行、expected 498 行。该统计不含公共文档、构建产物和用户无关修改。

## 4. 测试设计与修复前证据

新安全回归保存完整 EXPLAIN VERBOSE，并看护：

- B1：除法位于 Subquery Filter 之后；
- B2：聚合子查询仍由 Aggregate 计算；
- B3：显式编号结果与重复敏感差分；
- B4：UNION 容器 LIMIT 保留，无错误新增的叶子 LIMIT；
- B5：预期 id 1、n 1；
- B6：五个等距 id 全部保留；
- GUC 四组合：两者 off、仅裁列 on、仅 Top-K on、两者 on；
- 14 类普通 SQL × 3 个优化组合对基线，共 42 次差分，检查显式 alias、重排列、常量输出、重复分支、升降序、多排序键、typmod 变化、OFFSET 和空结果；
- `plan_cache_mode=force_generic_plan` 下的 WHERE/vector/LIMIT Param，含 LIMIT 0。
- `enable_expr_fusion=off/on` 两种表达式框架，以及 UNION 叶子中的 SRF。

修复前的重复敏感差分：

```text
rownum_differences = 4
limited differences = 2
srf differences = 1
ties differences = 3
```

B1/B2 的错误由 gsql stderr 捕获，不把脚本退出码 0 当作通过。正常 SQL 差分矩阵在旧二进制上已是 42 次 / 0 差异，作为修复不能破坏的正常路径基准。

ROWNUM 结果比较的两份 CTAS 都带 `ORDER BY row_id`，与直接 SELECT 的形状一致；不带 ORDER 的 CTAS 会触发不同的旧优化路径而掩盖差异。本次同时保存明确的 rn/id 期望值，避免仅凭差分零值误判。

## 5. 最终构建与回归结果

### 5.1 构建、二进制和恢复过程

使用工作区统一脚本 `deliverables/multimodal_optimizer/opengauss_build_install_restart.sh`。因 `prep.h` 返回类型改变且旧配置为 Release/O2，本轮先执行 full：

```bash
BUILD=/home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh
bash "$BUILD" full
```

21:39:41 开始 clean/configure，编译中途会话中断。随后沿用这次全新 Debug 配置，经同一脚本 incremental 续建；处理了中断留下的生成 `errmsg.h` 缺失和 3 个零字节对象文件。零字节文件移入临时证据目录保留，由构建系统重新生成；没有回滚源码、没有使用旧 Release 对象完成链接。21:58:10 首次完整安装并启动成功，补齐直接 SRF 检查后再次 incremental，于 22:03:23 完成最终安装与 SQL 连通性检查。

最终核验：

- `config.log`：`-O0`、`enable_debug='yes'`、`enable_cassert='yes'`；
- `pg_config.h`：`USE_ASSERT_CHECKING 1`；
- `version()`：`openGauss 7.0.0-RC3 build 098d77c8 ... debug on`；此 hash 表示源码 HEAD，另有本报告的未提交工作树修复；
- 实际进程：`openGauss-server/mppdb_temp_install/bin/gaussdb`，主实例端口 55440；
- 最终二进制 SHA-256：`c56a6e511a780ed1aed85417fbad344c2e5830204f5e20cc2d5ca6ae9cd570a7`；
- 两个 GUC 的 `boot_val`、`reset_val` 和新会话当前值均为 `off`。

构建和版本摘录见[构建与环境](深度复核证据/构建与环境.txt)。完整成功续建日志仍位于源码仓的 `build_logs/incremental_20260909_215314.log`、`build_logs/incremental_20260909_215949.log`；full 的起始日志为 `build_logs/full_20260909_213941.log`。

### 5.2 openGauss 原生回归

实际命令：

```bash
repo=/home/qiumc/data/code/cpp/openGauss-server
install=$repo/mppdb_temp_install
gcclib=/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3/gcc/lib64
env GAUSSHOME="$install" SSL_LIB_PATH="$install/lib" \
  LD_LIBRARY_PATH="$install/lib:$gcclib" \
  make -C "$repo/src/test/regress" union_all_required_columns_check \
  EXTRA_TESTS='diskann_vector setop reduce_orderby'
```

框架自行临时安装、initdb，在 25632 端口启动单机测试库，结束后自行停止。没有拿手工 gsql 输出替代原生 pg_regress 判定。

| 用例 | 最终结果 |
|---|---|
| `union_all_required_columns` | 通过：原四类目标 SQL、ANN、Merge/Hash Anti Join、列生命周期和结果 |
| `union_all_required_columns_safety` | 通过：六类 bug、42 次普通 SQL 差分、generic Param、两种表达式框架 |
| `diskann_vector` | 通过 |
| `setop` | 通过 |
| `reduce_orderby` | 通过 |

最终于 22:15:38 完成查询，**All 5 tests passed**，make 退出码 0。测试耗时数字仅是框架日志，不作为性能结论。

首轮为 4/5：新安全用例和三个既有用例通过；原特性用例仅存在 Hash/Merge Anti Join 计划基线差异，结果值、Output 裁列和 Limit 相对位置无变化。原 SQL 已禁用 hashjoin/nestloop，却未显式设置 mergejoin，旧 expected 全为 Hash Anti Join。本轮显式 `enable_mergejoin=on` 固定主计划，另切换开关独立看护 Hash Anti Join；审阅这两类实际计划后更新 expected，再重跑上述完整命令通过。没有将业务结果变化直接写成新的正确答案。

### 5.3 修复后语义核验

| 检查 | 修复后结果 |
|---|---|
| B1 残留过滤与除法 | 无除零，返回 4、3、2、1；除法未移到残留 Filter 之前 |
| B2 聚合后代 | 无 Aggref 错误，返回 (1,3)、(2,0)；Aggregate 节点保留 |
| B3 ROWNUM | 明确 rn/id 期望值正确，`rownum_differences=0` |
| B4 容器 LIMIT | id=1，`limited differences=0` |
| B5 零行 SRF | id=1/n=1，`srf differences=0`；表达式框架 off/on 均通过 |
| B6 WITH TIES | 保留 0、1、2、3、4 全部五行，`ties differences=0` |
| 普通查询四组合 | 14 个形状 × 3 个优化组合，`checked=42, differences=0` |
| 强制 generic plan | 三次绑定分别返回 3、4；3；空集（LIMIT 0） |
| 原目标查询 | 四种组合的重复敏感差分均为 0；有序位置差异为 0；volatile 序列次数一致 |

静态检查：内核、SQL 和 schedule 的 `git diff --check` 通过。expected 保留 gsql 原生表格中的尾空格，不做破坏 golden 的格式化。

### 5.4 可复核证据

- [安全回归：修复前原始输出](深度复核证据/安全回归_修复前.out)：含六类确认缺陷；这是当时的 SQL 版本，尚未追加最后的表达式框架用例，不能按当前文件行号机械对齐。
- [安全回归：最终原生输出](深度复核证据/安全回归_修复后.out)：含 verbose 计划、显式结果和差分。
- [目标计划与结果：最终原生输出](深度复核证据/目标计划与结果_修复后.out)：含 Merge/Hash、DiskANN 和四类目标 SQL。
- [首次原生回归日志](深度复核证据/原生回归_首次.log)与[最终原生回归日志](深度复核证据/原生回归_最终.log)：保留失败、修正和 5/5 通过的完整过程。

曾额外尝试在隔离目录启动保存的旧二进制补验，未成功建立可连接服务；该尝试不计入前后对照证据。修复前结论来自重建前已运行实例上的真实输出，而不是这次失败启动。

主实例最初正在运行，因此收尾保持 55440 在线，原 `union_all_topk_onecol_test` schema 保留。本轮自建测试 schema 已由 SQL 清理，临时回归服务已停止；未启动其他项目服务或修改其数据。

## 6. 边界

本轮验证面向单机 openGauss，保留默认 GUC off。没有多 DN/Stream 运行证据，也没有性能结论。

未执行整个 `parallel_schedule0`，5/5 仅指 §5.2 列出的五个用例。初始开发报告中另行记录的 `test_union_all_orderby` 基线偏差，本轮没有重新验收，也没有修改该旧用例；不能用本轮 5/5 表述为全库回归通过。

这些修复并不改变 heap 行存的数据页读取方式；源扫描 Output 变窄证明的是少投影、复制和层间传输无关字段。

旧特性提交/旧交付 patch 是修复前快照，迁移到其他同源分支时还必须携带本报告对应的后续代码和测试修正。
