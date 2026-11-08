# openGauss UNION ALL 必需列源头裁剪开发与测试报告

## 1. 结论

本报告记录初始交付。后续复核已发现六个可复现的安全边界缺陷，修复及最新验收请以[深度复核与缺陷修复报告](UNION_ALL必需列源头裁剪深度复核与缺陷修复报告.md)为准。

目标 SQL 场景已经完成开发，并接入 openGauss 原生 `pg_regress` 框架。专用回归 `union_all_required_columns` 为 `1/1` 通过；与仓库既有 `diskann_vector` 联跑为 `2/2` 通过。

本次用例同时看护两类契约：

- 计划形状：直接保存 `EXPLAIN (VERBOSE ON, COSTS OFF)` 的完整文本，锁住源扫描、Anti Join、Append/Merge Append、Sort 和 Limit 的 `Output` 与相对位置。
- 查询语义：以显式期望结果、`EXCEPT ALL` 双向差集、有序位置比较和 GUC off/on 对照看护结果、重复行、排序与 LIMIT。

新 GUC `enable_union_all_required_columns_pushdown` 为 `PGC_USERSET`，默认 `off`，与 `enable_union_all_diskann_topk_pushdown` 相互独立。55440 手工测试实例保持运行，目标 schema 为 `union_all_topk_onecol_test`。

## 2. 代码与用例规模

统计口径为相对当前分支 HEAD `bf59a66d8bfe` 的工作树差异，不包含用户已有的无关 dirty 文件。

| 范围 | 新增 | 删除 | 净增 |
|---|---:|---:|---:|
| 内核生产代码 | 1,151 | 1 | 1,150 |
| 回归用例及框架接入 | 1,044 | 2 | 1,042 |

内核中 `preptopk.cpp` 为 +1,104/-1 行，其余 GUC、planner、createplan 和头文件合计 +47 行。回归主体为 SQL 342 行、expected 693 行；另有 1 行专用 schedule，以及 GNUmakefile/主 schedule 的接入改动。

## 3. 实现范围

生产代码涉及：

- `src/gausskernel/optimizer/prep/preptopk.cpp`
  - 纯 UNION ALL 结构识别和安全检查；
  - 外层 WHERE 向所有叶子精确映射，成功后删除外层重复谓词；
  - 必需输出收集、位置映射、叶子 targetlist 和 SetOperationStmt 元数据压紧；
  - ORDER BY 非 Var 表达式在各叶子物化为标量排序键；
  - volatile/SRF/排序引用等语义输出保留；
  - 失败时基于完整 Query 副本原子回退。
- `src/gausskernel/optimizer/plan/planner.cpp`
  - 按“必需列压紧 → 可选 Top-K → 标量排序键投影”顺序调用；
  - 向子 planner 传递本次 Query 已压紧标志。
- `src/gausskernel/optimizer/plan/createplan.cpp`
  - 对已压紧 Query 的后代基表关闭 physical targetlist 回扩，避免源扫描重新输出死列。
- `src/common/backend/utils/misc/guc/guc_sql.cpp`、`src/include/knl/knl_guc/knl_session_attr_sql.h`
  - 注册和保存独立 GUC，默认关闭。
- `src/include/nodes/relation.h`、`src/include/optimizer/prep.h`
  - planner 状态与入口声明。

## 4. 原生回归框架接入

| 文件 | 作用 |
|---|---|
| `src/test/regress/sql/union_all_required_columns.sql` | 建表、造小数据、完整 EXPLAIN 和正确性 SQL |
| `src/test/regress/expected/union_all_required_columns.out` | 693 行原生 golden 输出 |
| `src/test/regress/parallel_schedule.union_all_required_columns` | 单用例隔离调度文件 |
| `src/test/regress/parallel_schedule0` | 纳入 openGauss 单机主调度 |
| `src/test/regress/GNUmakefile` | 安装测试数据并提供定向 target |

定向入口为：

```bash
env \
  GAUSSHOME=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install \
  SSL_LIB_PATH=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib \
  LD_LIBRARY_PATH=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib:/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3/gcc/lib64 \
  make -C /home/qiumc/data/code/cpp/openGauss-server/src/test/regress \
    union_all_required_columns_check
```

这一路径由 `pg_regress --temp-install --single_node` 创建全新临时实例、执行 SQL、逐行比较 expected，并在结束时停止临时实例；不是在现有业务 schema 上人工拼接断言。

## 5. 典型计划看护

测试表只有 7 行 delta、6 行 snapshot、3 行 delete。通过关闭顺序扫描、位图扫描并建立主键和 DiskANN 索引，使小数据也能稳定暴露列生命周期。

### 5.1 四种目标 SQL

| 场景 | golden 看护点 |
|---|---|
| WHERE | `delta_t`、`snapshot_t`、`delete_t` 均为 `Index Only Scan`，各扫描 `Output` 只有 `row_id`；无 `title`、`embedding` |
| WHERE + ORDER BY | UNION 上方只传 `row_id + float8 distance`；snapshot 扫描只在计算距离前输出 `row_id + embedding`；无 `title` |
| WHERE + LIMIT | 只有一个全局 Limit；三个源扫描均只输出 `row_id`，无分支 Limit |
| WHERE + ORDER BY + LIMIT | 全局 Limit + 每叶一个局部 Limit；`Merge Append` 只传 `row_id + distance`；snapshot Anti Join 位于局部 Limit 之下 |

计划检查使用完整 `EXPLAIN VERBOSE` golden，不使用只判断节点名称的布尔包装函数。因此，只要后续改动让 `snapshot_t.title`、原始 `embedding` 或其他死列重新进入任一相关 `Output`，回归就会直接产生 diff。

### 5.2 其他计划看护

- 代表性 DiskANN：delta 分支为 `Ann Index Scan`，输出 `row_id + distance`；`title` 只出现在本地 `Filter`。
- delete 生命周期：`reason` 只出现在 `delete_t` 的扫描 Filter，Anti Join 输入只输出 `d.row_id`，`unused_delete` 不进入任何计划 Output。
- Top-K GUC off：仍裁剪列和传递标量距离，但只保留全局 Limit。
- 必需列 GUC off：明确看护原有宽 targetlist 与 Subquery Scan，证明兼容开关可恢复旧计划。
- 四叶嵌套 UNION ALL：所有叶子均变为只输出 `row_id` 的 Index Only Scan，看护递归 SetOperationStmt 元数据压紧与列号重排。
- 安全回退：UNION distinct、叶子已有 LIMIT、security barrier 保持原规划路径和宽输出，不进行冒险压紧。

## 6. 查询结果正确性看护

| 检查 | 实际结果 |
|---|---|
| 显式结果集 | 10 行；`row_id=3` 的 UNION ALL 重复行保留，已删除的 snapshot 2/5 不返回 |
| 显式 Top-4 | 有序结果为 `101, 102, 3, 3` |
| 四象限 GUC off/on | 行数分别为 10、10、4、4；四组双向 `EXCEPT ALL` 的 `diff_rows` 均为 0 |
| 有序位置比较 | 前 8 行逐位置差异为 0，不只比较无序集合 |
| 投影/列号重排 | `length(title), row_id` 的列顺序、表达式与重复行均正确 |
| volatile 输出 | GUC off/on 都执行 13 次 `nextval`，未错误裁掉可观察副作用 |
| 回退结果 | distinct 13 行；叶子 LIMIT 为 `{1,2,3,101}`；security barrier 为 11 行 |
| 参数化计划 | 参数化 WHERE、vector、LIMIT 返回 `101, 102, 3, 3` |

## 7. 构建与测试记录

### 7.1 编译、安装、重启

使用统一开发脚本执行增量构建、安装和重启：

```bash
/home/qiumc/data/code/cpp/deliverables/multimodal_optimizer/opengauss_build_install_restart.sh incremental
```

结果：成功。日志为 `/home/qiumc/data/code/cpp/openGauss-server/build_logs/incremental_20260909_172619.log`。实例使用实际安装树 `/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install`，端口 55440。

后续核验纠正：该次 incremental 沿用原配置，`config.log` 实际为 `-O2`、debug/cassert 均为 no，运行版本也不含 `debug on`；脚本名不能证明实际为 Debug。后续缺陷修复已另行启动 clean/configure 后的 Debug/O0 + assertion 全量构建，详见深度复核报告。

### 7.2 专用回归

结果：

```text
test(1/1) union_all_required_columns .... ok
All 1 tests passed.
```

### 7.3 与既有 DiskANN 回归联跑

执行专用 schedule 并追加仓库原有 `diskann_vector`：

```bash
make -C /home/qiumc/data/code/cpp/openGauss-server/src/test/regress \
  union_all_required_columns_check EXTRA_TESTS=diskann_vector
```

结果：

```text
test(1/1) union_all_required_columns .... ok
test diskann_vector ..................... ok
All 2 tests passed.
```

### 7.4 静态工作树检查

`git diff --check` 通过，没有新增空白错误。本补丁只收录该特性的内核、回归和文档文件。

## 8. 手工测试入口

当前实例状态在报告完成时为：

| 项目 | 值 |
|---|---|
| 地址 | Unix socket `/tmp` 或 `127.0.0.1` |
| 端口 | `55440` |
| 数据库 | `postgres` |
| 用户 | `qiumc`，本地 trust |
| Schema | `union_all_topk_onecol_test` |
| 新 GUC 默认值 | `off` |

连接命令：

```bash
env LD_LIBRARY_PATH=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/lib \
  /home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install/bin/gsql \
  -X -h /tmp -p 55440 -d postgres
```

进入会话后：

```sql
SET search_path = union_all_topk_onecol_test, public;
SET enable_seqscan = off;
SET enable_union_all_required_columns_pushdown = on;
SET enable_union_all_diskann_topk_pushdown = on;
```

## 9. 已知边界

- 本轮完成的是目标单机 SQL 场景；未启动多 DN 集群，也未形成 Stream/RemoteQuery golden，因此不宣称完成 P3 分布式验收。
- 新 GUC 保持默认 `off`，默认开启需要额外灰度和更大范围回归。
- 当前分支既有 `test_union_all_orderby` golden 曾观察到与实际分支内 ORDER 顺序不一致；在新 GUC 明确为 off 时同样存在，属于本特性之前的基线差异。本次没有修改该旧用例或其 expected。与本特性直接相关的既有 `diskann_vector` 已通过。
- `EXPLAIN Output` 变窄表示不再投影、变形、复制和层间传输死列；对 heap 行存不等价于物理数据页只读取这些列。
