# DiskANN L2 / PQ 完整 SQL 测试例子

本例提供建表、装数、建立精确参考、创建 DiskANN 索引、EXPLAIN VERBOSE、实际查询、逐行距离对照、召回统计、断言及清理。数据为 **512 行、4 维**，查询向量固定为 `[0,0,0,0]`，包含 `WHERE title IS NOT NULL` 和 `LIMIT 10`。

脚本：[完整测试.sql](完整测试.sql)。需要 openGauss 已提供 `vector`、`diskann`、`vector_l2_squared_distance` 和 PL/pgSQL。

## 1. 必须先区分测试对象

| 值 | 含义 | 本 SQL 是否直接取得 |
|---|---|---|
| `returned_sql_l2` | DiskANN 查询的 SQL 投影值 `embedding <-> query` | 是 |
| `exact_l2` | 创建向量索引之前，在相同原始行上计算的完整 L2 参考值 | 是 |
| `reference_l2_squared` | SQL 函数在原始向量上计算的完整平方 L2 | 是；它不是 AM 观测值 |
| PQ 搜索过程的估计距离 | 图搜索中通过 PQ 编码查表得到的分数 | 否，需要第 5 节的内核观测 |
| `xs_orderbyvals[0]` | AM 返回接口的实际值 | 否，普通 SELECT 无接口读取 |

因此，本例不能把 `returned_sql_l2` 或 `reference_l2_squared` 命名为“PQ 距离”或“实测 AM 距离”。开了 `enable_pq=on` 也不代表 SELECT 自动返回 PQ 分数。

核对的当前源码在每轮 `SearchFixedPoint()` 后，对候选队列调用 `GetDistance()` 计算完整内部距离，再排序、返回。L2 下这个完整内部值为平方距离。SQL 投影另按原始向量计算开方后的 L2。这个现状与“某个迁移分支让 AM 直接返回 PQ 分数”的假设必须分开。

## 2. 执行方法

在普通非业务测试库执行，schema 名 `diskann_l2_distance_demo` 必须尚不存在。脚本不执行 DROP 已有对象；命名冲突会直接报错。

当前工作区客户端示例：

```bash
ANN_TEST_INSTALL=/home/qiumc/data/code/cpp/openGauss-server/mppdb_temp_install
ANN_TEST_SQL=/home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/测试/DiskANN_L2距离对照/完整测试.sql

# 真正的 PQ 测试：要求该实例已经正确加载 DiskANN PQ 动态库。
env LD_LIBRARY_PATH="$ANN_TEST_INSTALL/lib" \
  "$ANN_TEST_INSTALL/bin/gsql" -X -h /tmp -p 55440 -d postgres \
  -v use_pq=on -f "$ANN_TEST_SQL"

# 非 PQ 对照：用于验证 SQL、计划、完整距离与测试流程。
env LD_LIBRARY_PATH="$ANN_TEST_INSTALL/lib" \
  "$ANN_TEST_INSTALL/bin/gsql" -X -h /tmp -p 55440 -d postgres \
  -v use_pq=off -f "$ANN_TEST_SQL"
```

在其他环境使用对应安装目录、端口和测试数据库，不直接套用本机安装路径。`use_pq` 是 gsql 脚本变量，实际控制 CREATE INDEX 的 `enable_pq` 索引参数，不是新的数据库 GUC。

交互式 gsql 中也可以：

```text
\set use_pq on
\i /home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/测试/DiskANN_L2距离对照/完整测试.sql
```

脚本成功时执行 ROLLBACK，撤销本次全部测试对象和 LOCAL 配置。`gsql -f` 遇错退出时，未提交事务也会回滚。交互式执行遇错后，需要手工 `ROLLBACK;`，再修正问题重跑。

当前 openGauss 不允许事务内 ANALYZE。本例为了保持整体可回滚，不执行 ANALYZE，而是控制扫描开关并检查真实计划。因此本例用于功能、距离及计划通路核对，不用于自然 CBO 选择或性能结论。

## 3. 数据和判定方法

512 行中，481 行满足 title 非空。坐标由固定整数公式生成，未使用 random()；512 行超过每个子空间 256 个 PQ 中心的数量。这个规模只是小型链路用例，不代表训练质量或真实业务分布。

`pq_m=2` 将 4 维分为两个子空间，`index_size=16` 用于这个小数据例子；不创建 INCLUDE 索引，不更改索引格式。

脚本先展示几组易于核算的原始距离：

| 查询点 | 数据点 | 完整 L2 | 完整平方 L2 |
|---|---|---:|---:|
| `[0,0,0,0]` | `[3,4,0,0]` | 5 | 25 |
| `[0,0,0,0]` | `[6,8,0,0]` | 10 | 100 |
| `[0,0,0,0]` | `[0.375,0.5,0,0]` | 0.625 | 0.390625 |

这些是 SQL 参考计算，不是从 AM 读出的实测值。

随后进行以下检查：

1. 打印 `pg_get_indexdef` 和 reloptions，确认实际建成 DiskANN，PQ 参数与请求相符。
2. 打印 EXPLAIN VERBOSE。程序还对同一目标 SELECT 的 EXPLAIN 检查 `Ann Index Scan using vectors_l2_idx`，避免只凭 `enable_seqscan=off` 判定命中。
3. 用 PL/pgSQL 执行同一条 SELECT，记录它实际返回的顺序；不向目标查询添加窗口函数、额外排序键或其他会改变 ANN 路径的包装。
4. 将每个返回 ID 与同一原始行的精确参考比较，输出绝对误差、相对误差和 float8send 位级相等。距离为零时，相对误差输出 NULL，绝对误差仍保留。
5. 单独统计与全表精确 Top-10 的交集比例。召回不等于距离值正确性；不把 recall=1 当作所有 ANN 数据集都必须满足的断言。边界同距离时，基准的 row_id tie-break 也可能影响集合比较。
6. 断言 512 行源数据、10 行结果、结果 ID 不重复、WHERE/行关联正确、当前 SQL 投影与原始参考一致、实际返回序列非递减。

当前源码的预期 SQL 距离差为零，即使 PQ 开启也可能为零，因为 SQL 输出和最终完整向量精排不是 PQ 估计分数。**零差异不能证明 PQ 量化没有误差。** 如果目标分支有意将 Output 改为 AM 近似距离，应先记录本例误差报告，再明确调整近似验收口径，不可把断言失败静默忽略。

本例不覆盖并发 DML、索引槽位复用或多轮扩批全局有序性；一条小数据查询的有序结果不能证明所有迭代路径都严格有序。

## 4. 当前环境实测状态

核对日期：2026-09-14。工作树 `openGauss-server`，分支 `integration/multimodal-stacked`，HEAD `098d77c8b76d56eace4af8fddaf8d94b394b996c`，存在原有 dirty 修改。

实例：`/tmp:55440`，数据库 postgres；运行二进制为该工作树 `mppdb_temp_install/bin/gaussdb`，版本为 openGauss 7.0.0-RC3 / build 098d77c8 / Debug / x86_64。

- `use_pq=on` 已实际尝试，创建索引时报错：`this instance has not currently loaded the pq dynamic library.`；gsql 退出码为 3。这是未具备 PQ 执行条件，不是 PQ 用例通过。完整证据：[PQ环境缺失.out](PQ环境缺失.out)。
- `use_pq=off` 已实际通过，gsql 退出码为 0：实际计划为 `Limit → Ann Index Scan using vectors_l2_idx`，Filter Mode 为 Post-Filter；512 行源数据 / 481 行满足过滤，返回 10 行，7 项断言全部通过，同一返回行的最大 SQL 距离差为 0，本组数据 Recall@10 为 1。完整证据：[非PQ实测.out](非PQ实测.out)。
- 成功测试结束后 `remaining_test_schemas=0`；PQ 失败退出后的 schema 也已核验不存在。测试未改动内核或启停服务，原有运行实例保持运行。
- 普通 SQL 无法取得内部 PQ 分数，本轮未完成内部 PQ 距离误差的测量。

官方当前 [PQ 安装说明](https://docs.opengauss.org/zh/docs/latest/datavec/pq.html) 要求 DiskANN 单独安装 PQ 加速包、配置 DATAVEC_PQ_LIB_PATH，并说明官方包的架构支持范围。实际能否启用应以目标实例是否成功加载匹配动态库及建索引结果为准。不要复制其他 AM 的 PQ 库冒充 DiskANN 库，也不要关闭 PQ 后将结果标为 PQ 测试。

## 5. 要直接测 AM / PQ 距离，还必须观测哪些值

以下是具备 PQ 库的隔离调试环境中的观测步骤；本轮未执行，不应当作已有实测证据。不需要修改搜索分数来观察它，更不能删除精排后假装测的是当前实现。

相关源码均位于 `src/gausskernel/storage/access/datavec/diskannscan.cpp`：

1. `SearchFixedPoint()` 中 `GetPQDistance(...)` 成功返回之后：记录 query/scan、搜索轮次、节点 ID、PQ 平方距离和候选 TID。冻结入口点使用完整距离，不要把它归类为 PQ 样本。
2. `diskannsearch()` 中整队列 `GetDistance()` 更新前后：对同一节点记录旧估计值与新的完整平方距离。之后 qsort 会改变队列位置，应按节点身份关联，不能只按数组下标跨排序匹配。
3. `diskanngettuple_internal()` 写入 `xs_orderbyvals[0]` 后：记录真正返回事件的 TID 和该值。Datum 按目标构建的 float8 方式解码，不把原始整数位模式直接当数值。
4. 使用本例记录的 `source_tid`、row_id 和固定查询向量，与实际返回候选配对。若迁移分支改了 AM 返回口径，分别记录 PQ 搜索分数和最终发布值，不依据变量名称猜测。

对同一有效候选，如果两边都是非负平方 L2，则计算：

```text
平方距离绝对误差 = abs(pq_l2_squared - full_l2_squared)
L2 绝对误差     = abs(sqrt(pq_l2_squared) - sqrt(full_l2_squared))
L2 相对误差     = L2 绝对误差 / sqrt(full_l2_squared)
```

原始距离为零时相对误差不可定义；负数、NaN、Inf 分别记录，不用 abs 或随意截断掩盖异常。若 AM 已输出开方后的 L2，不能再开一次平方根。

在当前源码中，精排之后的 AM 返回值与原始平方 L2 对齐，并不等于第 1 个观测点的 PQ 分数。若需要测试“AM 不精排、直接返回量化距离”的另一个实现，应在那个目标分支执行本例及相应观测，不能用本工作树结果代替。
