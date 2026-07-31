# DISKANN

## 介绍

随着推荐系统、图像识别、自然语言处理等AI应用的普及，传统数据库难以应对海量高维向量的实时相似性搜索挑战。DiskANN作为一种基于磁盘的近似最近邻搜索技术，能够有效降低内存消耗，同时保持较高的查询性能，满足企业对大规模向量数据的高效管理需求。

>![]() **支持与限制：<br>**
>DISKANN暂时仅支持vector数据类型，在其他向量数据类型会导致执行失败，最高维度支持1536维。<br>
>DISKANN暂时仅兼容A\B\C\PG库。<br>
>DISKANN支持普通行存表，临时表，Toast表，Unlogged，段页式表等的向量数据存储。<br>
>DISKANN支持PQ量化压缩及并行构建。<br>

## 索引构建

DiskANN索引是一种高效地大规模向量近似最近邻搜索方案，采用Vamana算法，其核心目标是在单机上处理大规模数据，同时保持高召回率、低查询延迟和低内存占用，实现原理如下：

- 基于图的索引结构(Vamana算法)

    Vamana图索引相比于传统图算法（如HNSW、NSG），Vamana通过动态调整参数α（α≥1）优化图的构建，使其在保持高召回率的同时减少搜索路径长度。

- 混合存储架构

    内存：存储压缩后的向量（如PQ量化编码）和图索引的元数据，用于快速筛选候选集。

    SSD：存储完整的原始向量和详细的图结构，确保高精度距离计算。

## 索引检索

DiskANN的检索过程主要基于构建好的索引图结构，通过贪心搜索策略从入口点开始，逐步向目标向量靠近，核心流程如下：

- 初始化搜索状态

    从MetaPage读取索引的基本参数（如dim、frozenPoint等）

- 获取当前节点

    从搜索列表中选择距离查询向量最近且未被访问过的节点

- 访问邻居节点

    加载节点的邻居列表（从EdgePage中获取），计算每个邻居与查询向量的距离

- 更新搜索列表

    将未访问的邻居加入搜索列表

    若搜索列表大小超过diskann_probes，保留距离最近的diskann_probes个节点

- 迭代搜索

    重复上述步骤，直到搜索列表中所有的节点距离都不小于已访问节点中的最小距离，或达到最大迭代次数

## 使用DISKANN

### 创建索引（关闭PQ场景）

在关闭PQ场景下，DiskANN索引会默认使用精确距离计算。

```
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) ;
with (index_size = 100);
```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

### 创建索引（启用PQ场景）

在开启PQ场景下，DiskANN索引会使用PQ距离计算，最终再通过精确距离计算进行精排，在使用PQ之前需要先加载动态库。

```
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) ;
with (enable_pq = on, pq_m = 8);
```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

### DiskAnn支持的索引操作符

索引操作符 | operator | 描述
--- |--- |---
vector_l2_ops | <-> |L2距离
vector_ip_ops | <#> |内积
vector_cosine_ops | <=> |余弦距离

### 索引选项

- `index_size` - 索引构建参数，影响召回精度与构建时间，取值范围为16~1000（默认值为100），百万规模数据集建议设置为50
- `enable_pq` - 量化压缩参数，控制是否开启PQ，默认关闭
- `pq_m` - 量化压缩参数，取值范围为1~192（默认值为8），建议设置为```dim / 8```

**示例：** 使用带残差的L2距离计算创建DISKANN索引。

```
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (index_size = 100, enable_pq = on, pq_m = 8);
```

### 并行构建向量索引

通过开启并行构建功能来加速向量索引的创建：

```
ALTER TABLE [TABLE_NAME]
SET (parallel_workers = <CONCURRENCY_NUM>);
```

**示例：** 构建索引并行数设置为8。

```
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

### 查询选项

- `diskann_probes` - 查询时候选集的大小（默认为128）。

```
openGauss=# SET diskann_probes = 64;
```

- `enable_seqscan` - 查询时使用非向量索引(默认on)

```
openGauss=# SET enable_seqscan = off;
```

### 使用索引查询

```
openGauss=# SELECT * FROM [TABLE_NAME] ORDER BY [COLUMN_NAME] [operator] [VALUE];
```

- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 列名
- `operator` - 距离计算操作符，需要与创建索引时使用距离计算方法相同
- `VALUE` - 查询的向量
**示例：** 通过l2距离从小到大排序，查询items表中embedding这一列中与[1,2,3,4]向量相似的全部向量。

```
openGauss=# SELECT * FROM items ORDER BY embedding <-> '[1,2,3,4]';
```

### 增删改

DELETE/UPDATE之前需要打开主表的immediate_delete选项。
不开启该选项不会同步删除图的边关系。可能会导致图规模的增大。
开启后DELETE/UPDATE操作会实时更新到图关系内。
可以在创建表的时候指定：

```sql
CREATE TABLE [table_name] (cols) WITH (immediate_delete = on);
```

或者在创建完成后修改：

```sql
ALTER TABLE [TABLE_NAME] SET (immediate_delete = on);
```

## 约束

- 向量索引仅支持普通行存表，临时表，Toast表，Unlogged表，段页式表等，其他表仅支持对向量数据创建btree和ubtree索引。
- 若ALTER INDEX后不执行REINDEX，后插入的数据会根据新的索引选项构建索引，而索引中已存在的数据不会因此改变。
- DISKANN索引不支持ustore存储。
- 构建带有向量表时可以使用INDEX子句构建默认btree、ubtree索引，无法指定向量索引。
- 未指定向量列维度时无法构建向量索引，只支持构建btree、ubtree索引。
- 若在兼容B库中使用向量索引，需要执行```set dolphin.nulls_minimal=false```，用于关闭nulls处理策略。
- 数据量小于并行构建数与索引构建参数index_size乘积时，将通过串行构建提升构建精度。
