# 超低内存磁盘检索

## 介绍

随着推荐系统、图像识别、自然语言处理等AI应用的普及，向量数据的规模从百万级快速走向亿级，内存成为大规模向量检索的主要成本。DiskANN以图索引加磁盘存储的混合架构降低了内存消耗，但索引中仍然保存完整的原始向量，检索时的内存占用与原始数据同量级。超低内存磁盘检索在DiskANN的基础上引入PCA降维与RaBitQ量化，索引只保存压缩编码，图遍历用位运算估算距离，候选集再回堆表用原始向量精排，在保持查询性能与召回的同时进一步降低索引体积和检索内存，适用于内存受限的大规模向量检索场景。

本章节主要介绍openGauss数据库DataVec向量引擎超低内存磁盘检索特性的使用步骤，以指导用户顺利完成操作。

>[!NOTE]**说明**
>
>本特性支持ARM/x86架构环境。<br>
>本特性基于DiskANN，仅支持vector数据类型，最高维度支持16000维，与vector类型的维度上限一致，在其他向量数据类型上构建会报错。<br>
>本特性与PQ互斥，`enable_rabitq`和`enable_pq`不能同时开启。开启后索引不再保存原始向量。<br>
>本特性不支持表达式索引，需要直接对向量列创建索引。<br>
>本特性支持普通行存表，临时表，Toast表，Unlogged表，段页式表等，不支持ustore表。<br>
>兼容A/B/C/PG库。<br>
>支持并行构建。

## 特性原理

超低内存磁盘检索沿用DiskANN的Vamana图索引，用压缩编码替代索引中的原始向量来降低内存占用，实现原理如下：

- 向量压缩

    PCA降维把向量投影到方差最大的主成分方向上，缩短编码长度，`pca_dim`为0时不降维；RaBitQ量化对降维后的向量逐维做1bit或2bit量化，得到定长的压缩编码。PCA投影与随机正交旋转在创建索引时训练并固化，后续插入的数据沿用同一变换，不重新训练。

- 混合存储架构

    索引：存储压缩编码和Vamana图结构，用于图遍历和候选集筛选。

    堆表：保存完整的原始向量，用于候选集的精确距离计算。

- 索引检索

    图遍历阶段只读取压缩编码，用位运算估算距离，按`diskann_probes`维护候选集；候选集确定后回堆表读取原始向量计算精确距离并排序输出。量化误差只影响召回，不影响返回结果的距离值。

## 使用超低内存磁盘检索

### 启用特性

设置索引参数`enable_rabitq = on`启用超低内存磁盘检索。

### 关闭特性

设置索引参数`enable_rabitq = off`（默认）时，不启用超低内存磁盘检索。

### 创建索引

```
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops)
with (enable_rabitq = on, pca_dim = <PCA_DIM>, rabitq_bits = <BITS>, index_size = <INDEX_SIZE>);
```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

建议先导入数据再创建索引。设置`pca_dim`时，表中数据量不少于16384行，用于训练降维矩阵。

### 索引操作符

DISKANN索引操作符`[TYPE]_[DISTANCE_FUN]_ops`格式：

- `TYPE` - 向量类型
    - vector

支持向量数据维度：

名称 | 维度限制
--- | ---
vector | 16,000

- `DISTANCE_FUN` - 距离函数
    - l2
    - ip
    - cosine

索引操作符 | operator | 描述
--- |--- |---
vector_l2_ops | <-> |L2距离
vector_ip_ops | <#> |内积
vector_cosine_ops | <=> |余弦距离

### 索引选项

- `enable_rabitq` - 控制是否启用超低内存磁盘检索，默认关闭，不能与`enable_pq`同时开启
- `pca_dim` - PCA降维后的维度，取值为0（不降维，默认值）或8~维度-1，需先开启`enable_rabitq`，且表中数据量不少于16384行
- `rabitq_bits` - 每维量化位数，取值为1或2（默认值为1），需先开启`enable_rabitq`
- `index_size` - 索引构建参数，影响召回精度与构建时间，取值范围为16~1000（默认值为100），百万规模数据集建议设置为50

`enable_rabitq`、`pca_dim`、`rabitq_bits`决定索引的磁盘格式，索引建成后不可通过ALTER INDEX修改，需要删除后重建。

**示例：** 使用L2距离创建1bit、不降维的超低内存磁盘检索索引。

```
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (enable_rabitq = on);
```

**示例：** 使用余弦距离创建先PCA降至448维、再按2bit量化的超低内存磁盘检索索引，适用于高维、千万级规模数据集。

```
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_cosine_ops) WITH (enable_rabitq = on, pca_dim = 448, rabitq_bits = 2);
```

**示例：** 以下语句均会报错。

```
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (enable_rabitq = on, enable_pq = on);
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (pca_dim = 32);
openGauss=# ALTER INDEX items_embedding_idx SET (pca_dim = 16);
```

### 构建选项

- `parallel_workers` - 构建索引并行度（默认为0），数据量小于并行构建数与`index_size`乘积时，将通过串行构建提升构建精度

```
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

- `diskann_build_in_memory` - 构建模式，取值on/off（默认off）。off时构建过程按需从缓冲池读取向量，内存占用小；on时先将全部向量读入内存再构图，构建更快，约需`4 × 行数 × 维度`字节的额外内存。该参数只影响构建过程，不影响检索

```
openGauss=# SET diskann_build_in_memory = on;
```

### 查询选项

- `diskann_probes` - 查询时候选集的大小，默认为128，范围[1-32768]，参见[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/datavec_vector_engine_parameters.html)。降维越多、量化位数越低，相同`diskann_probes`下召回越低，需相应增大该值。候选耗尽且未满足LIMIT时，检索会自动翻倍重搜

```
openGauss=# SET diskann_probes = 64;
```

- `rbq_query_bits` - 设置是否对查询向量进行额外的标量量化，适当设置可以提升召回率。默认为8，范围[1-8]

```
openGauss=# SET rbq_query_bits = 8;
```

- `enable_seqscan` - 查询时使用非向量索引（默认on）

```
openGauss=# SET enable_seqscan = off;
```

### 使用索引查询

```
openGauss=# SELECT * FROM [TABLE_NAME] ORDER BY [COLUMN_NAME] [operator] [VALUE];
```

- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 列名
- `operator` - 距离计算操作符，需要与创建索引时使用的距离计算方法相同
- `VALUE` - 查询的向量

精排需要读取堆表中的原始向量，因此不支持仅索引扫描。

**示例：** 通过l2距离查询items表中与[1,2,3,4]向量最相似的10条数据。

```
openGauss=# SET diskann_probes = 128;
openGauss=# SELECT id FROM items ORDER BY embedding <-> '[1,2,3,4]' LIMIT 10;
```

### 增删改

INSERT/UPDATE/DELETE/VACUUM使用标准SQL，无新增语法。

新插入的向量沿用创建索引时的变换与量化参数，用估算距离选取邻居入图；完全相同的向量会合并到同一个索引节点，单个节点最多合并10条重复数据。

本特性不受主表`immediate_delete`选项影响，DELETE/UPDATE前无需开启该选项。删除或更新掉的旧向量由检索时的回表精排过滤，其索引项在VACUUM时清理，被清空的节点保留为路由节点，不再返回结果。

索引文件只增不减。大量插入导致数据分布变化，或大量删除导致空节点累积后，需要REINDEX重整，REINDEX保持原索引格式。

```
openGauss=# DELETE FROM items WHERE id = 1;
openGauss=# VACUUM items;
openGauss=# REINDEX INDEX items_embedding_idx;
```

## 设置建议

- 内存受限、向量维度适中时，使用默认的1bit、不降维配置。
- 向量维度高、数据量在千万级以上时，建议设置`pca_dim`与`rabitq_bits = 2`，并根据召回情况提高`diskann_probes`。
- 建议先导入数据再创建索引，设置`pca_dim`前确认表中数据量不少于16384行。
- 构建机器内存充足时，可设置`diskann_build_in_memory = on`缩短构建时间。
- 高维向量的PCA训练和随机正交旋转会增加构建时间及变换矩阵内存，应根据数据维度预留资源。

## 约束

- 向量索引仅支持普通行存表，临时表，Toast表，Unlogged表，段页式表等，其他表仅支持对向量数据创建btree和ubtree索引。
- 仅支持vector数据类型，未指定向量列维度时无法构建向量索引，维度不超过16000。未开启`enable_rabitq`的DiskANN索引仍受1536维限制。
- 不支持表达式索引。
- DISKANN索引不支持ustore存储。
- `enable_rabitq`与`enable_pq`不能同时开启；`pca_dim`、`rabitq_bits`需在`enable_rabitq = on`时使用。
- `pca_dim`取非0值时表中数据量不少于16384行，空表和小表只能使用不降维的超低内存磁盘检索。
- `enable_rabitq`、`pca_dim`、`rabitq_bits`不允许通过ALTER INDEX修改，需要删除索引后重建。
- 若在兼容B库中使用向量索引，需要执行```set dolphin.nulls_minimal=false```，用于关闭nulls处理策略。
- 数据量小于并行构建数与索引构建参数index_size乘积时，将通过串行构建提升构建精度。
- 本特性构建的索引无法被未完成升级的旧版本节点读取，版本回退前需要删除该索引。
