# 向量索引

DataVec向量引擎目前支持了[IVFFLAT](#ivfflat)、[HNSW](#hnsw)、[DISKANN](#diskann)、HNSWPQ以及IVFPQ等算法，基于openGauss中的ASTORE存储实现，通过索引结构能够高效地检索出查询结果。

向量索引创建语法支持普通索引创建语法中的内容

## HNSW

```sql
CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING hnsw (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (m=<M>, ef_construction=<EF_CONSTRUCTION>)
[TABLESPACE tablespace_name]
[COMMENT text]
[VISIBLE | INVISIBLE]
[WHERE predicate];
```

- `INDEX_NAME` - 索引名
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名
- TABLESPACE - 指定索引的表空间，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- COMMENT text - 指定索引的注释，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- VISIBLE|INVISIBLE - 指定索引是否可见，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- WHERE predicate - 创建一个部分索引，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同

### HNSW索引操作符

HNSW索引操作符`[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` - 向量类型
    - vector
    - bit
    - sparsevec

HNSW索引支持向量数据维度：

名称 | 维度限制 
--- | --- 
vector | 2,000 
bit | 64,000 
sparsevec | 1,000,000,000<br>1,000 非零元素数 

- `DISTANCE_FUN` - 距离函数
    - l2
    - ip
    - cosine
    - l1
    - hamming
    - jaccard

#### vector 索引操作符

索引操作符 | 描述 
--- | --- 
vector_l2_ops | L2距离
vector_ip_ops | 内积
vector_cosine_ops | 余弦距离
vector_l1_ops | L1距离

#### bit 索引操作符

索引操作符 | 描述 
--- | --- 
bit_hamming_ops | 汉明距离
bit_jaccard_ops | 杰卡德距离

#### sparsevec 索引操作符

索引操作符 | 描述 
--- | --- 
sparsevec_l2_ops | L2距离
sparsevec_ip_ops | 内积
sparsevec_cosine_ops | 余弦距离
sparsevec_l1_ops | L1距离

### 索引选项

- `m` - 每个图层最大连接数（默认为16），设置范围2~100，该参数值取决于数据集和应用场景。
- `ef_construction` - 用于图形构造的动态候选集大小（默认为64），设置范围4~1000，并且必须大于等于2*m。同时，为了保证搜索质量，ef_construction最好大于ef_search。

**示例1：** 使用L2距离计算创建HNSW索引并设置`m = 16, ef_construction = 64`。

```sql
openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64);
```

提高 `ef_construction` 和`m`可以提供更好的召回率，但会增加索引构建和插入时长。

### 查询选项

- `ef_search` - 查询时的动态候选集大小（默认为40），详情请参考[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/datavec_vector_engine_parameters.html)。

**示例2：** 设置当前会话中`ef_search=100`,并且通过l2距离查询TOP 10相似向量。

```sql 
openGauss=# SET hnsw_ef_search = 100;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

### 并发选项

- `parallel_workers` - 构建索引并行度（默认为0），设置范围1~32

**示例3：** 构建索引并行数设置为32。

```sql
openGauss=# ALTER TABLE items SET(parallel_workers=32);
```

>[!NOTE]说明
> hnsw构建图索引时会概率性出现某些数据点不可达的情况，这就导致索引查询返回数据量有误差。该情况和数据集、参数m、ef_construction有关，当增大m时会有改善。建议全量数据查找使用ivfflat索引，hnsw适用于limit远小于整体数据量的场景。

## IVFFlat

```sql
CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING ivfflat (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
WITH (lists = <LISTS>)
[TABLESPACE tablespace_name]
[COMMENT text]
[VISIBLE | INVISIBLE]
[WHERE predicate];
```

- `INDEX_NAME` - 索引名
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名
- TABLESPACE - 指定索引的表空间，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- COMMENT text - 指定索引的注释，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- VISIBLE|INVISIBLE - 指定索引是否可见，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- WHERE predicate - 创建一个部分索引，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同

### IVFFlat索引操作符

IVFFlat索引操作符`[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` - 向量类型
    - vector
    - bit

IVFFlat索引支持向量数据维度：

名称 | 维度限制
--- | --- 
vector | 2,000 
bit | 64,000 

- `DISTANCE_FUN` - 距离函数
    - l2
    - ip
    - cosine
    - hamming

#### vector 索引操作符

索引操作符 | 描述 
--- | --- 
vector_l2_ops | L2距离
vector_ip_ops | 内积
vector_cosine_ops | 余弦距离

#### bit 索引操作符

索引操作符 | 描述 
--- | --- 
bit_hamming_ops | 汉明距离

sparsevec不支持IVFFLAT索引

### 索引选项

- `lists` - 倒排表（单元格）聚类中心数量（默认为100），设置范围1~32768

**示例4：** 使用L2距离计算创建IVFFlat索引并设置lists = 200。

```sql
openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200);
```

>[!NOTE]说明
>
>优化召回率：
>
> - 在表中有数据后创建索引。
> - 选取适当的`lists`， 建议当数据集在100万行及以内：`rows / 1000`；超过100万行的数据：`sqrt(rows)`。
> - 查询时指定适当数量的probe，建议`sqrt(lists)`。

### 查询选项

- `probe` - 查询时候选集的大小（默认为1），详情请参考[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/datavec_vector_engine_parameters.html)。

**示例5：** 设置`probes`为10，并且通过l2距离查询TOP 10相似向量。
```sql
openGauss=# SET ivfflat_probes = 10;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

### 并发选项

- `parallel_workers` - 构建索引并行度（默认为0），设置范围1~32

**示例6：** 构建索引并行数设置为32。

```sql
openGauss=# ALTER TABLE items SET(parallel_workers=32);
```

## DISKANN

```sql
CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops)
WITH (index_size = <INDEX_SIZE>)
[TABLESPACE tablespace_name]
[COMMENT text]
[VISIBLE | INVISIBLE]
[WHERE predicate];
```

- `INDEX_NAME` - 索引名
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名
- TABLESPACE - 指定索引的表空间，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- COMMENT text - 指定索引的注释，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- VISIBLE|INVISIBLE - 指定索引是否可见，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同
- WHERE predicate - 创建一个部分索引，与[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/create_index.html)相同

### DISKANN索引操作符

DISKANN索引操作符`[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` - 向量类型
    - vector

DISKANN索引支持向量数据维度：

名称 | 维度限制
--- | ---
vector | 1,536

- `DISTANCE_FUN` - 距离函数
    - l2
    - ip
    - cosine

#### vector 索引操作符

索引操作符 | 描述
--- | ---
vector_l2_ops | L2距离
vector_ip_ops | 内积
vector_cosine_ops | 余弦距离

### 索引选项

- `index_size` - 索引构建参数，影响召回精度与构建时间，取值范围为16~1000（默认值为100），百万规模数据集建议设置为50
- `enable_pq` - 量化压缩参数，控制是否开启PQ，默认关闭
- `pq_m` - 量化压缩参数，取值范围为1~2000（默认值为8），建议设置为```dim / 8```

**示例7：** 使用L2距离计算创建DISKANN索引并设置index_size = 50。

```sql
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (index_size = 50);
```

>[!NOTE]说明
>
> - 建议先导入数据后创建索引。

### 查询选项

- `diskann_probes` - 查询时候选集的大小（默认为128），详情请参考[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/datavec_vector_engine_parameters.html)。

**示例8：** 设置`diskann_probes`为256,并且通过l2距离查询TOP 10相似向量。
```sql
openGauss=# SET diskann_probes = 256;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

### 并发选项

- `parallel_workers` - 构建索引并行度（默认为0），设置范围1~32

**示例9：** 构建索引并行数设置为32。

```sql
openGauss=# ALTER TABLE items SET(parallel_workers=32);
```

## 非向量化索引：Btree/Ubtree

向量数据类型同时也支持btree和ubtree索引构建，会跟随数据表存储类型自动选择。

```sql
CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
(COLUMN_NAME [TYPE]_ops) ;
```

### btree/ubtree 索引操作符

- vector_ops
- bit_ops
- sparsevec_ops

**示例10：** 构建btree索引。

```sql
openGauss=# CREATE INDEX ON t (val vector_ops);
```

>[!NOTE]说明
> Btree以及Ubtree索引不包含向量距离计算，只基于[向量函数和操作符](vector_functions_and_operators.md)中的向量比较。

## 并行构建向量索引

通过开启并行构建功能来加速向量索引的创建：

```sql
ALTER TABLE [TABLE_NAME] 
SET (parallel_workers = <CONCURRENCY_NUM>);
```

**示例11：** 构建索引并行数设置为8。

```sql
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

## 修改向量索引

仅支持修改向量索引选项

```sql
ALTER INDEX [INDEX_NAME]
SET (parameter=<OPTIONS>);
```

**示例12：** 修改向量索引选项

```sql
openGauss=# ALTER INDEX t_val_idx SET (m=24, ef_construction=200);
openGauss=# REINDEX INDEX t_val_idx;
```

注意：如果在进行增删改数据后重建索引报`ERROR: missing chunk number xx for toast value xx in pg_toast_xx`的错误，可能是因为数据库未及时清理更新/删除操作产生的死元组，导致reindex尝试访问已被回收的 TOAST 分块，这里需要`手动执行vacuum`后再使用reindex重建索引。

>[!NOTE]说明<br>
>修改向量索引参数后需使用`REINDEX`更新索引，并且REINDEX支持并行，并行度仍根据数据表中`parallel_workers`决定。<br>
>不支持修改索引操作符，如vector_l2_ops到vector_cosine_ops。<br>
>alter index修改索引选项时，HNSWPQ、IVFPQ索引无法校验PQ相关参数，在reindex时进行校验。

## 约束

- 向量索引仅支持普通行存表，临时表，Toast表，Unlogged表，段页式表等，暂不支持分区表，其他表仅支持对向量数据创建btree和ubtree索引。
- 若ALTER INDEX后不执行REINDEX，索引仍是原索引的选项配置，后插入的数据也会根据原索引选项构建索引，CREATE INDEX和REINDEX均不支持CONCURRENTLY在线构建。
- IVFFLAT、HNSW、DISKANN不支持ustore创建索引。
- 构建带有向量表时可以使用INDEX子句构建默认btree、ubtree索引，无法指定向量索引。
- 未指定向量列维度时无法构建向量索引，只支持构建btree、ubtree索引。
- 若在兼容B库中使用向量索引，需要执行`set dolphin.nulls_minimal_policy=false`，用于关闭nulls处理策略。
- 主备环境开启极致RTO情况下，不支持向量索引功能。
