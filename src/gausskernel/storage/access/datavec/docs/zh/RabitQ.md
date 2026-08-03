# RABITQ

## 介绍

RABITQ是一种具有理论保证的最先进的二进制量化方法，与HNSW、IVFFLAT结合使用时，可以确保向量数据在高度压缩的表示下仍能保持搜索的可靠性。本章节主要介绍openGauss数据库DataVec向量引擎RABITQ特性的使用步骤，以指导用户顺利完成操作。本特性将DataVec向量引擎和RABITQ算法相结合，以提高在内存受限的情况下向量检索的查询性能。
>[!NOTE]**说明**
>
>RABITQ特性支持ARM/x86架构环境。<br>
>RABITQ特性暂时只支持HNSW和IVF索引。<br>
>IVFFLAT-RABITQ特性暂时只支持vector数据类型，HNSW-RABITQ支持vector和halfvec类型。在其他向量数据类型构建HNSW-RABITQ以及IVF-RABITQ索引会报错。<br>
>创建IVF-RABITQ索引时，如果表数据量小于索引选项lists，会提示召回率低。<br>
>RABITQ不支持与PQ一起使用，HNSW-RABITQ不支持mmap。<br>
>RABITQ支持段页式表、行存表、非日志表、临时表，不支持ustore表、分区表、列存表、防篡改模式表。<br>
>兼容A/B/C/PG库。<br>
>支持并行构建。

## 安装准备

### 环境要求

RABITQ特性支持ARM和x86架构环境。

### 依赖包

RABITQ算法使用到openblas高性能线性代数库，需要在运行环境中安装该依赖包。安装时确保openblas的头文件安装在/usr/include中（该路径一般是默认路径）。

```
yum install openblas-devel
```

### 启用RABITQ特性

设置索引参数`enable_rabitq = on`启用RABITQ特性

### 关闭RABITQ特性

设置索引参数`enable_rabitq = off`关闭RABITQ特性

## 使用RABITQ

### HNSW-RABITQ

```
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING hnsw (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (m=<M>, ef_construction=<EF_CONSTRUCTION>, enable_rabitq = on, rabitq_refine_type=<REFINE_TYPE>, rabitq_fht=<on/off>);
```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

#### HNSW-RABITQ索引操作符

HNSW索引操作符`[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` - 向量类型
    - vector
    - halfvec

HNSW-RABITQ索引支持向量数据维度：

名称 | 维度限制 
--- | --- 
vector | 2,000
halfvec | 4,000

- `DISTANCE_FUN` - 距离函数
    - l2
    - ip
    - cosine

#### 索引操作符

索引操作符 | 描述 
--- | --- 
vector_l2_ops | vector类型-L2距离
vector_ip_ops | vector类型-内积
vector_cosine_ops | vector类型-余弦距离
halfvec_l2_ops | halfvec类型-L2距离
halfvec_ip_ops | halfvec类型-内积
halfvec_cosine_ops | halfvec类型-余弦距离

#### 索引选项

- `m` - 每个图层最大连接数 2~100（默认为16）
- `ef_construction` - 用于图形构造的动态候选集大小 4~1000，必须大于等于2*m（默认为64）
- `enable_rabitq` - 开启rabitq量化压缩（默认off）
- `rabitq_refine_type` - 定义启用refine精排时用于细化的数据表示。类型：字符串，范围[SQ8，FP32]，默认为none。一般选择精排会使召回率增大，qps下降。其中FP32类型的精排会使向量查询有更好的效果，并且索引所需存储空间大大减小。
- `rabitq_fht` - 定义是否对向量使用fht随机旋转。如果不设置默认使用RANDOM随机旋转。它可以使向量在几何空间中分布更加均匀，从而避免碰撞。

 **示例：** 使用L2距离创建HNSW-RABITQ索引，其中表items中向量为2000维vector数据类型。

 ```
 openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);
 ```

    **示例：** 使用L2距离创建HNSW-RABITQ索引，其中表items中向量为4000维halfvec数据类型。

 ```
 openGauss=# CREATE INDEX ON items USING hnsw (embedding halfvec_l2_ops) WITH (enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);
 ```

**设置建议：**

- 其余参数设置与[向量索引](./vector_index.md)中HNSW索引中相同。

#### GUC参数

- `hnsw_ef_search`  - 使用HNSW索引扫描时的动态候选集大小。参见[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest/database_reference/datavec_vector_engine_parameters.html)。
- `rbq_sample_rows` - 如果是先构建索引再插入数据的场景，可以通过该参数设置触发延迟索引的数据行数。由于rabitq算法构建索引时需要一定的数据进行训练，如果无数据创建索引会影响向量查询召回率，可以通过该参数调整延迟索引的阈值条件。如果是先插入数据再构建索引则无需设置。默认1000，范围[1000-20,0000,0000]

 **示例：** 使用L2距离计算创建HNSW-RABITQ索引并设置`m = 16, ef_construction = 64`，并设置`rbq_sample_rows`为2000。

 ```
    openGauss=# SET rbq_sample_rows = 2000;
 openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64, enable_rabitq=on, rabitq_refine_type='FP32');
 ```

- `rbq_query_bits` - 设置是否对查询向量进行额外的标量量化，适当设置可以提升召回率。默认为8，范围[1-8]。
- `rbq_refinek` - 选择精排的候选池范围，一般值越高，qps越低，但召回率越高。默认5，范围[1-1000]

    **示例：** 设置`rbq_query_bits`为8，`rbq_refinek`为10，进行向量查询。

 ```
    openGauss=# SET rbq_query_bits = 8;
 openGauss=# SET rbq_refinek = 10;
    openGauss=# SELECT id FROM itrms ORDER BY val <-> '[1,2,3,4,5]';
 ```

### IVF-RABITQ

```
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING ivfflat (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops)
with (lists = <LISTS>, enable_rabitq = on, rabitq_refine_type=<REFINE_TYPE>, rabitq_fht=<on/off>);
```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

#### IVF-RABITQ索引操作符

IVFFLAT索引操作符 `[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` -向量类型
  - vector

IVF-RABITQ索引执行向量数据维度：

名称 | 维度限制 
--- | --- 
 vector | 2,000 

- `DISTANCE_FUN` - 距离函数
  - l2
  - ip
  - cosine

#### vector索引操作符

索引操作符 | operator | 描述
--- | --- | ---
vector_l2_ops | <->|L2距离
vector_ip_ops | <#>|内积
vector_cosine_ops|<=>|余弦距离

#### 索引选项

- `lists` - 倒排表（单元格）聚类中心数量（默认为100）
- `enable_rabitq` - 开启rabitq量化压缩（默认off）
- `rabitq_refine_type` - 定义启用refine精排时用于细化的数据表示。类型：字符串，范围[SQ8，FP32]，默认为none。一般选择精排会使召回率增大，qps下降。其中FP32类型的精排会使向量查询有更好的效果，并且索引所需存储空间大大减小。
- `rabitq_fht` - 定义是否对向量使用fht随机旋转。如果不设置默认使用RANDOM随机旋转。它可以使向量在几何空间中分布更加均匀，从而避免碰撞。

 **示例：** 使用L2距离创建IVFFLAT-RABITQ索引，其中表items中向量为2000维vector数据类型，聚类中心lists=200。

 ```
 openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists=200, enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);
 ```

**设置建议：**

- 其余参数设置与[向量索引](./vector_index.md)中IVFFLAT索引相同。

#### 查询选项

- `ivfflat_probe` - 查询时候选集的大小，参见[DataVec向量引擎参数](https://docs.opengauss.org/zh/docs/latest/database_reference/datavec_vector_engine_parameters.html)。

 **示例：**

 ```
 openGauss=# SET ivfflat_probes = 10;
 ```

- `rbq_sample_rows` - 如果是先构建索引再插入数据的场景，可以通过该参数设置触发延迟索引的数据行数。由于rabitq算法构建索引时需要一定的数据进行训练，如果无数据创建索引会影响向量查询召回率，可以通过该参数调整延迟索引的阈值条件。如果是先插入数据再构建索引则无需设置。默认1000，范围[1000-20,0000,0000]

    **示例：** 使用L2距离计算创建IVFFLAT-RABITQ索引并设置`lists=200`，并设置`rbq_sample_rows`为2000。

    ```
    openGauss=# SET rbq_sample_rows = 2000;
    openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists=200, enable_rabitq=on, rabitq_refine_type='FP32');
    ```

- `rbq_query_bits` - 设置是否对查询向量进行额外的标量量化，适当设置可以提升召回率。默认为8，范围[1-8]。
- `rbq_refinek` - 选择精排的候选池范围，一般值越高，qps越低，但召回率越高。默认5，范围[1-1000]

    **示例：** 设置`rbq_query_bits`为8，`rbq_refinek`为10，进行向量查询。

 ```
    openGauss=# SET rbq_query_bits = 8;
 openGauss=# SET rbq_refinek = 10;
    openGauss=# SELECT id FROM itrms ORDER BY val <-> '[1,2,3,4,5]';
 ```
