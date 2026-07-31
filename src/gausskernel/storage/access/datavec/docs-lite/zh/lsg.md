# LSG

## 介绍

LSG(Local Scaling Grapgh)是一种局部缩放算法，可以提升HNSW索引的查询效率和召回率。本章节主要介绍openGauss数据库DataVec向量引擎LSG特性的使用步骤，以指导用户顺利完成操作。
>[!NOTE]**说明**
>LSG特性支持ARM/x86架构环境。<br>
>LSG特性暂时只支持HNSW索引。<br>
>LSG特性要求构建索引前插入一定量数据，否则会有报错提示。<br>
>LSG特性不支持和PQ以及RabitQ等量化方法一起使用，否则会有报错提示<br>
>兼容A/B/C/PG库。<br>
>支持并行构建。

## 安装准备

### 环境要求

LSG特性支持ARM和x86架构环境。

### 启用LSG特性

设置索引参数`enable_lsg = on`启用LSG特性

### 关闭LSG特性

设置索引参数`enable_lsg = off`关闭LSG特性

## 使用LSG

### HNSW-LSG

```
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING hnsw (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (m=<M>, ef_construction=<EF_CONSTRUCTION>, enable_lsg = on, lsg_degree=<REFINE_VALUE>, lsg_alpha=<REFINE_VALUE>);
```

- `INDEX_NAME` - 索引名称
- `TABLE_NAME` - 表名
- `COLUMN_NAME` - 向量数据列名

#### HNSW-LSG索引操作符

HNSW索引操作符`[TYPE]_[DISTANCE_FUN]_ops` 格式：

- `TYPE` - 向量类型
    - vector

HNSW-LSG索引支持向量数据维度：

名称 | 维度限制 
--- | --- 
vector | 2,000

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

#### 索引选项

- `m` - 每个图层最大连接数 2~100（默认为16）
- `ef_construction` - 用于图形构造的动态候选集大小 4~1000，必须大于等于2*m（默认为64）
- `enable_lsg` - 开启HNSW索引LSG构图（默认off）
- `lsg_degree` - 定义计算HNSW索引节点孤立度时选取的最近邻节点个数。类型：整数，范围[32，128]，默认为96。
- `lsg_alpha` - 定义HSNW索引LSG缩放的平滑度。类型：浮点数，范围[0, 3.0]，默认为2.0。

    **示例：** 使用L2距离创建HNSW-LSG索引，其中表items中向量为2000维vector数据类型。

    ```
    openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (enable_lsg=on, lsg_degree=96, lsg_alpha=2.0);
    ```

**设置建议：**

- 其余参数设置与[向量索引](./vector_index.md)中HNSW索引中相同。
