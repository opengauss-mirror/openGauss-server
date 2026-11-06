---
description: 本文介绍 openGauss GV_GRAPH向量索引 的相关内容，涵盖 功能介绍、编译指南、开始之前 等内容。
keywords: [openGauss, GV_GRAPH向量索引, 功能介绍, 编译指南, 开始之前]
---

# GV_GRAPH向量索引

## 功能介绍

GV_GRAPH是`gv_index`扩展提供的近似最近邻索引。它将向量组织为Vamana磁盘图，适合数据量较大、需要降低向量检索时延，并且能够接受近似结果的场景。

GV_GRAPH既可以用于ASTORE表，也可以用于USTORE表。索引默认在CPU上构建和查询；安装NPU版GaussVector组件后，可以用NPU加速构建阶段的图剪枝。NPU不参与查询，查询性能仍由CPU、磁盘和查询参数决定。

支持范围如下：

项目 | 支持范围
--- | ---
向量类型 | `vector`，向量列必须声明维度
索引维度 | 最大4096维。该限制仅适用于GV_GRAPH索引，不是`vector`数据类型的维度上限
距离度量 | L2距离、余弦距离
表存储格式 | ASTORE、USTORE
数据维护 | INSERT、UPDATE、DELETE、VACUUM

## 编译指南

1. 从[鲲鹏应用使能套件下载页面](https://www.hikunpeng.com/boostkit/download)下载与操作系统和硬件环境匹配的 `GaussVector_26.2.RC1***` 软件包，并解压。

2. 将解压后包含 `include` 和 `lib` 目录的 GaussVector 组件目录设置为环境变量 `VECTOR_HOME`。该目录下需要包含 `lib/libannlite.so`。例如：

   ```sh
   export VECTOR_HOME=/path/to/GaussVector-component
   ```

   `/path/to/GaussVector-component` 为示例路径，请替换为实际组件目录。使用 NPU 加速构建时，需要选择包含 NPU 组件的 GaussVector 软件包。

3. 按照[openGauss编译指南](https://docs.opengauss.org/zh/docs/latest/compilation_guide/compiling_the_version.html)准备编译环境和第三方依赖，然后在 openGauss-server 源码根目录执行 CMake 编译：

   ```sh
   sh build.sh -3rd 3rd_path --cmake
   ```

   `3rd_path` 为 openGauss 第三方依赖目录的实际路径，与 `VECTOR_HOME` 指向的 GaussVector 组件目录不同。CMake 会根据 `VECTOR_HOME` 自动编译 `gv_index` 扩展并安装所需的 GaussVector 动态库。

>[!NOTE]说明
>
>编译前必须在当前终端中设置 `VECTOR_HOME`。未设置该变量或目录不存在时，CMake 会跳过 `gv_index`；目录中缺少 `lib/libannlite.so` 时，编译会报错。

## 开始之前

安装环境需要包含GaussVector组件和`gv_index`扩展。连接需要创建索引的数据库后，执行：

```sql
CREATE EXTENSION IF NOT EXISTS gv_index;
SET enable_indexscan_optimization = on;
```

可以通过以下语句检查扩展和索引访问方法是否可用：

```sql
SELECT extname FROM pg_extension WHERE extname = 'gv_index';
SELECT amname FROM pg_am WHERE amname = 'gv_graph';
```

>[!NOTE]说明
>
>`gv_index`需要在每个使用GV_GRAPH的数据库中分别创建。`enable_indexscan_optimization`默认为`off`，并且只对当前会话生效；执行GV_GRAPH查询前，需要在当前会话中将其设置为`on`。

## 快速使用

### 创建ASTORE索引

以下示例创建一个ASTORE表和L2距离索引。`storage_type = auto`表示由GV_GRAPH识别表的存储格式，其余构建参数使用默认值。

```sql
CREATE TABLE items (
    id bigint PRIMARY KEY,
    embedding vector(3) NOT NULL
) WITH (storage_type = astore);

INSERT INTO items VALUES
    (1, '[0.1,0.2,0.3]'),
    (2, '[0.2,0.3,0.4]'),
    (3, '[0.8,0.7,0.6]');

CREATE INDEX items_embedding_gv_idx
ON items
USING gv_graph (embedding vector_l2_ops)
WITH (storage_type = auto);
```

按L2距离从近到远查询：

```sql
SET enable_indexscan_optimization = on;

SELECT /*+ indexscan(items items_embedding_gv_idx) */
       id,
       embedding <-> '[0.1,0.2,0.3]'::vector AS distance
FROM items
ORDER BY embedding <-> '[0.1,0.2,0.3]'::vector
LIMIT 10;
```

示例表的数据量很小，因此使用`indexscan` Hint明确指定索引，便于验证功能。实际业务中通常不需要添加Hint，由优化器根据数据量和代价选择执行计划。

可以使用`EXPLAIN`确认执行计划是否包含`items_embedding_gv_idx`：

```sql
EXPLAIN
SELECT /*+ indexscan(items items_embedding_gv_idx) */ id
FROM items
ORDER BY embedding <-> '[0.1,0.2,0.3]'::vector
LIMIT 10;
```

### 创建USTORE索引

USTORE表的建索引和查询方式相同，只需将表定义为USTORE。例如：

```sql
CREATE TABLE ustore_items (
    id bigint PRIMARY KEY,
    embedding vector(3) NOT NULL
) WITH (storage_type = ustore);

CREATE INDEX ustore_items_embedding_gv_idx
ON ustore_items
USING gv_graph (embedding vector_cosine_ops)
WITH (storage_type = auto);
```

余弦距离索引需要使用`vector_cosine_ops`操作符类，并在查询中使用`<=>`操作符：

```sql
SELECT id,
       embedding <=> '[0.1,0.2,0.3]'::vector AS distance
FROM ustore_items
ORDER BY embedding <=> '[0.1,0.2,0.3]'::vector
LIMIT 10;
```

支持的操作符类如下：

操作符类 | 查询操作符 | 距离度量
--- | --- | ---
`vector_l2_ops` | `<->` | L2距离
`vector_cosine_ops` | `<=>` | 余弦距离

建索引时选择的操作符类必须与查询中的距离操作符匹配，否则不能使用该索引。

## 设置构建参数

创建索引时可以在`WITH`子句中设置以下参数：

参数 | 取值范围 | 默认值 | 使用说明
--- | --- | --- | ---
`storage_type` | `auto`、`astore`、`ustore` | `auto` | 建议使用`auto`。显式设置`astore`或`ustore`时，取值必须与表的存储格式一致。
`quantization_type` | `lvq`、`pq` | `lvq` | 向量量化方式。先使用默认值；需要减小索引空间时，可使用业务数据测试`pq`的召回率后再决定是否切换。
`graph_degree` | 8\~256 | 96 | 每个图节点的连接规模。增大取值通常有利于召回率，但会增加构建开销和索引空间。
`subgraph_count` | 1\~64 | 1 | 子图数量。取值大于1，并且`enable_vector_copy`和`build_with_quantized_vector`均为`false`时，系统会将`build_with_quantized_vector`按`true`处理并输出WARNING。
`num_parallels` | 1\~64 | 32 | 构建索引使用的最大线程数。应根据可用CPU和同时运行的业务负载设置。
`prune_alg` | `vamana`、`npu_knn` | `vamana` | 图剪枝算法。`vamana`使用CPU；`npu_knn`使用NPU，并且仅支持带NPU组件的单机版本。
`enable_vector_copy` | `true`、`false` | `false` | 是否在构建过程中复制原始向量。一般保持默认值。
`build_with_quantized_vector` | `true`、`false` | `false` | 是否使用量化向量建图。开启后需要使用业务数据验证召回率。

这些参数决定索引的物理结构，在`CREATE INDEX`或`REINDEX`时生效。调整参数后，需要重建索引才能使新配置应用到索引数据。

## 使用NPU加速构建

当单机安装环境包含NPU版GaussVector组件时，可以将`prune_alg`设置为`npu_knn`，使用NPU完成构建阶段的图剪枝。CPU版组件或多节点版本不支持该取值，应使用默认的`vamana`。

使用前，需要在构建索引的会话中设置`enable_npu = on`，该参数默认为`off`。下面的示例选择0号和1号NPU，并将一次索引构建可使用的NPU显存限制为8GB：

```sql
SET enable_npu = on;
SET npu_device_mask = 3;
SET npu_device_max_mem = '8GB';

CREATE INDEX items_embedding_gv_npu_idx
ON items
USING gv_graph (embedding vector_l2_ops)
WITH (
    storage_type = auto,
    prune_alg = npu_knn,
    graph_degree = 96,
    num_parallels = 32
);
```

`npu_device_mask`是十进制位掩码，默认值为255，对应0至7号NPU。使用前应根据实际可用设备设置该参数；例如，`1`表示0号NPU，`3`表示0号和1号NPU。`npu_device_max_mem`的默认值为1GB，取值范围为2MB\~1TB。

这两个参数均为会话级参数，应在执行`CREATE INDEX`或`REINDEX`的会话中设置。详细说明请参见[`npu_device_mask`](https://docs.opengauss.org/zh/docs/latest/database_reference/datavec_vector_engine_parameters.html#npu_device_mask)和[`npu_device_max_mem`](https://docs.opengauss.org/zh/docs/latest/database_reference/datavec_vector_engine_parameters.html#npu_device_max_mem)。

>[!NOTE]说明
>
>NPU仅加速索引构建阶段的图剪枝。设置`prune_alg = npu_knn`不会改变查询的执行设备，也不会直接提升查询性能。

## 调整查询效果

`gv_graph_nprobes`控制一次索引扫描最多返回给执行器的候选记录数，默认值为128，取值范围为1\~65535。提高候选数通常能够改善召回率，但也会增加查询耗时。

该参数至少应设置为不小于SQL中的`LIMIT`。查询还有其他`WHERE`过滤条件时，可以适当增加候选数，避免候选记录在后续过滤后不足。

```sql
SET enable_indexscan_optimization = on;
SET gv_graph_nprobes = 256;

SELECT id,
       embedding <-> '[0.1,0.2,0.3]'::vector AS distance
FROM items
ORDER BY embedding <-> '[0.1,0.2,0.3]'::vector
LIMIT 100;
```

如果执行计划没有使用GV_GRAPH索引，请依次检查：

- 当前会话是否已设置`enable_indexscan_optimization = on`。
- 查询是否包含向量距离`ORDER BY`表达式。
- 查询操作符是否与索引的操作符类匹配。
- 查询向量的维度是否与向量列定义一致。

## 维护索引

- 执行INSERT、UPDATE或DELETE时，GV_GRAPH会同步维护索引，不需要手工刷新。
- 定期对ASTORE或USTORE表执行`VACUUM`，清理索引中的无效记录。
- 调整构建参数后，使用`REINDEX INDEX INDEX_NAME`重建索引。

## 使用限制

- 只支持ASTORE和USTORE行存表。
- 只支持单个`vector`列，不支持多列索引、`INCLUDE`列和唯一索引。
- 向量列必须声明维度，GV_GRAPH最大支持4096维。
- NULL向量不会写入索引。业务不需要NULL值时，建议将向量列定义为`NOT NULL`。
- 只支持L2距离和余弦距离，不支持内积操作符类。
- 索引扫描必须包含向量距离`ORDER BY`表达式。
- NPU剪枝只支持带NPU组件的单机版本，并且只加速索引构建，不加速查询。
