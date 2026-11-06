---
description: This topic describes the openGauss GV_GRAPH vector index, including its features, compilation instructions, and prerequisites.
keywords: [openGauss, GV_GRAPH Vector Index, Features, Compilation Guide, Prerequisites]
---

# GV_GRAPH Vector Index

## Features

GV_GRAPH is an approximate nearest neighbor index provided by the `gv_index` extension. It organizes vectors into a Vamana graph stored on disk and is suitable for large datasets that require low vector search latency and can tolerate approximate results.

GV_GRAPH supports both ASTORE and USTORE tables. Indexes are built and queried on the CPU by default. With the NPU edition of the GaussVector component installed, you can use NPUs to accelerate graph pruning during index construction. NPUs do not participate in queries; query performance still depends on the CPU, disk, and query parameters.

Supported capabilities are as follows:

Item | Support
--- | ---
Vector type | `vector`; the vector column must declare its dimensions.
Index dimensions | Up to 4096 dimensions. This limit applies only to GV_GRAPH indexes, not to the `vector` data type.
Distance metrics | L2 distance and cosine distance
Table storage formats | ASTORE and USTORE
Data maintenance | INSERT, UPDATE, DELETE, and VACUUM

## Compilation Guide

1. Download the `GaussVector_26.2.RC1***` package that matches your operating system and hardware from the [Kunpeng BoostKit download page](https://www.hikunpeng.com/boostkit/download), and extract it.

2. Set the `VECTOR_HOME` environment variable to the extracted GaussVector component directory that contains the `include` and `lib` directories. The directory must contain `lib/libannlite.so`. For example:

   ```sh
   export VECTOR_HOME=/path/to/GaussVector-component
   ```

   Replace `/path/to/GaussVector-component` with the actual component directory. To accelerate index construction with NPUs, select a GaussVector package that includes the NPU component.

3. Prepare the build environment and third-party dependencies as described in the [openGauss Compilation Guide](https://docs.opengauss.org/en/docs/latest/compilation_guide/compiling_the_version.html), and run the following CMake build command from the root directory of the openGauss-server source code:

   ```sh
   sh build.sh -3rd 3rd_path --cmake
   ```

   Replace `3rd_path` with the actual path to the openGauss third-party dependency directory. This is separate from the GaussVector component directory specified by `VECTOR_HOME`. CMake automatically builds the `gv_index` extension and installs the required GaussVector shared libraries based on `VECTOR_HOME`.

>[!NOTE]
>
>Set `VECTOR_HOME` in the current terminal before compiling. If the variable is unset or the directory does not exist, CMake skips `gv_index`. If `lib/libannlite.so` is missing from the directory, the build fails.

## Prerequisites

The installation must include the GaussVector component and the `gv_index` extension. Connect to the database where you want to create the index, and run:

```sql
CREATE EXTENSION IF NOT EXISTS gv_index;
SET enable_indexscan_optimization = on;
```

Use the following statements to check whether the extension and index access method are available:

```sql
SELECT extname FROM pg_extension WHERE extname = 'gv_index';
SELECT amname FROM pg_am WHERE amname = 'gv_graph';
```

>[!NOTE]
>
>Create `gv_index` separately in each database that uses GV_GRAPH. `enable_indexscan_optimization` defaults to `off`, and the setting applies only to the current session. Set it to `on` in the current session before running GV_GRAPH queries.

## Quick Start

### Creating an ASTORE Index

The following example creates an ASTORE table and an L2 distance index. With `storage_type = auto`, GV_GRAPH detects the table storage format. The other build parameters use their default values.

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

Query the results in ascending order of L2 distance:

```sql
SET enable_indexscan_optimization = on;

SELECT /*+ indexscan(items items_embedding_gv_idx) */
       id,
       embedding <-> '[0.1,0.2,0.3]'::vector AS distance
FROM items
ORDER BY embedding <-> '[0.1,0.2,0.3]'::vector
LIMIT 10;
```

The example table contains very little data, so the `indexscan` hint explicitly selects the index to help verify the feature. In production, hints are usually unnecessary: the optimizer selects the execution plan based on the data volume and estimated cost.

Use `EXPLAIN` to check whether the execution plan includes `items_embedding_gv_idx`:

```sql
EXPLAIN
SELECT /*+ indexscan(items items_embedding_gv_idx) */ id
FROM items
ORDER BY embedding <-> '[0.1,0.2,0.3]'::vector
LIMIT 10;
```

### Creating a USTORE Index

The index creation and query procedures are the same for USTORE tables. Define the table as a USTORE table, as shown below:

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

A cosine distance index requires the `vector_cosine_ops` operator class and the `<=>` operator in queries:

```sql
SELECT id,
       embedding <=> '[0.1,0.2,0.3]'::vector AS distance
FROM ustore_items
ORDER BY embedding <=> '[0.1,0.2,0.3]'::vector
LIMIT 10;
```

The following operator classes are supported:

Operator Class | Query Operator | Distance Metric
--- | --- | ---
`vector_l2_ops` | `<->` | L2 distance
`vector_cosine_ops` | `<=>` | Cosine distance

The operator class selected when creating the index must match the distance operator used in the query. Otherwise, the query cannot use the index.

## Configuring Build Parameters

You can set the following parameters in the `WITH` clause when creating an index:

Parameter | Value Range | Default | Description
--- | --- | --- | ---
`storage_type` | `auto`, `astore`, `ustore` | `auto` | `auto` is recommended. If you explicitly set `astore` or `ustore`, the value must match the table storage format.
`quantization_type` | `lvq`, `pq` | `lvq` | Vector quantization method. Start with the default value. To reduce index storage requirements, test the recall of `pq` with your application data before deciding whether to switch.
`graph_degree` | 8\~256 | 96 | Number of connections for each graph node. Increasing the value generally improves recall but increases construction overhead and index size.
`subgraph_count` | 1\~64 | 1 | Number of subgraphs. If the value is greater than 1 and both `enable_vector_copy` and `build_with_quantized_vector` are `false`, the system treats `build_with_quantized_vector` as `true` and issues a WARNING.
`num_parallels` | 1\~64 | 32 | Maximum number of threads used to build the index. Set this value based on available CPU resources and concurrent application workloads.
`prune_alg` | `vamana`, `npu_knn` | `vamana` | Graph pruning algorithm. `vamana` uses the CPU. `npu_knn` uses NPUs and is supported only in single-node deployments with the NPU component.
`enable_vector_copy` | `true`, `false` | `false` | Whether to copy the original vectors during construction. The default value is generally appropriate.
`build_with_quantized_vector` | `true`, `false` | `false` | Whether to build the graph using quantized vectors. If enabled, validate recall with your application data.

These parameters determine the physical structure of the index and take effect during `CREATE INDEX` or `REINDEX`. After changing parameters, rebuild the index to apply the new configuration to the index data.

## Accelerating Index Construction with NPUs

If a single-node installation includes the NPU edition of the GaussVector component, you can set `prune_alg` to `npu_knn` to use NPUs for graph pruning during construction. The CPU edition of the component and multi-node deployments do not support this value; use the default `vamana` instead.

Set `enable_npu = on` in the session used to build the index; this parameter defaults to `off`. The following example selects NPUs 0 and 1 and limits the NPU device memory available to one index construction operation to 8 GB:

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

`npu_device_mask` is a bitmask expressed in decimal. Its default value is 255, corresponding to NPUs 0 through 7. Set this parameter based on the devices actually available before use. For example, `1` selects NPU 0, and `3` selects NPUs 0 and 1. `npu_device_max_mem` defaults to 1 GB and accepts values from 2 MB to 1 TB.

Both are session parameters. Set them in the session that runs `CREATE INDEX` or `REINDEX`. For details, see [`npu_device_mask`](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html#npu_device_mask) and [`npu_device_max_mem`](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html#npu_device_max_mem).

>[!NOTE]
>
>NPUs accelerate only graph pruning during index construction. Setting `prune_alg = npu_knn` does not change the device used to execute queries or directly improve query performance.

## Tuning Queries

`gv_graph_nprobes` controls the maximum number of candidate records returned to the executor by a single index scan. It defaults to 128 and accepts values from 1 to 65535. Increasing the number of candidates generally improves recall but also increases query time.

Set this parameter to at least the SQL `LIMIT` value. If the query includes other `WHERE` conditions, consider increasing the number of candidates to ensure that enough records remain after filtering.

```sql
SET enable_indexscan_optimization = on;
SET gv_graph_nprobes = 256;

SELECT id,
       embedding <-> '[0.1,0.2,0.3]'::vector AS distance
FROM items
ORDER BY embedding <-> '[0.1,0.2,0.3]'::vector
LIMIT 100;
```

If the execution plan does not use the GV_GRAPH index, check the following in order:

- The current session has `enable_indexscan_optimization = on`.
- The query contains an `ORDER BY` expression for vector distance.
- The query operator matches the index operator class.
- The query vector dimensions match the vector column definition.

## Maintaining Indexes

- GV_GRAPH maintains the index when INSERT, UPDATE, or DELETE operations run. Manual refreshes are unnecessary.
- Run `VACUUM` periodically on ASTORE or USTORE tables to remove invalid index records.
- After adjusting build parameters, run `REINDEX INDEX INDEX_NAME` to rebuild the index.

## Restrictions

- Only ASTORE and USTORE row-store tables are supported.
- Only a single `vector` column is supported. Multicolumn indexes, `INCLUDE` columns, and unique indexes are not supported.
- The vector column must declare its dimensions. GV_GRAPH supports up to 4096 dimensions.
- NULL vectors are not added to the index. If your application does not need NULL values, define the vector column as `NOT NULL`.
- Only L2 distance and cosine distance are supported. Inner product operator classes are not supported.
- Index scans require an `ORDER BY` expression for vector distance.
- NPU pruning is supported only in single-node deployments with the NPU component. It accelerates index construction, but does not accelerate queries.
