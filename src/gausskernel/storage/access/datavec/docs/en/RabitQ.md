# RABITQ

## Introduction

RABITQ is a state-of-the-art binary quantization method with theoretical guarantees. When used with HNSW and IVFFLAT, it helps ensure reliable vector search even with highly compressed vector representations. This section describes how to use the RABITQ feature of the DataVec vector engine in the openGauss database, to guide users through the required operations. This feature combines the DataVec vector engine with the RABITQ algorithm to improve vector retrieval performance in memory-constrained environments.
> [!NOTE]
>
> - The RABITQ feature currently supports ARM and x86 architectures.
> - The RABITQ feature currently supports only HNSW and IVF indexes.
> - IVFFLAT-RABITQ currently supports only the `vector` data type, while HNSW-RABITQ supports the `vector` and `halfvec` data types. Creating HNSW-RABITQ or IVF-RABITQ indexes for other vector data types will cause index creation to fail.
> - When an IVF-RABITQ index is created and the amount of data in the table is smaller than the value of the `lists` index option, a warning is displayed indicating that recall may be low.
> - RABITQ cannot be used with PQ. HNSW-RABITQ does not support MMAP.
> - RABITQ supports segment-page tables, row-store tables, unlogged tables, and temporary tables, but does not support ustore tables, partitioned tables, column-store tables, or tamper-proof mode tables.
> - RABITQ is compatible with A, B, C, and PG databases.
> - Parallel index construction is supported.
>
## Installation Preparation

### Environment Requirements

The RABITQ feature supports ARM and x86 architectures.

### Dependencies

The RABITQ algorithm uses the OpenBLAS high-performance linear algebra library. This dependency must be installed in the runtime environment. Make sure that the OpenBLAS header files are installed in `/usr/include`, which is the default path in most environments.

```
yum install openblas-devel
```

### Enabling RABITQ

Set the index option `enable_rabitq = on` to enable the RABITQ feature.

### Disabling RABITQ

Set the index option `enable_rabitq = off` to disable the RABITQ feature.

## Using RABITQ

### HNSW-RABITQ

```
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING hnsw (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (m=<M>, ef_construction=<EF_CONSTRUCTION>, enable_rabitq = on, rabitq_refine_type=<REFINE_TYPE>, rabitq_fht=<on/off>);
```

- `INDEX_NAME` - index name.
- `TABLE_NAME` - table name.
- `COLUMN_NAME` - name of the vector data column.

#### HNSW-RABITQ Index Operators

HNSW index operators use the `[TYPE]_[DISTANCE_FUN]_ops` format.

- `TYPE` - vector type.
    - `vector`
    - `halfvec`

HNSW-RABITQ indexes support the following vector dimensions:

| Name      | Dimension Limit |
| --------- | --------------: |
| `vector`  |           2,000 |
| `halfvec` |           4,000 |

- `DISTANCE_FUN` - distance function.
    - `l2`
    - `ip`
    - `cosine`

#### Index Operators

| Index Operator       | Description                            |
| -------------------- | -------------------------------------- |
| `vector_l2_ops`      | L2 distance for the `vector` type      |
| `vector_ip_ops`      | Inner product for the `vector` type    |
| `vector_cosine_ops`  | Cosine distance for the `vector` type  |
| `halfvec_l2_ops`     | L2 distance for the `halfvec` type     |
| `halfvec_ip_ops`     | Inner product for the `halfvec` type   |
| `halfvec_cosine_ops` | Cosine distance for the `halfvec` type |

#### Index Options

- `m` - maximum number of connections per graph layer. The value ranges from 2 to 100. The default value is 16.
- `ef_construction` - size of the dynamic candidate set used to construct the graph. The value ranges from 4 to 1000 and must be greater than or equal to `2*m`. The default value is 64.
- `enable_rabitq` - enables RABITQ quantization and compression. The default value is `off`.
- `rabitq_refine_type` - data representation used for refinement when reranking is enabled. The type is string, and the valid values are `SQ8` and `FP32`. The default value is `none`. Enabling reranking generally improves recall but reduces QPS. `FP32` reranking provides better vector search accuracy and significantly reduces the storage space required for the index.
- `rabitq_fht` - specifies whether to apply an FHT-based random rotation to vectors. If this option is not set, RANDOM rotation is used by default. This rotation makes vectors more evenly distributed in the geometric space, thereby reducing collisions.

 **Example:** Create an HNSW-RABITQ index using L2 distance when the `items` table contains 2,000-dimensional vectors of the `vector` type.

 ```
 openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);
 ```

 **Example:** Create an HNSW-RABITQ index using L2 distance when the `items` table contains 4,000-dimensional vectors of the `halfvec` type.

 ```
 openGauss=# CREATE INDEX ON items USING hnsw (embedding halfvec_l2_ops) WITH (enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);
 ```

**Recommendations:**

- Set the remaining parameters in the same way as the HNSW index parameters described in [Vector Indexes](./vector_index.md).

#### GUC Parameters

- `hnsw_ef_search` - size of the dynamic candidate set used when scanning an HNSW index. For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).
- `rbq_sample_rows` - threshold for the number of data rows that triggers deferred index construction when data is inserted after the index is created. Because the RABITQ algorithm requires a certain amount of data to train the index, creating an index before data is inserted may affect vector search recall. This parameter can be used to adjust the threshold for triggering deferred index construction. This parameter does not need to be set when data is inserted before the index is created. The default value is 1,000. The valid range is `[1,000, 200,000,000]`.

 **Example:** Create an HNSW-RABITQ index using L2 distance with `m = 16` and `ef_construction = 64`, and set `rbq_sample_rows` to 2,000.

 ```
    openGauss=# SET rbq_sample_rows = 2000;
 openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64, enable_rabitq=on, rabitq_refine_type='FP32');
 ```

- `rbq_query_bits` - specifies whether to apply additional scalar quantization to query vectors. An appropriate value can improve recall. The default value is `8`. The valid range is `[1, 8]`.
- `rbq_refinek` - size of the candidate pool used for reranking. A larger value generally results in lower QPS but higher recall. The default value is `5`. The valid range is `[1, 1000]`.

    **Example:** Set `rbq_query_bits` to `8` and `rbq_refinek` to `10` for vector search.

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

- `INDEX_NAME` - index name.
- `TABLE_NAME` - table name.
- `COLUMN_NAME` - name of the vector data column.

#### IVF-RABITQ Index Operators

IVFFLAT index operators use the `[TYPE]_[DISTANCE_FUN]_ops` format.

- `TYPE` - vector type.
  - `vector`

IVF-RABITQ indexes support the following vector dimensions:

| Name     | Dimension Limit |
| -------- | --------------: |
| `vector` |           2,000 |

- `DISTANCE_FUN` - distance function.
  - `l2`
  - `ip`
  - `cosine`

#### `vector` Index Operators

| Index Operator      | Operator | Description     |
| ------------------- | -------- | --------------- |
| `vector_l2_ops`     | `<->`    | L2 distance     |
| `vector_ip_ops`     | `<#>`    | Inner product   |
| `vector_cosine_ops` | `<=>`    | Cosine distance |

#### Index Options

- `lists` - number of cluster centers for the inverted lists (cells). The default value is 100.
- `enable_rabitq` - enables RABITQ quantization and compression. The default value is `off`.
- `rabitq_refine_type` - data representation used for refinement when reranking is enabled. The type is string, and the valid values are `SQ8` and `FP32`. The default value is `none`. Enabling reranking generally improves recall but reduces QPS. `FP32` reranking provides better vector search accuracy and significantly reduces the storage space required for the index.
- `rabitq_fht` - specifies whether to apply an FHT-based random rotation to vectors. If this option is not set, RANDOM rotation is used by default. This rotation makes vectors more evenly distributed in the geometric space, thereby reducing collisions.

 **Example:** Create an IVFFLAT-RABITQ index using L2 distance when the `items` table contains 2,000-dimensional vectors of the `vector` type and `lists = 200`.

 ```
 openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists=200, enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);
 ```

**Recommendations:**

- Set the remaining parameters in the same way as the IVFFLAT index parameters described in [Vector Indexes](./vector_index.md).

#### Query Options

- `ivfflat_probes` - size of the candidate set during query processing. For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

 **Example:**

 ```
 openGauss=# SET ivfflat_probes = 10;
 ```

- `rbq_sample_rows` - threshold for the number of data rows that triggers deferred index construction when data is inserted after the index is created. Because the RABITQ algorithm requires a certain amount of data to train the index, creating an index before data is inserted may affect vector search recall. This parameter can be used to adjust the threshold for triggering deferred index construction. This parameter does not need to be set when data is inserted before the index is created. The default value is 1,000. The valid range is `[1,000, 200,000,000]`.

    **Example:** Create an IVFFLAT-RABITQ index using L2 distance with `lists = 200` and set `rbq_sample_rows` to 2,000.

    ```
    openGauss=# SET rbq_sample_rows = 2000;
    openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists=200, enable_rabitq=on, rabitq_refine_type='FP32');
    ```

- `rbq_query_bits` - specifies whether to apply additional scalar quantization to query vectors. An appropriate value can improve recall. The default value is `8`. The valid range is `[1, 8]`.
- `rbq_refinek` - size of the candidate pool used for reranking. A larger value generally results in lower QPS but higher recall. The default value is `5`. The valid range is `[1, 1000]`.

    **Example:** Set `rbq_query_bits` to `8` and `rbq_refinek` to `10` for vector search.

 ```
    openGauss=# SET rbq_query_bits = 8;
 openGauss=# SET rbq_refinek = 10;
    openGauss=# SELECT id FROM itrms ORDER BY val <-> '[1,2,3,4,5]';
 ```
