# PQ

## Introduction

This section describes how to install and use the PQ (Product Quantization) feature of the DataVec vector engine in the openGauss database, to help users complete the required operations. This feature combines the DataVec vector engine with a proprietary PQ algorithm to improve vector retrieval performance.
>[!NOTE]
>
>- The PQ feature currently supports only ARM environments.
>- The PQ feature currently supports only HNSW, IVF, and DISKANN indexes.
>- The PQ feature currently supports only the `vector` data type. Creating HNSWPQ, IVFPQ, or DISKANNPQ indexes for other vector data types will cause the operation to fail.
>- Data must be inserted before a PQ index is created. Creating a PQ index on an empty table will fail.
>- When the number of vectors in the table is smaller than `pq_ksub`, the PQ index can still be created successfully, but a warning is displayed indicating that the amount of data used to train the codebook is small and the recall rate may be low.
>- When an IVF-PQ index is created and the number of vectors in the table is smaller than the value of the `lists` index option, a warning is displayed indicating that the recall rate may be low.
>- PQ does not support creating indexes on ustore tables.
>- The PQ feature supports up to 100 million vectors.
>
## Installation Preparation

### Environment Requirements

The PQ feature supports only ARM environments.

### Installing the PQ Acceleration Package

The PQ acceleration package for IVFPQ and HNSWPQ is the same. DISKANNPQ requires a separate PQ package.
The PQ acceleration package currently does not support x86 environments.

(1) **PQ package for IVF and HNSW**<br>
The PQ retrieval acceleration package is installed by default in the third-party packages and binary installation packages for ARM environments. No additional configuration is required.

(2) **PQ package for DISKANN**<br>
**Prerequisite:** The environment must have the OpenBLAS package installed. You can install it by running `yum install openblas`.

Go to the [MindX SDK Community](https://www.hiascend.com/developer/download/community/result?module=sdk%2Bcann) website and download `Ascend-mindxsdk-mxindex_7.3.0_linux-aarch64.run`.

```bash
chmod +x Ascend-mindxsdk-mxindex*.run
./Ascend-mindxsdk-mxindex*.run --install
```

After installation, set the following environment variable:

```bash
export DATAVEC_PQ_LIB_PATH=/your_mxindex_install_path/mxIndex/lib
```

## Using PQ

### HNSW-PQ

```sql
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING hnsw (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (m=<M>, ef_construction=<EF_CONSTRUCTION>, enable_pq = on, pq_m = <PQ_M>, pq_ksub = <PQ_KSUB>);
```

- `INDEX_NAME` - index name.
- `TABLE_NAME` - table name.
- `COLUMN_NAME` - name of the vector data column.

#### HNSW-PQ Index Operators

HNSW index operators use the `[TYPE]_[DISTANCE_FUN]_ops` format.

- `TYPE` - vector type.
    - `vector`

HNSW-PQ indexes support the following vector dimensions:

| Name     | Dimension Limit |
| -------- | --------------: |
| `vector` |           2,000 |

- `DISTANCE_FUN` - distance function.
    - `l2`
    - `ip`
    - `cosine`

#### `vector` Index Operators

| Index Operator      | Description     |
| ------------------- | --------------- |
| `vector_l2_ops`     | L2 distance     |
| `vector_ip_ops`     | Inner product   |
| `vector_cosine_ops` | Cosine distance |

#### Index Options

- `m` - maximum number of connections per graph layer. The value ranges from 2 to 100. The default value is 16.
- `ef_construction` - size of the dynamic candidate set used to construct the graph. The value ranges from 4 to 1000 and must be greater than or equal to `2*m`. The default value is 64.
- `enable_pq` - enables PQ quantization and compression. The default value is `off`.
- `pq_m` - number of subspaces into which the vector is divided. The value ranges from 1 to 2000. The default value is 8. For high-dimensional vectors, the upper limit of `pq_m` is subject to the page size. An error may be reported when an index is created, indicating the maximum `pq_m` supported for the current vector dimension. Other restrictions on `pq_m` must also be considered when determining the final value.

 **Example:** Create an HNSW-PQ index using L2 distance when the vectors in the `items` table have 2,000 dimensions.

 ```sql
 openGauss=# CREATE INDEX ON items USING hnsw (embedding  vector_l2_ops) WITH (enable_pq=on, pq_m=2000);
 ERROR: vector and pqcode must on the same page, max pq_m is 72
 ```

For HNSW-PQ indexes, the maximum `pq_m` for a 2,000-dimensional vector is 72 due to the page size limit. Because `pq_m` must evenly divide the vector dimension, the maximum valid value of `pq_m` is `50`.

- `pq_ksub` - number of cluster centers for each subspace. The value ranges from 1 to 256. The default value is `256`.

**Recommendations:**

- `pq_m`: A larger number of subspaces provides higher accuracy but lower performance. Because HNSWPQ performs built-in reranking, increasing the number of subspaces may not significantly improve accuracy in some cases. The value must evenly divide the vector dimension; otherwise, the index cannot be created successfully. The recommended value is `dimension / 4`.
- `pq_ksub`: A larger number of cluster centers provides higher accuracy but lower performance. The recommended value is `256`.
- Set the remaining parameters in the same way as the HNSW index parameters described in [Vector Indexes](./vector_index.md).

#### GUC Parameters

- `hnsw_earlystop_threshold` - maximum number of consecutive iterations during graph search. The value ranges from 160 to `INT32_MAX - 1`. The default value is `INT32_MAX`.

 **Example:** Create an HNSW-PQ index using L2 distance with `m = 16`, `ef_construction = 64`, and `pq_m = 32`, and set `hnsw_earlystop_threshold` to 320.

 ```sql
 openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64, enable_pq=on,   pq_m=32);
 openGauss=# SET hnsw_earlystop_threshold = 320;
 ```

#### HNSW-PQ MMAP Beta Feature

##### Description

MMAP provides an efficient file access method and is particularly suitable for random reads in databases. All HNSW retrieval operations can be accelerated by enabling MMAP. This feature is currently in beta.

##### Usage

- `enable_mmap=on` - must be configured in the GUC file and takes effect after a restart.
- `hnsw_use_mmap=on` - session-level parameter.
- `use_mmap=true` - index creation option.
 **Example:** Create an HNSW-PQ index with MMAP enabled using L2 distance.

 ```sql
 openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (use_mmap=true);
 ```

>[!NOTE]  
> The MMAP feature currently supports only ARM environments.<br>
> The MMAP feature currently supports only HNSW indexes.<br>
> MMAP can currently load only one index. An index loaded with MMAP does not support DML operations. To switch the loaded index, restart the database.<br>

### IVF-PQ

```sql
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING ivfflat (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops)
WITH (lists=<LISTS>, enable_pq=on, pq_m=<PQ_M>, pq_ksub=<PQ_KSUB>, by_residual=on);
```

- `INDEX_NAME` - index name.
- `TABLE_NAME` - table name.
- `COLUMN_NAME` - name of the vector data column.

#### IVF-PQ Index Operators

IVFFLAT index operators use the `[TYPE]_[DISTANCE_FUN]_ops` format.

- `TYPE` - vector type.
  - `vector`

IVF-PQ indexes support the following vector dimensions:

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
- `enable_pq` - enables PQ quantization and compression. The default value is `off`.
- `pq_m` - valid only when `enable_pq` is enabled. Specifies the number of subspaces into which the vector is divided. The value ranges from 1 to 2000. The default value is 8. For high-dimensional vectors, the upper limit of `pq_m` is subject to the page size. An error may be reported when an index is created, indicating the maximum `pq_m` supported for the current vector dimension. Other restrictions on `pq_m` must also be considered when determining the final value.
- `pq_ksub` - valid only when `enable_pq` is enabled. Specifies the number of cluster centers for each subspace. The value ranges from 1 to 256. The default value is 256.
- `by_residual` - valid only when `enable_pq` is enabled. Enables residual computation. The default value is `off`.

 **Example:** Create an IVF-PQ index using L2 distance with residual computation enabled, and set `lists = 200`, `pq_m = 4`, and `pq_ksub = 256`.

 ```sql
 openGauss=# CREATE INDEX ON items USING ivfflat (embedding  vector_l2_ops) WITH (lists = 200,
 enable_pq = on, pq_m = 4, pq_ksub = 256, by_residual = on);
 ```

**Recommendations:**

- `pq_m`: A larger number of subspaces provides higher accuracy but lower performance. The value must evenly divide the vector dimension. The recommended value is `dimension / 4`.
- `pq_ksub`: A larger number of cluster centers provides higher accuracy but lower performance. The recommended value is `256`.
- `by_residual`: Enabling residual computation can improve accuracy but increases index construction time. The recommended value is `off`.
- Set the remaining parameters in the same way as the IVFFLAT index parameters described in [Vector Indexes](./vector_index.md).

#### Query Options

- `ivfflat_probes` - size of the candidate set during query processing. For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

  **Example:**

  ```sql
  openGauss=# SET ivfflat_probes = 10;
  ```

- `ivfpq_kreorder` - size of the candidate set used for reranking. For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

 **Example:**

 ```sql
 openGauss=# SET ivfpq_kreorder = 10;
 ```

### DISKANN-PQ

```sql
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops)
with (index_size = <INDEXSIZE>, enable_pq = on, pq_m = <PQ_M>);
```

- `INDEX_NAME` - index name.
- `TABLE_NAME` - table name.
- `COLUMN_NAME` - name of the vector data column.

#### DISKANN-PQ Index Operators

DISKANN index operators use the `[TYPE]_[DISTANCE_FUN]_ops` format.

- `TYPE` - vector type.
  - `vector`

DISKANN-PQ indexes support the following vector dimensions:

| Name     | Dimension Limit |
| -------- | --------------: |
| `vector` |           1,536 |

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

- `index_size` - index construction parameter that affects recall and index construction time. The value ranges from 16 to 1000. The default value is 100. A value of 50 is recommended for datasets containing approximately one million vectors.
- `enable_pq` - quantization and compression parameter that controls whether PQ is enabled. The default value is `off`.
- `pq_m` - quantization and compression parameter. The value ranges from 1 to 192. The default value is 8. The recommended value is `dimension / 8`.

 **Example:** Create a DISKANN-PQ index using L2 distance with `index_size = 16` and `pq_m = 2`.

 ```sql
 openGauss=# CREATE INDEX ON items USING diskann (embedding  vector_l2_ops) WITH (index_size = 16,
 enable_pq = on, pq_m = 2);
 ```

**Recommendations:**

- `pq_m`: A larger number of subspaces provides higher accuracy but lower performance. The value must evenly divide the vector dimension. The recommended value is `dimension / 8`.
- Set the remaining parameters in the same way as the DISKANN index parameters described in [Vector Indexes](./vector_index.md).

#### Query Options

- `diskann_probes` - size of the candidate set during query processing. The default value is 128. For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

 **Example:**

 ```sql
 openGauss=# SET diskann_probes = 10;
 ```
