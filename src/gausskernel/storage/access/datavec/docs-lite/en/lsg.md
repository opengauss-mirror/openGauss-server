# LSG

## Introduction

Local scaling graph (LSG) is a local scaling algorithm that improves the query efficiency and recall rate of HNSW indexes. This section mainly introduces the usage steps of the LSG feature of the DataVec vector engine in the openGauss database, to guide users through the operations smoothly.
> [!NOTE] **Note**
> The LSG feature supports ARM/x86 architecture environments.<br>
> The LSG feature currently supports only HNSW indexes.<br>
> The LSG feature requires a certain amount of data to be inserted before building the index; otherwise, an error is reported.<br>
> The LSG feature cannot be used together with quantization methods such as PQ and RabitQ; otherwise, an error is reported.<br>
> Compatible with A/B/C/PG databases.<br>
> Supports parallel building.

## Installation Preparation

### Environment Requirements

The LSG feature supports ARM and x86 architecture environments.

### Enabling the LSG Feature

Set the index parameter `enable_lsg = on` to enable the LSG feature.

### Disabling the LSG Feature

Set the index parameter `enable_lsg = off` to disable the LSG feature.

## Using LSG

### HNSW-LSG

```
openGauss=# CREATE INDEX [INDEX_NAME] 
ON [TABLE_NAME] 
USING hnsw (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) 
with (m=<M>, ef_construction=<EF_CONSTRUCTION>, enable_lsg = on, lsg_degree=<REFINE_VALUE>, lsg_alpha=<REFINE_VALUE>);
```

- `INDEX_NAME` - Index name
- `TABLE_NAME` - Table name
- `COLUMN_NAME` - Vector data column name

#### HNSW-LSG Index Operator

The HNSW index operator `[TYPE]_[DISTANCE_FUN]_ops` format:

- `TYPE` - Vector type
    - vector

The HNSW-LSG index supports the following vector data dimensions:

Name | Dimension limit 
--- | --- 
vector | 2,000

- `DISTANCE_FUN` - Distance function
    - l2
    - ip
    - cosine

#### Index Operator

Index Operator | Description
--- | ---
vector_l2_ops | vector type - L2 distance
vector_ip_ops | vector type - inner product
vector_cosine_ops | vector type - cosine distance

#### Index Options

- `m` - Maximum number of connections per layer, 2~100 (default 16)
- `ef_construction` - Dynamic candidate set size for graph construction, 4~1000, must be greater than or equal to 2*m (default 64)
- `enable_lsg` - Enable LSG graph construction for the HNSW index (default off)
- `lsg_degree` - Defines the number of nearest neighbor nodes selected when calculating the isolation degree of HNSW index nodes. Type: integer, range [32, 128], default 96.
- `lsg_alpha` - Defines the smoothness of LSG scaling for the HNSW index. Type: float, range [0, 3.0], default 2.0.

    **Example:** Create an HNSW-LSG index using L2 distance, where the vectors in the items table are of the 2000-dimensional vector data type.

    ```
    openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (enable_lsg=on, lsg_degree=96, lsg_alpha=2.0);
    ```

**Configuration Recommendations:**

- Other parameters are configured the same as those for the HNSW index in [Vector Index](./vector_index.md).
