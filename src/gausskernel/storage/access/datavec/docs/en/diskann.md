# DiskANN

## 1. Introduction

With the widespread adoption of AI applications such as recommendation systems, image recognition, and natural language processing, traditional databases struggle to meet the real-time similarity search challenges of massive high-dimensional vectors. As a disk-based approximate nearest neighbor search technology, DiskANN can effectively reduce memory consumption while maintaining high query performance, meeting enterprises' demands for efficient management of large-scale vector data.

>![]() **Support and limitations:<br>**
>DiskANN currently supports only the vector data type; using other vector data types will cause execution failure. The default and PQ formats support up to 1536 dimensions; enabling `enable_rabitq` supports up to the vector type limit of 16000 dimensions.<br>
>DiskANN is currently compatible only with A\B\C\PG databases.<br>
>DiskANN supports vector data storage in ordinary row-store tables, temporary tables, Toast tables, unlogged tables, and segment-page tables.<br>
>DiskANN supports PQ quantization compression and parallel index building.<br>

## 2. Index Construction

The DiskANN index is an efficient large-scale vector approximate nearest neighbor search solution that uses the Vamana algorithm. Its core goal is to process large-scale data on a single machine while maintaining high recall, low query latency, and low memory usage. The implementation principles are as follows:

- Graph-based index structure (Vamana algorithm)

    Compared with traditional graph algorithms (such as HNSW and NSG), the Vamana graph index optimizes graph construction by dynamically adjusting the parameter α (α≥1), so that it reduces the search path length while maintaining high recall.

- Hybrid storage architecture

    Memory: stores compressed vectors (such as PQ quantization encoding) and graph index metadata for fast candidate set filtering.

    SSD: stores the complete original vectors and detailed graph structure to ensure high-precision distance calculation.

## 3. Index Retrieval

The DiskANN retrieval process is primarily based on the constructed index graph structure. It uses a greedy search strategy starting from the entry point and gradually approaches the target vector. The core flow is as follows:

- Initialize the search state

    Read the basic index parameters (such as dim, frozenPoint, etc.) from MetaPage

- Obtain the current node

    Select the unvisited node that is closest to the query vector from the search list

- Visit neighbor nodes

    Load the node's neighbor list (obtained from EdgePage) and calculate the distance between each neighbor and the query vector

- Update the search list

    Add unvisited neighbors to the search list

    If the search list size exceeds diskann_probes, keep the diskann_probes nodes with the shortest distances

- Iterative search

    Repeat the above steps until the distances of all nodes in the search list are no less than the minimum distance among the visited nodes, or the maximum number of iterations is reached

## 4. Using DiskANN

### Create Index (with PQ Disabled)

When PQ is disabled, the DiskANN index uses exact distance calculation by default.

```
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) ;
with (index_size = 100);
```

- `INDEX_NAME` - index name
- `TABLE_NAME` - table name
- `COLUMN_NAME` - vector data column name

### Create an Index (PQ Enabled Scenario)

In the PQ enabled scenario, the DiskANN index uses PQ distance calculation and then performs reranking through exact distance calculation. Before using PQ, you need to load the dynamic library first.

```
openGauss=# CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING diskann (COLUMN_NAME [TYPE]_[DISTANCE_FUN]_ops) ;
with (enable_pq = on, pq_m = 8);
```

- `INDEX_NAME` - index name
- `TABLE_NAME` - table name
- `COLUMN_NAME` - vector data column name

### Index Operators Supported by DiskANN

Index Operator | Operator | Description
--- |--- |---
vector_l2_ops | <-> |L2 distance
vector_ip_ops | <#> |Inner product
vector_cosine_ops | <=> |Cosine distance

### Index Options

- `index_size` - Index build parameter that affects recall accuracy and build time. The value range is 16 to 1000 (default 100). For datasets of one million scale, it is recommended to set it to 50.
- `enable_pq` - Quantization compression parameter that controls whether PQ is enabled. It is disabled by default.
- `pq_m` - Quantization compression parameter. The value range is 1 to 192 (default 8). It is recommended to set it to ```dim / 8```.

**Example:** Create a DiskANN index using L2 distance calculation with residuals.

```
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (index_size = 100, enable_pq = on, pq_m = 8);
```

### Parallel Build of Vector Index

Speed up vector index creation by enabling the parallel build feature:

```
ALTER TABLE [TABLE_NAME]
SET (parallel_workers = <CONCURRENCY_NUM>);
```

**Example:** Set the parallel build degree of the index to 8.

```
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

### Query Options

- `diskann_probes` - The size of the candidate set during query (default is 128).

```
openGauss=# SET diskann_probes = 64;
```

- `enable_seqscan` - Use a non-vector index during query (default on)

```
openGauss=# SET enable_seqscan = off;
```

### Querying with an Index

```
openGauss=# SELECT * FROM [TABLE_NAME] ORDER BY [COLUMN_NAME] [operator] [VALUE];
```

- `TABLE_NAME` - table name
- `COLUMN_NAME` - column name
- `operator` - distance calculation operator, which must be the same as the distance calculation method used when creating the index
- `VALUE` - the query vector
**Example:** Sort by l2 distance in ascending order to query all vectors in the `embedding` column of the `items` table that are similar to the vector [1,2,3,4].

```
openGauss=# SELECT * FROM items ORDER BY embedding <-> '[1,2,3,4]';
```

### Add, Delete, and Update

Before performing DELETE/UPDATE, you need to enable the `immediate_delete` option on the main table.
If this option is not enabled, the edge relationships of the graph will not be deleted synchronously, which may cause the graph size to increase.
After enabling this option, DELETE/UPDATE operations will be updated to the graph relationships in real time.
You can specify this option when creating the table:

```sql
CREATE TABLE [table_name] (cols) WITH (immediate_delete = on);
```

Or modify it after creation:

```sql
ALTER TABLE [TABLE_NAME] SET (immediate_delete = on);
```

## 5. Constraints

- Vector indexes support only ordinary row-store tables, temporary tables, Toast tables, unlogged tables, and segment-page tables. For other tables, only btree and ubtree indexes can be created on vector data.
- If REINDEX is not executed after ALTER INDEX, subsequently inserted data is indexed according to the new index options, while the data already existing in the index remains unchanged.
- DISKANN indexes do not support ustore storage.
- When building a table with vector columns, you can use the INDEX clause to build default btree and ubtree indexes, but you cannot specify a vector index.
- When the vector column dimension is not specified, a vector index cannot be built; only btree and ubtree indexes can be built.
- To use a vector index in a B-compatible database, execute ```set dolphin.nulls_minimal=false``` to disable the nulls processing policy.
- When the data volume is less than the product of the parallel build count and the index build parameter index_size, serial build is used to improve build accuracy.
