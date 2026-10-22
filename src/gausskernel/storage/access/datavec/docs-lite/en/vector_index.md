# Vector Indexes

The DataVec vector engine supports the `IVFFlat`, `HNSW`, `DISKANN`, `HNSWPQ`, and `IVFPQ` algorithms. These algorithms are implemented based on `ASTORE` storage in openGauss, and their index structures enable efficient retrieval of query results.

The syntax for creating vector indexes supports the clauses available in the standard syntax for creating indexes.

## `HNSW`

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

- `INDEX_NAME` - index name
- `TABLE_NAME` - table name
- `COLUMN_NAME` - name of the vector data column
- `TABLESPACE` - specifies the tablespace for the index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `COMMENT text` - specifies the comment for the index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `VISIBLE | INVISIBLE` - specifies whether the index is visible. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `WHERE predicate` - creates a partial index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).

### `HNSW` Index Operators

The format of an `HNSW` index operator is `[TYPE]_[DISTANCE_FUN]_ops`:

- `TYPE` - vector type
    - `vector`
    - `bit`
    - `sparsevec`

`HNSW` supports the following vector dimensions.

| Name        | Dimension Limit                                     |
| ----------- | --------------------------------------------------- |
| `vector`    | 2,000                                               |
| `bit`       | 64,000                                              |
| `sparsevec` | 1,000,000,000<br>Number of non-zero elements: 1,000 |

- `DISTANCE_FUN` - distance function
    - `l2`
    - `ip`
    - `cosine`
    - `l1`
    - `hamming`
    - `jaccard`

#### `vector` Index Operators

| Index Operator      | Description     |
| ------------------- | --------------- |
| `vector_l2_ops`     | L2 distance     |
| `vector_ip_ops`     | Inner product   |
| `vector_cosine_ops` | Cosine distance |
| `vector_l1_ops`     | L1 distance     |

#### `bit` Index Operators

| Index Operator    | Description      |
| ----------------- | ---------------- |
| `bit_hamming_ops` | Hamming distance |
| `bit_jaccard_ops` | Jaccard distance |

#### `sparsevec` Index Operators

| Index Operator         | Description     |
| ---------------------- | --------------- |
| `sparsevec_l2_ops`     | L2 distance     |
| `sparsevec_ip_ops`     | Inner product   |
| `sparsevec_cosine_ops` | Cosine distance |
| `sparsevec_l1_ops`     | L1 distance     |

### Index Options

- `m` - maximum number of connections per graph layer (16 by default). The value ranges from 2 to 100. The appropriate value depends on the dataset and application scenario.
- `ef_construction` - size of the dynamic candidate set used for graph construction (64 by default). The value ranges from 4 to 1,000 and must be greater than or equal to `2 * m`. To ensure search quality, `ef_construction` should preferably be greater than `ef_search`.

**Example 1:** Create an `HNSW` index using L2 distance and set `m = 16` and `ef_construction = 64`.

```sql
openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64);
```

Increasing `ef_construction` and `m` can improve recall but increases index build time and insertion time.

### Query Options

- `ef_search` - size of the dynamic candidate set used during queries (40 by default). For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

**Example 2:** Set `ef_search` to `100` for the current session and query the top 10 most similar vectors using L2 distance.

```sql
openGauss=# SET hnsw_ef_search = 100;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance LIMIT 10;
```

### Parallelism Options

- `parallel_workers` - index build parallelism (0 by default). The value ranges from 1 to 32.

**Example 3:** Set the index build parallelism to 32.

```sql
openGauss=# ALTER TABLE items SET(parallel_workers=32);
```

> [!NOTE]  
> When `HNSW` builds the graph index, some data points may become unreachable, which can cause the number of rows returned by index queries to differ from the expected number. This is related to the dataset and the `m` and `ef_construction` parameters and can be improved by increasing `m`. For searches over the entire dataset, using an `IVFFlat` index is recommended. `HNSW` is suitable for scenarios where `LIMIT` is much smaller than the total number of rows.

## `IVFFlat`

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

- `INDEX_NAME` - index name
- `TABLE_NAME` - table name
- `COLUMN_NAME` - name of the vector data column
- `TABLESPACE` - specifies the tablespace for the index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `COMMENT text` - specifies the comment for the index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `VISIBLE | INVISIBLE` - specifies whether the index is visible. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `WHERE predicate` - creates a partial index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).

### `IVFFlat` Index Operators

The format of an `IVFFlat` index operator is `[TYPE]_[DISTANCE_FUN]_ops`:

- `TYPE` - vector type
    - `vector`
    - `bit`

`IVFFlat` supports the following vector dimensions.

| Name     | Dimension Limit |
| -------- | --------------- |
| `vector` | 2,000           |
| `bit`    | 64,000          |

- `DISTANCE_FUN` - distance function
    - `l2`
    - `ip`
    - `cosine`
    - `hamming`

#### `vector` Index Operators

| Index Operator      | Description     |
| ------------------- | --------------- |
| `vector_l2_ops`     | L2 distance     |
| `vector_ip_ops`     | Inner product   |
| `vector_cosine_ops` | Cosine distance |

#### `bit` Index Operators

| Index Operator    | Description      |
| ----------------- | ---------------- |
| `bit_hamming_ops` | Hamming distance |

`sparsevec` does not support `IVFFlat` indexes.

### Index Options

- `lists` - number of cluster centers in the inverted lists (cells) (100 by default). The value ranges from 1 to 32,768.

**Example 4:** Create an `IVFFlat` index using L2 distance and set `lists = 200`.

```sql
openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200);
```

> [!NOTE]
>
> To optimize recall:
>
> - Create the index after data has been inserted into the table.
> - Select an appropriate `lists` value. For datasets with up to 1 million rows, `rows / 1000` is recommended. For datasets with more than 1 million rows, `sqrt(rows)` is recommended.
> - Specify an appropriate number of probes for queries. `sqrt(lists)` is recommended.

### Query Options

- `probe` - size of the candidate set used during queries (1 by default). For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

**Example 5:** Set `probes` to `10` and query the top 10 most similar vectors using L2 distance.
```sql
openGauss=# SET ivfflat_probes = 10;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance LIMIT 10;
```

### Parallelism Options

- `parallel_workers` - index build parallelism (0 by default). The value ranges from 1 to 32.

**Example 6:** Set the index build parallelism to 32.

```sql
openGauss=# ALTER TABLE items SET(parallel_workers=32);
```

## `DISKANN`

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

- `INDEX_NAME` - index name
- `TABLE_NAME` - table name
- `COLUMN_NAME` - name of the vector data column
- `TABLESPACE` - specifies the tablespace for the index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `COMMENT text` - specifies the comment for the index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `VISIBLE | INVISIBLE` - specifies whether the index is visible. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).
- `WHERE predicate` - creates a partial index. The usage is the same as that in [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).

### `DISKANN` Index Operators

The format of a `DISKANN` index operator is `[TYPE]_[DISTANCE_FUN]_ops`:

- `TYPE` - vector type
    - `vector`

`DISKANN` supports the following vector dimensions.

| Name     | Dimension Limit |
| -------- | --------------- |
| `vector` | 1,536           |

- `DISTANCE_FUN` - distance function
    - `l2`
    - `ip`
    - `cosine`

#### `vector` Index Operators

| Index Operator      | Description     |
| ------------------- | --------------- |
| `vector_l2_ops`     | L2 distance     |
| `vector_ip_ops`     | Inner product   |
| `vector_cosine_ops` | Cosine distance |

### Index Options

- `index_size` - index build parameter that affects recall and build time. The value ranges from 16 to 1,000 (100 by default). A value of 50 is recommended for datasets with millions of rows.
- `enable_pq` - quantization and compression parameter that controls whether PQ is enabled. Disabled by default.
- `pq_m` - quantization and compression parameter. The value ranges from 1 to 2,000 (8 by default). `dim / 8` is recommended.

**Example 7:** Create a `DISKANN` index using L2 distance and set `index_size = 50`.

```sql
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (index_size = 50);
```

> [!NOTE]
>
> - Creating the index after importing the data is recommended.

### Query Options

- `diskann_probes` - size of the candidate set used during queries (128 by default). For details, see [DataVec Vector Engine Parameters](https://docs.opengauss.org/en/docs/latest/database_reference/datavec_vector_engine_parameters.html).

**Example 8:** Set `diskann_probes` to `256` and query the top 10 most similar vectors using L2 distance.
```sql
openGauss=# SET diskann_probes = 256;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance LIMIT 10;
```

### Parallelism Options

- `parallel_workers` - index build parallelism (0 by default). The value ranges from 1 to 32.

**Example 9:** Set the index build parallelism to 32.

```sql
openGauss=# ALTER TABLE items SET(parallel_workers=32);
```

## Non-Vector Indexes: `B-tree/Ubtree`

Vector data types also support B-tree and Ubtree index creation. The index type is automatically selected based on the storage type of the table.

```sql
CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
(COLUMN_NAME [TYPE]_ops);
```

### `B-tree/Ubtree` Index Operators

- `vector_ops`
- `bit_ops`
- `sparsevec_ops`

**Example 10:** Create a B-tree index.

```sql
openGauss=# CREATE INDEX ON t (val vector_ops);
```

> [!NOTE]  
> B-tree and Ubtree indexes do not support vector distance calculations. They only support vector comparisons using the vector functions and operators in [Vector Functions and Operators](vector_functions_and_operators.md).

## Parallel Vector Index Building

Enable parallel index building to accelerate vector index creation:

```sql
ALTER TABLE [TABLE_NAME]
SET (parallel_workers = <CONCURRENCY_NUM>);
```

**Example 11:** Set the index build parallelism to 8.

```sql
openGauss=# ALTER TABLE items SET (parallel_workers = 8);
```

## Modifying Vector Indexes

Only vector index options can be modified.

```sql
ALTER INDEX [INDEX_NAME]
SET (parameter=<OPTIONS>);
```

**Example 12:** Modify vector index options.

```sql
openGauss=# ALTER INDEX t_val_idx SET (m=24, ef_construction=200);
openGauss=# REINDEX INDEX t_val_idx;
```

Note that if an error such as `ERROR: missing chunk number xx for toast value xx in pg_toast_xx` occurs when rebuilding an index after data has been inserted, deleted, or updated, the database may not have cleaned up the dead tuples generated by the update or delete operations in time. As a result, `REINDEX` may attempt to access TOAST chunks that have already been reclaimed. In this case, manually run `VACUUM` before using `REINDEX` to rebuild the index.

> [!NOTE]  
> - After modifying vector index parameters, run `REINDEX` to update the index. `REINDEX` supports parallel execution, and the parallelism is still determined by `parallel_workers` in the table.
> - Modifying index operators is not supported, such as changing `vector_l2_ops` to `vector_cosine_ops`.
> - When modifying index options using `ALTER INDEX`, `HNSWPQ` and `IVFPQ` indexes cannot validate PQ-related parameters. These parameters are validated during `REINDEX`.

## Constraints

- Vector indexes are supported only on ordinary row-store tables, temporary tables, TOAST tables, unlogged tables, and segment-page tables. Partitioned tables are not currently supported. For other table types, only B-tree and Ubtree indexes can be created on vector data.
- If `REINDEX` is not executed after `ALTER INDEX`, the index retains its original option configuration, and subsequently inserted data is indexed according to the original index options. `CREATE INDEX` and `REINDEX` do not support online concurrent index building with `CONCURRENTLY`.
- `IVFFlat`, `HNSW`, and `DISKANN` indexes do not support USTORE.
- When creating a table with vector columns, the `INDEX` clause can be used to create the default B-tree or Ubtree index, but vector indexes cannot be specified.
- Vector indexes cannot be created when the dimension of the vector column is not specified. Only B-tree and Ubtree indexes can be created in this case.
- When using vector indexes in a B-compatible database, execute `set dolphin.nulls_minimal_policy=false` to disable the null handling policy.
- Vector indexes are not supported when extreme RTO mode is enabled in a primary/standby environment.
