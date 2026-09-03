# Vector Database Quick Start

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:07:53.977Z pushedAt=2026-07-30T12:23:58.599Z -->

## Quick Deployment

For details, see [Installing the Container Image](../installation_guide/installing_the_container_image.md).

## Creating a Vector Table

DataVec introduces various [vector data types](./vector_data_type.md), including `vector`, `bitvector`, and `sparsevector`. Creating a vector table follows the same syntax as native openGauss, requiring only the specification of the vector type during creation.

```bash
CREATE TABLE [TABLE_NAME]
(
    COL1 DATATYPE,
    ...,
    COLN VECTORTYPE,
);
```

Example 1: Create a table with 3-dimensional vectors.

```bash
openGauss=# CREATE TABLE items (val vector(3));
```

## Data Insertion

Vector data insertion follows the same syntax as native openGauss. Use `INSERT` or `COPY` to insert data, with data type specified.

```bash
INSERT INTO [TABLE_NAME] VALUES 
(
    DATA1,
    ...,
    [0.1, 0.3, 0.6, ...]
);
```

Example 2: Insert vector data.

```bash
openGauss=# INSERT INTO items (val) VALUES ('[1,2,3]'), ('[4,5,6]');
```

## Vector Index Creation

DataVec currently supports various [vector indexes](./vector_index.md) based on algorithms such as IVFFLAT, HNSW, IVFPQ, and HNSWPQ. Built upon the ASTORE storage engine in openGauss, these index structures enable efficient retrieval of query results.

```bash
CREATE INDEX [INDEX_NAME]
ON [TABLE_NAME]
USING [ivfflat|hnsw|...]
WITH (
    lists=<LISTS>,|
    m=<M>,
    ef_construction=<EF_CONSTRUCTION>,
    ...
);
```

Example 3: Create an index.

```bash
openGauss=# CREATE INDEX ON items USING ivfflat (val vector_l2_ops) WITH (lists = 100);
openGauss=# CREATE INDEX ON items USING hnsw (val vector_cosine_ops) WITH (m = 16, ef_construction=200);
```

## Vector Retrieval

With ANN indexes, DataVec can perform efficient approximate search. In addition, exact retrieval without indexes is also supported.

```bash
SELECT COL1, COLN 
FROM [TABLE_NAME] 
ORDER BY COLN [VECTOR_OPERATER]  '[0.1, 0.3, 0.7, ...]' 
LIMIT <TOPK>;
```

Example 4: Compute nearest neighbors.

```bash
openGauss=# SELECT * FROM items ORDER BY val <-> '[3,1,2]' LIMIT 5;
openGauss=# SELECT * FROM items ORDER BY val <#> '[3,1,2]' LIMIT 5;
openGauss=# SELECT * FROM items ORDER BY val <=> '[3,1,2]' LIMIT 5;
```

>[!NOTE]
>If a distance computation operator that does not exist in the current index is used for scanning, sequential scan will still be performed even after it is disabled.
>
>If the vector table contains null values or the distance computation result is `NAN`, the query result will automatically filter them out.
>
>Currently, the vector index query syntax only supports the `order by <field> <operator> <query vector>` clause. Adding `desc` to the `order by` clause, using `non-order-by` clauses, and other variations are not supported for vector index.

For more usage details, see:

- [Vector Data Types](./vector_data_type.md)

- [Vector Functions and Operators](./vector_functions_and_operators.md)

- [Vector Index](./vector_index.md)
