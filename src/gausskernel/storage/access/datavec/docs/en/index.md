# Vector Index Summary and Comparison

An index uses additional data organization so that queries do not need to scan the entire dataset row by row, but can quickly locate the target data. In high-dimensional vector retrieval scenarios, choosing an appropriate index structure is crucial. Different index algorithms differ in speed, precision, memory usage, and applicable data scale. By systematically summarizing and comparing the existing indexes of the openGauss vector database, this document aims to help users quickly determine and select an appropriate index solution in different scenarios, thereby achieving the best balance between performance and cost.

## Overview

The vector database component DataVec of openGauss, for efficient management and querying of high-dimensional vector data, adapts multiple advanced index structures, including **graph-based indexes (HNSW, DiskANN), inverted index (IVFFLAT), and quantized indexes (PQ, RabitQ)**, fully supporting four data types: vector (full precision), halfvec (half precision), bit (binary), and sparsevec (sparse vector). The following is a basic introduction to each index type.

Index Type | Description | Supported Data Types
--- | --- | --- 
[IVFFLAT](./vector_index.md)| Inverted file index | vector, halfvec, bit |
[HNSW](./vector_index.md)| Graph index based on the Hierarchical Navigable Small World (HNSW) algorithm | vector, halfvec, bit, sparsevec |
[DiskANN](./vector_index.md)| Disk-based graph index | vector |
[PQ](./pq.md)| Index algorithm based on product quantization, supports use with IVFFLAT, HNSW, and DiskANN | vector |
[RabitQ](./RabitQ.md)| Index algorithm based on 1-bit quantization, supports use with IVFFLAT and HNSW | vector, halfvec |

## Vector Index Details

### Data Structure

The vector index data structures currently supported by the openGauss vector database DataVec mainly include the following two types:

- `Inverted file structure`: IVFFLAT is an approximate nearest neighbor search index structure based on inverted files. It first partitions the vector set into multiple clusters through k-means clustering. During queries, the system performs exact search only within several candidate partitions closest to the query vector, thereby greatly improving retrieval speed while maintaining a high recall rate. This method is especially suitable for large-scale datasets, but its effectiveness depends on the clustering quality.

- `Graph structure`: Both HNSW and DiskANN are graph-based vector index algorithms. Among them, HNSW (Hierarchical Navigable Small World) builds a multi-layer sparse graph in memory and uses "highways" to achieve efficient navigation. It is known for extremely low query latency and high precision, but consumes more memory, making it suitable for scenarios that are extremely latency-sensitive. DiskANN, on the other hand, is specifically designed for disk-memory hybrid storage. By maintaining a high-quality navigation graph (Vamana graph) and storing complete vectors on disk, it achieves cost-controllable storage for ultra-large-scale vector data (over 100 million) while ensuring high performance, making it suitable for low-memory scenarios.

### Quantization

Vector quantization is a method that reduces storage space and computational cost by compressing high-dimensional vectors. DataVec mainly supports PQ and RabitQ quantization.

- `PQ (Product Quantization)`: By splitting a high-dimensional vector into multiple low-dimensional sub-vectors and independently clustering each sub-vector, the original vector is represented as a series of centroids, thereby significantly reducing memory usage and improving retrieval speed.

- `RabitQ (1-bit quantization)`: It quantizes each dimension value of a vector into a 1-bit value containing only 0/1, which is an extreme form of scalar quantization. After compression, the original vector is converted into a compact binary code. For example, data of the vector type can be compressed from float32 to 1 bit through RabitQ quantization.

### Fine Ranking

Since quantization causes loss of data details, it has a certain impact on query recall rate. Therefore, we achieve a balance between precision and performance through "fine ranking after quantization".

"Fine ranking after quantization" usually refers to the second stage of a two-stage retrieval process, that is, first performing coarse retrieval through a quantized index (such as PQ, RabitQ) to obtain a large number of candidate results, and then re-ranking or fine ranking these candidates using high-precision computation to improve the accuracy of the final results.

For example, when using RabitQ, you can choose the reranking type FP32 or SQ8.

## Full-Memory Performance Comparison

Different vector index types have their own performance emphases, which can be compared mainly from three dimensions: recall rate, QPS (queries per second), and index construction time. We will conduct a full-machine performance test of the openGauss vector database DataVec based on the vector performance testing tool VectorDB-Benchmark on the Cohere 1M dataset (`1M dataset, 768 dim`).

- Hardware environment: Kunpeng 920 ARM server
- Software environment: openGauss 7.0.0-RC3

### Index Construction Time

Index construction time refers to the total time from input of raw vector data to complete generation and persistence of the index file.

The following shows the index construction time data for the Cohere1M dataset under cosine distance metric, with a concurrency of 32 and maintenance_work_mem of 4GB.

Index Type | Index Parameter Configuration | Index Construction Time (s)
--- | --- | ---
 IVFFLAT| lists=1024 | 121.10
 IVFFLAT-RabitQ |lists=1024, reranking type (none/FP32)| 265.61
 IVFFLAT-PQ |lists=1024, pq_m=192, pq_ksub=256| 701.26
 HNSW|m=16, ef_construction=200| 149.55
 HNSW-RabitQ|m=16, ef_construction=200, reranking type (FP32) | 385.091
 HNSW-PQ|m=16, ef_construction=200, pq_m=96 | 855.75
 DiskANN |index_size=50 | 2070.25
 DiskANN-PQ| index_size=50,pq_m=192| 4695.41

The general conclusions are as follows:

- The IVFFLAT series has the fastest index construction speed, while the DiskANN series takes far longer to construct than other indexes.
- Adding quantization significantly increases index construction time, and the construction time increment of RabitQ is lower than that of PQ quantization.

### Queries Per Second (QPS)

Queries per second (QPS) is a core metric for measuring the query performance of a vector database. It represents the total number of query requests that the system can successfully process per unit time (1 second), directly reflecting the system's concurrent processing capability and throughput upper limit.

The following test data is based on the Cohere1M dataset, with test conditions of 99% recall and 8 concurrent requests.

Index Type | Parameter Configuration | Recall | QPS
--- | --- | --- | ---
 IVFFLAT| lists=1024, nprobes=128 | 0.9947 | 22.8738
 IVFFLAT-RabitQ |lists=1024, reranking type (none/FP32), nprobes=32| 0.993 | 111.4228
 IVFFLAT-PQ |lists=1024, pq_m=192, pq_ksub=256, nprobes=128| 0.9943 | 39.6979
 HNSW|m=16, ef_construction=200, ef_search=400| 0.9907 | 366.6528
 HNSW-RabitQ|m=16, ef_construction=200, reranking type (FP32) | 0.99 | 498.6068
 HNSW-PQ|m=16, ef_construction=200, pq_m=96, ef_search=400, hnsw_earlystop_threshold=160 | 0.989 | 628.361
 HNSW + MMAP|m=16, ef_construction=200, ef_search=400 | 0.9956 | 620.1976
 DiskANN |index_size=50 | 0.9923 | 218.7175
 DiskANN-PQ| index_size=50, pq_m=192| 0.9917| 281.388

The general conclusions are as follows:

- The HNSW series achieves the highest QPS, but its construction time is usually longer than that of IVFFLAT.
- PQ and RabitQ quantization slightly increase construction time, but can significantly improve query QPS.

### Performance Tuning Recommendations

When conducting vector retrieval performance tests, reasonable parameter configuration has a decisive impact on the test results. As shown in the performance data above, under the same recall rate, the QPS of HNSW-PQ and HNSW-RabitQ are both significantly better than that of native HNSW. Therefore, it is recommended to prioritize quantized indexes to achieve better cost-effectiveness. The following provides recommended parameter baselines for the two quantized indexes. The specific values need to be adjusted according to the dataset characteristics and data volume.

#### Recommended Parameters for HNSW + PQ

Parameter | Recommended Value | Description
--- | --- | ---
m | 16 | Maximum number of connections per node, affecting graph connectivity and search precision
ef_construction | 200 | Search width during graph construction; a larger value yields higher index quality
ef_search | 200 | Search width during query; a larger value yields higher recall
pq_m | dim / 8 or 16 | Number of PQ subspaces; 16 is recommended when dim=128, and 96 can be set when dim=768

```sql
-- Create an HNSW-PQ index
CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)
WITH (m = 16, ef_construction = 200, enable_pq = true, pq_m = 16);

-- Set query parameters
SET hnsw_ef_search = 200;
```

>[!NOTE]Description
>
>The choice of pq_m requires a trade-off between precision and compression ratio: pq_m = dim / 8 offers a higher compression ratio and is suitable for large-scale datasets; pq_m = 16 is a general baseline that balances precision and memory.

#### HNSW + RabitQ Recommended Parameters

Parameter | Recommended Value | Description
--- | --- | ---
m | 16 | Maximum number of connections per node
ef_construction | 200 | Search width during graph construction
ef_search | 200 | Search width during query
refine_k | 20 ~ 25 | Number of fine ranking candidates, taking Top-K from RabitQ coarse filtering results for exact distance reranking
refine_type | FP32 | Data precision used during fine ranking, FP32 ensures the highest precision

```sql
-- Create an HNSW-RabitQ index
SET rbq_sample_rows = 2000;
CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)
WITH (m = 16, ef_construction = 200, enable_rabitq = on, rabitq_refine_type = 'FP32', rabitq_fht = on);

-- Query parameter settings
SET hnsw_ef_search = 200;
SET rbq_refinek = 20;
```

>[!NOTE]Description
>
>refine_k = 20 is suitable for latency-sensitive scenarios; refine_k = 25 is suitable for scenarios with higher recall requirements. An excessively large refine_k increases the computational overhead of the fine ranking stage. It is recommended to adjust it based on the actual Recall@K metric.

### Capacity

In a vector database, index performance, memory, and disk are strongly coupled in the capacity dimension. The core logic is that the capacity upper limit of an index is determined by disk storage, while index performance is strongly correlated with memory capacity. If memory is sufficient, the database memory-related parameter (shared_buffers) is generally set to the index size. When memory capacity is insufficient, frequent data exchange between disk and memory (Page Cache swapping in and out) is triggered, which may directly degrade index query performance.

The following shows the storage space occupied by indexes built on the Cohere1M dataset using cosine distance. These values are directly related to the index parameter configuration.

Index Type | Index Parameter Configuration | Index Size
--- | --- | --- 
 IVFFLAT| lists=1024 | 3912MB
 IVFFLAT-RabitQ |lists=1024, reranking type (none/FP32)| 139MB
 IVFFLAT-PQ |lists=1024, pq_m=192, pq_ksub=256| 3914MB
 HNSW|m=16, ef_construction=200| 3906MB
 HNSW-RabitQ|m=16, ef_construction=200, reranking type (FP32) | 395MB
 HNSW-PQ|m=16, ef_construction=200, pq_m=96 | 3908MB
 DiskANN |index_size=50 | 7813MB
 DiskANN-PQ| index_size=50,pq_m=192| 7815MB

The general conclusions are as follows:

- If the amount of stored data is large but available disk space is limited, consider using IVFFLAT-RabitQ/HNSW-RabitQ to reduce the space occupied by the index.
- If disk space is sufficient but memory is relatively small, consider using the disk index DiskANN.
- If memory is sufficient, consider using IVFFLAT or HNSW directly.

>[!NOTE]Description
>
>The above data does not cover all scenarios. It is recommended to try different indexes based on actual conditions to determine the index type suitable for your current business.<br>

## Small Memory Performance Comparison

When the available memory is insufficient to hold the complete index, index queries will frequently trigger data exchange between disk and memory, resulting in performance that differs significantly from the full-memory scenario. We will conduct small memory scenario performance testing on the openGauss vector database DataVec using the vector performance testing tool VectorDB-Benchmark on the Cohere 1M dataset (`1M dataset, 768 dim`).

- Hardware environment: Kunpeng 920 ARM server
- Software environment: openGauss 7.0.0-RC3, with 3GB available memory for the openGauss process, shared_buffers=1GB

### Queries Per Second (QPS)

Queries Per Second (QPS) is a core metric for measuring the query performance of a vector database. It represents the total number of query requests that the system can successfully process per unit time (1 second), directly reflecting the system's concurrent processing capability and throughput upper limit.

The following test data is based on the Cohere1M dataset. Before each test, the system cache is cleared, the database is restarted, and 8 concurrent queries are executed.

Index Type | Parameter Configuration | Recall | QPS | Disk Read I/O Volume
--- | --- | --- | --- | ---
 HNSW|m=16, ef_construction=250 | 0.9909 | 12.51 | 149GB
 HNSW-RabitQ|m=16, ef_construction=250, reranking type (FP32), rbq_refinek=30 | 0.9905 | 148.23 | 44GB

>[!NOTE]Description
>
>HNSW-PQ requires all data to be loaded into memory for index construction, so it is not applicable to the current small memory test scenario.

The general conclusions are as follows:

- In the small memory scenario, the HNSW index causes frequent disk I/O due to insufficient memory, resulting in extremely low QPS; whereas HNSW-RabitQ significantly reduces the memory occupied by the index through quantization, thereby greatly reducing the disk I/O volume.
- In the small memory scenario, it is recommended to use HNSW-RabitQ to improve query performance.

### Capacity

In a vector database, if memory is sufficient, the database memory-related parameter (shared_buffers) is generally set to the index size. When memory capacity is insufficient, frequent data exchange between disk and memory (Page Cache swapping in and out) is triggered, which may directly degrade index query performance.

The following shows the space occupied by indexes built on the Cohere1M dataset based on cosine distance. These values are directly related to the index parameter configuration.

Index Type | Index Parameter Configuration | Index Size
--- | --- | ---
 HNSW|m=16, ef_construction=200| 3906MB
 HNSW-RabitQ|m=16, ef_construction=200, reranking type (FP32) | 395MB
 HNSW-PQ|m=16, ef_construction=200, pq_m=96 | 3908MB

The general conclusions are as follows:

- In small memory scenarios, the HNSW index size far exceeds the available memory, causing frequent disk I/O; the HNSW-PQ index cannot be built successfully in small memory; the HNSW-RabitQ index occupies only 395MB and can run efficiently within limited memory.

>[!NOTE]Description
>
>The above data does not cover all scenarios. It is recommended to try different indexes based on actual conditions to determine the index type suitable for the current business.<br>

## Vector Index Construction & Query Examples

Only SQL examples are provided here. For specific parameter settings, click the documentation links for each index algorithm in the `Overview` table.

- HNSW related examples

```sql
--Construct an HNSW index
openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64);
--Construct an HNSW index with PQ
openGauss=# CREATE INDEX ON items USING hnsw (embedding  vector_l2_ops) WITH (m = 16, ef_construction = 64, enable_pq=on, pq_m=16);
--Construct an HNSW index with RabitQ
openGauss=# SET rbq_sample_rows = 2000;
openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64, enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);

--L2 distance vector query (applicable to HNSW and HNSW-PQ)
openGauss=# SET hnsw_ef_search = 100;  --Query candidate set related parameters
openGauss=# SET hnsw_earlystop_threshold = 320; --Early stop parameter
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;

--L2 distance vector query (applicable to HNSW-RabitQ)
openGauss=# SET rbq_query_bits = 8;
openGauss=# SET rbq_refinek = 10;
openGauss=# SET hnsw_ef_search = 100;  --Parameters related to the query candidate set
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

- ivfflat related examples

```sql
--Construct an ivfflat index
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200);
--Construct an ivfflat index with pq
openGauss=# CREATE INDEX ON items USING ivfflat (embedding  vector_l2_ops) WITH (lists = 200, enable_pq=on, pq_m=16);
--Construct an ivfflat index with rabitq
openGauss=# SET rbq_sample_rows = 2000;
openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200, enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);

--L2 distance vector query (applicable to ivfflat and ivfflat-pq)
openGauss=# SET ivfflat_probes = 10;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
--L2 distance vector query (applicable to ivfflat-rabitq)
openGauss=# SET rbq_query_bits = 8;
openGauss=# SET rbq_refinek = 10;
openGauss=# SET ivfflat_probes = 10;  --Parameters related to the query candidate set
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

- diskann related examples

```sql
--Construct a diskann index
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (index_size = 50);
--Construct a diskann index with PQ.
openGauss=# CREATE INDEX ON items USING diskann (embedding  vector_l2_ops) WITH (index_size = 16,enable_pq = on, pq_m = 2);

--Query by L2 distance.
openGauss=# SET diskann_probes = 10;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```
