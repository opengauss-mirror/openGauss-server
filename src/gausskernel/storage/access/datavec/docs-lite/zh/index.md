# 向量索引汇总与对比

索引通过额外的数据组织方式，使得查询不必逐行扫描整个数据集，而是能快速定位到目标数据。在高维向量检索的场景中，选择合适的索引结构至关重要。不同的索引算法在速度、精度、内存占用和适用数据规模上各有差异。通过对openGauss向量数据库已有索引进行系统化的汇总与对比，本文旨在帮助用户在不同场景下，能够快速判断并选择合适的索引方案，从而在性能与成本之间取得最佳平衡。

## 概述

openGauss的向量数据库组件DataVec，为高效管理与查询高维向量数据，通过适配多种先进的索引结构，包含**图结构索引（HNSW、DiskANN）、倒排索引(IVFFLAT)、量化索引(PQ、RabitQ)**，全面支持 vector（全精度）、halfvec（半精度）、bit（二进制）和 sparsevec（稀疏向量）四种数据类型。以下是各类索引的基本介绍。

索引类型 | 描述 | 支持的数据类型
--- | --- | --- 
[IVFFLAT](./vector_index.md)| 倒排文件索引 |  vector、halfvec、bit |
[HNSW](./vector_index.md)| 基于分层导航小世界（HNSW）算法的图索引 | vector、halfvec、bit、sparsevec  |
[DiskANN](./vector_index.md)| 基于磁盘的图索引 | vector  |
[PQ](./pq.md)| 基于乘积量化的索引算法，支持结合IVFFLAT、HNSW、DiskANN使用 | vector  |
[RabitQ](./Rabitq.md)| 基于1bit量化的索引算法，支持结合IVFFLAT、HNSW使用 | vector、halfvec  |

## 向量索引详解

### 数据结构

openGauss向量数据库DataVec现在已支持的向量索引数据结构主要包含以下两种：

- `倒排文件结构`：IVFFLAT是一种基于倒排文件的近似最近邻搜索索引结构。它首先通过k-means聚类将向量集划分为多个分区。查询时，系统仅在与查询向量最近的若干个候选分区内进行精确搜索，从而在保证较高召回率的前提下，大幅提升检索速度。该方法尤其适用于大规模数据集，但其效果依赖于聚类质量。

- `图结构`：HNSW和DiskANN都是基于图的向量索引算法。其中，HNSW（Hierarchical Navigable Small World） 在内存中构建一个多层稀疏图，利用“高速公路”实现高效导航，以查询延迟极低、精度高著称，但内存消耗较大，适合对延迟极度敏感的场景。而 DiskANN 则专为磁盘-内存混合存储设计，通过维护一份高质量的导航图（Vamana图）和磁盘上存储完整向量，在保证高性能的同时，实现了超大规模向量数据（亿级以上） 的成本可控存储，适合低内存场景。

### 量化

向量量化是一种通过压缩高维向量来降低其存储空间与计算成本的方法。DataVec主要支持PQ、RabitQ量化。

- `PQ(乘积量化)`：通过将高维向量分割成为多个低维子向量，并对每个子向量进行独立聚类，将原始向量表示为一系列质心，从而显著减少内存使用和提升检索速度。

- `RabitQ(1bit量化)`：将向量各维度值量化为仅包含0/1的1比特值，是一种极致的标量量化形式。压缩后，原始向量被转换为一个紧凑的二进制码，比如vector类型的数据可以通过RabitQ量化将float32压缩为1bit。

### 精排

由于量化会造成数据细节丢失，对查询召回率是有一定影响的。因此，我们通过“量化后的精排”来实现精度和性能之间的平衡。

“量化后的精排”通常指的是两阶段检索流程中的第二阶段，即先通过量化索引（如PQ, RabitQ）进行粗召回（Coarse Retrieval），得到大量候选结果，再使用高精度计算对这批候选结果进行重排序（Re-ranking）或精排（Fine Ranking），以提升最终结果的准确性。

例如在使用RabitQ时，可以选择精排类型FP32或者SQ8。

## 全内存性能对比

不同的向量索引类型在性能表现上各有侧重，主要可以从召回率、QPS（每秒查询数）和构建索引时间三个维度进行对比。我们将基于向量性能测试工具VectorDB-Benchmark在Cohere 1M数据集（`1M dataset, 768 dim`）上进行openGauss向量数据库DataVec整机性能测试。

- 硬件环境：Kunpeng 920 arm服务器
- 软件环境：openGauss 7.0.0-RC3

### 索引构建时间

索引构建时间指从原始向量数据输入到索引文件完全生成并持久化的总耗时。

以下是 Cohere1M 数据集在余弦距离度量下的索引构建时间数据，并发数为32，maintenance_work_mem为4GB。

索引类型 | 索引参数配置 | 索引构建时间（s）
--- | --- | --- 
 IVFFLAT| lists=1024 | 121.10
 IVFFLAT-RabitQ |lists=1024, 重排类型（none/FP32）| 265.61
 IVFFLAT-PQ |lists=1024, pq_m=192, pq_ksub=256| 701.26
 HNSW|m=16, ef_construction=200| 149.55
 HNSW-RabitQ|m=16, ef_construction=200，重排类型（FP32） | 385.091
 HNSW-PQ|m=16, ef_construction=200，pq_m=96 | 855.75
 DiskANN |index_size=50 | 2070.25
 DiskANN-PQ| index_size=50,pq_m=192| 4695.41

一般结论如下：

- IVFFLAT 系列索引构建速度最快，DiskANN 系列构建时间远超其他索引。
- 增加量化会显著增加索引构建时间，RabitQ 的构建耗时增量低于 PQ 量化。

### 每秒查询次数（QPS）

每秒查询次数（Queries Per Second，QPS） 是衡量向量数据库查询性能核心指标，用于表示系统在单位时间（1 秒）内能够成功处理的查询请求总数，直接反映系统的并发处理能力和吞吐量上限。

以下为针对 Cohere1M 数据集的测试数据，测试条件为召回率 99%、并发数 8。

索引类型 | 参数配置 | 召回率 | QPS
--- | --- | --- | --- 
 IVFFLAT| lists=1024，nprobes=128 | 0.9947 | 22.8738
 IVFFLAT-RabitQ |lists=1024, 重排类型（none/FP32）, nprobes=32| 0.993 | 111.4228
 IVFFLAT-PQ |lists=1024, pq_m=192, pq_ksub=256,nprobes=128| 0.9943 | 39.6979
 HNSW|m=16, ef_construction=200,ef_search=400| 0.9907 | 366.6528
 HNSW-RabitQ|m=16, ef_construction=200，重排类型（FP32） | 0.99 | 498.6068
 HNSW-PQ|m=16, ef_construction=200，pq_m=96,,ef_search=400,hnsw_earlystop_threshold=160 | 0.989 | 628.361
 HNSW + MMAP|m=16, ef_construction=200,ef_search=400 | 0.9956 | 620.1976
 DiskANN |index_size=50 | 0.9923 | 218.7175
 DiskANN-PQ| index_size=50,pq_m=192| 0.9917| 281.388

一般结论如下：

- HNSW 系列的 QPS 最高，但构建时间通常长于 IVFFLAT。
- PQ 和 RabitQ 量化会小幅增加构建时间，但能显著提升查询 QPS。

### 性能调优推荐

在进行向量检索性能测试时，合理的参数配置对测试结果有决定性影响。如上述性能数据所示，在相同召回率条件下，HNSW-PQ 和 HNSW-RabitQ 的 QPS 均显著优于原生 HNSW，因此推荐优先使用量化索引以获取更优的性价比。以下提供两种量化索引的推荐参数基线，具体数值需根据数据集特征和数据量进行调整。

#### HNSW + PQ 推荐参数

参数 | 推荐值 | 说明
--- | --- | ---
m | 16 | 每个节点的最大连接数，影响图的连通性和搜索精度
ef_construction | 200 | 建图时的搜索宽度，值越大索引质量越高
ef_search | 200 | 查询时的搜索宽度，值越大召回率越高
pq_m | dim / 8 或 16 | PQ 子空间数量，dim=128 时推荐 16，dim=768 时可设为 96

```sql
-- 创建 HNSW-PQ 索引
CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)
WITH (m = 16, ef_construction = 200, enable_pq = true, pq_m = 16);

-- 查询参数设置
SET hnsw_ef_search = 200;
```

>[!NOTE]说明
>
>pq_m 的选择需要权衡精度与压缩率：pq_m = dim / 8 压缩率较高，适合大规模数据集；pq_m = 16 是通用基线，在精度和内存之间取得平衡。

#### HNSW + RabitQ 推荐参数

参数 | 推荐值 | 说明
--- | --- | ---
m | 16 | 每个节点的最大连接数
ef_construction | 200 | 建图时的搜索宽度
ef_search | 200 | 查询时的搜索宽度
refine_k | 20 ~ 25 | 精排候选数量，从 RabitQ 粗筛结果中取 Top-K 进行精确距离重排
refine_type | FP32 | 精排时使用的数据精度，FP32 保证最高精度

```sql
-- 创建 HNSW-RabitQ 索引
SET rbq_sample_rows = 2000;
CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)
WITH (m = 16, ef_construction = 200, enable_rabitq = on, rabitq_refine_type = 'FP32', rabitq_fht = on);

-- 查询参数设置
SET hnsw_ef_search = 200;
SET rbq_refinek = 20;
```

>[!NOTE]说明
>
>refine_k = 20 适合对延迟敏感的场景；refine_k = 25 适合对召回率要求更高的场景。refine_k 过大会增加精排阶段的计算开销，建议根据实际 Recall@K 指标调整。

### 容量

### 容量

在向量数据库中，索引性能、内存、磁盘三者在容量维度上是强耦合的关系，核心逻辑是：索引的容量上限由磁盘存储决定，而索引的性能和内存承载能力强相关。如果内存充足，数据库内存相关参数（shared_buffers）一般设置成索引大小，内存容量不足时，会触发磁盘与内存的频繁数据交换（Page Cache 换入换出），可能直接导致索引查询性能下降。

以下是针对 Cohere1M 数据集、基于余弦距离构建的索引所占用的空间大小，该数值与索引参数的配置直接相关。

索引类型 | 索引参数配置 | 索引大小
--- | --- | --- 
 IVFFLAT| lists=1024 | 3912MB
 IVFFLAT-RabitQ |lists=1024, 重排类型（none/FP32）| 139MB
 IVFFLAT-PQ |lists=1024, pq_m=192, pq_ksub=256| 3914MB
 HNSW|m=16, ef_construction=200| 3906MB
 HNSW-RabitQ|m=16, ef_construction=200，重排类型（FP32） | 395MB
 HNSW-PQ|m=16, ef_construction=200，pq_m=96 | 3908MB
 DiskANN |index_size=50 | 7813MB
 DiskANN-PQ| index_size=50,pq_m=192| 7815MB

一般结论如下：

- 如果存储的数据量大，但是可用磁盘空间有限，可以考虑使用IVFFLAT-RabitQ/HNSW-RabitQ减少索引所占空间。
- 如果磁盘空间足够，内存比较小，可以考虑磁盘索引DiskANN。
- 如果内存足够，可以考虑直接使用IVFFLAT或者HNSW。

>[!NOTE]说明
>
>以上数据并不覆盖所有场景，建议根据实际情况尝试不同索引，以确定当前业务适合的索引类型。<br>

## 小内存性能对比

当可用内存不足以容纳完整索引时，索引查询将频繁触发磁盘与内存的数据交换，性能表现与全内存场景有显著差异。我们将基于向量性能测试工具VectorDB-Benchmark在Cohere 1M数据集（`1M dataset, 768 dim`）上进行openGauss向量数据库DataVec小内存场景性能测试。

- 硬件环境：Kunpeng 920 arm服务器
- 软件环境：openGauss 7.0.0-RC3，openGauss进程可用内存3GB，shared_buffers=1GB

### 每秒查询次数（QPS）

每秒查询次数（Queries Per Second，QPS） 是衡量向量数据库查询性能核心指标，用于表示系统在单位时间（1 秒）内能够成功处理的查询请求总数，直接反映系统的并发处理能力和吞吐量上限。

以下为针对 Cohere1M 数据集的测试数据，每次测试前清空系统缓存，重启数据库，8并发查询。

索引类型 | 参数配置 | 召回率 | QPS | 读磁盘IO量
--- | --- | --- | --- | ---
 HNSW|m=16, ef_construction=250 | 0.9909 | 12.51 | 149GB
 HNSW-RabitQ|m=16, ef_construction=250，重排类型（FP32），rbq_refinek=30 | 0.9905 | 148.23 | 44GB

>[!NOTE]说明
>
>HNSW-PQ 需要数据全内存构建索引，不适用当前小内存测试场景。

一般结论如下：

- 小内存场景下，HNSW索引因内存不足导致频繁磁盘IO，QPS极低；而HNSW-RabitQ通过量化大幅减少索引占用内存，磁盘IO量显著降低。
- 小内存场景，建议使用HNSW-RabitQ，可以提高查询性能。

### 容量

在向量数据库中，如果内存充足，数据库内存相关参数（shared_buffers）一般设置成索引大小，内存容量不足时，会触发磁盘与内存的频繁数据交换（Page Cache 换入换出），可能直接导致索引查询性能下降。

以下是针对 Cohere1M 数据集、基于余弦距离构建的索引所占用的空间大小，该数值与索引参数的配置直接相关。

索引类型 | 索引参数配置 | 索引大小
--- | --- | ---
 HNSW|m=16, ef_construction=200| 3906MB
 HNSW-RabitQ|m=16, ef_construction=200，重排类型（FP32） | 395MB
 HNSW-PQ|m=16, ef_construction=200，pq_m=96 | 3908MB

一般结论如下：

- 在小内存场景下，HNSW索引大小远超可用内存，会导致频繁的磁盘IO；HNSW-PQ索引无法在小内存中构建成功；HNSW-RabitQ索引仅占395MB，可在有限内存中高效运行。

>[!NOTE]说明
>
>以上数据并不覆盖所有场景，建议根据实际情况尝试不同索引，以确定当前业务适合的索引类型。<br>

## 向量索引构建&查询示例

这里仅提供sql示例，具体的参数设置请点击`概述`表格中各类索引算法介绍的文档链接进行查看。

- hnsw相关示例

```sql
--构建hnsw索引
openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64);
--构建带pq的hnsw索引
openGauss=# CREATE INDEX ON items USING hnsw (embedding  vector_l2_ops) WITH (m = 16, ef_construction = 64，enable_pq=on, pq_m=16);
--构建带rabitq的hnsw索引
openGauss=# SET rbq_sample_rows = 2000;
openGauss=# CREATE INDEX ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 64，enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);

--l2距离向量查询（hnsw和hnsw-pq适用）
openGauss=# SET hnsw_ef_search = 100;  --查询候选集相关参数
openGauss=# SET hnsw_earlystop_threshold = 320; --早停参数
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;

--l2距离向量查询（hnsw-rabitq适用）
openGauss=# SET rbq_query_bits = 8;
openGauss=# SET rbq_refinek = 10;
openGauss=# SET hnsw_ef_search = 100;  --查询候选集相关参数
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

- ivfflat相关示例

```sql
--构建ivfflat索引
CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200);
--构建带pq的ivfflat索引
openGauss=# CREATE INDEX ON items USING ivfflat (embedding  vector_l2_ops) WITH (lists = 200，enable_pq=on, pq_m=16);
--构建带rabitq的ivfflat索引
openGauss=# SET rbq_sample_rows = 2000;
openGauss=# CREATE INDEX ON items USING ivfflat (embedding vector_l2_ops) WITH (lists = 200，enable_rabitq=on, rabitq_refine_type='FP32', rabitq_fht=on);

--l2距离向量查询（ivfflat和ivfflat-pq适用）
openGauss=# SET ivfflat_probes = 10;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
--l2距离向量查询（ivfflat-rabitq适用）
openGauss=# SET rbq_query_bits = 8;
openGauss=# SET rbq_refinek = 10;
openGauss=# SET ivfflat_probes = 10;  --查询候选集相关参数
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```

- diskann相关示例

```sql
--构建diskann索引
openGauss=# CREATE INDEX ON items USING diskann (embedding vector_l2_ops) WITH (index_size = 50);
--构建带pq的diskann索引
openGauss=# CREATE INDEX ON items USING diskann (embedding  vector_l2_ops) WITH (index_size = 16,enable_pq = on, pq_m = 2);

--l2距离向量查询
openGauss=# SET diskann_probes = 10;
openGauss=# SELECT id, embedding <-> '[1,2,3,4,5]'::vector AS distance FROM items ORDER BY distance limit 10;
```