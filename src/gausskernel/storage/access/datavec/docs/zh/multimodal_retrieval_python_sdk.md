# Python SDK 多模检索

本文介绍如何使用 openGauss Python SDK（psycopg2）实现多模检索能力，包括向量检索、BM25 全文检索、混合检索（Hybrid Search）以及 AI 模型集成（Embedding、Rerank、Chat）。

## 1. 概述

openGauss Python SDK 在 psycopg2 驱动基础上扩展了多模检索能力，提供统一的高级接口，帮助开发者快速构建 RAG（Retrieval-Augmented Generation）应用。

### 1.1 核心能力

| 能力 | 说明 |
| --- | --- |
| **向量检索** | 支持 L2、Cosine、Inner Product、L1 四种距离度量，支持 HNSW、IVFFlat、DiskANN 索引 |
| **全文检索** | 基于 openGauss BM25 索引的关键词检索，支持参数调优 |
| **混合检索** | 多路召回 + 融合排序，支持 RRF、加权融合、模型重排序等策略 |
| **AI 模型集成** | 统一接口对接 DashScope（通义千问）、OpenAI、Ollama，提供 Embedding、Rerank、Chat 能力 |
| **连接池管理** | 内置线程安全的连接池，支持并发检索 |

## 2. 安装与环境准备

### 2.1 环境要求

- Python 3.6 及以上版本（非 OM 安装建议 3.11 及以上）
- 使用模型API能力建议根据官方模型 API 文档选择对应版本

### 2.2 安装 SDK

参考 [openGauss](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2/blob/master/README.md) 官方文档安装 psycopg2 驱动：

### 2.3 安装可选依赖

根据需要安装 AI 模型对接的依赖：

```bash
# DashScope（通义千问）
pip install -U dashscope

# OpenAI
pip install openai

# Ollama（需本地部署 Ollama 服务）
pip install requests
```

## 3. 快速开始

以下示例展示了从连接数据库到执行混合检索的完整流程：

```python
from psycopg2.vector_client import MultiRetrieverClient
from psycopg2.vector_types import TableSchema, ColumnSchema, ColumnType, IndexConfig, IndexType, DistanceMetric
from psycopg2.retrievers import VectorRetriever, FullTextRetriever
from psycopg2.multi_retrieval import RRFFusion

# 1. 创建客户端
client = MultiRetrieverClient(
    host="localhost",
    port=5432,
    database="testdb",
    user="test_user",
    password="YourPassword"
)

# 2. 创建表
schema = TableSchema(columns=[
    ColumnSchema(name="id", type=ColumnType.INTEGER, primary_key=True),
    ColumnSchema(name="content", type=ColumnType.TEXT),
    ColumnSchema(name="embedding", type=ColumnType.VECTOR, dimension=3),
])
client.create_table("documents", schema)

# 3. 创建索引
# 向量索引
vector_index = IndexConfig(
    name="idx_embedding_hnsw",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16,
    ef_construction=200
)
client.create_index("documents", vector_index)

# BM25 全文索引
bm25_index = IndexConfig(
    name="idx_content_bm25",
    column="content",
    index_type=IndexType.BM25
)
client.create_index("documents", bm25_index)

# 4. 插入数据
data = [
    {"id": 1, "content": "openGauss是一款开源关系型数据库", "embedding": "[0.1, 0.2, 0.3]"},
    {"id": 2, "content": "向量数据库支持相似性检索", "embedding": "[0.4, 0.5, 0.6]"},
    {"id": 3, "content": "BM25是经典的全文检索算法", "embedding": "[0.7, 0.8, 0.9]"},
]
client.insert("documents", data)

# 5. 向量检索
results = client.vector_search(
    table_name="documents",
    query_vector=[0.1, 0.2, 0.3],
    vector_column="embedding",
    metric="cosine",
    top_k=5
)
print("向量检索结果：", results)

# 6. 全文检索
results = client.fulltext_search(
    table_name="documents",
    query_text="openGauss数据库",
    text_column="content",
    top_k=5
)
print("全文检索结果：", results)

# 7. 混合检索（向量 + 全文，RRF融合）
retrievers = [
    VectorRetriever(query_vector=[0.1, 0.2, 0.3], metric="cosine"),
    FullTextRetriever(query_text="openGauss数据库")
]
results = client.hybrid_search(
    table_name="documents",
    retrievers=retrievers,
    top_k=5,
    fusion_strategy=RRFFusion(k=60, weights=[0.6, 0.4])
)
print("混合检索结果：", results)

# 8. 关闭连接
client.close()
```

## 4. API 参考

### 4.1 MultiRetrieverClient — 统一客户端

`MultiRetrieverClient` 是多模检索的统一入口，提供表管理、索引管理、数据操作和检索接口。

#### 构造函数

```python
client = MultiRetrieverClient(
    host="localhost",       # 数据库主机地址
    port=5432,              # 端口号
    database="postgres",    # 数据库名
    user="postgres",        # 用户名
    password="",            # 密码
    pool_size=5,            # 连接池最小连接数
    max_overflow=10,        # 连接池最大溢出连接数
    timeout=30              # 连接超时（秒）
)
```

支持上下文管理器：

```python
with MultiRetrieverClient(host="localhost", ...) as client:
    results = client.vector_search(...)
# 自动关闭连接池
```

#### 表管理接口

| 方法 | 说明 |
| --- | --- |
| `create_table(table_name, schema, if_not_exists=True)` | 创建表 |
| `drop_table(table_name, if_exists=True, cascade=False)` | 删除表 |
| `describe_table(table_name)` | 查看表结构 |
| `list_tables(pattern=None)` | 列出所有表 |

#### 索引管理接口

| 方法 | 说明 |
| --- | --- |
| `create_index(table_name, index_config, if_not_exists=True)` | 创建索引 |
| `drop_index(index_name, if_exists=True, cascade=False)` | 删除索引 |
| `list_indexes(table_name=None)` | 列出索引 |

#### 数据操作接口

| 方法 | 说明 |
| --- | --- |
| `insert(table_name, data, batch_size=1000)` | 插入数据（支持批量） |
| `update(table_name, data, condition, params=None)` | 更新数据 |
| `delete(table_name, condition=None, ids=None, id_column="id")` | 删除数据 |
| `query(table_name, columns=None, condition=None, limit=None)` | 查询数据 |

### 4.2 向量检索 — vector_search

```python
results = client.vector_search(
    table_name="documents",          # 表名
    query_vector=[0.1, 0.2, 0.3],   # 查询向量
    vector_column="embedding",       # 向量列名（默认 "embedding"）
    top_k=10,                        # 返回结果数（默认 10）
    metric="cosine",                 # 距离度量（l2/cosine/inner_product/l1）
    id_column="id",                  # 主键列名（默认 "id"）
    filter_condition=None,           # SQL WHERE 过滤条件
    filter_params=None,              # 过滤条件参数
    output_columns=None,             # 指定输出列
    ef_search=None,                  # HNSW 查询参数
    probes=None,                     # IVFFlat 查询参数
    diskann_probes=None,             # DiskANN 查询参数
    rbq_query_bits=None,             # RabitQ 量化查询位数
    rbq_refinek=None,                # RabitQ 精炼候选池范围
)
```

**距离度量说明：**

| 度量 | 操作符 | 说明 | 得分计算 |
| --- | --- | --- | --- |
| `l2` | `<->` | 欧氏距离，越小越相似 | `1 / (1 + distance)` |
| `cosine` | `<=>` | 余弦距离，越小越相似 | `1 - distance` |
| `inner_product` | `<#>` | 内积距离（取负） | `-distance` |
| `l1` | `<+>` | 曼哈顿距离，越小越相似 | `1 / (1 + distance)` |

**带过滤条件的向量检索：**

```python
results = client.vector_search(
    table_name="documents",
    query_vector=[0.1, 0.2, 0.3],
    metric="cosine",
    top_k=10,
    filter_condition="user_id = %s AND created_time > %s",
    filter_params={"user_id": 100, "created_time": "2025-01-01"},
    output_columns=["id", "content", "user_id"]
)
```

### 4.3 全文检索 — fulltext_search

```python
results = client.fulltext_search(
    table_name="documents",          # 表名
    query_text="openGauss 向量数据库",  # 查询文本
    text_column="content",           # 文本列名（默认 "content"，需建有 BM25 索引）
    top_k=10,                        # 返回结果数
    id_column="id",                  # 主键列名
    filter_condition=None,           # SQL WHERE 过滤条件
    output_columns=None,             # 指定输出列
    bm25_k1=None,                    # BM25 k1 参数（词频饱和度）
    bm25_b=None,                     # BM25 b 参数（文档长度归一化）
    bm25_topk=None,                  # 动态 top-k 候选集大小
    use_bm25_taat=False,             # 是否使用 TAAT 方法
)
```

>[!NOTE]说明
>
>全文检索要求目标文本列已创建 BM25 索引。BM25 索引的创建方法参考 [BM25 索引使用指南](./bm25_usage_guide.md)。

### 4.4 混合检索 — hybrid_search

混合检索支持任意多路召回（多个向量检索、多个全文检索或任意组合），通过融合策略合并排序。

```python
results = client.hybrid_search(
    table_name="documents",          # 表名
    retrievers=[...],                # 检索器列表
    top_k=10,                        # 最终返回结果数
    fusion_strategy=RRFFusion(),     # 融合策略（默认 RRF）
    parallel=True,                   # 是否并行执行（默认 True）
)
```

#### 4.4.1 检索器（Retriever）

每个检索器可独立配置过滤条件和输出列：

```python
from psycopg2.retrievers import VectorRetriever, FullTextRetriever

# 向量检索器
vec_retriever = VectorRetriever(
    query_vector=[0.1, 0.2, 0.3],
    vector_column="embedding",
    metric="cosine",
    ef_search=100,
    filter_condition="category = %s",
    filter_params={"category": "tech"},
    output_columns=["id", "content", "category"]
)

# 全文检索器
ft_retriever = FullTextRetriever(
    query_text="深度学习入门",
    text_column="content",
    bm25_k1=1.2,
    bm25_b=0.75
)
```

#### 4.4.2 融合策略

**RRF 融合（Reciprocal Rank Fusion）**

RRF 是最常用的融合策略，公式为：`score = Σ (weight / (k + rank))`

```python
from psycopg2.multi_retrieval import RRFFusion

# 等权 RRF（默认）
fusion = RRFFusion(k=60)

# 自定义权重 RRF
fusion = RRFFusion(k=60, weights=[0.6, 0.4])
```

**加权融合（Weighted Fusion）**

对各路检索分数进行归一化后加权求和：

```python
from psycopg2.multi_retrieval import WeightedFusion, NormMethod

# arctan 归一化（默认）
fusion = WeightedFusion(weights=[0.7, 0.3])

# min-max 归一化
fusion = WeightedFusion(weights=[0.7, 0.3], norm_method=NormMethod.MIN_MAX)

# 不归一化（分数已在同一量级时使用）
fusion = WeightedFusion(weights=[0.7, 0.3], norm_method=NormMethod.NONE)
```

**模型重排序融合（Model Rerank Fusion）**

利用 AI 模型的 Rerank 能力对多路召回结果进行语义重排序：

```python
from psycopg2.multi_retrieval import ModelRerankFusion
from psycopg2.models import DashScopeModel

model = DashScopeModel(api_key="sk-xxx")
fusion = ModelRerankFusion(
    model=model,
    query="什么是深度学习？",
    text_field="content",        # 用于重排序的文本字段
    fallback_to_rrf=True         # 重排序失败时回退到 RRF
)
```

#### 4.4.3 混合检索完整示例

```python
from psycopg2.vector_client import MultiRetrieverClient
from psycopg2.retrievers import VectorRetriever, FullTextRetriever
from psycopg2.multi_retrieval import RRFFusion, WeightedFusion, ModelRerankFusion
from psycopg2.models import DashScopeModel

client = MultiRetrieverClient(
    host="localhost", port=5432,
    database="testdb", user="test_user", password="YourPassword"
)

# 定义检索器
vec_ret = VectorRetriever(
    query_vector=[0.12, 0.34, 0.56, ...],  # 256维向量
    metric="cosine",
    ef_search=100
)
ft_ret = FullTextRetriever(
    query_text="openGauss向量数据库特性"
)

# 方式1：RRF 融合
results = client.hybrid_search(
    "documents",
    retrievers=[vec_ret, ft_ret],
    top_k=10,
    fusion_strategy=RRFFusion(k=60, weights=[0.6, 0.4])
)

# 方式2：加权融合
results = client.hybrid_search(
    "documents",
    retrievers=[vec_ret, ft_ret],
    top_k=10,
    fusion_strategy=WeightedFusion(weights=[0.7, 0.3])
)

# 方式3：模型重排序融合
model = DashScopeModel(api_key="sk-xxx")
results = client.hybrid_search(
    "documents",
    retrievers=[vec_ret, ft_ret],
    top_k=10,
    fusion_strategy=ModelRerankFusion(
        model=model,
        query="openGauss向量数据库特性"
    )
)

client.close()
```

## 5. AI 模型集成

SDK 提供统一的 AI 模型接口，支持 Embedding（文本嵌入）、Rerank（重排序）、Chat（对话）三种能力。

### 5.1 模型提供商
参考供应商官网获取更多支持类型。

| 提供商 | 类名 | Embed                        | Rerank | Chat | 安装依赖 |
| --- | --- |------------------------------| --- | --- | --- |
| DashScope（通义千问） | `DashScopeModel` | text-embedding-v3等           | gte-rerank-v2 / qwen3-rerank | qwen-plus / qwen-max | `pip install dashscope` |
| OpenAI | `OpenAIModel` | text-embedding-3-small/large | 不支持 | gpt-4o / gpt-4o-mini | `pip install openai` |
| Ollama（本地部署） | `OllamaModel` | nomic-embed-text             | 不支持 | llama3 / qwen2.5 | `pip install requests` |

### 5.2 使用模型

**方式一：直接构造**

```python
from psycopg2.models import DashScopeModel, OpenAIModel, OllamaModel

# DashScope
model = DashScopeModel(api_key="sk-xxx")

# OpenAI
model = OpenAIModel(api_key="sk-xxx")

# Ollama（本地部署）
model = OllamaModel(chat_model="qwen2.5")
```

**方式二：工厂函数**

```python
from psycopg2.models import create_model

model = create_model("dashscope", api_key="sk-xxx")
model = create_model("openai", api_key="sk-xxx")
model = create_model("ollama", chat_model="llama3")
```

### 5.3 文本嵌入（Embed）

```python
model = DashScopeModel(api_key="sk-xxx")

# 单文本嵌入
vector = model.embed_single("openGauss是一款开源数据库")

# 批量嵌入
vectors = model.embed(["文本1", "文本2", "文本3"])
```

### 5.4 文本重排序（Rerank）

```python
model = DashScopeModel(api_key="sk-xxx")

results = model.rerank(
    query="什么是向量数据库？",
    documents=[
        "向量数据库用于存储和检索高维向量数据",
        "关系型数据库使用SQL进行查询",
        "openGauss DataVec提供向量检索能力"
    ],
    top_n=2
)
# 返回: [{"index": 0, "score": 0.95}, {"index": 2, "score": 0.82}]
```

### 5.5 对话生成（Chat）

```python
model = DashScopeModel(api_key="sk-xxx")

# 单轮对话
response = model.generate("请解释什么是RAG？")

# 多轮对话
response = model.chat([
    {"role": "system", "content": "你是一个数据库专家"},
    {"role": "user", "content": "openGauss有哪些向量检索能力？"}
])
```

## 6. 索引类型与配置

### 6.1 向量索引

**HNSW 索引**

```python
from psycopg2.vector_types import IndexConfig, IndexType, DistanceMetric

index = IndexConfig(
    name="idx_hnsw",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16,                    # 每层最大连接数 2~100（默认 16）
    ef_construction=64       # 构建参数 4~1000（默认 64）
)
```

**HNSW + RabitQ 量化索引**

```python
from psycopg2.vector_types import RabitQRefineType

index = IndexConfig(
    name="idx_hnsw_rbq",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16,
    ef_construction=64,
    enable_rabitq=True,                           # 启用 RabitQ 量化
    rabitq_refine_type=RabitQRefineType.FP32,     # 精炼类型：SQ8 / FP32
    rabitq_fht=True                               # 使用 FHT 随机旋转
)
```

**HNSW + PQ 量化索引**

```python
index = IndexConfig(
    name="idx_hnsw_pq",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.L2,
    m=16,
    ef_construction=64,
    enable_pq=True,         # 启用 PQ 量化
    pq_m=64,                # 子空间数量（推荐 dim/4）
    pq_ksub=256             # 每个子空间聚类中心数
)
```

**IVFFlat 索引**

```python
index = IndexConfig(
    name="idx_ivfflat",
    column="embedding",
    index_type=IndexType.IVFFLAT,
    metric=DistanceMetric.L2,
    lists=200                # 聚类中心数量
)
```

**DiskANN 索引**

```python
index = IndexConfig(
    name="idx_diskann",
    column="embedding",
    index_type=IndexType.DISKANN,
    metric=DistanceMetric.L2,
    index_size=100           # 构建参数 16~1000（默认 100）
)
```

### 6.2 BM25 全文索引

```python
# 基本 BM25 索引
bm25_index = IndexConfig(
    name="idx_bm25",
    column="content",
    index_type=IndexType.BM25
)

# 并行构建 BM25 索引
bm25_index = IndexConfig(
    name="idx_bm25_parallel",
    column="content",
    index_type=IndexType.BM25,
    parallel_workers=8       # 并行构建线程数 1~32
)
```

### 6.3 向量数据类型

| 类型 | 枚举值 | 维度上限 | 说明 |
| --- | --- | --- | --- |
| `VECTOR` | `ColumnType.VECTOR` | 16000 | 标准浮点向量 |
| `BIT` | `ColumnType.BIT` | 64000 | 二进制向量 |
| `SPARSEVEC` | `ColumnType.SPARSEVEC` | 1,000,000,000 | 稀疏向量（非零元素上限 1000） |

## 7. 完整 RAG 应用示例

以下展示一个完整的 RAG（检索增强生成）应用示例，结合 Embedding、混合检索和大模型生成：

```python
from psycopg2.vector_client import MultiRetrieverClient
from psycopg2.vector_types import (
    TableSchema, ColumnSchema, ColumnType,
    IndexConfig, IndexType, DistanceMetric
)
from psycopg2.retrievers import VectorRetriever, FullTextRetriever
from psycopg2.multi_retrieval import ModelRerankFusion
from psycopg2.models import DashScopeModel

# ========== 初始化 ==========

# 创建模型（用于 Embedding、Rerank 和 Chat）
model = DashScopeModel(api_key="sk-xxx")

# 创建数据库客户端
client = MultiRetrieverClient(
    host="localhost", port=5432,
    database="ragdb", user="rag_user", password="YourPassword"
)

# ========== 建表和索引 ==========

schema = TableSchema(columns=[
    ColumnSchema(name="id", type=ColumnType.INTEGER, primary_key=True),
    ColumnSchema(name="content", type=ColumnType.TEXT),
    ColumnSchema(name="category", type=ColumnType.VARCHAR, max_length=64),
    ColumnSchema(name="embedding", type=ColumnType.VECTOR, dimension=1024),
])
client.create_table("knowledge_base", schema)

# HNSW 向量索引
client.create_index("knowledge_base", IndexConfig(
    name="idx_kb_hnsw",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16, ef_construction=200
))

# BM25 全文索引
client.create_index("knowledge_base", IndexConfig(
    name="idx_kb_bm25",
    column="content",
    index_type=IndexType.BM25
))

# ========== 数据入库（含 Embedding） ==========

documents = [
    {"id": 1, "content": "openGauss DataVec 支持向量存储与检索，适用于 RAG 场景。", "category": "database"},
    {"id": 2, "content": "BM25 是一种经典的基于词频的全文检索算法。", "category": "algorithm"},
    {"id": 3, "content": "HNSW 索引通过分层图结构实现高效近似最近邻搜索。", "category": "index"},
    # ... 更多文档
]

# 批量生成 Embedding
texts = [doc["content"] for doc in documents]
embeddings = model.embed(texts)

# 将 Embedding 写入文档
for doc, emb in zip(documents, embeddings):
    doc["embedding"] = str(emb)

client.insert("knowledge_base", documents)

# ========== RAG 检索 + 生成 ==========

user_question = "openGauss有哪些向量检索能力？"

# 1. 生成查询向量
query_vector = model.embed_single(user_question)

# 2. 混合检索（向量 + BM25 + 模型重排序）
results = client.hybrid_search(
    table_name="knowledge_base",
    retrievers=[
        VectorRetriever(query_vector=query_vector, metric="cosine", ef_search=100),
        FullTextRetriever(query_text=user_question)
    ],
    top_k=5,
    fusion_strategy=ModelRerankFusion(
        model=model,
        query=user_question,
        fallback_to_rrf=True
    )
)

# 3. 构建上下文
context = "\n".join([f"- {r['content']}" for r in results])

# 4. 调用大模型生成回答
answer = model.chat([
    {"role": "system", "content": f"请根据以下参考资料回答用户问题。\n\n参考资料：\n{context}"},
    {"role": "user", "content": user_question}
])

print(f"问题：{user_question}")
print(f"回答：{answer}")

client.close()
```

## 8. 高级特性

### 8.1 并行多路检索

`MultiRetrievalEngine` 支持使用线程池并行执行多路检索，显著降低多路召回的总延迟：

```python
from psycopg2.multi_retrieval import MultiRetrievalEngine, RRFFusion

engine = MultiRetrievalEngine(
    retrievers=[vec_ret1, vec_ret2, ft_ret],
    fusion_strategy=RRFFusion(k=60, weights=[0.4, 0.3, 0.3])
)

results = engine.search(
    client=client,
    table_name="documents",
    top_k=10,
    parallel=True,          # 并行执行（默认）
    max_workers=3,          # 最大线程数
    timeout=30              # 每路超时（秒）
)
```

### 8.2 RabitQ / PQ 量化加速查询

在使用量化索引时，可通过 GUC 参数调优查询效果：

```python
# RabitQ 量化加速
results = client.vector_search(
    table_name="documents",
    query_vector=query_vector,
    metric="cosine",
    ef_search=100,
    rbq_query_bits=8,       # 查询向量量化位数 1~8
    rbq_refinek=10.0        # 精炼候选池范围 1~2000000000
)

# HNSW-PQ 查询
results = client.vector_search(
    table_name="documents",
    query_vector=query_vector,
    metric="l2",
    ef_search=100,
    hnsw_earlystop_threshold=320  # PQ 提前终止阈值
)

# DiskANN 查询
results = client.vector_search(
    table_name="documents",
    query_vector=query_vector,
    metric="l2",
    diskann_probes=64       # DiskANN 查询候选集大小
)
```

### 8.3 自定义模型提供商

可通过继承 `BaseModel` 扩展自定义模型提供商：

```python
from psycopg2.models import BaseModel, register_provider

class MyModel(BaseModel):
    @property
    def provider(self) -> str:
        return "my_provider"
    
    def embed(self, texts, **kwargs):
        # 实现嵌入逻辑
        pass
    
    def rerank(self, query, documents, top_n=None, **kwargs):
        # 实现重排序逻辑
        pass
    
    def chat(self, messages, **kwargs):
        # 实现对话逻辑
        pass

# 注册提供商
register_provider("my_provider", MyModel)

# 使用工厂函数创建
model = create_model("my_provider", api_key="xxx")
```

## 9. 类型参考

### 9.1 ColumnType 列类型枚举

| 枚举值 | SQL 类型 | 说明 |
| --- | --- | --- |
| `INTEGER` | INTEGER | 整数 |
| `BIGINT` | BIGINT | 大整数 |
| `TEXT` | TEXT | 文本 |
| `VARCHAR` | VARCHAR | 变长字符串（需指定 `max_length`） |
| `VECTOR` | VECTOR(dim) | 浮点向量（需指定 `dimension`） |
| `BIT` | BIT(dim) | 二进制向量（需指定 `dimension`） |
| `SPARSEVEC` | SPARSEVEC(dim) | 稀疏向量（需指定 `dimension`） |
| `BOOLEAN` | BOOLEAN | 布尔值 |
| `TIMESTAMP` | TIMESTAMP | 时间戳 |
| `JSON` | JSON | JSON 数据 |
| `JSONB` | JSONB | 二进制 JSON 数据 |

### 9.2 IndexType 索引类型枚举

| 枚举值 | 说明 |
| --- | --- |
| `HNSW` | 分层可导航小世界图索引 |
| `IVFFLAT` | 倒排文件扁平索引 |
| `DISKANN` | 基于磁盘的近似最近邻索引（向量维度上限 1536） |
| `BM25` | BM25 全文检索索引 |
| `BTREE` | B-Tree 索引 |
| `GIN` | 通用倒排索引 |
| `HASH` | 哈希索引 |

### 9.3 DistanceMetric 距离度量枚举

| 枚举值 | 操作符 | 适用向量类型 |
| --- | --- | --- |
| `L2` | `<->` | vector |
| `COSINE` | `<=>` | vector |
| `INNER_PRODUCT` | `<#>` | vector |
| `L1` | `<+>` | vector |
| `HALFVEC_L2` | `<->` | halfvec |
| `BIT_HAMMING` | `<~>` | bit |
| `BIT_JACCARD` | `<%>` | bit |
| `SPARSEVEC_L2` | `<->` | sparsevec |
| `SPARSEVEC_COSINE` | `<=>` | sparsevec |

### 9.4 SearchResult 检索结果结构

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | Any | 文档 ID |
| `score` | float | 相关性分数（越高越相关） |
| `source` | str | 来源标识（`vector` / `fulltext` / `rrf_fused` / `weighted_fused` / `model_rerank`） |
| `data` | dict | 文档数据（包含查询到的所有列） |

## 10. 常见问题

**Q: 如何选择融合策略？**

- **RRF 融合**：适用于大多数场景，对各路检索的分数尺度不敏感，推荐作为默认选择。
- **加权融合**：适用于需要精细控制各路检索权重的场景，需配合归一化方法使用。
- **模型重排序**：适用于对检索精度要求极高的场景，需要额外的 AI 模型调用，延迟较高。

**Q: 向量维度应该设置多少？**

取决于使用的 Embedding 模型。常见维度：
- DashScope text-embedding-v3：默认 1024 维（支持自定义 256/512/1024/1536）
- OpenAI text-embedding-3-small：1536 维
- OpenAI text-embedding-3-large：3072 维

**Q: 混合检索中各路权重如何设置？**

- 如果不提供权重，系统会自动均匀分配。
- 一般建议向量检索权重略高于全文检索（如 0.6:0.4），具体需根据业务场景调优。
- 权重之和必须等于 1。

**Q: 模型重排序失败会怎样？**

当 `fallback_to_rrf=True`（默认开启）时，重排序失败会自动回退到 RRF 融合策略，确保检索服务可用性。

[更多示例和源码参考](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2)
