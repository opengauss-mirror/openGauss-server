# Python SDK for Multimodal Retrieval

This document describes how to use the openGauss Python SDK (`psycopg2`) to implement multimodal retrieval capabilities, including vector retrieval, BM25 full-text search, hybrid search, and AI model integration (embedding, rerank, chat).

## 1. Overview

The openGauss Python SDK extends the `psycopg2` driver with multimodal retrieval capabilities, providing a unified high-level interface to help developers quickly build retrieval-augmented generation (RAG) applications.

### 1.1 Core Capabilities

| Capability | Description |
| --- | --- |
| **Vector retrieval** | Supports four distance metrics: L2, Cosine, Inner Product, L1; supports Hierarchical Navigable Small World (HNSW), Inverted File Flat (IVFFlat), and Disk-based Approximate Nearest Neighbor (DiskANN) indexes |
| **Full-text search** | Keyword search based on the openGauss BM25 index, with parameter tuning support |
| **Hybrid search** | Multi-channel recall + fusion ranking, supporting reciprocal rank fusion (RRF), weighted fusion, model reranking, and other strategies |
| **AI model integration** | Unified interface connecting DashScope (Qwen), OpenAI, Ollama, providing embedding, rerank, and chat capabilities |
| **Connection pool management** | Built-in thread-safe connection pool supporting concurrent retrieval |

## 2. Installation and Environment Setup

### 2.1 Environment Requirements

- Python 3.6 or later (3.11 or later recommended for non-OM installations)
- When using model API capabilities, you are advised to choose the Python version according to the official model API documentation.

### 2.2 Installing the SDK

Refer to the [openGauss](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2/blob/master/README.md) official documentation to install the `psycopg2` driver.

### 2.3 Installing Optional Dependencies

Install the AI model integration dependencies as needed:

```bash
# DashScope (Qwen)
pip install -U dashscope

# OpenAI
pip install openai

# Ollama (requires a local Ollama service)
pip install requests
```

## 3. Quick Start

The following example demonstrates the complete workflow from connecting to the database to executing a hybrid search:

```python
from psycopg2.vector_client import MultiRetrieverClient
from psycopg2.vector_types import TableSchema, ColumnSchema, ColumnType, IndexConfig, IndexType, DistanceMetric
from psycopg2.retrievers import VectorRetriever, FullTextRetriever
from psycopg2.multi_retrieval import RRFFusion

# 1. Creates the client
client = MultiRetrieverClient(
    host="localhost",
    port=5432,
    database="testdb",
    user="test_user",
    password="YourPassword"
)

# 2. Creates the table
schema = TableSchema(columns=[
    ColumnSchema(name="id", type=ColumnType.INTEGER, primary_key=True),
    ColumnSchema(name="content", type=ColumnType.TEXT),
    ColumnSchema(name="embedding", type=ColumnType.VECTOR, dimension=3),
])
client.create_table("documents", schema)

# 3. Creates the index
# Vector index
vector_index = IndexConfig(
    name="idx_embedding_hnsw",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16,
    ef_construction=200
)
client.create_index("documents", vector_index)

# BM25 full-text index
bm25_index = IndexConfig(
    name="idx_content_bm25",
    column="content",
    index_type=IndexType.BM25
)
client.create_index("documents", bm25_index)

# 4. Inserts data
data = [
    {"id": 1, "content": "openGauss是一款开源关系型数据库", "embedding": "[0.1, 0.2, 0.3]"},
    {"id": 2, "content": "向量数据库支持相似性检索", "embedding": "[0.4, 0.5, 0.6]"},
    {"id": 3, "content": "BM25是经典的全文检索算法", "embedding": "[0.7, 0.8, 0.9]"},
]
client.insert("documents", data)

# 5. Vector search
results = client.vector_search(
    table_name="documents",
    query_vector=[0.1, 0.2, 0.3],
    vector_column="embedding",
    metric="cosine",
    top_k=5
)
print("Vector search results:", results)

# 6. Full-text search
results = client.fulltext_search(
    table_name="documents",
    query_text="openGauss数据库",
    text_column="content",
    top_k=5
)
print("Full-text search results:", results)

# 7. Hybrid search (vector + full-text, RRF)
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
print("Hybrid search results:", results)

# 8. Closes the connection
client.close()
```

## 4. API Reference

### 4.1 Unified Client — `MultiRetrieverClient`

`MultiRetrieverClient` is the unified entry point for multimodal retrieval, providing table management, index management, data operations, and retrieval interfaces.

#### Constructor

```python
client = MultiRetrieverClient(
    host="localhost",       # Database host address
    port=5432,              # Port number
    database="postgres",    # Database name
    user="postgres",        # Username
    password="",            # Password
    pool_size=5,            # Minimum pool size
    max_overflow=10,        # Max overflow connections
    timeout=30              # Connection timeout (seconds)
)
```

Context managers are supported:

```python
with MultiRetrieverClient(host="localhost", ...) as client:
    results = client.vector_search(...)
# Connection pool closes automatically
```

#### Table Management Interface

| Method                                                  | Description            |
| ------------------------------------------------------- | ---------------------- |
| create_table(table_name, schema, if_not_exists=True)      | Creates a table.       |
| drop_table(table_name, if_exists=True, cascade=False)     | Drops a table.         |
| describe_table(table_name)                                | Views table structure. |
| list_tables(pattern=None)                                 | Lists all tables.      |

#### Index Management Interface

| Method                                                       | Description       |
| ------------------------------------------------------------ | ----------------- |
| create_index(table_name, index_config, if_not_exists=True) | Creates an index. |
| drop_index(index_name, if_exists=True, cascade=False)      | Drops an index.   |
| list_indexes(table_name=None)                              | Lists indexes.    |

#### Data Operation Interface

| Method                                                         | Description                           |
| -------------------------------------------------------------- | ------------------------------------- |
| insert(table_name, data, batch_size=1000)                    | Inserts data (supports batch insert). |
| update(table_name, data, condition, params=None)             | Updates data.                         |
| delete(table_name, condition=None, ids=None, id_column="id") | Deletes data.                         |
| query(table_name, columns=None, condition=None, limit=None)  | Queries data.                         |

### 4.2 Vector Search — `vector_search`

```python
results = client.vector_search(
    table_name="documents",          # Table name
    query_vector=[0.1, 0.2, 0.3],    # Query vector
    vector_column="embedding",       # Vector column name (defaults to "embedding")
    top_k=10,                        # Number of results (defaults to 10)
    metric="cosine",                 # Distance metric (l2/cosine/inner_product/l1)
    id_column="id",                  # Primary key column name (defaults to "id")
    filter_condition=None,           # SQL WHERE filter condition
    filter_params=None,              # Filter condition parameters
    output_columns=None,             # Output columns to return
    ef_search=None,                  # HNSW query parameter
    probes=None,                     # IVFFlat query parameter
    diskann_probes=None,             # DiskANN query parameter
    rbq_query_bits=None,             # RaBitQ quantization query bits
    rbq_refinek=None,                # RaBitQ refine candidate pool size
)
```

**Distance Metrics:**

| Metric | Operator | Description | Score Calculation |
| --- | --- | --- | --- |
| `l2` | `<->` | Euclidean distance, smaller is more similar | `1 / (1 + distance)` |
| `cosine` | `<=>` | Cosine distance, smaller is more similar | `1 - distance` |
| `inner_product` | `<#>` | Inner product distance (negated) | `-distance` |
| `l1` | `<+>` | Manhattan distance, smaller is more similar | `1 / (1 + distance)` |

**Vector Search with Filter Conditions:**

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

### 4.3 Full-Text Search — `fulltext_search`

```python
results = client.fulltext_search(
    table_name="documents",          # Table name
    query_text="openGauss 向量数据库",  # Query text
    text_column="content",           # Text column name (defaulting to "content", requires BM25 index)
    top_k=10,                        # Number of results
    id_column="id",                  # Primary key column name
    filter_condition=None,           # SQL WHERE filter condition
    output_columns=None,             # Output columns to return
    bm25_k1=None,                    # BM25 k1 parameter (term frequency saturation)
    bm25_b=None,                     # BM25 b parameter (document length normalization)
    bm25_topk=None,                  # Dynamic top-k candidate set size
    use_bm25_taat=False,             # Whether to use Term-At-A-Time (TAAT) method
)
```

> [!NOTE]
>
> Full-text search requires that a BM25 index has been created on the target text column. For information on creating BM25 indexes, refer to the [BM25 Index Usage Guide](./bm25_usage_guide.md).

### 4.4 Hybrid Search — `hybrid_search`

Hybrid search supports arbitrary multi-channel recall (multiple vector retrievals, multiple full-text searches, or any combination), merging and ranking results through a fusion strategy.

```python
results = client.hybrid_search(
    table_name="documents",          # Table name
    retrievers=[...],                # Retriever list
    top_k=10,                        # Final number of results
    fusion_strategy=RRFFusion(),     # Fusion strategy (defaulting to RRF)
    parallel=True,                   # Whether to execute in parallel (defaulting to True)
)
```

#### 4.4.1 Retriever

Each retriever can independently configure filter conditions and output columns:

```python
from psycopg2.retrievers import VectorRetriever, FullTextRetriever

# Vector retriever
vec_retriever = VectorRetriever(
    query_vector=[0.1, 0.2, 0.3],
    vector_column="embedding",
    metric="cosine",
    ef_search=100,
    filter_condition="category = %s",
    filter_params={"category": "tech"},
    output_columns=["id", "content", "category"]
)

# Full-text retriever
ft_retriever = FullTextRetriever(
    query_text="深度学习入门",
    text_column="content",
    bm25_k1=1.2,
    bm25_b=0.75
)
```

#### 4.4.2 Fusion Strategies

**RRF**

RRF is the most commonly used fusion strategy, with the formula: `score = Σ (weight / (k + rank))`

```python
from psycopg2.multi_retrieval import RRFFusion

# Equal-weight RRF (default)
fusion = RRFFusion(k=60)

# Custom-weight RRF
fusion = RRFFusion(k=60, weights=[0.6, 0.4])
```

**Weighted Fusion**

Normalizes scores from each retrieval channel and computes a weighted sum:

```python
from psycopg2.multi_retrieval import WeightedFusion, NormMethod

# arctan normalization (default)
fusion = WeightedFusion(weights=[0.7, 0.3])

# min-max normalization
fusion = WeightedFusion(weights=[0.7, 0.3], norm_method=NormMethod.MIN_MAX)

# No normalization (use when scores are already on the same scale)
fusion = WeightedFusion(weights=[0.7, 0.3], norm_method=NormMethod.NONE)
```

**Model Rerank Fusion**

Leverages the rerank capability of the AI model to perform semantic reranking on multi-channel recall results:

```python
from psycopg2.multi_retrieval import ModelRerankFusion
from psycopg2.models import DashScopeModel

model = DashScopeModel(api_key="sk-xxx")
fusion = ModelRerankFusion(
    model=model,
    query="什么是深度学习？",
    text_field="content",        # Text field for reranking
    fallback_to_rrf=True         # Falls back to RRF when reranking fails
)
```

#### 4.4.3 Complete Hybrid Search Example

```python
from psycopg2.vector_client import MultiRetrieverClient
from psycopg2.retrievers import VectorRetriever, FullTextRetriever
from psycopg2.multi_retrieval import RRFFusion, WeightedFusion, ModelRerankFusion
from psycopg2.models import DashScopeModel

client = MultiRetrieverClient(
    host="localhost", port=5432,
    database="testdb", user="test_user", password="YourPassword"
)

# Defines retrievers
vec_ret = VectorRetriever(
    query_vector=[0.12, 0.34, 0.56, ...],  # 256-dimensional vector
    metric="cosine",
    ef_search=100
)
ft_ret = FullTextRetriever(
    query_text="openGauss向量数据库特性"
)

# Method 1: RRF
results = client.hybrid_search(
    "documents",
    retrievers=[vec_ret, ft_ret],
    top_k=10,
    fusion_strategy=RRFFusion(k=60, weights=[0.6, 0.4])
)

# Method 2: Weighted fusion
results = client.hybrid_search(
    "documents",
    retrievers=[vec_ret, ft_ret],
    top_k=10,
    fusion_strategy=WeightedFusion(weights=[0.7, 0.3])
)

# Method 3: Model rerank fusion
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

## 5. AI Model Integration

The SDK provides a unified AI model interface supporting three capabilities: embedding, rerank, and chat.

### 5.1 Model Providers
Refer to the vendor's official website for more supported types.

| Provider | Class Name | Embed | Rerank | Chat | Dependency Installation |
| --- | --- |------------------------------| --- | --- | --- |
| DashScope (Qwen) | DashScopeModel | text-embedding-v3, etc. | gte-rerank-v2 / qwen3-rerank | qwen-plus / qwen-max | `pip install dashscope` |
| OpenAI | OpenAIModel | text-embedding-3-small/large | Not supported | gpt-4o / gpt-4o-mini | `pip install openai` |
| Ollama (local) | OllamaModel | nomic-embed-text | Not supported | llama3 / qwen2.5 | `pip install requests` |

### 5.2 Using Models

**Method 1: Direct Construction**

```python
from psycopg2.models import DashScopeModel, OpenAIModel, OllamaModel

# DashScope
model = DashScopeModel(api_key="sk-xxx")

# OpenAI
model = OpenAIModel(api_key="sk-xxx")

# Ollama (local deployment)
model = OllamaModel(chat_model="qwen2.5")
```

**Method 2: Factory Function**

```python
from psycopg2.models import create_model

model = create_model("dashscope", api_key="sk-xxx")
model = create_model("openai", api_key="sk-xxx")
model = create_model("ollama", chat_model="llama3")
```

### 5.3 Text Embedding

```python
model = DashScopeModel(api_key="sk-xxx")

# Single text embedding
vector = model.embed_single("openGauss是一款开源数据库")

# Batch embedding
vectors = model.embed(["文本1", "文本2", "文本3"])
```

### 5.4 Text Reranking

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
# Returns: [{"index": 0, "score": 0.95}, {"index": 2, "score": 0.82}]
```

### 5.5 Chat Generation

```python
model = DashScopeModel(api_key="sk-xxx")

# Single-turn chat
response = model.generate("请解释什么是RAG？")

# Multi-turn chat
response = model.chat([
    {"role": "system", "content": "你是一个数据库专家"},
    {"role": "user", "content": "openGauss有哪些向量检索能力？"}
])
```

## 6. Index Types and Configuration

### 6.1 Vector Indexes

**HNSW Index**

```python
from psycopg2.vector_types import IndexConfig, IndexType, DistanceMetric

index = IndexConfig(
    name="idx_hnsw",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16,                    # Max connections per layer ranges from 2 to 100 (defaulting to 16)
    ef_construction=64       # Build parameter ranges from 4 to 1000 (defaulting to 64)
)
```

**HNSW + RaBitQ Quantization Index**

```python
from psycopg2.vector_types import RaBitQRefineType

index = IndexConfig(
    name="idx_hnsw_rbq",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16,
    ef_construction=64,
    enable_rabitq=True,                           # Enables RaBitQ quantization
    rabitq_refine_type=RaBitQRefineType.FP32,     # Refines type: SQ8 / FP32
    rabitq_fht=True                               # Uses Fast Hadamard Transform (FHT) random rotation
)
```

**HNSW + Product Quantization (PQ) Index**

```python
index = IndexConfig(
    name="idx_hnsw_pq",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.L2,
    m=16,
    ef_construction=64,
    enable_pq=True,         # Enables PQ quantization
    pq_m=64,                # Number of subspaces (recommended dim/4)
    pq_ksub=256             # Cluster centers per subspace
)
```

**IVFFlat Index**

```python
index = IndexConfig(
    name="idx_ivfflat",
    column="embedding",
    index_type=IndexType.IVFFLAT,
    metric=DistanceMetric.L2,
    lists=200                # Number of cluster centers
)
```

**DiskANN Index**

```python
index = IndexConfig(
    name="idx_diskann",
    column="embedding",
    index_type=IndexType.DISKANN,
    metric=DistanceMetric.L2,
    index_size=100           # Build parameter ranges from 16 to 1000 (defaulting to 100)
)
```

### 6.2 BM25 Full-Text Index

```python
# Basic BM25 index
bm25_index = IndexConfig(
    name="idx_bm25",
    column="content",
    index_type=IndexType.BM25
)

# Builds the BM25 index in parallel
bm25_index = IndexConfig(
    name="idx_bm25_parallel",
    column="content",
    index_type=IndexType.BM25,
    parallel_workers=8       # Parallel build threads range from 1 to 32
)
```

### 6.3 Vector Data Types

| Type | Enum Value | Max Dimension | Description |
| --- | --- | --- | --- |
| `VECTOR` | `ColumnType.VECTOR` | 16000 | Standard floating-point vector |
| `BIT` | `ColumnType.BIT` | 64000 | Binary vector |
| `SPARSEVEC` | `ColumnType.SPARSEVEC` | 1,000,000,000 | Sparse vector (max non-zero elements: 1000) |

## 7. Complete RAG Application Example

The following demonstrates a complete RAG application example, combining embedding, hybrid search, and LLM generation:

```python
from psycopg2.vector_client import MultiRetrieverClient
from psycopg2.vector_types import (
    TableSchema, ColumnSchema, ColumnType,
    IndexConfig, IndexType, DistanceMetric
)
from psycopg2.retrievers import VectorRetriever, FullTextRetriever
from psycopg2.multi_retrieval import ModelRerankFusion
from psycopg2.models import DashScopeModel

# ========== Initialization ==========

# Creates a model (for embedding, rerank, and chat)
model = DashScopeModel(api_key="sk-xxx")

# Creates a database client
client = MultiRetrieverClient(
    host="localhost", port=5432,
    database="ragdb", user="rag_user", password="YourPassword"
)

# ========== Table and index creation ==========

schema = TableSchema(columns=[
    ColumnSchema(name="id", type=ColumnType.INTEGER, primary_key=True),
    ColumnSchema(name="content", type=ColumnType.TEXT),
    ColumnSchema(name="category", type=ColumnType.VARCHAR, max_length=64),
    ColumnSchema(name="embedding", type=ColumnType.VECTOR, dimension=1024),
])
client.create_table("knowledge_base", schema)

# HNSW vector index
client.create_index("knowledge_base", IndexConfig(
    name="idx_kb_hnsw",
    column="embedding",
    index_type=IndexType.HNSW,
    metric=DistanceMetric.COSINE,
    m=16, ef_construction=200
))

# BM25 full-text index
client.create_index("knowledge_base", IndexConfig(
    name="idx_kb_bm25",
    column="content",
    index_type=IndexType.BM25
))

# ========== Data ingestion (with embedding) ==========

documents = [
    {"id": 1, "content": "openGauss DataVec 支持向量存储与检索，适用于 RAG 场景。", "category": "database"},
    {"id": 2, "content": "BM25 是一种经典的基于词频的全文检索算法。", "category": "algorithm"},
    {"id": 3, "content": "HNSW 索引通过分层图结构实现高效近似最近邻搜索。", "category": "index"},
    # ... more documents
]

# Generates embeddings in batches
texts = [doc["content"] for doc in documents]
embeddings = model.embed(texts)

# Writes embeddings to documents
for doc, emb in zip(documents, embeddings):
    doc["embedding"] = str(emb)

client.insert("knowledge_base", documents)

# ========== RAG Retrieval + Generation ==========

user_question = "openGauss有哪些向量检索能力？"

# 1. Generates the query vector
query_vector = model.embed_single(user_question)

# 2. Hybrid search (vector + BM25 + model rerank)
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

# 3. Builds the context
context = "\n".join([f"- {r['content']}" for r in results])

# 4. Calls the LLM to generate an answer
answer = model.chat([
    {"role": "system", "content": f"请根据以下参考资料回答用户问题。\n\n参考资料：\n{context}"},
    {"role": "user", "content": user_question}
])

print(f"Question: {user_question}")
print(f"Answer: {answer}")

client.close()
```

## 8. Advanced Features

### 8.1 Parallel Multi-Channel Retrieval

`MultiRetrievalEngine` supports parallel execution of multi-channel retrieval using a thread pool, significantly reducing the total latency of multi-channel recall:

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
    parallel=True,          # Executes in parallel (default)
    max_workers=3,          # Maximum worker threads
    timeout=30              # Per-channel timeout (seconds)
)
```

### 8.2 RaBitQ / PQ Quantization Acceleration

When using quantized indexes, query performance can be tuned via Grand Unified Configuration (GUC) parameters:

```python
# RaBitQ quantization acceleration
results = client.vector_search(
    table_name="documents",
    query_vector=query_vector,
    metric="cosine",
    ef_search=100,
    rbq_query_bits=8,       # Query vector quantization bits range from 1 to 8
    rbq_refinek=10.0        # Refine candidate pool size ranges from 1 to 2000000000
)

# HNSW-PQ query
results = client.vector_search(
    table_name="documents",
    query_vector=query_vector,
    metric="l2",
    ef_search=100,
    hnsw_earlystop_threshold=320  # PQ early stop threshold
)

# DiskANN query
results = client.vector_search(
    table_name="documents",
    query_vector=query_vector,
    metric="l2",
    diskann_probes=64       # DiskANN query candidate set size
)
```

### 8.3 Custom Model Provider

You can extend custom model providers by inheriting from `BaseModel`:

```python
from psycopg2.models import BaseModel, register_provider

class MyModel(BaseModel):
    @property
    def provider(self) -> str:
        return "my_provider"
    
    def embed(self, texts, **kwargs):
        # Implements embedding logic
        pass
    
    def rerank(self, query, documents, top_n=None, **kwargs):
        # Implements reranking logic
        pass
    
    def chat(self, messages, **kwargs):
        # Implements chat logic
        pass

# Registers the provider
register_provider("my_provider", MyModel)

# Creates using the factory function
model = create_model("my_provider", api_key="xxx")
```

## 9. Type Reference

### 9.1 ColumnType Enum

| Enum Value | SQL Type | Description |
| --- | --- | --- |
| INTEGER | INTEGER | Integer |
| BIGINT | BIGINT | Large integer |
| TEXT | TEXT | Text |
| VARCHAR | VARCHAR | Variable-length string (requires `max_length`) |
| VECTOR | VECTOR(dim) | Floating-point vector (requires `dimension`) |
| BIT | BIT(dim) | Binary vector (requires `dimension`) |
| SPARSEVEC | SPARSEVEC(dim) | Sparse vector (requires `dimension`) |
| BOOLEAN | BOOLEAN | Boolean value |
| TIMESTAMP | TIMESTAMP | Timestamp |
| JSON | JSON | JSON data |
| JSONB | JSONB | Binary JSON data |

### 9.2 IndexType Enum

| Enum Value | Description |
| --- | --- |
| HNSW | Hierarchical Navigable Small World graph index |
| IVFFLAT | Inverted File Flat index |
| DISKANN | Disk-based Approximate Nearest Neighbor index (max vector dimension 1536) |
| BM25 | BM25 full-text search index |
| BTREE | B-Tree index |
| GIN | Generalized Inverted Index |
| HASH | Hash index |

### 9.3 DistanceMetric Enum

| Enum Value | Operator | Applicable Vector Type |
| --- | --- | --- |
| L2 | <-> | vector |
| COSINE | <=> | vector |
| INNER_PRODUCT | <#> | vector |
| L1 | <+> | vector |
| HALFVEC_L2 | <-> | halfvec |
| BIT_HAMMING | <~> | bit |
| BIT_JACCARD | <%> | bit |
| SPARSEVEC_L2 | <-> | sparsevec |
| SPARSEVEC_COSINE | <=> | sparsevec |

### 9.4 SearchResult Structure

| Field | Type | Description |
| --- | --- | --- |
| id | Any | Document ID |
| score | float | Relevance score (higher is more relevant) |
| source | str | Source identifier (`vector` / `fulltext` / `rrf_fused` / `weighted_fused` / `model_rerank`) |
| data | dict | Document data (contains all queried columns) |

## 10. Frequently Asked Questions

**Q: How do I choose a fusion strategy?**

- **RRF**: Suitable for most scenarios. It is insensitive to the score scales of different retrieval channels and serves as the default choice.
- **Weighted Fusion**: Suitable for scenarios requiring fine-grained control over the weights of each retrieval channel. Use it with normalization methods.
- **Model Rerank**: Suitable for scenarios requiring extremely high retrieval accuracy. It requires additional AI model calls and has higher latency.

**Q: What should the vector dimension be set to?**

It depends on the embedding model used. Common dimensions:
- DashScope text-embedding-v3: Default 1024 dimensions (supports custom 256/512/1024/1536)
- OpenAI text-embedding-3-small: 1536 dimensions
- OpenAI text-embedding-3-large: 3072 dimensions

**Q: How should the weights for each channel in hybrid search be configured?**

- If no weights are provided, the system will automatically distribute them evenly.
- It is generally advisable to set the vector search weight slightly higher than the full-text search weight (for example, 0.6:0.4). Specific tuning depends on the business scenario.
- The sum of weights must equal 1.

**Q: What happens if model reranking fails?**

When `fallback_to_rrf=True` (enabled by default), if reranking fails, the system automatically falls back to the RRF strategy, ensuring retrieval service availability.

[More examples and source code reference](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2)
