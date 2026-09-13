# Python SDK OpenSearch 适配

本文介绍如何使用 openGauss 连接器中的 OpenSearch 兼容接口，以 OpenSearch 风格的索引、文档和搜索 API 访问 openGauss 向量能力。

> 本特性自 **openGauss 8.0.0-RC1** 起支持，更低版本的数据库和驱动不提供该兼容层。

该接口是 **DSL 到 SQL 的兼容层**，不是官方 `opensearch-py` 的替换包。业务需要改 import、连接参数和部分查询语义，不能指望“代码零修改”。

## 1. 概述

兼容层位于 [openGauss-connector-python-psycopg2](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2) 的 `opensearch_sdk/` 目录，主入口为 `opensearch_sdk.OpenGauss`。它把 OpenSearch 风格的 `indices.create`、`index`、`search`、`knn_search` 等调用转换成 openGauss SQL，并在需要时走 BM25、HNSW/IVF 向量索引和混合检索。

### 1.1 和现有文档的关系

| 文档 | 适用场景 | 入口 |
| --- | --- | --- |
| [Python SDK对接向量数据库](./integrationpython.md) | 直接编写 SQL 做向量 CRUD、多向量并发查询 | `psycopg2.connect`、`execute_multi_search` |
| [融合查询使用指南](./fusion_queries_using_guide.md) | 用 SQL 表达向量与全文的融合查询 | SQL 语法，不经过 SDK |
| **本文** | 已有 OpenSearch DSL 习惯，希望少改查询形态迁到 openGauss | `opensearch_sdk.OpenGauss` |
| [从OpenSearch迁移至openGauss](./opensearch_to_opengauss.md) | 把 OpenSearch 索引数据迁入 openGauss | 迁移脚本，不是查询 API |

混合检索在兼容层里通过 `client.multi` 调用，见第 7 节。若业务没有 OpenSearch DSL 包袱，也可直接用 SQL 完成融合查询，见 [融合查询使用指南](./fusion_queries_using_guide.md)。

### 1.2 术语对照

| OpenSearch 术语 | openGauss 中的含义 |
| --- | --- |
| 索引（Index） | 表（Table） |
| 文档（Document） | 行（Row） |
| 字段（Field） | 列（Column） |
| Mapping | 建表 DDL + 列类型 + 可选向量/BM25 索引 |

兼容层连接的是 **openGauss 数据库端口**（默认 5432），不是 OpenSearch HTTP 端口 9200。

### 1.3 能力范围

| 能力 | 说明 |
| --- | --- |
| 索引与文档 | `indices.create` / `exists` / `delete`，`index`（UPSERT）、`get`、`delete`、批量写入 |
| 查询 DSL | `match`、`term` / `terms`、`range`、`bool`（must / should / must_not / filter），`_source` 过滤、from/size |
| 向量检索 | Mapping 中的 `knn_vector` / `dense_vector`，`knn_search` 或 `search` body 中的 `knn` |
| 向量索引 | Mapping `method.name` 主要为 **hnsw**、**ivf**；DiskANN 请走 `client.multi.vector_search` |
| 混合检索 | `client.multi.hybrid_search`，支持 RRF、加权融合、模型重排序 |
| 连接 | `ThreadedConnectionPool`，默认最小 5、最大 20 个连接 |
| SQL 追踪 | `enable_sql_trace=True`，参数脱敏、结果采样 |

**不是**完整 OpenSearch 集群 API：不提供 HTTP Transport、节点发现、真实分片副本、Ingest Pipeline、Index Template 等。`hosts` 列表只使用第一项；`number_of_shards` / `number_of_replicas` 仅作兼容字段返回，不会在 openGauss 上创建 OpenSearch 分片。

## 2. 安装与环境准备

### 2.1 环境要求

| 组件 | 版本要求 |
| --- | --- |
| openGauss | 8.0.0-RC1 及以上向量数据库 |
| Python | 3.8 及以上（3.6/3.7 需额外安装 `typing_extensions`；非 OM 安装建议 3.11 及以上） |
| psycopg2 | openGauss 版驱动，需包含 `psycopg2.vector_types` |

其他前置条件：

- openGauss 已开启向量能力
- 数据库编码为 **UTF8**（`SQL_ASCII` 库无法按文档示例写入中文）
- 按 [Python SDK对接向量数据库](./integrationpython.md) 的方式安装 **openGauss 版 psycopg2**（含配套 `libpq`）

兼容层会 `from psycopg2.vector_types import TrustedSQL`。社区 `psycopg2-binary` **无法 import 本包**，即便 `--no-deps` 装上兼容层也一样。请确认：

```python
from psycopg2.vector_types import TrustedSQL, ColumnType
from psycopg2.extras import execute_multi_search
```

> 不要用社区 `psycopg2-binary` 覆盖已安装的 openGauss 驱动。兼容层 `setup.py` 声明了 `psycopg2-binary>=2.8.0`，安装时请加 `--no-deps`，避免 pip 换掉驱动。用户目录若已有 binary，它会优先于系统里的 openGauss 驱动，需要先卸掉或调整 `PYTHONPATH`。

### 2.2 安装兼容层

兼容层尚未发布到 PyPI，需从连接器源码安装。`pip install .` 必须在 **`opensearch_sdk` 子目录**执行，而不是连接器仓库根目录。

```bash
git clone https://gitcode.com/opengauss/openGauss-connector-python-psycopg2.git
cd openGauss-connector-python-psycopg2/opensearch_sdk
pip install . --no-deps
```

开发调试可用：

```bash
pip install -e . --no-deps
```

## 3. 快速开始

```python
from opensearch_sdk import OpenGauss

client = OpenGauss(
    hosts=[{"host": "localhost", "port": 5432}],
    database="testdb",
    user="test_user",
    password="YourPassword"
)

mapping = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "category": {"type": "keyword"},
            "embedding": {
                "type": "knn_vector",
                "dimension": 3,
                "space_type": "cosinesimil",
                "method": {"name": "hnsw"}
            }
        }
    }
}

client.indices.create(index="products", body=mapping)

client.index(
    index="products",
    id="1",
    body={
        "title": "openGauss 向量检索",
        "category": "database",
        "embedding": [0.1, 0.2, 0.3]
    }
)

text_result = client.search(
    index="products",
    body={"query": {"match": {"title": "向量"}}}
)
print("全文检索：", text_result)

knn_result = client.knn_search(
    index="products",
    field="embedding",
    query_vector=[0.1, 0.2, 0.3],
    k=5,
    similarity="cosine"
)
print("向量检索：", knn_result)

client.close()
```

## 4. 客户端与连接

```python
client = OpenGauss(
    hosts=[{"host": "localhost", "port": 5432}],
    database="testdb",
    user="test_user",
    password="YourPassword",
    use_connection_pool=True,
    pool_min_conn=5,
    pool_max_conn=20,
    enable_sql_trace=False
)
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `hosts` | 形如 OpenSearch 的主机列表，**仅第一项生效** |
| `database` / `user` / `password` | openGauss 连接信息，密码键名是 `password`，不是 `pwd` |
| `use_connection_pool` | 是否使用连接池，默认开启 |
| `pool_min_conn` / `pool_max_conn` | 连接池大小，默认 5 / 20；也兼容 `minconn` / `maxconn`、`pool_maxsize` |
| `enable_sql_trace` | 是否记录转换后的 SQL，便于对照 DSL 与执行计划 |

事务：

```python
client.commit()
client.rollback()
client.close()
```

健康检查：`client.ping()`。

`hosts` 走 TCP（`127.0.0.1` / `localhost`）。若实例只对 Unix socket 做 trust、TCP 仍要密码，需使用能通过认证的用户密码，或把 `host` 设为 socket 目录（如 `/tmp`）。社区 libpq 对 openGauss TCP 常报 SASL 不支持，这也是必须用 openGauss 版驱动的原因。

## 5. 索引与文档

### 5.1 创建索引

字段必须在 mapping 中预先声明。文本字段用于 BM25 类 `match` 查询，`keyword` 用于精确过滤，向量字段使用 `knn_vector` 或 `dense_vector`。

```python
mapping = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "category": {"type": "keyword"},
            "price": {"type": "float"},
            "embedding": {
                "type": "knn_vector",
                "dimension": 768,
                "space_type": "cosinesimil",
                "method": {
                    "name": "hnsw",
                    "parameters": {
                        "m": 16,
                        "ef_construction": 100
                    }
                }
            }
        }
    }
}
client.indices.create(index="products", body=mapping)
```

IVF 示例：将 `method.name` 设为 `"ivf"`，并在 `parameters` 中设置 `nlist` / `nprobes`。

索引管理：

```python
client.cat.indices()
client.indices.delete(index="products")
```

`indices.exists()` 内部查询 `information_schema.tables`。部分 openGauss 实例没有该 schema，调用会直接报错。请用 `client.cat.indices()`，或查 `pg_tables`。

也可用 `Index` 辅助类：

```python
from opensearch_sdk.helpers import Index

index = Index("products", using=client)
index.mapping({
    "title": {"type": "text"},
    "embedding": {"type": "knn_vector", "dimension": 768, "method": {"name": "hnsw"}}
})
index.create()
```

`settings.number_of_shards` / `number_of_replicas` 不会在 openGauss 上创建搜索集群分片，请按普通表理解。

### 5.2 文档操作

| 方法 | 行为 |
| --- | --- |
| `index` | UPSERT：存在则更新，不存在则插入 |
| `insert` | 仅插入，冲突时失败 |
| `update` | 仅更新，不存在时失败 |
| `delete` | 按 id 删除 |

```python
client.index(index="products", id="1", body={"title": "示例", "category": "tutorial"})
doc = client.get(index="products", id="1")
client.delete(index="products", id="1")
```

## 6. 搜索

### 6.1 全文与过滤

```python
result = client.search(
    index="products",
    body={
        "query": {
            "bool": {
                "must": [{"match": {"title": "向量数据库"}}],
                "filter": [{"term": {"category": "database"}}]
            }
        },
        "size": 10
    }
)
```

支持的查询类型包括 `match`、`term`、`terms`、`range` 以及 `bool` 组合。`match_phrase` 受内核限制，当前会降级，见第 8 节。

### 6.2 向量检索

推荐使用 OpenSearch 风格的 `knn_vector` mapping，再调用 `knn_search`：

```python
result = client.knn_search(
    index="products",
    field="embedding",
    query_vector=[0.1, 0.2, 0.3],
    k=10,
    similarity="cosine",
    ef_search=100
)
```

`similarity` 可选 `cosine`、`l2_norm`、`dot_product`，分别对应 openGauss 操作符 `<=>`、`<->`、`<#>`。

也可以把 `knn` 放进 `search` 的 body，与 `bool` 过滤组合。详细 mapping 与 HNSW/IVF 参数见连接器仓 `opensearch_sdk/doc/developer/06_Vector_Search.md`。

## 7. 混合检索

向量 + BM25 的多路召回通过 `client.multi` 完成。若希望直接用 SQL 表达融合查询，见 [融合查询使用指南](./fusion_queries_using_guide.md)。

```python
from opensearch_sdk.retrieval import VectorRetriever, FullTextRetriever, RRFFusion

retrievers = [
    VectorRetriever(
        query_vector=[0.1, 0.2, 0.3],
        vector_column="embedding",
        metric="cosine",
        output_columns=["title", "category"]
    ),
    FullTextRetriever(
        query_text="openGauss 向量",
        text_column="title",
        output_columns=["title", "category"]
    )
]

results = client.multi.hybrid_search(
    table_name="products",
    retrievers=retrievers,
    top_k=5,
    fusion_strategy=RRFFusion(k=60),
    parallel=True
)
```

加权融合使用 `WeightedFusion(weights=[0.7, 0.3])`。模型重排序需要额外安装对应模型依赖，示例见连接器仓 `opensearch_sdk/examples/example_hybrid_search.py`。

含 `text` 字段的 mapping 在 `indices.create` 时通常会**自动创建 BM25 索引**。先用 `\di` 确认，不要再执行一遍 `IndexConfig(index_type=IndexType.BM25)`，否则会报关系已存在。仅当自动创建失败时，再按 [BM25索引使用指导](./bm25_usage_guide.md) 补建。

## 8. 已知限制

以下限制来自兼容层 `KNOWN_ISSUES.md`，合入业务前应评估。

| 限制 | 现象 | 建议 |
| --- | --- | --- |
| `match_phrase` | 无法走 BM25 短语算子，降级为 `LIKE '%phrase%'`，大数据量变慢且无 BM25 相关性分 | 分类/枚举改 `keyword` + `terms`；可接受分词则用 `match` |
| `nested` | 数组只保留**第一个**对象，后续元素静默丢弃 | 一对一 nested 可用；一对多请拆关联表或 JSONB，不要依赖静默截断 |
| `terms`（字符串 keyword） | 多个值拼成一句再走 BM25，可能因分词/词干产生额外命中 | 需要严格精确匹配时改用规范化 keyword + 确认分词行为 |
| 非 drop-in | 不能 `from opensearchpy import OpenSearch` 后只改地址 | 必须改用 `OpenGauss`，并改连接参数 |
| `hosts` | 多主机列表只连第一项 | 高可用请用 openGauss 自身主备连接方式，而不是 OpenSearch 节点列表 |
| DiskANN | OpenSearch mapping 的 `method` 主要识别 hnsw / ivf | DiskANN 查询走 `client.multi.vector_search` |
| `indices.exists` | 查询 `information_schema.tables`，部分实例无此 schema | 改用 `client.cat.indices()` 或 `pg_tables` |
| 中文数据 | 库编码为 `SQL_ASCII` 时插入/检索中文会 `ascii codec` 失败 | 使用 UTF8 数据库 |
| Python 3.7 | `typing.Literal` 不可用，缺 `typing_extensions` 则无法 import | 建议 3.8+，或 `pip install typing_extensions` |

完整说明见连接器仓 [KNOWN_ISSUES.md](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2/blob/master/opensearch_sdk/opensearch_sdk/KNOWN_ISSUES.md)。

其中 `indices.exists`、中文编码两条与具体实例的初始化方式有关，请在自己的环境中先行确认。

## 9. 示例与源码

连接器仓 `opensearch_sdk/examples/` 中与本文对应的脚本：

| 脚本 | 内容 |
| --- | --- |
| `example_ping_method.py` | 连接与 `ping()` |
| `example_index.py` / `example_document.py` | 索引与文档 CRUD |
| `example_search.py` | match / 过滤 / 多字段查询 |
| `example_hybrid_search.py` | RRF、加权融合、模型重排序 |
| `example_sql_trace.py` | SQL 追踪 |

更多开发说明见 `opensearch_sdk/doc/developer/`。源码与 Issue 跟踪：

[openGauss-connector-python-psycopg2](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2)
