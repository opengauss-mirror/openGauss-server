# 使用 CamelAI 连接 openGauss 向量存储

## CamelAI 与 openGauss 的位置

CamelAI 是面向智能体（agent）和检索增强生成（RAG）应用的 Python 框架。本适配器把 openGauss 的向量能力接入 CamelAI 的向量存储接口：CamelAI 负责记忆、文档和查询流程，openGauss 负责向量记录的持久化、相似度计算和元数据筛选。

适用场景包括：需要关系型数据库治理能力的长期记忆、带租户或业务标签的向量检索、以及希望复用 openGauss 运维体系的 RAG 应用。该适配器不是完整的 embedding 服务，也不会替应用生成向量。

## 功能概述

`camel-opengauss` 为 CamelAI 提供 `OpenGaussVectorStorage` 适配器，用于将向量记录写入 openGauss，并执行相似度查询。适配器复用 CamelAI 的 `VectorRecord` 和 `VectorDBQuery` 接口，应用可以在保持 CamelAI 向量存储调用方式不变的情况下使用 openGauss。

本文示例以 `camel-ai==0.2.91a5` 和 openGauss `7.0.0-LTS` 为适用版本组合。实际部署时，请根据项目的软件包锁定策略和数据库版本进行兼容性评估。

## 安装依赖

在 Python 3.11 或更高版本环境中创建并激活虚拟环境：

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# POSIX shell
source .venv/bin/activate
```

安装 CamelAI 和适配器依赖：

```bash
python -m pip install "camel-ai==0.2.91a5" "psycopg[binary]>=3.1.18,<4" "pgvector>=0.2.4,<0.3"
```

## 获取并安装 openGauss 适配器

`camel_opengauss` 当前随 KunpengRAG 的 `CamelAI_OpenGauss` 源码目录发布，尚不是可直接从 PyPI 安装的独立包。获取 KunpengRAG 正式仓库内容后，从该目录安装：

```bash
git clone --no-checkout https://gitee.com/kunpeng_compute/KunpengRAG.git
cd KunpengRAG
git sparse-checkout init --cone
git sparse-checkout set CamelAI_OpenGauss
cd CamelAI_OpenGauss
python -m pip install .
```

## 创建连接信息

连接信息通过字典传入适配器。请将以下占位符替换为实际部署参数，并通过环境变量或密钥管理系统提供密码，不要将密码写入源代码或提交到代码仓库。

```python
conn_info = {
    "host": "<HOST>",
    "port": 5432,  # <PORT>
    "dbname": "<DATABASE>",
    "user": "<USER>",
    "password": "<PASSWORD>",
}
```

## 初始化存储

`vector_dim` 必须与写入向量的维度一致。`table_name` 未指定时使用默认表名 `vectors`；建议在不同业务或租户之间使用明确的表名。`distance` 支持 `COSINE`、`EUCLIDEAN` 和 `DOT`。

```python
from camel.types import VectorDistance
from camel_opengauss import OpenGaussVectorStorage

storage = OpenGaussVectorStorage(
    vector_dim=3,
    conn_info=conn_info,
    table_name="camel_vectors",
    distance=VectorDistance.COSINE,
)
```

## 写入向量记录

使用 `VectorRecord` 描述记录。`id` 用于标识记录，`vector` 是浮点向量，`payload` 可以保存业务元数据。

```python
from camel.storages.vectordb_storages import VectorRecord

storage.add([
    VectorRecord(
        id="record-001",
        vector=[1.0, 0.0, 0.0],
        payload={"tenant": "demo", "text": "示例文本"},
    )
])
```

## 相似度查询

通过 `VectorDBQuery` 指定查询向量和返回数量。`metadata_filter` 可按 JSON 元数据筛选结果。

```python
from camel.storages.vectordb_storages import VectorDBQuery

results = storage.query(
    VectorDBQuery(query_vector=[0.9, 0.1, 0.0], top_k=5),
    metadata_filter={"tenant": "demo"},
)
for record in results:
    print(record)
```

## Memory 与 RAG 示例

下面的示例使用仓库提供的 integration API。Memory 和 RAG 文档共用一个向量存储，但通过 payload 中的 `kind` 字段隔离，查询时不会混淆两类记录。

```python
from camel_opengauss import (
    retrieve_memory,
    retrieve_rag_documents,
    write_memory,
    write_rag_document,
)

write_memory(
    storage,
    memory_id="memory-001",
    embedding=[0.9, 0.1, 0.0],
    content="用户偏好简洁的中文技术说明。",
    metadata={"tenant": "demo"},
)
memories = retrieve_memory(
    storage,
    embedding=[0.9, 0.1, 0.0],
    top_k=3,
    metadata={"tenant": "demo"},
)

write_rag_document(
    storage,
    document_id="doc-001",
    embedding=[0.0, 1.0, 0.0],
    content="openGauss 向量检索文档片段。",
    metadata={"tenant": "demo", "source": "guide"},
)
documents = retrieve_rag_documents(
    storage,
    embedding=[0.0, 1.0, 0.0],
    top_k=5,
    metadata={"tenant": "demo"},
)
```

返回值是 payload 字典列表，其中包含 `kind`、`content` 和调用方提供的元数据。

## 关闭存储

使用完毕后调用 `close()` 释放数据库连接。建议放在 `finally` 代码块中，确保异常路径也能释放资源。

```python
try:
    # 写入和查询操作
    pass
finally:
    storage.close()
```

## 限制与兼容性

- 目标组合是 Python 3.11+、`camel-ai==0.2.91a5`、`psycopg` 3.x、`pgvector` 0.2.x 和 openGauss 7.0.0-LTS；部署时请根据实际软件包和数据库版本进行兼容性评估。
- 本适配器依赖 openGauss 的向量类型、JSONB 和 HNSW 能力；如果索引创建失败，初始化仍可继续，但查询性能可能下降。
- `vector_dim` 必须与 embedding 维度一致；表名只允许小写字母、数字和下划线。
