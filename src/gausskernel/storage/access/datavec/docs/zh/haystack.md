# 使用 openGauss 和 Haystack 构建 RAG

[Haystack](https://haystack.deepset.ai/) 是 deepset 开源的大语言模型应用框架，提供文档存储、检索器、Pipeline 编排等组件，常用于构建 RAG（检索增强生成）与语义搜索应用。其官方集成包 `pgvector-haystack` 提供 `PgvectorDocumentStore`，用于基于 PostgreSQL/pgvector 的文档向量存储与检索。

openGauss DataVec 内置 `vector` 向量类型、向量距离算子与 HNSW 索引，语法与 pgvector 对齐，因此可复用 `PgvectorDocumentStore` 接口访问 openGauss，无需重新开发整套 DocumentStore。本文通过适配层 `OpenGaussPgvectorDocumentStore` 介绍如何在 openGauss 上完成文档写入、embedding 存储、向量召回、metadata 过滤与问答链路。

## 准备工作

### 环境配置

**（1）配置 Docker 环境**

本文以容器方式启动 openGauss 实例，因此需要安装 Docker，可参考 [Manuals | Docker Docs](https://docs.docker.com/manuals/) 。

若本地已安装 openGauss 或不需要以容器方式启动可跳过该步骤。

**（2）配置 Python 环境**

适配层要求 Python 3.10 及以上，参考文档：[Installation | uv](https://docs.astral.sh/uv/getting-started/installation/)

```shell
# 使用脚本一键安装
$ curl -LsSf https://astral.sh/uv/install.sh | sh

# 刷新环境变量
$ source ~/.bashrc

# 验证是否安装成功
$ uv --version
```

### LLM 和 Embedding 模型

**（1）LLM 模型**

本文的问答环节可选用任意 OpenAI 兼容的 `/v1/chat/completions` 服务（如 vLLM、DeepSeek、Qwen 等）。未配置 LLM 时示例会走确定性 Mock，保证链路可复现。

通过以下环境变量将模型信息传递给示例程序。

| 环境变量       | 值                                          |
| -------------- | ------------------------------------------- |
| `LLM_ENDPOINT` | http://{llm_ip}:{port}/v1/chat/completions  |
| `LLM_NAME`     | qwen                                        |
| `LLM_API_KEY`  | sk-your-api-key（服务无需鉴权时可不设置）    |

**（2）Embedding 模型**

向量模型使用 `BAAI/bge-small-zh-v1.5`（512 维），以 OpenAI 兼容的 `/v1/embeddings` 服务形式独立部署（如 llama.cpp、TEI、vLLM 等均可）。本文不要求在 Python 进程内加载模型。

通过以下环境变量将模型信息传递给示例程序。

| 环境变量         | 值                                      |
| ---------------- | --------------------------------------- |
| `EMBED_ENDPOINT` | http://{embed_ip}:{port}/v1/embeddings  |
| `EMBED_MODEL`    | BAAI/bge-small-zh-v1.5                  |
| `EMBEDDING_DIM`  | 512                                     |

> [!NOTE]说明
> `EMBEDDING_DIM` 必须与服务实际返回的向量长度一致，否则写入前会报错提示。

### 启动 openGauss 实例

以容器方式启动 openGauss 实例。

```shell
# 启动容器
$ docker run -d \
    --name opengauss \
    --privileged=true \
    --restart=always \
    -p 5432:5432 \
    -e GS_PASSWORD=openGauss@123 \
    -v ./opengauss:/var/lib/opengauss/data \
    opengauss/opengauss:7.0.0-RC1

# 确认容器正常运行
$ docker ps
CONTAINER ID   IMAGE                           COMMAND                  CREATED       STATUS       PORTS                                         NAMES
9a5d6aec69df   opengauss/opengauss:7.0.0-RC1   "entrypoint.sh gauss…"   6 hours ago   Up 6 hours   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp   opengauss
```

> [!NOTE]说明
> openGauss 容器默认已创建 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。DataVec 的 `vector` 类型为内核内置类型，无需执行 `CREATE EXTENSION vector`。

通过以下环境变量将 openGauss 连接信息传递给示例程序。连接串遵循 PostgreSQL libpq 格式。

| 环境变量       | 值                                                              |
| -------------- | --------------------------------------------------------------- |
| `PG_CONN_STR`  | host=127.0.0.1 port=5432 dbname=postgres user=gaussdb password=openGauss@123 |

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir haystack_rag && cd haystack_rag

# 创建虚拟环境
$ uv venv
$ source .venv/bin/activate

# 安装 Haystack 与数据库驱动
$ uv pip install haystack-ai pgvector-haystack "psycopg[binary]>=3.1" requests python-dotenv

# 安装 openGauss 适配层（从 KunpengRAG 仓库获取 haystack_OpenGauss 模块）
$ git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
$ uv pip install -e KunpengRAG/haystack_OpenGauss
```

之后的操作均在该项目环境中进行。

## 构建 RAG

**（1）在项目根目录编写 .env 配置文件，提供 embedding、openGauss 等相关信息**

```ini
# embedding 服务（OpenAI 兼容 /v1/embeddings）
EMBED_ENDPOINT=http://127.0.0.1:9997/v1/embeddings
EMBED_MODEL=BAAI/bge-small-zh-v1.5
EMBEDDING_DIM=512

# openGauss 连接信息
PG_CONN_STR=host=127.0.0.1 port=5432 dbname=postgres user=gaussdb password=openGauss@123

# 可选：真实 LLM 问答（不配置则使用 Mock）
# LLM_ENDPOINT=http://127.0.0.1:8000/v1/chat/completions
# LLM_NAME=qwen
```

**（2）创建 DocumentStore 并写入文档**

适配层只覆盖与 openGauss 有差异的部分：不创建 `vector` 扩展、best-effort 注册向量编解码、HNSW 搜索参数改用 `hnsw_ef_search`。建表、写入、距离算子与 metadata 过滤逻辑与上游一致。

```python
import os

from dotenv import load_dotenv
from haystack import Document
from haystack.document_stores.types import DuplicatePolicy
from haystack.utils.auth import Secret
from opengauss_document_store import OpenGaussPgvectorDocumentStore

load_dotenv()

store = OpenGaussPgvectorDocumentStore(
    connection_string=Secret.from_env_var("PG_CONN_STR"),
    embedding_dimension=int(os.environ["EMBEDDING_DIM"]),
    table_name="haystack_demo_documents",   # 专用表，避免影响既有业务表
    # 索引名在库内唯一，按表名隔离，避免与既有表冲突
    keyword_index_name="haystack_demo_documents_keyword_idx",
    hnsw_index_name="haystack_demo_documents_hnsw_idx",
    vector_function="cosine_similarity",
    search_strategy="exact_nearest_neighbor",  # 先精确检索验证，再切 hnsw
    recreate_table=True,
)

docs = [
    Document(
        content="openGauss 7.0.0-LTS 通过 DataVec 向量引擎原生支持 vector、halfvec、sparsevec 等向量类型。",
        meta={"lang": "zh", "topic": "vector", "category": "guide"},
    ),
    Document(
        content="openGauss 的 HNSW 索引创建语法为 USING hnsw (embedding vector_cosine_ops)。",
        meta={"lang": "zh", "topic": "index", "category": "guide"},
    ),
    Document(
        content="openGauss 距离计算算子与 pgvector 一致：<-> 为欧氏距离，<=> 为余弦距离，<#> 为负内积。",
        meta={"lang": "zh", "topic": "distance", "category": "guide"},
    ),
]
```

**（3）生成 embedding 并入库**

```python
from remote_embedding import RemoteDocumentEmbedder

doc_embedder = RemoteDocumentEmbedder(model=os.environ["EMBED_MODEL"])
docs = doc_embedder.run(documents=docs)["documents"]

written = store.write_documents(docs, policy=DuplicatePolicy.OVERWRITE)
print(f"written={written} count={store.count_documents()}")
```

预期输出结果如下。

```shell
written=3 count=3
```

`RemoteDocumentEmbedder` 的输出类型为 `documents`，因此也可直接接入 Haystack Pipeline 的 `DocumentWriter` 组件。

**（4）向量召回与 metadata 过滤**

```python
from remote_embedding import RemoteTextEmbedder

text_embedder = RemoteTextEmbedder(model=os.environ["EMBED_MODEL"])
query = "openGauss 支持哪些向量索引？"
query_vector = text_embedder.run(text=query)["embedding"]

for doc in store._embedding_retrieval(query_vector, top_k=3):
    print(f"score={doc.score:.4f} {doc.content[:40]}")

guide_docs = store.filter_documents(
    {"field": "meta.category", "operator": "==", "value": "guide"}
)
print(f"filter category=guide -> {len(guide_docs)} 条")
```

预期输出结果如下。

```shell
score=0.7349 openGauss 7.0.0-LTS 通过 DataVec 向量引擎原生支持 ...
score=0.6456 openGauss 的 HNSW 索引创建语法为 USING hnsw (emb...
score=0.4654 openGauss 距离计算算子与 pgvector 一致：<-> 为欧氏距离，
filter category=guide -> 3 条
```

**（5）编排问答链路**

```python
from haystack import Pipeline
from haystack_integrations.components.retrievers.pgvector import (
    PgvectorEmbeddingRetriever,
)

retrieval = Pipeline()
retrieval.add_component("text_embedder", text_embedder)
retrieval.add_component(
    "retriever",
    PgvectorEmbeddingRetriever(document_store=store, top_k=3),
)
retrieval.connect("text_embedder.embedding", "retriever.query_embedding")

documents = retrieval.run({"text_embedder": {"text": query}})["retriever"]["documents"]
context = "\n".join(doc.content for doc in documents)
```

将 `context` 与问题一起交给 Step（2）中配置的 OpenAI 兼容 LLM 服务即可得到答案；未配置 `LLM_ENDPOINT` 时，示例输出确定性 Mock 结果：

```shell
[Mock] 根据知识片段，openGauss 支持 HNSW、IVFFLAT、DISKANN 等向量索引。
```

**（6）清理演示数据**

```python
store.delete_table()   # 仅删除本示例的专用表
store.close()
```

## 附录

本文最终完整的测试用例如下。

```python
"""Build and query a knowledge base on openGauss DataVec using Haystack."""

import os

from dotenv import load_dotenv
from haystack import Document, Pipeline
from haystack.document_stores.types import DuplicatePolicy
from haystack.utils.auth import Secret
from haystack_integrations.components.retrievers.pgvector import (
    PgvectorEmbeddingRetriever,
)

from opengauss_document_store import OpenGaussPgvectorDocumentStore
from remote_embedding import RemoteDocumentEmbedder, RemoteTextEmbedder

TABLE_NAME = "haystack_demo_documents"
QUERY = "openGauss 支持哪些向量索引？"

SAMPLE_TEXTS = [
    (
        "openGauss 7.0.0-LTS 通过 DataVec 向量引擎原生支持 vector、halfvec、sparsevec 等向量类型。",
        {"lang": "zh", "topic": "vector", "category": "guide"},
    ),
    (
        "openGauss 的 HNSW 索引创建语法为 USING hnsw (embedding vector_cosine_ops)。",
        {"lang": "zh", "topic": "index", "category": "guide"},
    ),
    (
        "openGauss 距离计算算子与 pgvector 一致：<-> 为欧氏距离，<=> 为余弦距离，<#> 为负内积。",
        {"lang": "zh", "topic": "distance", "category": "guide"},
    ),
]


def build_store() -> OpenGaussPgvectorDocumentStore:
    """Create the document store on a dedicated demo table."""
    return OpenGaussPgvectorDocumentStore(
        connection_string=Secret.from_env_var("PG_CONN_STR"),
        embedding_dimension=int(os.environ["EMBEDDING_DIM"]),
        table_name=TABLE_NAME,
        # 索引名在库内唯一，按表名隔离避免与业务表冲突
        keyword_index_name=f"{TABLE_NAME}_keyword_idx",
        hnsw_index_name=f"{TABLE_NAME}_hnsw_idx",
        vector_function="cosine_similarity",
        search_strategy="exact_nearest_neighbor",
        recreate_table=True,
    )


def write_documents(store: OpenGaussPgvectorDocumentStore) -> int:
    """Embed and write sample documents."""
    embedder = RemoteDocumentEmbedder(model=os.environ["EMBED_MODEL"])
    docs = [
        Document(content=text, meta=dict(meta)) for text, meta in SAMPLE_TEXTS
    ]
    docs = embedder.run(documents=docs)["documents"]
    return store.write_documents(docs, policy=DuplicatePolicy.OVERWRITE)


def retrieve(store: OpenGaussPgvectorDocumentStore) -> list:
    """Run the embedding-retriever pipeline and return the top-k documents."""
    retrieval = Pipeline()
    retrieval.add_component(
        "text_embedder", RemoteTextEmbedder(model=os.environ["EMBED_MODEL"])
    )
    retrieval.add_component(
        "retriever", PgvectorEmbeddingRetriever(document_store=store, top_k=3)
    )
    retrieval.connect("text_embedder.embedding", "retriever.query_embedding")
    return retrieval.run({"text_embedder": {"text": QUERY}})["retriever"]["documents"]


def main() -> None:
    """Orchestrate the pipeline: init -> write -> retrieve -> filter -> cleanup."""
    load_dotenv()

    store = build_store()
    try:
        print(f"written={write_documents(store)} count={store.count_documents()}")

        for doc in retrieve(store):
            print(f"score={doc.score:.4f} {doc.content[:40]}")

        guides = store.filter_documents(
            {"field": "meta.category", "operator": "==", "value": "guide"}
        )
        print(f"filter category=guide -> {len(guides)} 条")

        empty = store.filter_documents(
            {"field": "meta.category", "operator": "==", "value": "not-exist"}
        )
        print(f"filter not-exist -> {len(empty)} 条")
    finally:
        store.delete_table()
        store.close()


if __name__ == "__main__":
    main()
```
