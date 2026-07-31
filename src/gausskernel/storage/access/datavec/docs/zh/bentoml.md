# 使用 BentoML 和 openGauss 构建 RAG

[BentoML](https://www.bentoml.com/) 是一个开源的 AI 模型服务化框架，用于将机器学习模型打包、部署并作为高性能 API 服务运行。它支持自适应批处理、请求队列、CPU/GPU 资源调度，以及一键 Docker 容器化和 BentoCloud 部署。

在本教程中，我们将向您展示如何使用 BentoML（模型服务化）、DeepSeek（LLM）、FastEmbed（本地向量嵌入）与 openGauss（向量存储）构建一套完整的检索增强生成（RAG）管道。该管道集成了 BentoML Service（RAG 管道服务化）、FastEmbed（本地文本向量化）、openGauss DataVec（向量存储与检索）和 DeepSeek LLM（答案生成），最终通过 BentoML Service 对外提供统一的 API 接口。

与传统的单脚本 RAG 不同，BentoML 化的 RAG 管道可以一键部署为独立服务，支持水平扩展、请求批处理和容器化部署。

## 准备工作

### 环境配置

**（1）配置 Docker 环境**

本文以容器方式启动 openGauss 实例，因此需要安装 Docker，可参考 [Manuals | Docker Docs](https://docs.docker.com/manuals/) 。

若本地已安装 openGauss 或不需要以容器方式启动可跳过该步骤。



**（2）配置 uv 环境**

参考文档：[Installation | uv](https://docs.astral.sh/uv/getting-started/installation/)

```shell
# 使用脚本一键安装
$ curl -LsSf https://astral.sh/uv/install.sh | sh

# （可选）为 uv 命令启用 shell 自动补全
$ echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc

# 刷新环境变量
$ source ~/.bashrc

# 验证是否安装成功
$ uv --version
```

### 模型与 API 说明

本文使用 BentoML 作为服务化框架，FastEmbed 作为本地 Embedding 模型，DeepSeek 作为 LLM 后端。

| 环节     | 模型 / 工具         | 用途                             |
| -------- | ------------------- | -------------------------------- |
| 文档分块 | Markdown 标题分割器 | 将文档按 `# ` 标题分割为文本块   |
| 向量嵌入 | FastEmbed           | 本地文本转向量                   |
| 向量存储 | openGauss DataVec   | 向量相似度检索，返回 Top-K 候选  |
| 答案生成 | DeepSeek            | 基于检索到的参考资料生成回答     |
| 服务化   | BentoML Service     | 将 RAG 管道打包为标准化 API 服务 |



**（1）LLM 模型**

本文使用 deepseek-v4-pro 模型，通过 [DeepSeek 开放平台](https://platform.deepseek.com/api_keys)获取 api_key。

模型详细信息如下。

| 项目       | 值                       |
| ---------- | ------------------------ |
| `base_url` | https://api.deepseek.com |
| `api_key`  | sk-xxxxxxxxxx            |
| 模型名称   | deepseek-v4-pro          |



**（2）Embedding 模型**

向量模型使用 [FastEmbed](https://qdrant.tech/documentation/fastembed/?spm=5176.28103460.0.0.7d442988HWGLXi) 的 BAAI/bge-small-zh-v1.5 本地模型。

模型详细信息如下。

| 项目     | 值                     |
| -------- | ---------------------- |
| 模型名称 | BAAI/bge-small-zh-v1.5 |
| 向量维度 | 512                    |

### 启动 openGauss 实例

本文以容器方式启动 openGauss 实例。

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
0b27fd4856a6   opengauss/opengauss:7.0.0-RC1   "entrypoint.sh gauss…"   4 hours ago   Up 4 hours   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp   opengauss
```

> [!NOTE]说明
> openGauss 容器默认已创建 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir bentoml && cd bentoml

# 创建虚拟环境
$ uv venv

# 安装依赖包
$ uv pip install bentoml fastembed numpy openai psycopg2-binary pydantic tqdm
```

之后的操作均在该项目环境中进行。

## 构建 RAG 管道

定义使用 openGauss 作为向量存储、FastEmbed 作为本地嵌入模型、DeepSeek 作为 LLM 的 RAG 管道。

以下代码保存为 `rag.py`。

### openGauss 客户端

`OpenGaussClient` 负责与 openGauss 数据库的所有交互。包含以下方法：

- **`__init__`**：创建数据库连接，连接参数通过环境变量配置（`OG_HOST`、`OG_PORT`、`OG_USER`、`OG_PASSWORD`、`OG_DBNAME`）。
- **`drop_table`**：删除指定表，用于重置数据。
- **`create_table`**：创建向量存储表，包含 `id`（主键）、`content`（文本）、`embedding`（向量）、`created_at`（时间戳）四个字段。
- **`insert_into_table`**：批量插入向量行数据。
- **`query`**：执行向量相似度查询，使用 `<=>` 运算符计算距离，返回 Top-K 结果。
- **`close`**：关闭数据库连接。

```python
import os
from typing import List, Any

import psycopg2
from psycopg2 import sql


class OpenGaussClient:
    """A simple openGauss client using psycopg2."""

    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host or os.getenv("OG_HOST", "localhost")
        self.port = port or int(os.getenv("OG_PORT", "5432"))
        self.user = user or os.getenv("OG_USER", "gaussdb")
        self.password = password or os.getenv("OG_PASSWORD", "openGauss@123")
        self.database = database or os.getenv("OG_DBNAME", "postgres")
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )
        self.cursor = self.conn.cursor()

    def drop_table(self, table_name: str):
        """Drop table."""
        self.cursor.execute(
            sql.SQL(
                """
                DROP TABLE IF EXISTS public.{table_name};
                """
            ).format(table_name=sql.Identifier(table_name))
        )
        self.conn.commit()

    def create_table(self, table_name: str, dim: int):
        """Create table."""
        self.cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    embedding vector({dim}) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            ).format(table_name=sql.Identifier(table_name), dim=sql.Literal(dim)),
        )
        self.conn.commit()

    def insert_into_table(self, table_name: str, rows: List[Any]) -> None:
        """Insert into the table."""
        self.cursor.executemany(
            sql.SQL(
                """
                INSERT INTO public.{table_name} (id, content, embedding)
                VALUES (%s, %s, %s)
                """
            )
            .format(table_name=sql.Identifier(table_name))
            .as_string(self.cursor),
            rows,
        )
        self.conn.commit()

    def query(self, table_name: str, question_vec: List[float], top_k: int = 5) -> List[str]:
        """Execute a vector similarity query and return the results."""
        self.cursor.execute(
            sql.SQL(
                """
                SELECT content
                FROM public.{table_name}
                ORDER BY embedding <-> %s::vector
                LIMIT %s::int;
                """
            ).format(table_name=sql.Identifier(table_name)),
            (question_vec, top_k),
        )
        return [row[0] for row in self.cursor.fetchall()]

    def close(self):
        """Close the connection."""
        self.cursor.close()
        self.conn.close()
```

### 文档分块

`chunk_source` 函数负责将文档源（URL 或本地文件路径）按 Markdown 的 `# ` 标题标记分割为文本块。以 `# ` 为分隔符可以自然地按文档章节边界进行分块，保留每个章节的语义完整性。

```python
import random
import string
import urllib.request

def chunk_source(urls: List[str]) -> List[str]:
    """Chunk text from URLs into chunks."""
    all_chunks: List[str] = []

    for url in urls:
        if url.startswith("http://") or url.startswith("https://"):
            random_name = "".join(random.choices(string.ascii_letters + string.digits, k=8))
            file_path = f"./{random_name}.md"
            try:
                urllib.request.urlretrieve(url, file_path)
                with open(file_path, "r", encoding="utf-8") as file:
                    file_text = file.read()
                text_lines = file_text.split("# ")
                text_lines = [line for line in text_lines if line.strip()]
                all_chunks.extend(text_lines)
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)
        else:
            with open(url, "r", encoding="utf-8") as f:
                file_text = f.read()
            text_lines = file_text.split("# ")
            text_lines = [line for line in text_lines if line.strip()]
            all_chunks.extend(text_lines)

    return all_chunks
```

### RAG 类

`RAG` 类是整个 RAG 管道的核心，集成了 FastEmbed 本地嵌入模型和 DeepSeek LLM。它接收 `OpenGaussClient` 和 `DeepSeek` 客户端作为参数，通过 `_prepare_*` 方法完成初始化。

核心方法如下：

- **`_prepare_deepseek`**：初始化 DeepSeek LLM 客户端和 FastEmbed 嵌入模型（`BAAI/bge-small-zh-v1.5`），定义系统提示和用户提示模板。
- **`_prepare_opengauss`**：绑定 openGauss 客户端，删除旧表并创建新的向量存储表。
- **`_embedder`**：调用本地 FastEmbed 模型将单条文本转为向量。
- **`load`**：将文档分块、向量化，并写入 openGauss。
- **`retrieve`**：将用户问题向量化后，在 openGauss 中执行相似度检索，返回 Top-K 文本块。
- **`answer`**：基于检索到的文本块构建提示词，调用 DeepSeek LLM 生成答案。支持 `return_retrieved_text` 参数同时返回检索结果。

```python
import uuid
import numpy as np
from openai import OpenAI
from fastembed import TextEmbedding


class RAG:
    """RAG (Retrieval-Augmented Generation) class built upon FastEmbed, DeepSeek and openGauss."""

    def __init__(self, deepseek_client: OpenAI, opengauss_client: OpenGaussClient):
        self._prepare_deepseek(deepseek_client)
        self._prepare_opengauss(opengauss_client)

    def _embedder(self, text: str) -> List[float]:
        """Embed text into embedding vector."""
        response = list(self.embedding_model.embed([text]))
        result = response[0]
        return result.tolist() if isinstance(result, np.ndarray) else result

    def _prepare_deepseek(self, deepseek_client: OpenAI, llm_model: str = "deepseek-v4-pro"):
        """Prepare DeepSeek client and embedding model."""
        self.deepseek_client = deepseek_client
        self.llm_model = llm_model
        self.embedding_model = TextEmbedding(model_name="BAAI/bge-small-zh-v1.5")
        self.SYSTEM_PROMPT = """你是一个智能问答助手。
你的任务是根据提供的参考资料（Context）回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息。
2. 如果资料中没有答案，请直接说"根据提供的资料，无法回答该问题"。
3. 回答要简洁、准确。"""
        self.USER_PROMPT = """请根据以下参考资料回答问题。

<参考资料>
{contexts_text}
</参考资料>

<问题>
{question}
</问题>

请开始回答："""

    def _prepare_opengauss(self, opengauss_client: OpenGaussClient, table_name: str = "bentoml_rag"):
        """Prepare openGauss client."""
        self.opengauss_client = opengauss_client
        self.table_name = table_name

        self.opengauss_client.drop_table(self.table_name)
        dim = len(self._embedder("get_dim"))
        self.opengauss_client.create_table(self.table_name, dim)

    def load(self, urls: List[str]):
        """Load the urls."""
        rows: list[Any] = []
        texts = chunk_source(urls)
        for text in texts:
            rows.append(
                (
                    str(uuid.uuid4()),
                    text,
                    self._embedder(text),
                )
            )

        self.opengauss_client.insert_into_table(table_name=self.table_name, rows=rows)

    def retrieve(self, question: str, top_k: int = 3):
        """Retrieve the top k results."""
        question_vec = self._embedder(question)
        retrieve_texts = self.opengauss_client.query(self.table_name, question_vec, top_k)
        return retrieve_texts

    def answer(self, question: str, top_k: int = 3):
        """Answer the question."""
        retrieve_texts = self.retrieve(question, top_k)

        retrieve_texts_str: str = ""
        for i, c in enumerate(retrieve_texts, start=1):
            retrieve_texts_str += f"[{i}] {c}\n\n"

        completion = self.deepseek_client.chat.completions.create(
            model=self.llm_model,
            messages=[
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": self.USER_PROMPT.format(
                        contexts_text=retrieve_texts_str, question=question
                    ),
                },
            ],
            stream=True,
            reasoning_effort="high",
            extra_body={"thinking": {"type": "enabled"}},
        )

        is_answering = False
        answer_chunks = []
        print("\n" + "=" * 20 + "思考过程" + "=" * 20)
        for chunk in completion:
            delta = chunk.choices[0].delta
            if hasattr(delta, "reasoning_content") and delta.reasoning_content:
                if not is_answering:
                    print(delta.reasoning_content, end="", flush=True)
            if hasattr(delta, "content") and delta.content:
                if not is_answering:
                    print("\n" + "=" * 20 + "完整回复" + "=" * 20)
                    is_answering = True
                print(delta.content, end="", flush=True)
                answer_chunks.append(delta.content)
        print()
        return "".join(answer_chunks)
```

### 构建 RAG

创建 DeepSeek 客户端和 openGauss 客户端并构建 RAG。

```python
def build_rag():
    """Build Rag."""
    deepseek_client = OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com",
    )
    opengauss_client = OpenGaussClient()
    rag = RAG(deepseek_client=deepseek_client, opengauss_client=opengauss_client)
    return rag
```

### 测试 RAG 管道

调用 `load` 方法将 openGauss DataVec 文档页面加载到 RAG 管道中。

```python
rag = build_rag()

urls = ["https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md"]

rag.load(urls)
```



调用 `answer` 方法进行问答查询。设置 `return_retrieved_text=True` 可以同时获取答案和检索到的上下文文本。

```python
question = "openGauss支持哪些向量数据类型"

response = rag.answer(question=question, top_k=3)

print(response)
```



预期输出结果如下。

```shell
====================思考过程====================
我们被问到："openGauss支持哪些向量数据类型"。参考资料的[3]部分明确列出了四种向量数据类型：vector, bitvec, sparsevec, halfvec，并附有简要说明。因此可以直接回答。
====================完整回复====================
根据参考资料，openGauss 支持以下四种向量数据类型：
- **vector**：float 向量，最高支持 16000 维
- **bitvec**：bit 向量，最高支持 83,886,080 维
- **sparsevec**：sparse 向量，最高支持 1,000,000,000 维，最高支持 16000 非零元素数
- **halfvec**：halfvec 向量，最高支持 16000 维
```

## 部署 BentoML 服务

以上以脚本方式完成了 RAG 管道的构建和测试。现在需要将整个 RAG 管道打包为 BentoML Service，使其可以独立部署和水平扩展。

### 编写 Service 定义

创建 `service.py`，通过 `import rag as rag_module` 导入第 2 节定义的类，使用 `@bentoml.service` 装饰器定义服务。

```python
import json
import os

import bentoml
import pydantic
from openai import OpenAI
from starlette.responses import Response

import rag as rag_module


def json_response(data: dict) -> Response:
    """Return a JSON response with unicode preserved."""
    return Response(
        json.dumps(data, ensure_ascii=False, indent=2),
        media_type="application/json",
    )


class IngestRequest(pydantic.BaseModel):
    """Request schema for the /ingest endpoint."""

    sources: list[str] = pydantic.Field(default_factory=list)
    table_name: str = pydantic.Field(default="bentoml_rag")


class QueryRequest(pydantic.BaseModel):
    """Request schema for the /query endpoint."""

    question: str
    table_name: str = pydantic.Field(default="bentoml_rag")
    top_k: int = pydantic.Field(default=5, ge=1)


@bentoml.service(
    name="opengauss_rag",
    traffic={"timeout": 300},
)
class OpenGaussRAG:
    """BentoML service that wraps a RAG pipeline backed by openGauss DataVec."""

    def __init__(self) -> None:
        deepseek_client = OpenAI(
            api_key=os.getenv("DEEPSEEK_API_KEY"),
            base_url="https://api.deepseek.com",
        )
        self.client = rag_module.OpenGaussClient()
        self.rag = rag_module.RAG(
            deepseek_client=deepseek_client, opengauss_client=self.client
        )

    @bentoml.api
    async def ingest(self, request: IngestRequest):
        """Ingest documents: chunk, embed, and store into openGauss."""
        if not request.sources:
            return json_response({"status": "ok", "skipped": True})

        self.rag.table_name = request.table_name
        self.rag._prepare_opengauss(self.client, request.table_name)
        self.rag.load(request.sources)

        return json_response({"status": "ok", "table_name": request.table_name})

    @bentoml.api
    async def query(self, request: QueryRequest):
        """Query the RAG pipeline with a question."""
        answer = self.rag.answer(request.question, top_k=request.top_k)
        return json_response({
            "question": request.question,
            "answer": answer,
        })

    @bentoml.api
    async def health(self):
        """Health check endpoint."""
        return json_response({"status": "healthy"})

```

> [!NOTE]说明
> - `service.py` 通过 `import rag as rag_module` 导入第 2 节定义的 `OpenGaussClient` 和 `RAG` 类。
> - 服务启动时在 `__init__` 中创建 DeepSeek 客户端、openGauss 客户端和 RAG 实例。
> - `ingest` 端点调用 `_prepare_opengauss` 初始化表结构，再调用 `load` 摄入数据。
> - `query` 端点直接调用 RAG 实例的 `retrieve` 和 `answer` 方法。
> - `@bentoml.service` 声明一个 BentoML 服务，`traffic.timeout` 设置请求超时时间（秒）。
> - `@bentoml.api` 将方法暴露为 HTTP 端点，请求和响应由 Pydantic 模型自动序列化。
> - BentoML 支持异步 API（`async def`），可以透明地支持自适应批处理。


### 编写 BentoML 构建配置

创建 `bentofile.yaml`，描述服务的构建和部署配置。

```yaml
service: "service:OpenGaussRAG"
description: "RAG pipeline backed by openGauss DataVec vector store"
labels:
  owner: yanzhicong
  project: bentoml-rag
include:
  - "rag.py"
  - "service.py"
python:
  packages:
    - fastembed>=0.5.1
    - numpy>=2.0.0
    - openai>=2.33.0
    - psycopg2-binary>=2.9.12
    - pydantic>=2.0.0
    - tqdm>=4.67.3
```

> [!NOTE]说明
> - `service` 字段格式为 `"<文件名>:<类名>"`，指定 BentoML 服务的入口点。
> - `include` 字段列出需要打包到 Bento 中的 Python 文件。
> - `python.packages` 声明运行时的 pip 依赖，BentoML 会自动安装。

### 本地启动 BentoML 服务

使用 `bentoml serve` 命令在本地启动 RAG 服务。

```shell
# 进入项目目录
$ cd bentoml

# 启动服务
$ uv run bentoml serve service:OpenGaussRAG --port 3000
```

预期输出：

```shell
2026-05-03T19:36:26+0800 [INFO] [cli] Starting production HTTP BentoServer from "service:OpenGaussRAG" listening on http://localhost:3000 (Press CTRL+C to quit)
2026-05-03T19:36:28+0800 [INFO] [entry_service:opengauss_rag:1] Service opengauss_rag initialized
```

### 调用 BentoML 服务 API

服务启动后，可以通过 HTTP 客户端调用 API。

#### 使用 curl

**（1）摄入数据**

```shell
$ curl -X POST http://localhost:3000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "sources": [
        "https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md",
        "https://gitcode.com/opengauss/docs/blob/master/docs/zh/datavec/index.md"
      ],
      "table_name": "bentoml_rag"
    }
  }'
```

预期输出：

```json
{
  "status": "ok",
  "table_name": "bentoml_rag"
}
```



**（2）查询问答**

```shell
$ curl -X POST http://localhost:3000/query \
  -H "Content-Type: application/json" \
  -d '{
    "request": {
      "question": "openGauss支持哪些向量数据类型",
      "table_name": "bentoml_rag",
      "top_k": 5
    }
  }'
```

预期输出：

```json
{
  "question": "openGauss支持哪些向量数据类型",
  "answer": "根据参考资料，openGauss支持的向量数据类型包括以下四种：\n\n- **vector**：float向量，最高支持16000维。\n- **bitvec**：bit向量，最高支持83,886,080维。\n- **sparsevec**：sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数。\n- **halfvec**：halfvec向量，最高支持16000维。"
```



**（3）健康检查**

```shell
$ curl -X POST http://localhost:3000/health
```

预期输出：

```json
{
  "status": "healthy"
}
```

#### 使用 Python 客户端

BentoML 也提供了 Python 原生客户端，可以直接在代码中调用部署的服务。

**（1）摄入数据**

```python
import json
import bentoml

client = bentoml.SyncHTTPClient("http://localhost:3000")

ingest_result = client.call(
    "ingest",
    request={
        "sources": [
            "https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md",
        ],
        "table_name": "bentoml_rag",
    },
)
print(json.dumps(ingest_result, indent=2, ensure_ascii=False))
```

预期输出：

```json
{
  "status": "ok",
  "table_name": "bentoml_rag"
}
```



**（2）查询问答**

```python
query_result = client.call(
    "query",
    request={
        "question": "openGauss支持哪些向量数据类型",
        "table_name": "bentoml_rag",
        "top_k": 5,
    }
)
print(json.dumps(query_result, indent=2, ensure_ascii=False))
```

预期输出：

```json
{
  "question": "openGauss支持哪些向量数据类型",
  "answer": "根据参考资料，openGauss DataVec 支持的向量数据类型包括：\n\n- **vector**：float 向量，最高支持 16000 维  \n- **bitvec**：bit 向量，最高支持 83,886,080 维  \n- **sparsevec**：sparse 向量，最高支持 1,000,000,000 维，最高支持 16000 非零元素数  \n- **halfvec**：halfvec 向量，最高支持 16000 维"
}
```



**（3）健康检查**

```python
health_result = client.call(
    "health",
)
print(json.dumps(health_result, indent=2, ensure_ascii=False))
```

预期输出：

```json
{
  "status": "healthy"
}
```

## 构建并部署 Bento

将服务打包为标准化 Bento 格式，便于分发和部署。

```shell
# 进入项目目录
$ cd bentoml

# 构建 Bento
$ uv run bentoml build
INFO: Adding BentoML requirement to the image: bentoml==1.4.38.
INFO: Locking PyPI package versions.

██████╗ ███████╗███╗   ██╗████████╗ ██████╗ ███╗   ███╗██╗
██╔══██╗██╔════╝████╗  ██║╚══██╔══╝██╔═══██╗████╗ ████║██║
██████╔╝█████╗  ██╔██╗ ██║   ██║   ██║   ██║██╔████╔██║██║
██╔══██╗██╔══╝  ██║╚██╗██║   ██║   ██║   ██║██║╚██╔╝██║██║
██████╔╝███████╗██║ ╚████║   ██║   ╚██████╔╝██║ ╚═╝ ██║███████╗
╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝    ╚═════╝ ╚═╝     ╚═╝╚══════╝

Successfully built Bento(tag="opengauss_rag:wh66tgcg5gmvuaav").

Next steps:

* Deploy to BentoCloud:
    $ bentoml deploy opengauss_rag:wh66tgcg5gmvuaav -n ${DEPLOYMENT_NAME}

* Update an existing deployment on BentoCloud:
    $ bentoml deployment update --bento opengauss_rag:wh66tgcg5gmvuaav ${DEPLOYMENT_NAME}

* Containerize your Bento with `bentoml containerize`:
    $ bentoml containerize opengauss_rag:wh66tgcg5gmvuaav

* Push to BentoCloud with `bentoml push`:
    $ bentoml push opengauss_rag:wh66tgcg5gmvuaav

# 查看已构建的 Bento 列表
$ uv run bentoml list
 Tag                             Size       Model Size  Creation Time
 opengauss_rag:wh66tgcg5gmvuaav  34.06 KiB  0.00 B      2026-05-03 20:14:58
```



也可以将其构建为 Docker 镜像进行部署。

```shell
# 进入项目目录
$ cd bentoml

# 容器化 Bento（生成 Dockerfile 并构建镜像）
$ uv run bentoml containerize opengauss_rag:latest
INFO: Building OCI-compliant image for opengauss_rag:wh66tgcg5gmvuaav with docker

[+] Building 402.7s (17/17) FINISHED                                                                   
......
Successfully built Bento container for "opengauss_rag:latest" with tag(s) "opengauss_rag:wh66tgcg5gmvuaav"
To run your newly built Bento container, run:
    docker run --rm -p 3000:3000 opengauss_rag:wh66tgcg5gmvuaav

# 查看构建的镜像
$ docker images | grep opengauss_rag
opengauss_rag         wh66tgcg5gmvuaav   5456263e088c   About a minute ago   883MB

# 启动容器
$ docker run --rm \
    -e DEEPSEEK_API_KEY="sk-xxxxxxxxxx" \
    -e OG_HOST="172.18.247.170" \
    -e OG_PORT="5432"\
    -e OG_USER="gaussdb"\
    -e OG_PASSWORD="openGauss@123"\
    -e OG_DBNAME="postgres"\
    -p 3000:3000 \
    opengauss_rag:wh66tgcg5gmvuaav
```
