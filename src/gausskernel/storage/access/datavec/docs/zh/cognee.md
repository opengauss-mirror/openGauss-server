# 使用 openGauss 和 Cognee 构建 RAG

[Cognee](https://docs.cognee.ai/) 是一个开源认知数据处理框架，支持将非结构化文档通过 LLM 提取实体与关系、构建知识图谱，并提供基于图谱增强和文档块检索两种工作模式，适用于 RAG 和语义搜索等场景。

本文重点介绍如何将 openGauss 作为 Cognee 的后端向量数据库构建 RAG 管道。

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

### LLM 和 Embedding 模型

**（1）LLM 模型**

本文使用 deepseek-v4-pro 模型，通过[DeepSeek 开放平台](https://platform.deepseek.com/api_keys)获取 api_key。

通过以下环境变量将模型信息传递到 Cognee 。

| 环境变量       | 值                       |
| -------------- | ------------------------ |
| `LLM_PROVIDER` | custom                   |
| `LLM_MODEL`    | deepseek/deepseek-v4-pro |
| `LLM_API_KEY`  | sk-your-api-key          |



**（2）Embedding 模型**

向量模型使用 [FastEmbed](https://qdrant.tech/documentation/fastembed/?spm=5176.28103460.0.0.7d442988HWGLXi) 的 BAAI/bge-small-zh-v1.5 本地模型。

通过以下环境变量将模型信息传递到 Cognee 。

| 环境变量               | 值                     |
| ---------------------- | ---------------------- |
| `EMBEDDING_PROVIDER`   | fastembed              |
| `EMBEDDING_MODEL`      | BAAI/bge-small-zh-v1.5 |
| `EMBEDDING_DIMENSIONS` | 512                    |

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
> openGauss 容器默认已创建 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。



通过以下环境变量将 openGauss 连接信息传递到 Cognee 。

| 环境变量        | 值                                                           |
| --------------- | ------------------------------------------------------------ |
| `OPENGAUSS_URL` | postgresql://gaussdb:openGauss%40123@localhost:5432/postgres |



另外，可选的环境变量如下：

| 环境变量                      | 说明                   | 默认值 |
| ----------------------------- | ---------------------- | ------ |
| `OPENGAUSS_SCHEMA_NAME`       | 在指定的 schema 创建表 | cognee |
| `OPENGAUSS_CREATE_INDEX`      | 是否创建索引           | false  |
| `OPENGAUSS_INDEX_TYPE`        | 索引类型               | HNSW   |
| `OPENGAUSS_DISTANCE_STRATEGY` | 距离策略               | COSINE |

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir cognee_rag && cd cognee_rag

# 创建虚拟环境
$ uv venv

# 安装 cognee-community-vector-adapter-opengauss 包（若已发布）
$ uv pip install cognee-community-vector-adapter-opengauss

# 直接从官方 github 仓库安装
$ uv pip install "git+https://github.com/topoteretes/cognee-community.git@main#subdirectory=packages/vector/opengauss"

# 安装其他依赖包（本地 fastembed 模型需要）
$ uv pip install cognee[fastembed]
```

之后的操作均在该项目环境中进行。

## 构建 RAG

**（1）在项目根目录编写 .env 配置文件，提供 LLM、Embddding、openGauss 等相关信息**

```ini
# LLM configuration (required by Cognee)
LLM_PROVIDER=custom
LLM_MODEL=deepseek/deepseek-v4-pro
LLM_API_KEY=sk-your-api-key

# Embedding configuration (required by Cognee)
EMBEDDING_PROVIDER=fastembed
EMBEDDING_MODEL=BAAI/bge-small-zh-v1.5
EMBEDDING_DIMENSIONS=512

# openGauss connection
OPENGAUSS_URL=postgresql://gaussdb:openGauss%40123@localhost:5432/postgres
ENABLE_BACKEND_ACCESS_CONTROL=false

# Optional: advanced settings (defaults shown)
# OPENGAUSS_SCHEMA_NAME=cognee
# OPENGAUSS_CREATE_INDEX=false
# OPENGAUSS_INDEX_TYPE=HNSW
# OPENGAUSS_DISTANCE_STRATEGY=COSINE
```



**（2）设置工作目录**

```python
import pathlib

root = pathlib.Path(__file__).parent
config.system_root_directory(str(root / ".cognee_system"))
config.data_root_directory(str(root / ".cognee_data"))
```



**（3）配置 Cognee 使用 openGauss 作为向量数据库**

```python
from os
import asyncio
from dotenv import load_dotenv
from cognee import SearchType, add, cognify, config, prune, search
from cognee_community_vector_adapter_opengauss import register

load_dotenv()

config.set_vector_db_config({
    "vector_db_provider": "opengauss",
    "vector_db_url": os.getenv(
        "OPENGAUSS_URL",
        "postgresql://gaussdb:openGauss%40123@localhost:5432/postgres",
    ),
})
```



**（4）重置 Cognee 数据**

```python
await prune.prune_data()
await prune.prune_system(metadata=True)
```



**（5）预处理文档**

本文使用 [DataVec向量数据库](https://gitcode.com/opengauss/openGauss-server/blob/master/src/gausskernel/storage/access/datavec/docs/zh/datavec_overview.md)作为实例文档，使用 `# ` 进行简单的分块处理。

```python
import random
import string
import urllib.request
from typing import List

url="https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md"

all_chunks: List[str] = []

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
```



**（6）分别调用 `add()` 和 `cognify()` 添加数据集，提取实体、关系和摘要，构建知识图谱，并完成入库。**

```python
for chunk in all_chunks:
    await add(chunk)
await cognify()
```



**（7）进行问答测试**

```python
question="DataVec支持的向量数据类型有哪些"

result = await search(
    query_type=SearchType.RAG_COMPLETION,
    query_text=question,
)

print(result)
```



预期输出结果如下。

```shell
['DataVec支持四种向量数据类型：1. vector（float向量，最高16000维）2. bitvec（bit向量，最高83,886,080维）3. sparsevec（sparse向量，最高1,000,000,000维，最高16000非零元素数）4. halfvec（halfvec向量，最高16000维）。']
```

## 参考资料

- Cognee 官网文档：[Introduction - Cognee Documentation](https://docs.cognee.ai/getting-started/introduction)

- openGauss 官网文档：[产品描述 | openGauss文档 | openGauss社区](https://docs.opengauss.org/zh/docs/latest/about_opengauss/about_opengauss.html)

- DeepSeek API：[DeepSeek 开放平台](https://platform.deepseek.com/api_keys)


## 附录

本文最终完整的测试用例如下。

```python
"""Build and query a knowledge base on openGauss DataVec using the Cognee framework."""

import asyncio
import os
import pathlib
import random
import string
import urllib.request
from typing import List

from dotenv import load_dotenv
from cognee import SearchType, add, cognify, config, prune, search

# Side-effect import: registers the openGauss vector adapter
from cognee_community_vector_adapter_opengauss import register  # noqa: F401


def setup_config() -> None:
    """Initialize Cognee system directories and openGauss vector DB connection."""
    root = pathlib.Path(__file__).parent
    config.system_root_directory(str(root / ".cognee_system"))
    config.data_root_directory(str(root / ".cognee_data"))

    config.set_vector_db_config(
        {
            "vector_db_provider": "opengauss",
            "vector_db_url": os.getenv(
                "OPENGAUSS_URL",
                "postgresql://gaussdb:openGauss%40123@localhost:5432/postgres",
            ),
        }
    )


def fetch_markdown_chunks(url: str) -> List[str]:
    """Download a remote Markdown file and split it by top-level headings."""
    all_chunks: List[str] = []

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

    return all_chunks


async def clear_existing_data() -> None:
    """Purge existing vector data and system metadata."""
    await prune.prune_data()
    await prune.prune_system(metadata=True)


async def ingest_chunks(chunks: List[str]) -> None:
    """Add text chunks into the vector store and run cognitive processing."""
    for chunk in chunks:
        await add(chunk)
    await cognify()


async def ask_question(question: str) -> str:
    """Semantic search over ingested documents via graph completion."""
    return await search(
        query_type=SearchType.RAG_COMPLETION,
        query_text=question,
    )


async def main() -> None:
    """Orchestrate the full pipeline: init → clear → fetch & ingest → query."""
    load_dotenv()
    setup_config()

    await clear_existing_data()

    url = "https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md"
    chunks = fetch_markdown_chunks(url)
    await ingest_chunks(chunks)

    question = "DataVec的向量数据类型"
    result = await ask_question(question)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

