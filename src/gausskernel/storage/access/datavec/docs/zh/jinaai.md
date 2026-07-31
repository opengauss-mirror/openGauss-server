# 使用 Jina AI 和 openGauss 构建 RAG

[Jina AI](https://jina.ai/) 是一家总部位于德国柏林的搜索基础模型（Search Foundation）公司。其专注于构建小型、高性能的 AI 模型，该模型在多项基准测试中以远小于同类模型的参数量达到了领先水平。所有核心模型均以 Apache 2.0 协议开源，并可通过 API 或本地部署使用。

Jina AI 提供三大核心能力，分别对应 RAG 管道的三个关键环节：

| 核心能力       | 代表模型           | 功能简介                                                     |
| -------------- | ------------------ | ------------------------------------------------------------ |
| **Reader**     | ReaderLM-v2 (1.5B) | 将任意 URL 或 HTML 转换为干净的 Markdown / JSON，支持无头浏览器渲染、PDF 提取，日均处理约 3000 亿 token |
| **Embeddings** | jina-embeddings-v5-text-small | 多语言文本向量化（30+ 语言），支持 32K 上下文、Matryoshka 变长维度、任务特定 LoRA 适配器，可缩至 32 维并配合量化实现最高 95% 存储节省 |
| **Reranker**   | jina-reranker-v3   | 对候选文档进行深度语义重排序，荣获 AAAI-2026 FrontierIR Workshop 最佳论文，在多项检索基准上达到 SOTA 精度 |

此外，Jina AI 还提供 jina-clip-v2（图文多模态嵌入）、jina-vlm（视觉语言模型）、jina-code-embeddings（代码专用嵌入）等模型，以及 CLI 工具和 MCP Server，方便开发者在命令行和 AI Agent 中直接调用。

在本教程中，我们将向您展示如何使用 Jina AI（Reader + Embeddings + Reranker）与 openGauss 构建一套完整的“两阶段检索增强生成（RAG）”管道。该管道集成了 Jina Reader（网页内容提取）、Jina Embeddings（向量嵌入）、openGauss（向量存储与粗排检索）和 Jina Reranker（精排重排序），最终由 DeepSeek LLM 生成上下文感知的精准回答。

与传统的单阶段向量检索相比，两阶段检索（粗排 + 精排）能显著提升检索结果的准确率和召回率。

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

本文使用 Jina AI 的三大核心 API 构建 RAG 管道，并使用 DeepSeek LLM 作为最终生成模型。

| 环节     | 模型 / API                     | 用途                                  |
| -------- | ------------------------------ | ------------------------------------- |
| 数据摄入 | Jina Reader API (`r.jina.ai`)  | 将任意 URL 转换为干净 Markdown        |
| 向量嵌入 | jina-embeddings-v5-text-small  | 文本转向量，支持 32K 上下文、30+ 语言 |
| 粗排检索 | openGauss DataVec              | 向量相似度检索，返回 Top-K 候选       |
| 精排重排 | jina-reranker-v3               | 对候选文档深度重排序                  |
| 答案生成 | DeepSeek LLM (deepseek-v4-pro) | 基于精排后的资料生成回答              |



首先需要获取对应的 API Key：

- **Jina AI API Key**：访问 [Jina AI](https://jina.ai) 注册获取
- **DeepSeek API Key**：访问 [DeepSeek Platform](https://platform.deepseek.com) 获取

其次将 API Key 设置为环境变量：

```shell
export JINA_API_KEY="jina_xxxxxxxxxxxx"
export DEEPSEEK_API_KEY="sk-xxxxxxxxxxxx"
```



另外，jina-embeddings-v5-text-small 引入了任务特定 LoRA 适配器，通过 `task` 参数为不同场景生成优化嵌入：

| task 值             | 适用场景                   |
| ------------------- | -------------------------- |
| `retrieval.passage` | 索引文档块（存入数据库）   |
| `retrieval.query`   | 编码查询问题（检索时使用） |
| `text-matching`     | 语义文本相似度匹配         |
| `classification`    | 文本分类                   |
| `separation`        | 聚类分析                   |

在本教程中，入库时使用 `retrieval.passage`，检索时使用 `retrieval.query`，以获得最佳检索效果。

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
$ mkdir jinaai && cd jinaai

# 创建虚拟环境
$ uv venv

# 安装依赖包
$ uv pip install openai psycopg2-binary
```

之后的操作均在该项目环境中进行。

## 使用 Jina Reader API 摄入数据

### 使用 Jina Reader 获取网页内容

Jina Reader API 可以将任意网页 URL 转换为干净、结构化的 Markdown，是 RAG 管道的数据入口。只需在 URL 前加上 `r.jina.ai/`，或直接调用 API。

```python
import os
import urllib.request

JINA_API_KEY = os.getenv("JINA_API_KEY", "")

def jina_read(url: str) -> str:
    """Convert a web page to clean markdown via Jina Reader API."""
    reader_url = f"https://r.jina.ai/{url}"
    req = urllib.request.Request(reader_url)
    req.add_header("User-Agent", "Mozilla/5.0")
    if JINA_API_KEY:
        req.add_header("Authorization", f"Bearer {JINA_API_KEY}")
    req.add_header("X-Return-Format", "markdown")
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode("utf-8")
```

> [!NOTE]说明
> Reader API 内部使用无头浏览器渲染动态页面，支持 PDF 提取，能处理绝大多数现代网页。免费用户无需 API Key 即可使用（有速率限制），付费用户带上 Key 可获得更高吞吐。



本文以 [HTML](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html) 文件作为源文件进行操作演示。

```python
url="https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html"

file=jina_read(url)

print(file)
```



预期输出结果如下。

```shell
Title: DataVec向量数据库 | openGauss文档 | openGauss社区

URL Source: https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html

Published Time: Fri, 01 May 2026 19:39:43 GMT

Markdown Content:
# DataVec向量数据库 | openGauss文档 | openGauss社区

[![Image 1: openGauss logo](https://docs.opengauss.org/assets/latest/logo.CITHvJJ-.svg)](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html)

*   首页
*   下载
*   开发
*   文档
*   学习
*   支持
*   社区
*   动态

*   首页
*   下载
*   开发
*   文档
*   学习
*   支持
*   社区
*   动态

热门搜索
......
```

### 文档分块处理

获取 Markdown 后，以 `# ` 为分隔符进行简单的分块处理，每个 Markdown 标题段落作为一个独立的文本块。

```python
from typing import List

def text_chunks(source: str) -> List[str]:
    """Fetch and chunk a document into text chunks."""
    all_chunks: List[str] = []

    if source.startswith("http://") or source.startswith("https://"):
        text = jina_read(source)
        text_lines = text.split("# ")
        text_lines = [line for line in text_lines if line.strip()]
        all_chunks.extend(text_lines)
    else:
        with open(source, "r", encoding="utf-8") as f:
            text = f.read()
        text_lines = text.split("# ")
        text_lines = [line for line in text_lines if line.strip()]
        all_chunks.extend(text_lines)

    return all_chunks
```



将文本分块处理。

```python
chunk_texts = text_chunks(url)

for i, chunk in enumerate(chunk_texts[:3]):
    print(f"[Chunk {i}]: {chunk[:80]}...")
```



预期输出结果如下。

```shell
[Chunk 0]: Title: DataVec向量数据库 | openGauss文档 | openGauss社区

URL Source: https://docs.openga...
[Chunk 1]: DataVec向量数据库 | openGauss文档 | openGauss社区

[![Image 1: openGauss logo](https://do...
[Chunk 2]: SDK](https://docs.opengauss.org/zh/docs/latest/datavec/integration_csharp.html) ...
```

## 生成向量数据

### 初始化 Jina Embeddings

使用 OpenAI 兼容接口初始化 Jina AI 客户端。

```python
from openai import OpenAI

jina_client = OpenAI(
    api_key=os.getenv("JINA_API_KEY", ""),
    base_url="https://api.jina.ai/v1",
)
```



调用 jina-embeddings-v5-text-small 模型将数据向量化处理，通过 `task` 参数指定 `retrieval.passage`（文档入库模式）。

```python
def batch_embedder(texts: List[str], task: str = "retrieval.passage") -> List[List[float]]:
    """Generate embedding vectors via Jina Embeddings API."""
    if not texts:
        return []
    vectors: List[List[float]] = []
    max_batch_size = 16
    for i in range(0, len(texts), max_batch_size):
        batch = texts[i : i + max_batch_size]
        response = jina_client.embeddings.create(
            input=batch,
            model="jina-embeddings-v5-text-small",
            extra_body={"task": task, "truncate": True},
        )
        vectors.extend(item.embedding for item in response.data)
    return vectors
```

### 生成向量数据

构造包含 ID、文本、向量的行数据。

```python
import uuid
from typing import Any

vectors = batch_embedder(chunk_texts)
dim = len(vectors[0])

rows: list[Any] = []
for chunk_text, vector in zip(chunk_texts, vectors):
    rows.append(
        (
            str(uuid.uuid4()),
            chunk_text,
            vector,
        )
    )

print(dim, rows[0])
```



预期输出结果如下。

```shell
1024 ('4ae05e97-4155-4271-bb29-6b87d62c2f28', 'Title: DataVec向量数据库 | openGauss文档 | openGauss社区\n\nURL Source: https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html\n\nMarkdown Content:\n', [-0.07923993468284607, ......, -0.002191937994211912])
```

## 将向量数据载入 openGauss

### 连接 openGauss 数据库

```python
import psycopg2

conn = psycopg2.connect(
    dbname = os.getenv("OG_DBNAME", "postgres"),
    user = os.getenv("OG_USER", "gaussdb"),
    password = os.getenv("OG_PASSWORD", "openGauss@123"),
    host = os.getenv("OG_HOST", "localhost"),
    port = int(os.getenv("OG_PORT", "5432"))
)
cursor = conn.cursor()

print(conn)
print(cursor)
```



预期输出结果如下。

```shell
<connection object at 0x79868eb4a700; dsn: 'user=gaussdb password=xxx dbname=postgres host=localhost port=5432', closed: 0>
<cursor object at 0x79868e7809a0; closed: 0>
```

### 创建向量表

创建表之前先删除表，防止多次执行脚本时数据被重复写入。

```python
from psycopg2 import sql

table_name = "jina_test"

cursor.execute(
    sql.SQL(
        """
        DROP TABLE IF EXISTS public.{table_name};
        """
    ).format(table_name=sql.Identifier(table_name))
)
```



创建表，表结构包含：

- **`id`：** 每条 chunk 的唯一标识
- **`content`：** chunk 的原始文本内容
- **`embedding`：** `content` 对应的向量表示（vector 类型）
- **`created_at`：** 插入时的时间戳

```python
cursor.execute(
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
conn.commit()
```



查询表结构。

```python
cursor.execute(
    sql.SQL(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name={table_name}
        ORDER BY ordinal_position;
        """
    ).format(table_name=sql.Literal(table_name)),
)

print(cursor.fetchall())
```



预期输出结果如下。

```json
[('id', 'text'), ('content', 'text'), ('embedding', 'vector'), ('created_at', 'timestamp without time zone')]
```

### 载入向量数据

将向量数据写入到表中。

```python
cursor.executemany(
    sql.SQL(
        """
        INSERT INTO public.{table_name} (id, content, embedding)
        VALUES (%s, %s, %s)
        """
    )
    .format(table_name=sql.Identifier(table_name))
    .as_string(cursor),
    rows,
)
conn.commit()
```



查询是否成功写入。

```python
cursor.execute(
    sql.SQL(
        """
        SELECT id, content FROM public.{table_name}
        LIMIT 1;
        """
    )
    .format(table_name=sql.Identifier(table_name))
    .as_string(cursor), 
)

print(cursor.fetchall())
```



预期输出结果如下。

```shell
[('6e84c6ae-1d32-4b32-b004-c8b8ede7cf83', 'Title: DataVec向量数据库 | openGauss文档 | openGauss社区\n\nURL Source: https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html\n\nPublished Time: Fri, 01 May 2026 19:39:43 GMT\n\nMarkdown Content:\n')]
```

## 构建两阶段 RAG 检索

这是本文区别于传统 RAG 的核心章节。两阶段检索流程如下：

```shell
用户问题 → Jina Embeddings (retrieval.query) → openGauss 粗排 (Top-20)
→ Jina Reranker 精排 (Top-5) → DeepSeek LLM → 最终答案
```

### 将问题向量化处理

将问题以 `retrieval.query` 模式向量化。

```python
question = "openGauss支持哪些向量数据类型"

question_vec = batch_embedder(question, task="retrieval.query")[0]

print(question_vec)
```



预期输出结果如下。

```shell
[-0.026076041162014008, 0.0035898603964596987, ......, -0.03911406174302101, 0.02554747276008129]
```

### 第一阶段：openGauss 粗排检索

在 openGauss 中执行向量相似度检索，返回候选集（例如 Top-20）。

```python
candidate_k = 20

cursor.execute(
    sql.SQL(
        """
        SELECT content FROM public.{table_name}
        ORDER BY embedding <-> %s::vector
        LIMIT %s::int;
        """
    ).format(table_name=sql.Identifier(table_name)),
    (question_vec, candidate_k),
)

candidates = [row[0] for row in cursor.fetchall()]

print(len(candidates))
print(candidates[0])
```



预期输出结果如下。

```shell
13
Title: DataVec向量数据库 | openGauss文档 | openGauss社区

URL Source: https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html

Published Time: Fri, 01 May 2026 19:39:43 GMT

Markdown Content:
```

### 第二阶段：Jina Reranker 精排

将粗排候选交给 jina-reranker-v3 做深度语义匹配和重排序，返回最相关的 Top-K（例如 5 条）。

```python
import json

def rerank(query: str, documents: List[str], top_n: int = 5) -> List[dict]:
    """Rerank documents against a query via Jina Reranker API."""
    if not documents:
        return []
    payload = json.dumps({
        "model": "jina-reranker-v3",
        "query": query,
        "documents": documents,
        "top_n": top_n,
    }).encode("utf-8")
    req = urllib.request.Request("https://api.jina.ai/v1/rerank", data=payload)
    req.add_header("User-Agent", "Mozilla/5.0")
    req.add_header("Content-Type", "application/json")
    if JINA_API_KEY:
        req.add_header("Authorization", f"Bearer {JINA_API_KEY}")
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return body.get("results", [])

results = rerank(question, candidates, top_n=5)
reranked_contexts = [candidates[r["index"]] for r in results]

print("精排后的文档顺序及相关性得分:")
for r in results:
    idx = r["index"]
    score = r["relevance_score"]
    print(f"  [{idx}] score={score:.4f}  {candidates[idx][:80]}...")
```



预期输出结果如下。

```shell
精排后的文档顺序及相关性得分:
  [3] score=0.6454  向量数据类型 [​](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.ht...
  [1] score=0.3892  特性简介 [​](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html...
  [0] score=0.1954  Title: DataVec向量数据库 | openGauss文档 | openGauss社区

URL Source: https://docs.openga...
  [5] score=0.1140  DataVec向量数据库 | openGauss文档 | openGauss社区

[![Image 1: openGauss logo](https://do...
  [7] score=0.0851  索引支持 [​](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html...
```

可以看到，Reranker 将"向量数据类型"这条最相关的文档排到了第一位，而原始粗排中它可能不在最前面。



将检索到的文本转换为带编号的字符串，作为给大模型的参考资料。

```python
contexts_text: str = ""
for i, c in enumerate(reranked_contexts, start=1):
    contexts_text += f"[{i}] {c}\n\n"

print(contexts_text)
```



预期输出结果如下。

```shell
[1] 向量数据类型 [​](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html#user-content-%E5%90%91%E9%87%8F%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B)

*   [vector](https://docs.opengauss.org/zh/docs/latest/datavec/vector_data_type.html#vector) - float向量，最高支持16000维
*   [bitvec](https://docs.opengauss.org/zh/docs/latest/datavec/vector_data_type.html#bit) - bit向量，最高支持83,886,080维
*   [sparsevec](https://docs.opengauss.org/zh/docs/latest/datavec/vector_data_type.html#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数
*   [halfvec](https://docs.opengauss.org/zh/docs/latest/datavec/vector_data_type.html#halfvec) - halfvec向量，最高支持16000维

说明

这里的最高维度是在使用索引场景下的最大维度上限值。

支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](https://docs.opengauss.org/zh/docs/latest/datavec/vector_functions_and_operators.html)

##

[2] 特性简介 [​](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html#user-content-%E7%89%B9%E6%80%A7%E7%AE%80%E4%BB%8B)[](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html)

......

[5] 索引支持 [​](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html#user-content-%E7%B4%A2%E5%BC%95%E6%94%AF%E6%8C%81)

*   [IVFFLAT](https://docs.opengauss.org/zh/docs/latest/datavec/vector_index.html#ivfflat) 倒排索引
*   [IVF-PQ](https://docs.opengauss.org/zh/docs/latest/datavec/pq.html) PQ量化压缩倒排索引
*   [IVF-RabitQ](https://docs.opengauss.org/zh/docs/latest/datavec/RabitQ.html) RabitQ量化压缩倒排索引
*   [HNSW](https://docs.opengauss.org/zh/docs/latest/datavec/vector_index.html#hnsw) 图索引
*   [HNSW-PQ](https://docs.opengauss.org/zh/docs/latest/datavec/pq.html) PQ量化压缩图索引
*   [HNSW-RabitQ](https://docs.opengauss.org/zh/docs/latest/datavec/RabitQ.html) RabitQ量化压缩图索引

#
```

### 构建 RAG 提示词并生成回答

将精排后的上下文组装为 Prompt，调用 DeepSeek LLM 生成最终答案。

```python
from openai.types.chat import ChatCompletionMessageParam

SYSTEM_PROMPT = """你是一个智能问答助手。
你的任务是根据提供的参考资料（Context）回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息。
2. 如果资料中没有答案，请直接说"根据提供的资料，无法回答该问题"。
3. 回答要简洁、准确。"""

USER_PROMPT_TEMPLATE = """请根据以下参考资料回答问题。

<参考资料>
{contexts_text}
</参考资料>

<问题>
{question}
</问题>

请开始回答："""

messages: List[ChatCompletionMessageParam] = [
    {"role": "system", "content": SYSTEM_PROMPT},
    {
        "role": "user",
        "content": USER_PROMPT_TEMPLATE.format(
            contexts_text=contexts_text, question=question
        ),
    },
]
```



初始化客户端并调用 DeepSeek LLM 生成最终答案。

```python
deepseek_client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY", ""),
    base_url="https://api.deepseek.com",
)

completion = deepseek_client.chat.completions.create(
    model="deepseek-v4-pro",
    messages=messages,
    stream=True,
    reasoning_effort="high",
    extra_body={"thinking": {"type": "enabled"}},
)

is_answering = False
answer_chunks = []
print("\n" + "=" * 20 + "思考过程" + "=" * 20)
for chunk in completion:
    delta = chunk.choices[0].delta
    if hasattr(delta, "reasoning_content") and delta.reasoning_content is not None:
        if not is_answering:
            print(delta.reasoning_content, end="", flush=True)
    if hasattr(delta, "content") and delta.content:
        if not is_answering:
            print("\n" + "=" * 20 + "完整回复" + "=" * 20)
            is_answering = True
        print(delta.content, end="", flush=True)
        answer_chunks.append(delta.content)
```



预期输出结果如下。

```shell
====================思考过程====================
我们被问到：openGauss支持哪些向量数据类型。需要基于参考资料回答。参考资料[1]列出：vector, bitvec, sparsevec, halfvec。也提到最高维度等。所以回答涵盖这些即可。
====================完整回复====================
根据提供的参考资料，openGauss 支持以下向量数据类型：

- **vector**：float 向量，最高支持 16000 维。
- **bitvec**：bit 向量，最高支持 83,886,080 维。
- **sparsevec**：sparse 向量，最高支持 1,000,000,000 维，且非零元素数最高为 16000。
- **halfvec**：halfvec 向量，最高支持 16000 维。

这些维度上限均指使用索引场景下的最大维度。
```

## 参考资料

- Jina AI 官网：[Jina AI — Search Foundation Models](https://jina.ai)

- Jina Reader API 文档：[Jina AI Reader](https://jina.ai/reader)

- Jina Embeddings API 文档：[Jina AI Embeddings](https://jina.ai/embeddings)

- Jina Reranker API 文档：[Jina AI Reranker](https://jina.ai/reranker)

- DeepSeek API 文档：[首次调用 API | DeepSeek API Docs](https://api-docs.deepseek.com/zh-cn/)

- openGauss DataVec 文档：[openGauss DataVec 向量数据库](https://docs.opengauss.org/zh/docs/latest/docs/DataVec/)


## 附录

本文最终完整的测试用例如下。

```python
#!/usr/bin/env python3
"""RAG module based on Jina AI (Reader + Embeddings + Reranker) and openGauss."""

import os
import json
import hashlib
import urllib.request
from typing import List, Any
from tqdm import tqdm

from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam

import psycopg2
from psycopg2 import sql


JINA_API_KEY = os.getenv("JINA_API_KEY", "")
JINA_READER_URL = "https://r.jina.ai/"
JINA_RERANKER_URL = "https://api.jina.ai/v1/rerank"

jina_client = OpenAI(
    api_key=JINA_API_KEY,
    base_url="https://api.jina.ai/v1",
)

deepseek_client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
)

DB_CONFIG = {
    "dbname": os.getenv("OG_DBNAME", "postgres"),
    "user": os.getenv("OG_USER", "gaussdb"),
    "password": os.getenv("OG_PASSWORD", "openGauss@123"),
    "host": os.getenv("OG_HOST", "localhost"),
    "port": int(os.getenv("OG_PORT", "5432")),
}

TABLE_NAME = os.getenv("OG_TABLE", "jinaai")


def jina_read(url: str) -> str:
    """Convert a web page to clean markdown via Jina Reader API."""
    reader_url = f"{JINA_READER_URL}{url}"
    req = urllib.request.Request(reader_url)
    req.add_header("User-Agent", "Mozilla/5.0")
    if JINA_API_KEY:
        req.add_header("Authorization", f"Bearer {JINA_API_KEY}")
    req.add_header("X-Return-Format", "markdown")
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode("utf-8")


def batch_embedder(
    texts: List[str], task: str = "retrieval.passage"
) -> List[List[float]]:
    """Generate embedding vectors via Jina Embeddings API."""
    if not texts:
        return []
    vectors: List[List[float]] = []
    max_batch_size = 16
    for i in range(0, len(texts), max_batch_size):
        batch = texts[i : i + max_batch_size]
        response = jina_client.embeddings.create(
            input=batch,
            model="jina-embeddings-v5-text-small",
            extra_body={"task": task, "truncate": True},
        )
        vectors.extend(item.embedding for item in response.data)
    return vectors


def embedder(text: str, task: str = "retrieval.passage") -> List[float]:
    """Generate an embedding vector for a single text."""
    return batch_embedder([text], task=task)[0]


def rerank(query: str, documents: List[str], top_n: int = 5) -> List[dict]:
    """Rerank documents against a query via Jina Reranker API."""
    if not documents:
        return []
    payload = json.dumps(
        {
            "model": "jina-reranker-v3",
            "query": query,
            "documents": documents,
            "top_n": top_n,
        }
    ).encode("utf-8")
    req = urllib.request.Request(JINA_RERANKER_URL, data=payload)
    req.add_header("User-Agent", "Mozilla/5.0")
    req.add_header("Content-Type", "application/json")
    if JINA_API_KEY:
        req.add_header("Authorization", f"Bearer {JINA_API_KEY}")
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return body.get("results", [])


def text_chunks(source: str) -> List[str]:
    """Fetch and chunk a document into text chunks."""
    all_chunks: List[str] = []

    if source.startswith("http://") or source.startswith("https://"):
        text = jina_read(source)
        text_lines = text.split("# ")
        text_lines = [line for line in text_lines if line.strip()]
        all_chunks.extend(text_lines)
    else:
        with open(source, "r", encoding="utf-8") as f:
            text = f.read()
        text_lines = text.split("# ")
        text_lines = [line for line in text_lines if line.strip()]
        all_chunks.extend(text_lines)

    return all_chunks


def batch_text_chunks(sources: List[str]) -> List[tuple]:
    """Convert multiple document sources into (source, chunk) tuples."""
    items: List[Any] = []
    for source in tqdm(sources, desc="Reading & chunking"):
        for text in text_chunks(source):
            items.append((source, text))
    return items


def compute_doc_id(source: str, text: str) -> str:
    """Compute a SHA-256 document ID from a source and its text chunk."""
    return hashlib.sha256(f"{source}\n{text}".encode("utf-8")).hexdigest()


def filter_new_items(cursor, table_name: str, items: List[tuple]) -> List[tuple]:
    """Filter out items whose document IDs already exist in the database table."""
    ids = [compute_doc_id(u, t) for u, t in items]
    try:
        cursor.execute(
            sql.SQL(
                """
                SELECT id FROM public.{table_name}
                WHERE id = ANY(%s);
                """
            ).format(table_name=sql.Identifier(table_name)),
            (ids,),
        )
        existing = {row[0] for row in cursor.fetchall()}
    except psycopg2.errors.UndefinedTable:
        cursor.connection.rollback()
        existing = set()

    new_items: List[Any] = []
    seen = set()
    for item, doc_id in zip(items, ids):
        if doc_id in existing or doc_id in seen:
            continue
        seen.add(doc_id)
        new_items.append(item)
    return new_items


def build_vector_rows(items: List[tuple]):
    """Build database rows with embeddings from (source, text) tuples."""
    texts = [t for _, t in items]
    vectors = batch_embedder(texts, task="retrieval.passage")
    dim = len(vectors[0])

    rows: list[Any] = []
    for i, ((source, text), vector) in enumerate(zip(items, vectors)):
        rows.append(
            (
                compute_doc_id(source, text),
                text,
                vector,
                json.dumps({"source": source, "chunk_index": i}, ensure_ascii=False),
            )
        )
    return rows, dim


def create_connection():
    """Create and return a database connection and cursor."""
    conn = psycopg2.connect(
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
    )
    return conn, conn.cursor()


def close_connection(conn, cursor):
    """Close the database cursor and connection."""
    cursor.close()
    conn.close()


def create_table(conn, cursor, table_name: str, dim: int):
    """Create the vector store table if it does not already exist."""
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS public.{table_name} (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                embedding vector({dim}) NOT NULL,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ).format(table_name=sql.Identifier(table_name), dim=sql.Literal(dim)),
    )
    conn.commit()


def insert_rows(conn, cursor, table_name: str, rows):
    """Insert vector rows into the database table."""
    if not rows:
        return
    stmt = (
        sql.SQL(
            """
            INSERT INTO public.{table_name} (id, content, embedding, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            """
        )
        .format(table_name=sql.Identifier(table_name))
        .as_string(cursor)
    )

    for row in tqdm(rows, desc="Inserting", unit="row"):
        cursor.execute(stmt, row)
    conn.commit()


def retrieve(cursor, table_name: str, question: str, top_k: int = 5) -> List[str]:
    """Retrieve the top-k most relevant document chunks for a question."""
    question_vec = embedder(question, task="retrieval.query")
    cursor.execute(
        sql.SQL(
            """
            SELECT content FROM public.{table_name}
            ORDER BY embedding <-> %s::vector
            LIMIT %s::int;
            """
        ).format(table_name=sql.Identifier(table_name)),
        (question_vec, top_k),
    )
    return [row[0] for row in cursor.fetchall()]


def retrieve_with_rerank(
    cursor, table_name: str, question: str, top_k: int = 5, candidate_k: int = 20
) -> List[str]:
    """Two-stage retrieval: coarse vector search + Jina Reranker fine ranking."""
    candidates = retrieve(cursor, table_name, question, top_k=candidate_k)
    if len(candidates) <= top_k:
        return candidates

    results = rerank(question, candidates, top_n=top_k)
    reranked = [candidates[r["index"]] for r in results]
    return reranked


def reasoning(messages: list[ChatCompletionMessageParam]) -> str:
    """Send messages to DeepSeek LLM and return the response with streaming output."""
    completion = deepseek_client.chat.completions.create(
        model="deepseek-v4-pro",
        messages=messages,
        stream=True,
        reasoning_effort="high",
        extra_body={"thinking": {"type": "enabled"}},
    )

    is_answering = False
    answer_chunks = []
    print("\n" + "=" * 20 + "思考过程" + "=" * 20)
    for chunk in completion:
        delta = chunk.choices[0].delta
        if hasattr(delta, "reasoning_content") and delta.reasoning_content is not None:
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


def rag(  # pylint: disable=too-many-positional-arguments
    cursor,
    table_name: str,
    question: str,
    top_k: int = 5,
    use_rerank: bool = True,
    candidate_k: int = 20,
) -> str:
    """Run the full RAG pipeline: retrieve + optional rerank + generate answer."""
    if use_rerank:
        contexts = retrieve_with_rerank(
            cursor, table_name, question, top_k=top_k, candidate_k=candidate_k
        )
    else:
        contexts = retrieve(cursor, table_name, question, top_k=top_k)

    contexts_text: str = ""
    for i, c in enumerate(contexts, start=1):
        contexts_text += f"[{i}] {c}\n\n"

    SYSTEM_PROMPT = """你是一个智能问答助手。
你的任务是根据提供的参考资料（Context）回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息。
2. 如果资料中没有答案，请直接说"根据提供的资料，无法回答该问题"。
3. 回答要简洁、准确。"""

    USER_PROMPT_TEMPLATE = """请根据以下参考资料回答问题。

<参考资料>
{contexts_text}
</参考资料>

<问题>
{question}
</问题>

请开始回答："""

    messages: List[ChatCompletionMessageParam] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": USER_PROMPT_TEMPLATE.format(
                contexts_text=contexts_text, question=question
            ),
        },
    ]
    return reasoning(messages)


if __name__ == "__main__":
    urls = ["https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html"]
    table_name = TABLE_NAME

    items = batch_text_chunks(urls)

    conn, cursor = create_connection()
    try:
        new_items = filter_new_items(cursor, table_name, items)

        if not new_items:
            print(f"[SKIP] No new chunks. table=public.{table_name}")
        else:
            rows, dim = build_vector_rows(new_items)
            create_table(conn, cursor, table_name, dim)
            insert_rows(conn, cursor, table_name, rows)
            print(
                f"[OK]   Inserted {len(rows)} rows. table=public.{table_name}, dim={dim}"
            )

        QUESTION = "openGauss支持哪些向量数据类型"
        print("问题:", QUESTION)
        print("\n--- 未使用 Reranker ---")
        rag(cursor, table_name, QUESTION, top_k=5, use_rerank=False)
        print("\n--- 使用 Jina Reranker 精排后 ---")
        rag(cursor, table_name, QUESTION, top_k=5, use_rerank=True)
    finally:
        close_connection(conn, cursor)
```
