# 使用 openGauss 和 Docling 构建 RAG

[Docling](https://github.com/docling-project/docling) 是由 IBM 研究院主导开发的开源文档智能处理框架，它摒弃了传统的线性文本提取方式，转而采用统一的文档表示模型，并通过集成 DocLayNet 布局分析与 TableFormer 表格识别等先进算法，实现了对 PDF、Office 及图像等多模态文档在转换过程中的语义保真与布局还原，且能与 LangChain、LlamaIndex 等主流 AI 开发框架无缝集成，从而成为构建企业级检索增强生成（RAG）系统及高质量大模型训练数据集的关键基础设施。

在本教程中，我们将向您展示如何使用 openGauss 和 Docling 构建一个检索-增强生成（RAG）管道。该管道集成了 Docling（用于文档解析）、openGauss（用于向量存储）和 OpenAI（用于生成具有洞察力的上下文感知响应）。

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

### LLM 和 Embeddings 模型

本文使用千问模型，通过[大模型服务平台百炼控制台](https://bailian.console.aliyun.com/cn-beijing?spm=5176.12818093_47.resourceCenter.1.1b8416d0xa1cot&tab=model#/api-key)获取对应的 API Key 。

模型详细信息如下。

| 项目       | 值                                                |
| ---------- | ------------------------------------------------- |
| `base_url` | https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1 |
| `api_key`  | sk-xxxxxxxxxx                                     |
| 推理模型   | qwen3.5-plus                                      |
| 向量模型   | text-embedding-v4                                 |

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
> openGauss  容器默认已存在 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir docling_rag && cd docling_rag

# 创建虚拟环境
$ uv venv

# 安装依赖包
$ uv pip install docling openai psycopg2-binary tqdm
```

之后的操作均在该项目环境中进行。

## 使用 Docling 处理文件

Docling 的核心优势在于能将复杂的 PDF、Word、PPT 等文档高质量地转换为统一、表达丰富的 DoclingDocument 表示格式，并支持多种导出格式和选项。有关支持的输入和输出格式的完整列表，请参阅[官方文档](https://docling-project.github.io/docling/usage/supported_formats/)。

本文以 [HTML](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html) 文件作为源文件进行操作演示。

### 将文件转换为 DoclingDocument 格式

将 HTML 文件转换为 DoclingDocument 格式。

```python
from docling.document_converter import DocumentConverter

converter = DocumentConverter()

source = "https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html"

doc = converter.convert(source).document
print(doc)
```



预期输出结果如下。

```shell
schema_name='DoclingDocument' version='1.10.0' name='datavec_overview' origin=DocumentOrigin(mimetype='text/html', binary_hash=7477499220466221947, filename='datavec_overview.html', uri=None) furniture=GroupItem(self_ref='#/furniture', parent=None, children=[], content_layer=<ContentLayer.FURNITURE: 'furniture'>, meta=None, name='_root_', label=<GroupLabel.UNSPECIFIED: 'unspecified'>) body=GroupItem(self_ref='#/body', parent=None, children=[RefItem(cref='#/texts/0'), RefItem(cref='#/texts/1'), RefItem(cref='#/texts/2'), RefItem(cref='#/texts/3'), RefItem(cref='#/texts/4'), RefItem(cref='#/texts/5'), RefItem(cref='#/texts/6'), RefItem(cref='#/texts/7')], ......
```

### 将文件分块

直接将整个文档存入向量数据库效果不佳。我们需要将其切分成更小的、有语义的片段（chunks）。

从 DoclingDocument 出发，原则上存在两种可能的分块方法：

- 将 DoclingDocument 导出为 Markdown（或类似格式），然后作为后处理步骤执行用户定义的分块操作，例如使用 LangChain
- 使用原生 Docling 分块器，即直接在 DoclingDocument 上操作

本文使用原生的 Docling 分块器 Hierarchical Chunker。

```python
from docling_core.transforms.chunker import HierarchicalChunker

chunker = HierarchicalChunker()

chunks = chunker.chunk(doc)

for index, chunk in enumerate(chunks, start=1):
    print(f"\n===== chunk {index} =====")
    print(chunk)
```



预期输出结果如下。

```shell
===== chunk 1 =====
text='本特性自openGauss 6.0.3版本开始引入。' meta=DocMeta(schema_name='docling_core.transforms.chunker.DocMeta', version='1.0.0', doc_items=[TextItem(self_ref='#/texts/9', parent=RefItem(cref='#/texts/8'), children=[], content_layer=<ContentLayer.BODY: 'body'>, meta=None, label=<DocItemLabel.TEXT: 'text'>, prov=[], source=[], comments=[], orig='本特性自openGauss 6.0.3版本开始引入。', text='本特性自openGauss 6.0.3版本开始引入。', formatting=None, hyperlink=None)], headings=['DataVec向量数据库 ', '可获得性 '], captions=None, origin=DocumentOrigin(mimetype='text/html', binary_hash=7477499220466221947, filename='datavec_overview.html', uri=None))

......

===== chunk 28 =====
text='版权所有 ©  openGauss 2026 保留一切权利' meta=DocMeta(schema_name='docling_core.transforms.chunker.DocMeta', version='1.0.0', doc_items=[TextItem(self_ref='#/texts/87', parent=RefItem(cref='#/texts/68'), children=[], content_layer=<ContentLayer.BODY: 'body'>, meta=None, label=<DocItemLabel.TEXT: 'text'>, prov=[], source=[], comments=[], orig='版权所有 ©  openGauss 2026 保留一切权利', text='版权所有 ©  openGauss 2026 保留一切权利', formatting=None, hyperlink=None)], headings=['DataVec向量数据库 ', '使用场景 '], captions=None, origin=DocumentOrigin(mimetype='text/html', binary_hash=7477499220466221947, filename='datavec_overview.html', uri=None))
```



从以上结果可以看到每个分块的数据由 text（核心内容） 和 meta（上下文背景） 两部分组成，最终需要的数据为 text 部分，因此需要进一步处理。

```python
texts: list[Any] = []
for chunk in chunks:
    text = (chunk.text or "").strip()
    if text:
        texts.append(text)

for index, text in enumerate(texts, start=1):
    print(f"\n===== text {index} =====")
    print(text)
```



预期输出结果如下。

```shell
===== text 1 =====
本特性自openGauss 6.0.3版本开始引入。

===== text 2 =====
openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。

......

===== text 28 =====
版权所有 ©  openGauss 2026 保留一切权利
```

## 生成向量数据

### 初始化 Embeddings

使用 DASHSCOPE_API_KEY 环境变量指定 api_key。

```python
import os
from typing import List, Any

from openai import OpenAI


client = OpenAI(
    api_key=os.getenv(
        "DASHSCOPE_API_KEY"
    ),
    base_url="https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
)
```



定义使用 OpenAI 客户端生成文本嵌入的函数。

```python
def batch_embedder(texts: List[str]) -> List[List[float]]:
    vectors: List[List[float]] = []
    max_batch_size: int = 10

    for i in range(0, len(texts), max_batch_size):
        batch = texts[i: i + max_batch_size]
        response = client.embeddings.create(
            input=batch,
            model="text-embedding-v4",
        )
        vectors.extend(item.embedding for item in response.data)
    return vectors
```



生成一个测试嵌入，并打印其维度及第一个元素。

```python
vector = batch_embedder(["Is it okay?"])
print(len(vector[0]))
print(vector[0])
```



预期输出结果如下。

```shell
1024
[-0.03726935014128685, -0.024831822142004967, 0.02821863442659378, 0.04081469401717186, 0.03643345832824707, -0.016703471541404724, ......]
```

### 生成向量数据

将获取的 text 向量化处理，构造包含 ID、文本、向量数据、元数据 JSON 的行式数据结构。

```python
import uuid
import json

vectors = batch_embedder(texts)
dim = len(vectors[0])

rows: list[Any] = []
for i, (text, vector) in enumerate(zip(texts, vectors)):
    rows.append(
        (
            str(uuid.uuid4()),
            text,
            vector,
            json.dumps({"source": source, "chunk_index": i}, ensure_ascii=False),
        )
    )

print(dim, rows[0])
```



预期输出结果如下。

```shell
1024 ('b8f54eda-e100-425c-9a55-38404a278f9c', '本特性自openGauss 6.0.3版本开始引入。', [-0.011304672807455063, 0.013176502659916878, -0.006658260710537434, 0.004182139411568642, -0.012962790206074715, -0.045071303844451904, ......], '{"source": "https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html", "chunk_index": 0}')
```

## 将向量数据载入 openGauss

### 连接 openGauss 数据库

连接 openGauss 数据库。

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
<connection object at 0x7f0dfd733100; dsn: 'user=gaussdb password=xxx dbname=postgres host=localhost port=5432', closed: 0>
<cursor object at 0x7f0dfd3cb100; closed: 0>
```

### 创建表

创建表之前先删除表，防止多次执行脚本时数据被重复写入。

```python
from psycopg2 import sql

table_name = "docling_test"

cursor.execute(
    sql.SQL(
        """
        DROP TABLE IF EXISTS public.{table_name};
        """
    ).format(table_name=sql.Identifier(table_name))
)
```



创建表，表结构对应在第 3 章中构造出的行数据：

- **`id`：** 每条 chunk 的唯一标识
- **`content`：** chunk 的原始文本内容
- **`embedding`： **`content` 对应的向量表示
- **`metadata`：** 结构化元信息
- **`created_at`：** 插入时的时间戳

```python
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
```



查询表是否创建成功且表结构是否正确。

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
[('id', 'text'), ('content', 'text'), ('embedding', 'vector'), ('metadata', 'jsonb'), ('created_at', 'timestamp without time zone')]
```

### 载入向量数据

将向量数据写入到表中。

```python
cursor.executemany(
    sql.SQL(
        """
        INSERT INTO public.{table_name} (id, content, embedding, metadata)
        VALUES (%s, %s, %s, %s::jsonb)
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
        LIMIT 3;
        """
    )
    .format(table_name=sql.Identifier(table_name))
    .as_string(cursor), 
)

print(cursor.fetchall())
```



预期输出结果如下。

```shell
[('7c7c67b0-87ac-4a65-9cc0-5fbd825d1406', '本特性自openGauss 6.0.3版本开始引入。'), ('34f55878-c234-4ccd-a0fb-dacf216f9c72', 'openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。'), ('a3dccf20-ace5-41be-b45d-b90fff626c52', 'DataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。')]
```

## 构建 RAG

### 将问题向量化处理

指定查询问题，并将问题向量化处理。

```python
question = "openGauss支持哪些向量数据类型"

question_vec = batch_embedder([question])[0]

print(question_vec)
```



预期输出结果如下。

```shell
[-0.0018750301096588373, 0.05385503172874451, -0.05283939093351364, 0.020286783576011658, 0.02252640388906002, -0.017109649255871773, ......]
```

### 执行向量检索

进行向量检索，取前 5 条最接近的结果。

```python
topk: int = 5

cursor.execute(
    sql.SQL(
        """
        SELECT content FROM public.{table_name}
        ORDER BY embedding <-> %s::vector
        LIMIT %s::int;
        """
    )
    .format(table_name=sql.Identifier(table_name)),
    (question_vec, topk),
)

contexts = [row[0] for row in cursor.fetchall()]

print(contexts)
```



预期输出结果如下。

```shell
['DataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。', 'openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。', '- [vector](vector_data_type.html#vector) - float向量，最高支持16000维\n- [bitvec](vector_data_type.html#bit) - bit向量，最高支持83,886,080维\n- [sparsevec](vector_data_type.html#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数\n- [halfvec](vector_data_type.html#halfvec) - halfvec向量，最高支持16000维', '支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考 [向量函数和操作符](vector_functions_and_operators.html)', 'openGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。 更详细的指导，参考 [向量数据库工具编排使用](dify.html)']
```



将检索到的文本转换为带编号的字符串，作为给大模型的参考资料。

```python
context_text: str = ""
for i, c in enumerate(contexts, start=1):
    context_text += f"[{i}] {c}\n\n"
    
print(context_text)
```



预期输出结果如下。

```shell
[1] DataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。

[2] openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。

[3] - [vector](vector_data_type.html#vector) - float向量，最高支持16000维
- [bitvec](vector_data_type.html#bit) - bit向量，最高支持83,886,080维
- [sparsevec](vector_data_type.html#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数
- [halfvec](vector_data_type.html#halfvec) - halfvec向量，最高支持16000维

[4] 支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考 [向量函数和操作符](vector_functions_and_operators.html)

[5] openGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。 更详细的指导，参考 [向量数据库工具编排使用](dify.html)
```

### 构建 RAG

构建上下文，为 LLM 定义系统和用户提示。

```python
from openai.types.chat import ChatCompletionMessageParam

SYSTEM_PROMPT = """你是一个智能问答助手。
你的任务是根据提供的参考资料（Context）回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息。
2. 如果资料中没有答案，请直接说“根据提供的资料，无法回答该问题”。
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
            contexts_text=context_text, question=question
        ),
    },
]
```



调用大模型得到最终回答。

```python
completion = client.chat.completions.create(
    model="qwen3.5-plus",
    messages=messages,
    extra_body={"enable_thinking": True},
    stream=True,
)
is_answering = False
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
```



预期输出结果如下。

```shell
====================思考过程====================
Thinking Process:

1.  **Analyze the Request:**
    *   Role: Intelligent Q&A Assistant.
    *   Task: Answer the user's question based *only* on the provided context (References).
    *   Rules:
        1.  Use only provided information (no hallucination).
        2.  If the answer isn't in the context, state "根据提供的资料，无法回答该问题".
        3.  Keep the answer concise and accurate.
    *   Input Context: 5 references about openGauss DataVec.
    *   User Question: "openGauss 支持哪些向量数据类型" (Which vector data types does openGauss support?)

2.  **Scan the Context for Keywords:**
......
====================完整回复====================
根据参考资料，openGauss 支持的向量数据类型包括：
1. vector（float 向量）
2. bitvec（bit 向量）
3. sparsevec（sparse 向量）
4. halfvec（halfvec 向量）
```

## 参考资料

- Docling 官网：[Documentation - Docling](https://docling-project.github.io/docling/)

- Milvus 官网文档：[使用 Milvus 和 Docling 构建 RAG | Milvus 文档](https://milvus.io/docs/zh/build_RAG_with_milvus_and_docling.md)

- 阿里云百炼模型 API：[大模型服务平台百炼控制台](https://bailian.console.aliyun.com/cn-beijing?spm=5176.12818093_47.resourceCenter.1.1b8416d0xa1cot&tab=model#/model-market/detail/qwen3.5-plus?serviceSite=asia-pacific-china)


## 附录

本文最终完整的测试用例如下。

```python
#!/usr/bin/env python3
"""RAG (Retrieval-Augmented Generation) module based on Docling and openGauss."""

import os
import json
import hashlib
from typing import List, Any
from tqdm import tqdm

from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from docling.document_converter import DocumentConverter
from docling_core.transforms.chunker import HierarchicalChunker

import psycopg2
from psycopg2 import sql


converter = DocumentConverter()
chunker = HierarchicalChunker()

client = OpenAI(
    api_key=os.getenv(
        "DASHSCOPE_API_KEY",
        "sk-03cac48e1e4f4d9badc4f02a73756dc6"
    ),
    base_url="https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
)

DB_CONFIG = {
    "dbname": os.getenv("OG_DBNAME", "postgres"),
    "user": os.getenv("OG_USER", "gaussdb"),
    "password": os.getenv("OG_PASSWORD", "openGauss@123"),
    "host": os.getenv("OG_HOST", "localhost"),
    "port": int(os.getenv("OG_PORT", "5432")),
}

TABLE_NAME = os.getenv("OG_TABLE", "docling")


def reasoning(messages: list[ChatCompletionMessageParam]) -> str:
    """Send messages to the LLM and return the response with streaming output."""
    completion = client.chat.completions.create(
        model="qwen3.5-plus",
        messages=messages,
        extra_body={"enable_thinking": True},
        stream=True,
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


def batch_embedder(texts: List[str]) -> List[List[float]]:
    """Generate embedding vectors for a batch of texts."""
    vectors: List[List[float]] = []
    max_batch_size: int = 10

    for i in range(0, len(texts), max_batch_size):
        batch = texts[i: i + max_batch_size]
        response = client.embeddings.create(
            input=batch,
            model="text-embedding-v4",
        )
        vectors.extend(item.embedding for item in response.data)
    return vectors


def embedder(text: str) -> List[float]:
    """Generate an embedding vector for a single text."""
    return batch_embedder([text])[0]


def text_chunks(source: str) -> List[str]:
    """Convert a document source into a list of text chunks."""
    doc = converter.convert(source).document
    chunks = chunker.chunk(doc)
    texts: list[Any] = []
    for chunk in chunks:
        text = (chunk.text or "").strip()
        if text:
            texts.append(text)
    return texts


def batch_text_chunks(sources: List[str]) -> List[tuple]:
    """Convert multiple document sources into (source, chunk) tuples."""
    items: List[Any] = []
    for s in sources:
        for t in text_chunks(s):
            items.append((s, t))
    return items


def compute_doc_id(source: str, text: str) -> str:
    """Compute a SHA-256 document ID from a source and its text chunk."""
    return hashlib.sha256(f"{source}\n{text}".encode("utf-8")).hexdigest()


def filter_new_items(cursor, table_name: str, items: List[tuple]) -> List[tuple]:
    """Filter out items whose document IDs already exist in the database table."""
    ids = [compute_doc_id(s, t) for s, t in items]
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
    vectors = batch_embedder(texts)
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
    question_vec = embedder(question)
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


def rag(cursor, table_name: str, question: str, top_k: int = 5) -> str:
    """Run the full RAG pipeline: retrieve relevant chunks and generate an answer."""
    contexts = retrieve(cursor, table_name, question, top_k)

    contexts_text: str = ""
    for i, c in enumerate(contexts, start=1):
        contexts_text += f"[{i}] {c}\n\n"

    SYSTEM_PROMPT = """你是一个智能问答助手。
你的任务是根据提供的参考资料（Context）回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息。
2. 如果资料中没有答案，请直接说“根据提供的资料，无法回答该问题”。
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
    sources = [
        "https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html",
        "https://docs.opengauss.org/zh/docs/latest/datavec/vector_index.html",
        "https://docs.opengauss.org/zh/docs/latest/datavec/datavec_architecture.html",
    ]
    table_name = TABLE_NAME

    items = batch_text_chunks(sources)

    conn, cursor = create_connection()
    try:
        new_items = filter_new_items(cursor, table_name, items)

        if not new_items:
            print(f"[SKIP] No new chunks. table=public.{table_name}")
        else:
            rows, dim = build_vector_rows(new_items)
            create_table(conn, cursor, table_name, dim)
            insert_rows(conn, cursor, table_name, rows)
            print(f"[OK]   Inserted {len(rows)} rows. table=public.{table_name}, dim={dim}")

        question = "openGauss支持哪些向量数据类型"    # pylint: disable=invalid-name
        print("问题:", question)
        rag(cursor, table_name, question, top_k=5)
    finally:
        close_connection(conn, cursor)
```

