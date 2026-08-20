# 使用 openGauss 和 DeepSeek 构建 RAG

在本教程中，我们将向您展示如何使用 openGauss 和 DeepSeek 构建一个检索-增强生成（RAG）管道。该管道集成了 openGauss（用于向量存储）和 DeepSeek（用于生成具有洞察力的上下文感知响应）。

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
$ mkdir deepseek_rag && cd deepseek_rag

# 创建虚拟环境
$ uv venv

# 安装依赖包
$ uv pip install openai psycopg2-binary tqdm fastembed
```

之后的操作均在该项目环境中进行。

## 将文件分块处理

本文以 [Markdown](https://gitcode.com/opengauss/openGauss-server/blob/master/src/gausskernel/storage/access/datavec/docs/zh/datavec_overview.md) 文件作为源文件进行操作演示。

此处以 “#” 为分隔符进行简单的分块处理。

```python
import random
import string
import urllib.request
from tqdm import tqdm

def chunk_source(urls: List[str]) -> List[str]:
    """Chunk text into chunks."""
    
    all_chunks: List[str] = []

    for url in tqdm(urls, desc="Chunking text"):
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
```



将文本分块处理。

```python
urls = [
    "https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md"
]

chunk_texts = chunk_source(urls)
print(chunk_texts)
```



预期输出结果如下。

```shell
['DataVec向量数据库\n\n#', '可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#', '特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#', '快速部署指南\n\nDataVec向量数据库可通过[容器镜像安装](https://docs.opengauss.org/zh/docs/latest/installation_guide/installing_the_container_image.html)快速部署，并快速对接大模型，打造本地RAG智能问答服务。\n\nDataVec向量数据库可通过安装[spqplugin_v2插件](../extension_reference/spqplugin_v2.md)，在大数据量场景下获得数据库分布式存储检索能力。\n\n#', '客户价值\n\n向量数据库通过高效的相似性搜索能力，让大模型能精准召回相关私有知识，从而在检索增强生成（RAG）、推荐、语义搜索等场景中实现更低延迟、更高准确率的企业级AI应用。\n\n#', '特性描述\n\nDataVec能够无缝对接自研大模型。通过嵌入技术将非结构化数据（如文本、图像等）转换为向量数据，DataVec为之提供存储和检索能力。嵌入是一种将非结构化数据映射到向量空间的技术，使得相似文本、图像在向量空间中的距离相近，从而提高检索的准确性和效率。\n\n此外，DataVec还支持鲲鹏指令集加速，实现毫秒级响应。鲲鹏指令集是华为自主研发的一套高性能计算指令集，能够显著提升数据处理和计算的效率。通过利用鲲鹏指令集，DataVec可以在处理大规模向量数据时，提供更快的响应速度和更高的处理能力。\n\n在实际应用中，DataVec可以广泛应用于各种需要高效向量检索的场景。例如，在推荐系统中，DataVec可以根据用户的历史行为和偏好，快速找到与用户兴趣相似的内容，从而提供个性化的推荐。在图像检索中，DataVec可以通过图像特征向量，快速找到与查询图像相似的图片。在自然语言处理(NLP)中，DataVec可以通过文本嵌入，快速找到与查询文本语义相似的文档。\n\nDataVec架构与特性实现详情可参考[向量存储引擎](datavec_architecture.md)介绍。\n\n##', '向量数据类型\n\n- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维\n- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维\n- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数\n- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维\n\n>[!NOTE]说明\n这里的最高维度是在使用索引场景下的最大维度上限值。\n\n支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)\n\n##', '索引支持\n\n- [IVFFLAT](./vector_index.md#ivfflat)  倒排索引\n- [IVF-PQ](./pq.md)  PQ量化压缩倒排索引\n- [IVF-RabitQ](./RabitQ.md)  RabitQ量化压缩倒排索引\n- [HNSW](./vector_index.md#hnsw)  图索引\n- [HNSW-PQ](./pq.md)  PQ量化压缩图索引\n- [HNSW-RabitQ](./RabitQ.md)  RabitQ量化压缩图索引\n\n#', '生态对接\n\nopenGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。\n更详细的指导，参考[向量数据库工具编排使用](dify.md)\n\n#', '使用场景\n\n- 图像识别：用于安全监控、身份验证等场景，通过分析图像中的人脸特征进行识别。\n- 车辆检索：通过摄像头捕捉车辆图像，进行车牌识别和车辆特征分析。\n- 实时轨迹跟踪：在物流行业，通过实时跟踪获取运输轨迹，提高物流效率和安全性。\n- 推荐系统：根据用户浏览和购买力，推荐相关产品，提高用户满意度。\n- 声纹匹配：在金融、安防等领域，通过声纹识别技术进行身份验证，确保交易和操作的安全性。\n- 基因筛选：在药物研发过程中，通过检索特定基因序列，找到潜在的药物靶点，加速新药研发。\n\n这些应用场景展示了DataVec在各个领域的能力，用户可以自由的将向量数据库使能到各个应用中去。通过[向量数据库教程案例](opengauss_ragpratice.md)， 我们为你展示了向量数据库的多种应用模式。\n']
```

## 生成向量数据

### 初始化 Embeddings

```python
from typing import List
import numpy as np
from fastembed import TextEmbedding

embedding_model = TextEmbedding(model_name="BAAI/bge-small-zh-v1.5")

def embedder(texts: List[str]) -> List[List[float]]:
    """ Embed text into embedding vector """

    response = list(embedding_model.embed(texts))
    result = [vec.tolist() if isinstance(vec, np.ndarray) else vec for vec in response]
    return result
```



生成一个测试嵌入，并打印其维度及第一个元素。

```python
vector = embedder(["Is it okay?", "Please help me!"])
print(len(vector[0]))
print(vector)
```



首次运行时会下载模型。

```shell
Fetching 5 files: 100%|██████████████████████████████| 5/5 [00:22<00:00,  4.41s/it]
Download complete: : 95.2MB [00:22, 4.60MB/s]              512
```



预期输出结果如下。

```shell
512
[[-0.07524508982896805, ......, 0.013276959769427776], [-0.05499866232275963, 0.02038949728012085, ......, 0.027692100033164024]]
```

### 生成向量数据

将获取的 text 向量化处理，构造包含 ID、文本、向量数据的行式数据结构。

```python
import uuid
from typing import Any

vectors = embedder(chunk_texts)
dim = len(vectors[0])

rows: list[Any] = []
for chunk_text, vector in zip(chunk_texts, vectors):
    rows.append(
        (
            str(uuid.uuid4()),
            chunk_text,
            vector
        )
    )

print(dim, rows[0])
```



预期输出结果如下。

```shell
512 ('187b2b0f-5224-4c80-a7db-c57736d45604', 'DataVec向量数据库\n\n#', [0.017475901171565056, ......, 0.06092354655265808])
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
<connection object at 0x7822a140a5c0; dsn: 'user=gaussdb password=xxx dbname=postgres host=localhost port=5432', closed: 0>
<cursor object at 0x7822a145a5c0; closed: 0>
```

### 创建表

创建表之前先删除表，防止多次执行脚本时数据被重复写入。

```python
from psycopg2 import sql

table_name = "deepseek_test"

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
- **`embedding`：** `content` 对应的向量表示
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
[('a5e7ce51-bc57-485d-abef-812b07021f09', 'DataVec向量数据库\n\n#'), ('c7815b7e-81f8-43bd-a35b-2dc663d30b26', '可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#'), ('f60fd6ed-92b7-4c0f-b303-a60a50e462b0', '特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#')]
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
[0.0013706306926906109, ......, -0.022342616692185402, 0.027002019807696342]
```

### 执行向量检索

进行向量检索，取前 3 条最接近的结果。

```python
topk: int = 3

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
['DataVec向量数据库\n\n#', '特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#', '向量数据类型\n\n- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维\n- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维\n- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数\n- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维\n\n>[!NOTE]说明\n这里的最高维度是在使用索引场景下的最大维度上限值。\n\n支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)\n\n##']
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
[1] DataVec向量数据库

#

[2] 特性简介

openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。

DataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。

#

[3] 向量数据类型

- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维
- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维
- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数
- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维

>[!NOTE]说明
这里的最高维度是在使用索引场景下的最大维度上限值。

支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)

##
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
import os
from openai import OpenAI

deepseek_client = OpenAI(
    api_key=os.getenv(
        "DEEPSEEK_API_KEY"
    ),
    base_url="https://api.deepseek.com",
)

completion = deepseek_client.chat.completions.create(
    model="deepseek-v4-pro",
    messages=messages,
    stream=True,
    reasoning_effort="high",
    extra_body={"thinking": {"type": "enabled"}}
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

result = "".join(answer_chunks)
print(result)
```



预期输出结果如下。

```shell
====================思考过程====================
我们被要求根据参考资料回答问题。问题是：“openGauss支持哪些向量数据类型”。在参考资料[3]中明确列出了向量数据类型：vector（float向量，最高16000维）、bitvec（bit向量，最高83,886,080维）、sparsevec（sparse向量，最高1,000,000,000维，最高支持16000非零元素数）、halfvec（halfvec向量，最高16000维）。所以回答应该列出这些类型。
====================完整回复====================
根据参考资料，openGauss DataVec 向量数据库支持的向量数据类型包括：

- **vector**：float向量，最高支持 16,000 维
- **bitvec**：bit向量，最高支持 83,886,080 维
- **sparsevec**：sparse向量，最高支持 1,000,000,000 维，且非零元素数最高 16,000 个
- **halfvec**：halfvec向量，最高支持 16,000 维

注：以上最高维度均为使用索引场景下的最大值。根据参考资料，openGauss DataVec 向量数据库支持的向量数据类型包括：

- **vector**：float向量，最高支持 16,000 维
- **bitvec**：bit向量，最高支持 83,886,080 维
- **sparsevec**：sparse向量，最高支持 1,000,000,000 维，且非零元素数最高 16,000 个
- **halfvec**：halfvec向量，最高支持 16,000 维

注：以上最高维度均为使用索引场景下的最大值。
```

## 参考资料

- FastEmbed 官网文档：[Quickstart - Qdrant](https://qdrant.tech/documentation/fastembed/fastembed-quickstart/)

- DeepSeek 官网文档：[首次调用 API | DeepSeek API Docs](https://api-docs.deepseek.com/zh-cn/)


## 附录

本文最终完整的测试用例如下。

```python
#!/usr/bin/env python3
"""RAG (Retrieval-Augmented Generation) module based on DeepSeek and openGauss."""

import os
import json
import hashlib
import random
import string
import urllib.request
from typing import List, Any
from tqdm import tqdm
import numpy as np

from fastembed import TextEmbedding
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam

import psycopg2
from psycopg2 import sql


embedding_model = TextEmbedding(model_name="BAAI/bge-small-zh-v1.5")
deepseek_client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY", "sk-776af5c01af54c77ae6aa9d46fe7f14e"),
    base_url="https://api.deepseek.com",
)

DB_CONFIG = {
    "dbname": os.getenv("OG_DBNAME", "postgres"),
    "user": os.getenv("OG_USER", "gaussdb"),
    "password": os.getenv("OG_PASSWORD", "openGauss@123"),
    "host": os.getenv("OG_HOST", "localhost"),
    "port": int(os.getenv("OG_PORT", "5432")),
}

TABLE_NAME = os.getenv("OG_TABLE", "deepseek")


def reasoning(messages: list[ChatCompletionMessageParam]) -> str:
    """Send messages to the LLM and return the response with streaming output."""

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

    return "".join(answer_chunks)


def batch_embedder(texts: List[str]) -> List[List[float]]:
    """Embed text into embedding vector"""

    response = list(embedding_model.embed(texts))
    result = [vec.tolist() if isinstance(vec, np.ndarray) else vec for vec in response]
    return result


def embedder(text: str) -> List[float]:
    """Generate an embedding vector for a single text."""

    return batch_embedder([text])[0]


def text_chunks(url: str) -> List[str]:
    """Chunk text into chunks."""

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


def batch_text_chunks(urls: List[str]) -> List[tuple]:
    """Convert multiple document sources into (source, chunk) tuples."""
    items: List[Any] = []
    for url in tqdm(urls, desc="Batching text chunks"):
        for text in text_chunks(url):
            items.append((url, text))
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
    vectors = batch_embedder(texts)
    dim = len(vectors[0])

    rows: list[Any] = []
    for i, ((url, text), vector) in enumerate(zip(items, vectors)):
        rows.append(
            (
                compute_doc_id(url, text),
                text,
                vector,
                json.dumps({"url": url, "chunk_index": i}, ensure_ascii=False),
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
    urls = [
        "https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md"
    ]
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

        question = "openGauss支持哪些向量数据类型"  # pylint: disable=invalid-name
        print("问题:", question)
        rag(cursor, table_name, question, top_k=5)
    finally:
        close_connection(conn, cursor)
```

