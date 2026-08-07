# 使用 openGauss 和 Unstructured 构建 RAG

[Unstructured](https://docs.unstructured.io/welcome) 库（GitHub、PyPI）提供了一个开源工具包，旨在简化各种数据格式的摄取和预处理，包括图像和基于文本的文档，如 PDF、HTML 文件、Word 文档等。该开源库专注于优化大型语言模型（LLM）的数据工作流程，提供了可无缝协作的模块化功能和连接器。这一整合性系统确保了非结构化数据到结构化格式的高效转换，同时还提供了对各种平台和用例的适应性。

在本教程中，我们将向您展示如何使用 openGauss 和 Unstructured 构建一个检索-增强生成（RAG）管道。该管道集成了 Unstructured（用于文档处理）、openGauss（用于向量存储）和 OpenAI（用于生成具有洞察力的上下文感知响应）。

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
> openGauss  容器默认已创建 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir unstructured_rag && cd unstructured_rag

# 创建虚拟环境
$ uv venv

# 安装依赖包
$ uv pip install unstructured[md] openai psycopg2-binary tqdm
```

> [!NOTE]说明
> 本文以 Markdown 文档为例进行演示，因此安装的依赖包为 `unstructured[md]`，关于其他格式的支持和安装方法可参考：[Full installation - Unstructured](https://docs.unstructured.io/open-source/installation/full-installation)

之后的操作均在该项目环境中进行。

## 使用 Unstructured 处理文件

Unstructured 能够在 `unstructured` 数据中使用分块功能，通过 `partition` 功能检测元数据和文档元素，并将元素后处理成更有用的“块”，以用于检索增强生成 (RAG) 等用例。有关支持的输入格式的完整列表，请参阅[官方文档](https://docs.unstructured.io/open-source/introduction/supported-file-types)。

本文以 [Markdown](https://gitcode.com/opengauss/openGauss-server/blob/master/src/gausskernel/storage/access/datavec/docs/zh/vector_functions_and_operators.md) 文件作为源文件进行操作演示。

先将文档进行分区处理。

```python
from unstructured.partition.md import partition_md

file = "./vector_functions_and_operators.md"

elements = partition_md(filename=file)

for index, element in enumerate(elements, start=1):
    print(f"\n===== element {index} =====")
    print(element)
```



预期输出结果如下。

```shell
===== element 1 =====
向量函数和操作符

===== element 2 =====
Vector

===== element 3 =====
Vector 操作符

......

===== element 436 =====
[!NOTE]说明 向量数据类型会和其他类型使用同样的函数名、操作符，为确保执行向量操作无误，建议对至少一个入参进行显式类型转换，如 SELECT l2_distance('[0,0]'::vector, '[3,4]');。
```



再将分区的结果进行分块处理，即将分区产生的连续的元素组合起来。

```python
from unstructured.chunking.title import chunk_by_title

chunks = chunk_by_title(elements)

for index, chunk in enumerate(chunks, start=1):
    print(f"\n===== chunk {index} =====")
    print(chunk.text)
    print("*" * 20)
    print(chunk.metadata.__dict__)
```



预期输出结果如下。

```shell
===== chunk 1 =====
向量函数和操作符

Vector

Vector 操作符
********************
{'file_directory': '.', 'filename': 'vector_functions_and_operators.md', 'filetype': 'text/markdown', 'languages': ['eng'], 'last_modified': '2026-04-23T11:52:23', 'orig_elements': [<unstructured.documents.elements.Title object at 0x7f6fcbbad990>, <unstructured.documents.elements.Title object at 0x7f6fcbbe2bd0>, <unstructured.documents.elements.Title object at 0x7f6fcbbe2a90>]}

......

===== chunk 38 =====
TEXT/VARCHAR 转 Halfvec

示例：

openGauss=# SELECT '[1,2,3,4,5]'::halfvec;
   halfvec    
-------------
 [1,2,3,4,5]
(1 row)

Vector 转 Halfvec

示例：

openGauss=# SELECT '[1,2,3,4,5]'::vector::halfvec;
   halfvec    
-------------
 [1,2,3,4,5]
(1 row)

Halfvec 转 Vector

示例：

openGauss=# SELECT '[1,2,3,4,5]'::halfvec::vector(5);
    vector      
-------------
 [1,2,3,4,5]
(1 row)

[!NOTE]说明 向量数据类型会和其他类型使用同样的函数名、操作符，为确保执行向量操作无误，建议对至少一个入参进行显式类型转换，如 SELECT l2_distance('[0,0]'::vector, '[3,4]');。
********************
{'file_directory': '.', 'filename': 'vector_functions_and_operators.md', 'filetype': 'text/markdown', 'languages': ['eng'], 'last_modified': '2026-04-23T11:52:23', 'orig_elements': [<unstructured.documents.elements.Title object at 0x7f4f0f918210>, <unstructured.documents.elements.ListItem object at 0x7f4f0f91b4d0>, <unstructured.documents.elements.CodeSnippet object at 0x7f4f0f918c10>, <unstructured.documents.elements.Title object at 0x7f4f0fc6dc10>, <unstructured.documents.elements.ListItem object at 0x7f4f0fc6f150>, <unstructured.documents.elements.CodeSnippet object at 0x7f4f0fc6e850>, <unstructured.documents.elements.Title object at 0x7f4f0fc6e790>, <unstructured.documents.elements.ListItem object at 0x7f4f0fea9e50>, <unstructured.documents.elements.CodeSnippet object at 0x7f4f0fde8690>, <unstructured.documents.elements.Text object at 0x7f4f0fc82c90>]}
```



从以上结果可以看到每个分块的数据由 text（核心内容） 和 metadata（上下文背景） 两部分组成，最终需要的数据为 text 部分，因此需要进一步处理。

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
向量函数和操作符

Vector

Vector 操作符

===== text 2 =====
操作符 描述 + 元素级加法 - 元素级减法 * 元素级乘法 \= 等于 \<> 不等于 || 向量拼接 <-> 欧几里得距离 (L2) <#> 负内积 <=> 余弦距离 <+> 曼哈顿距离 (L1)

......

===== text 38 =====
TEXT/VARCHAR 转 Halfvec

示例：

openGauss=# SELECT '[1,2,3,4,5]'::halfvec;
   halfvec    
-------------
 [1,2,3,4,5]
(1 row)

Vector 转 Halfvec

示例：

openGauss=# SELECT '[1,2,3,4,5]'::vector::halfvec;
   halfvec    
-------------
 [1,2,3,4,5]
(1 row)

Halfvec 转 Vector

示例：

openGauss=# SELECT '[1,2,3,4,5]'::halfvec::vector(5);
    vector      
-------------
 [1,2,3,4,5]
(1 row)

[!NOTE]说明 向量数据类型会和其他类型使用同样的函数名、操作符，为确保执行向量操作无误，建议对至少一个入参进行显式类型转换，如 SELECT l2_distance('[0,0]'::vector, '[3,4]');。
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
    max_batch_size = 10

    for i in range(0, len(texts), max_batch_size):
        batch = texts[i : i + max_batch_size]
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
[-0.03726935014128685, -0.024831822142004967, 0.02821863442659378, 0.04081469401717186, 0.03643345832824707, -0.016703471541404724, -0.002462645061314106, 0.0009011984220705926, -0.055457256734371185, 0.14181376993656158, 0.03251340240240097, -0.033118702471256256, ......]
```

### 生成向量数据

将获取的 text 向量化处理，构造包含 ID、文本、向量数据的行式数据结构。

```python
import uuid

vectors = batch_embedder(texts)
dim = len(vectors[0])

rows: list[Any] = []
for text, vector in zip(texts, vectors):
    rows.append(
        (
            str(uuid.uuid4()),
            text,
            vector,
        )
    )

print(dim, rows[0])
```



预期输出结果如下。

```shell
1024 ('cb32f8df-0486-4ad4-8fe7-23a6a275b14a', '向量函数和操作符\n\nVector\n\nVector 操作符', [0.019089097157120705, -0.002286019502207637, ......])
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
<connection object at 0x7fbc14e6fc40; dsn: 'user=gaussdb password=xxx dbname=postgres host=localhost port=5432', closed: 0>
<cursor object at 0x7fbc14a58130; closed: 0>
```

### 创建表

创建表之前先删除表，防止多次执行脚本时数据被重复写入。

```python
from psycopg2 import sql

table_name = "unstructured_test"

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
- **`created_at`：** 插入数据时的时间戳

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
[('04b052fc-104d-413b-b19d-cc67b599d1e4', '向量函数和操作符\n\nVector\n\nVector 操作符'), ('801866f9-02c9-4948-945a-dcc6a67211b9', '操作符 描述 + 元素级加法 - 元素级减法 * 元素级乘法 \\= 等于 \\<> 不等于 || 向量拼接 <-> 欧几里得距离 (L2) <#> 负内积 <=> 余弦距离 <+> 曼哈顿距离 (L1)'), ('f6268a01-1847-4088-bc17-3e19ed8b40fb', "描述：元素级加法。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector + '[4,5,6]'; ?column?\n\n[5,7,9] (1 row) ```\n\n描述：元素级减法。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector - '[4,5,6]'; ?column?\n\n[-3,-3,-3] (1 row) ```\n\n描述：元素级乘法。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector * '[4,5,6]'; ?column?\n\n[4,10,18] (1 row) ```\n\n\\=\n\n描述：等于。\n\n示例：\n\n``` openGauss=# select '[1,2,3]'::vector = '[4,5,6]'; ?column?\n\nf (1 row) ```\n\n\\<>\n\n描述：不等于。\n\n示例：\n\n``` openGauss=# select '[1,2,3]'::vector <> '[4,5,6]'; ?column?")]
```

## 构建 RAG

### 将问题向量化处理

指定查询问题，并将问题向量化处理。

```python
question = "openGauss支持哪些向量操作符？"

question_vec = batch_embedder([question])[0]

print(question_vec)
```



预期输出结果如下。

```shell
[-0.005004278849810362, 0.02674989216029644, -0.05009854584932327, 0.038640279322862625, 0.017689218744635582, -0.04733852669596672, ......]
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
    ).format(table_name=sql.Identifier(table_name)),
    (question_vec, topk),
)

contexts = [row[0] for row in cursor.fetchall()]

print(contexts)
```



预期输出结果如下。

```shell
["描述：元素级加法。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector + '[4,5,6]'; ?column?\n\n[5,7,9] (1 row) ```\n\n描述：元素级减法。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector - '[4,5,6]'; ?column?\n\n[-3,-3,-3] (1 row) ```\n\n描述：元素级乘法。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector * '[4,5,6]'; ?column?\n\n[4,10,18] (1 row) ```\n\n\\=\n\n描述：等于。\n\n示例：\n\n``` openGauss=# select '[1,2,3]'::vector = '[4,5,6]'; ?column?\n\nf (1 row) ```\n\n\\<>\n\n描述：不等于。\n\n示例：\n\n``` openGauss=# select '[1,2,3]'::vector <> '[4,5,6]'; ?column?", "``` openGauss=# SELECT '[0,0]'::vector <+> '[3,4]'; ?column?\n\n    7\n\n(1 row) ```\n\nVector 函数", "``` openGauss=# SELECT subvector('[1,2,3,4,5]'::vector, 1, 3); subvector\n\n[1,2,3] (1 row) ```\n\nvector_dims\n\n描述：向量的维度数。\n\n返回类型：int\n\n示例：\n\n``` openGauss=# SELECT vector_dims('[1,2,3]'::vector); vector_dims\n\n       3\n\n(1 row) ```\n\nvector_norm\n\n描述：欧几里得范数。\n\n返回类型：float8\n\n示例：\n\n``` openGauss=# SELECT vector_norm('[3,4]'); vector_norm\n\n       5\n\n(1 row) ```\n\nVector 聚合函数", "t (1 row) ```\n\n||\n\n描述：向量拼接。\n\n示例：\n\n``` openGauss=> select '[1,2,3]'::vector || '[4,5,6]'; ?column?\n\n[1,2,3,4,5,6] (1 row) ```\n\n<->\n\n描述：欧几里得距离 (L2)。\n\n示例：\n\n``` openGauss=# SELECT '[0,0]'::vector <-> '[3,4]'; ?column?\n\n    5\n\n(1 row) ```\n\n<#>\n\n描述：负内积。\n\n示例：\n\n``` openGauss=# SELECT '[1,2]'::vector <#> '[3,4]'; ?column?\n\n  -11\n\n(1 row) ```\n\n<=>\n\n描述：余弦距离。\n\n示例：\n\n``` openGauss=# SELECT '[1,2]'::vector <=> '[2,4]'; ?column?\n\n    0\n\n(1 row) ```\n\n<+>\n\n描述：曼哈顿距离。\n\n示例：", "返回类型：float8\n\n示例：\n\n``` openGauss=# SELECT l1_distance('[0,0]'::vector, '[3,4]'); l1_distance\n\n       7\n\n(1 row) ```\n\nl2_distance\n\n描述：欧几里得距离 (L2)。\n\n返回类型：float8\n\n示例：\n\n``` openGauss=# SELECT l2_distance('[0,0]'::vector, '[3,4]'); l2_distance\n\n       5\n\n(1 row) ```\n\nl2_normalize\n\n描述：归一化（使用L2距离）。\n\n返回类型：vector\n\n示例：\n\n``` openGauss=# SELECT l2_normalize('[3,4]'::vector); l2_normalize\n\n[0.6,0.8] (1 row) ```\n\nsubvector\n\n描述：截取子向量。\n\n返回类型：vector\n\n示例："]
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
[1] 描述：元素级加法。

示例：

``` openGauss=> select '[1,2,3]'::vector + '[4,5,6]'; ?column?

[5,7,9] (1 row) ```

描述：元素级减法。

示例：

``` openGauss=> select '[1,2,3]'::vector - '[4,5,6]'; ?column?

[-3,-3,-3] (1 row) ```

描述：元素级乘法。

示例：

``` openGauss=> select '[1,2,3]'::vector * '[4,5,6]'; ?column?

[4,10,18] (1 row) ```

\=

描述：等于。

示例：

``` openGauss=# select '[1,2,3]'::vector = '[4,5,6]'; ?column?

f (1 row) ```

\<>

描述：不等于。

示例：

``` openGauss=# select '[1,2,3]'::vector <> '[4,5,6]'; ?column?

[2] ``` openGauss=# SELECT '[0,0]'::vector <+> '[3,4]'; ?column?

    7

(1 row) ```

Vector 函数

[3] ``` openGauss=# SELECT subvector('[1,2,3,4,5]'::vector, 1, 3); subvector

[1,2,3] (1 row) ```

vector_dims

描述：向量的维度数。

返回类型：int

示例：

``` openGauss=# SELECT vector_dims('[1,2,3]'::vector); vector_dims

       3

(1 row) ```

vector_norm

描述：欧几里得范数。

返回类型：float8

示例：

``` openGauss=# SELECT vector_norm('[3,4]'); vector_norm

       5

(1 row) ```

Vector 聚合函数

[4] t (1 row) ```

||

描述：向量拼接。

示例：

``` openGauss=> select '[1,2,3]'::vector || '[4,5,6]'; ?column?

[1,2,3,4,5,6] (1 row) ```

<->

描述：欧几里得距离 (L2)。

示例：

``` openGauss=# SELECT '[0,0]'::vector <-> '[3,4]'; ?column?

    5

(1 row) ```

<#>

描述：负内积。

示例：

``` openGauss=# SELECT '[1,2]'::vector <#> '[3,4]'; ?column?

  -11

(1 row) ```

<=>

描述：余弦距离。

示例：

``` openGauss=# SELECT '[1,2]'::vector <=> '[2,4]'; ?column?

    0

(1 row) ```

<+>

描述：曼哈顿距离。

示例：

[5] 返回类型：float8

示例：

``` openGauss=# SELECT l1_distance('[0,0]'::vector, '[3,4]'); l1_distance

       7

(1 row) ```

l2_distance

描述：欧几里得距离 (L2)。

返回类型：float8

示例：

``` openGauss=# SELECT l2_distance('[0,0]'::vector, '[3,4]'); l2_distance

       5

(1 row) ```

l2_normalize

描述：归一化（使用L2距离）。

返回类型：vector

示例：

``` openGauss=# SELECT l2_normalize('[3,4]'::vector); l2_normalize

[0.6,0.8] (1 row) ```

subvector

描述：截取子向量。

返回类型：vector

示例：
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
    *   Task: Answer the user's question based *only* on the provided Context (References).
    *   Rules:
        1.  Only use provided context, no fabrication.
        2.  If the answer isn't in the context, state "根据提供的资料，无法回答该问题" (Based on the provided materials, unable to answer this question).
        3.  Keep the answer concise and accurate.
    *   Input Context: A list of snippets describing vector operations, operators, and functions in openGauss.
    *   User Question: "openGauss 支持哪些向量操作符？" (Which vector operators does openGauss support?)
    
......

====================完整回复====================
根据提供的资料，openGauss 支持的向量操作符及其描述如下：

- `+`：元素级加法
- `-`：元素级减法
- `*`：元素级乘法
- `=`：等于
- `<>`：不等于
- `||`：向量拼接
- `<->`：欧几里得距离 (L2)
- `<#>`：负内积
- `<=>`：余弦距离
- `<+>`：曼哈顿距离
```

## 参考资料

- Unstructured 官网：[Welcome to Unstructured! - Unstructured](https://docs.unstructured.io/welcome)

- Milvus 官网文档：[利用 Milvus 和 Unstructured 创建 RAG | Milvus 文档](https://milvus.io/docs/zh/rag_with_milvus_and_unstructured.md)

- 阿里云百炼模型 API：[大模型服务平台百炼控制台](https://bailian.console.aliyun.com/cn-beijing?spm=5176.12818093_47.resourceCenter.1.1b8416d0xa1cot&tab=model#/model-market/detail/qwen3.5-plus?serviceSite=asia-pacific-china)


## 附录

本文最终完整的测试用例如下。

```python
#!/usr/bin/env python3
"""RAG (Retrieval-Augmented Generation) module based on Unstructured and openGauss."""

import os
import hashlib
from typing import List, Any
from tqdm import tqdm

from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from unstructured.partition.md import partition_md
from unstructured.chunking.title import chunk_by_title

import psycopg2
from psycopg2 import sql


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

TABLE_NAME = os.getenv("OG_TABLE", "unstructured")


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
    max_batch_size = 10

    for i in range(0, len(texts), max_batch_size):
        batch = texts[i : i + max_batch_size]
        response = client.embeddings.create(
            input=batch,
            model="text-embedding-v4",
        )
        vectors.extend(item.embedding for item in response.data)
    return vectors


def embedder(text: str) -> List[float]:
    """Generate an embedding vector for a single text."""
    return batch_embedder([text])[0]


def text_chunks(file: str) -> List[str]:
    """Convert a document file into a list of text chunks."""
    elements = partition_md(filename=file)
    chunks = chunk_by_title(elements)
    texts: list[Any] = []
    for chunk in chunks:
        text = (chunk.text or "").strip()
        if text:
            texts.append(text)
    return texts


def batch_text_chunks(files: List[str]) -> List[tuple]:
    """Convert multiple document file into (file, chunk) tuples."""
    items: List[Any] = []
    for file in files:
        for t in text_chunks(file):
            items.append((file, t))
    return items


def compute_doc_id(file: str, text: str) -> str:
    """Compute a SHA-256 document ID from a file and its text chunk."""
    return hashlib.sha256(f"{file}\n{text}".encode("utf-8")).hexdigest()


def filter_new_items(cursor, table_name: str, items: List[tuple]) -> List[tuple]:
    """Filter out items whose document IDs already exist in the database table."""
    ids = [compute_doc_id(f, t) for f, t in items]
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

    new_items = []
    seen = set()
    for item, doc_id in zip(items, ids):
        if doc_id in existing or doc_id in seen:
            continue
        seen.add(doc_id)
        new_items.append(item)
    return new_items


def build_vector_rows(items: List[tuple]):
    """Build database rows with embeddings from (file, text) tuples."""
    texts = [t for _, t in items]
    vectors = batch_embedder(texts)
    dim = len(vectors[0])

    rows: list[Any] = []
    for (file, text), vector in zip(items, vectors):
        rows.append(
            (
                compute_doc_id(file, text),
                text,
                vector,
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
        INSERT INTO public.{table_name} (id, content, embedding)
        VALUES (%s, %s, %s)
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
        "./vector_data_type.md",
        "./vector_functions_and_operators.md",
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
            print(
                f"[OK]   Inserted {len(rows)} rows. table=public.{table_name}, dim={dim}"
            )

        question = "openGauss支持哪些向量操作符？"    # pylint: disable=invalid-name
        print("问题:", question)
        rag(cursor, table_name, question, top_k=5)
    finally:
        close_connection(conn, cursor)
```

