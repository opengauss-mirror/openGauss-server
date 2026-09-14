# 使用 openGauss 和 vLLM 构建 RAG

在本教程中，我们将向您展示如何使用 openGauss 和 vLLM 构建一个检索-增强生成（RAG）管道。该管道集成了 openGauss（用于向量存储）和 vLLM（用于本地大模型推理）。

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



**（3）配置 NPU 环境**

本文基于 Ascend 910C 环境进行演示，关于 Ascend 环境的配置可以参考：[安装 — vllm-ascend](https://docs.vllm.ai/projects/ascend/zh-cn/latest/installation.html)，此处不做详述。

本文使用的主要软件版本如下。

| 软件        | 版本      |
| ----------- | --------- |
| NPU Driver  | 25.5.0    |
| CAAN        | 8.5.1     |
| Python      | 3.11.14   |
| vllm        | 0.19.1    |
| vllm-ascend | 0.19.1rc1 |
| torch       | 2.9.0     |
| torch-npu   | 2.9.0     |

### LLM 和 Embedding 模型

本文使用 modelscope 提供的本地模型，具体信息如下表所示。

| 项目      | 模型名称           | 参考文档                                                     |
| --------- | ------------------ | ------------------------------------------------------------ |
| Embedding | Qwen3-Embedding-8B | https://docs.vllm.ai/projects/ascend/zh-cn/latest/tutorials/models/Qwen3_embedding.html |
| LLM       | Qwen3-30B-A3B      | https://docs.vllm.ai/projects/ascend/zh-cn/latest/tutorials/models/Qwen3-30B-A3B.html |

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
    opengauss/opengauss:latest
    
# 确认容器正常运行
$ docker ps
CONTAINER ID   IMAGE                           COMMAND                  CREATED       STATUS       PORTS                                         NAMES
e0af667c5477   opengauss/opengauss:latest   "entrypoint.sh gauss…"   14 seconds ago   Up 10 seconds   0.0.0.0:5432->5432/tcp, :::5432->5432/tcp   opengauss
```

> [!NOTE]说明
> openGauss 容器默认已创建 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir vllm_rag && cd vllm_rag

# 创建虚拟环境
$ uv venv

# 安装其他依赖包
$ uv pip install setuptools<70 modelscope psycopg2-binary
```

之后的操作均在该项目环境中进行。

## 处理文档

本文以 [Markdown](https://gitcode.com/opengauss/openGauss-server/blob/master/src/gausskernel/storage/access/datavec/docs/zh/datavec_overview.md) 文件作为源文件进行操作演示。

此处以 “#” 为分隔符进行简单的分块处理。

```python
import random
import string
import urllib.request

def text_chunks(urls: List[str]) -> List[str]:
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



测试文档加载与向量生成。

```python
sources = [ "https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md" ]

all_chunks = text_chunks(sources)

print(all_chunks)
```



预期输出结果如下。

```shell
['DataVec向量数据库\n\n#', '可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#', '特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#', '快速部署指南\n\nDataVec向量数据库可通过[容器镜像安装](https://docs.opengauss.org/zh/docs/latest/installation_guide/installing_the_container_image.html)快速部署，并快速对接大模型，打造本地RAG智能问答服务。\n\nDataVec向量数据库可通过安装[spqplugin_v2插件](../extension_reference/spqplugin_v2.md)，在大数据量场景下获得数据库分布式存储检索能力。\n\n#', '客户价值\n\n向量数据库通过高效的相似性搜索能力，让大模型能精准召回相关私有知识，从而在检索增强生成（RAG）、推荐、语义搜索等场景中实现更低延迟、更高准确率的企业级AI应用。\n\n#', '特性描述\n\nDataVec能够无缝对接自研大模型。通过嵌入技术将非结构化数据（如文本、图像等）转换为向量数据，DataVec为之提供存储和检索能力。嵌入是一种将非结构化数据映射到向量空间的技术，使得相似文本、图像在向量空间中的距离相近，从而提高检索的准确性和效率。\n\n此外，DataVec还支持鲲鹏指令集加速，实现毫秒级响应。鲲鹏指令集是华为自主研发的一套高性能计算指令集，能够显著提升数据处理和计算的效率。通过利用鲲鹏指令集，DataVec可以在处理大规模向量数据时，提供更快的响应速度和更高的处理能力。\n\n在实际应用中，DataVec可以广泛应用于各种需要高效向量检索的场景。例如，在推荐系统中，DataVec可以根据用户的历史行为和偏好，快速找到与用户兴趣相似的内容，从而提供个性化的推荐。在图像检索中，DataVec可以通过图像特征向量，快速找到与查询图像相似的图片。在自然语言处理(NLP)中，DataVec可以通过文本嵌入，快速找到与查询文本语义相似的文档。\n\nDataVec架构与特性实现详情可参考[向量存储引擎](datavec_architecture.md)介绍。\n\n##', '向量数据类型\n\n- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维\n- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维\n- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数\n- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维\n\n>[!NOTE]说明\n这里的最高维度是在使用索引场景下的最大维度上限值。\n\n支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)\n\n##', '索引支持\n\n- [IVFFLAT](./vector_index.md#ivfflat)  倒排索引\n- [IVF-PQ](./pq.md)  PQ量化压缩倒排索引\n- [IVF-RabitQ](./RabitQ.md)  RabitQ量化压缩倒排索引\n- [HNSW](./vector_index.md#hnsw)  图索引\n- [HNSW-PQ](./pq.md)  PQ量化压缩图索引\n- [HNSW-RabitQ](./RabitQ.md)  RabitQ量化压缩图索引\n\n#', '生态对接\n\nopenGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。\n更详细的指导，参考[向量数据库工具编排使用](dify.md)\n\n#', '使用场景\n\n- 图像识别：用于安全监控、身份验证等场景，通过分析图像中的人脸特征进行识别。\n- 车辆检索：通过摄像头捕捉车辆图像，进行车牌识别和车辆特征分析。\n- 实时轨迹跟踪：在物流行业，通过实时跟踪获取运输轨迹，提高物流效率和安全性。\n- 推荐系统：根据用户浏览和购买力，推荐相关产品，提高用户满意度。\n- 声纹匹配：在金融、安防等领域，通过声纹识别技术进行身份验证，确保交易和操作的安全性。\n- 基因筛选：在药物研发过程中，通过检索特定基因序列，找到潜在的药物靶点，加速新药研发。\n\n这些应用场景展示了DataVec在各个领域的能力，用户可以自由的将向量数据库使能到各个应用中去。通过[向量数据库教程案例](opengauss_ragpratice.md)， 我们为你展示了向量数据库的多种应用模式。\n']
```

## 生成向量数据

### 初始化 Embeddings

使用 Qwen3-Embedding-8B 作为嵌入模型，并对生成的向量进行 L2 归一化。

```python
from typing import List
import gc
import torch
from vllm import LLM, SamplingParams
from vllm.distributed.parallel_state import (
    destroy_distributed_environment,
    destroy_model_parallel,
)


def clean_up():
    destroy_model_parallel()
    destroy_distributed_environment()
    gc.collect()
    torch.npu.empty_cache()


embed_model = LLM(
    model="/root/.cache/modelscope/hub/models/Qwen/Qwen3-Embedding-8B",
    tensor_parallel_size=2,
    distributed_executor_backend="mp",
)


def embedder(texts: List[str]) -> List[List[float]]:
    outputs = embed_model.embed(texts)
    embeddings = [output.outputs.embedding for output in outputs]
    embeddings_tensor = torch.tensor(embeddings)
    embeddings_normalized = torch.nn.functional.normalize(embeddings_tensor, p=2, dim=1)
    return embeddings_normalized.tolist()
```



生成测试嵌入。

```python
texts = ["你好，世界！", "openGauss 是一款开源数据库。"]

results = embedder(texts)

print(len(results[0]))
print(results)

del embed_model
clean_up()
```



预期输出结果如下。

```shell
......
Processed prompts: 100%|█| 2/2 [00:02<00:00,  1.34s/it, est. speed input: 0.00 toks/s, output: 0.00 tok
4096
[[0.031090451404452324, 0.010062219575047493, ......, -0.00665793614462018, 0.020124439150094986], [-0.012694469653069973, -0.01405248325318098, ......, -0.010214620269834995, -0.0075281159952282906]]
```

### 生成向量数据

生成向量数据。

```python
vectors = embedder(all_chunks)
dim = len(vectors[0])

print(dim, vectors[0])
```



预期输出结果如下。

```shell
Processed prompts: 100%|██████████████████████████████████████████████████████████████| 6/6 [00:00<00:00, 33.37it/s, est. speed input: 0.00 toks/s, output: 0.00 toks/s]
4096 [0.014150355011224747, -0.005343080032616854, ......, 0.0025424007326364517, 0.023074889555573463]
```



构造包含 ID、文本、向量数据的行式数据结构。

```python
import uuid
from typing import Any

rows: list[Any] = []
for text, vector in zip(all_chunks, vectors):
    rows.append(
        (
            str(uuid.uuid4()),
            text,
            vector,
        )
    )

print(rows[0])
```



预期输出结果如下。

```shell
('38ceffe5-8bfc-4c68-ba6b-eb2881d0fc63', 'DataVec向量数据库\n\n#', [0.014150355011224747, -0.005343080032616854, ......, 0.0025422237813472748, 0.023073282092809677])
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
    host = os.getenv("OG_HOST", "172.22.0.188"),
    port = int(os.getenv("OG_PORT", "5432"))
)
cursor = conn.cursor()

print(conn)
print(cursor)
```



预期输出结果如下。

```shell
<connection object at 0xffff079f8a40; dsn: 'user=gaussdb password=xxx dbname=postgres host=172.22.0.188 port=5432', closed: 0>
<cursor object at 0xffff0707bd30; closed: 0>
```

### 创建表

创建表之前先删除表，防止多次执行脚本时数据被重复写入。

```python
from psycopg2 import sql

table_name = "vllm_test"

cursor.execute(
    sql.SQL(
        """
        DROP TABLE IF EXISTS public.{table_name};
        """
    ).format(table_name=sql.Identifier(table_name))
)
```



创建表，表结构对应在第 3 章中构造出的行数据：

- **`id`：**每条 chunk 的唯一标识
- **`content`：**chunk 的原始文本内容
- **`embedding`：**`content` 对应的向量表示
- **`created_at`：**插入时的时间戳

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
[('38ceffe5-8bfc-4c68-ba6b-eb2881d0fc63', 'DataVec向量数据库\n\n#'), ('46d9c119-519c-491d-8802-54b420d6dc93', '可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#'), ('5f1e3716-639b-48e8-8ad8-2dc8808273c1', '特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#')]
```

## 构建 RAG

### 执行向量检索

将查询向量化，并在 openGauss 向量数据库中检索最相似的 chunk。

```python
question = "openGauss支持哪些向量数据类型"

question_vec = embedder(question)[0]

top_k = 5

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

contexts = [row[0] for row in cursor.fetchall()]

del embed_model
clean_up()

print(contexts)
```



预期输出结果如下。

```shell
['特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#', 'DataVec向量数据库\n\n#', '生态对接\n\nopenGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。\n更详细的指导，参考[向量数据库工具编排使用](dify.md)\n\n#', '可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#', '向量数据类型\n\n- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维\n- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维\n- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数\n- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维\n\n>[!NOTE]说明\n这里的最高维度是在使用索引场景下的最大维度上限值。\n\n支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)\n\n##']
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
[1] 特性简介

openGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。

DataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。

#

[2] DataVec向量数据库

#

[3] 生态对接

openGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。
更详细的指导，参考[向量数据库工具编排使用](dify.md)

#

[4] 可获得性
本特性自openGauss 6.0.3版本开始引入。

#

[5] 向量数据类型

- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维
- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维
- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数
- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维

>[!NOTE]说明
这里的最高维度是在使用索引场景下的最大维度上限值。

支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)

##
```

### 初始化 LLM

使用 vLLM 的 Python 库直接加载本地大模型进行离线批量推理。

```python
llm_model = LLM(
    model="/root/.cache/modelscope/hub/models/Qwen/Qwen3-30B-A3B",
    tensor_parallel_size=4,
    distributed_executor_backend="mp",
    max_model_len=4096,
    enable_expert_parallel=True,
)

sampling_params = SamplingParams(
    temperature=0.0,
    top_p=0.95,
    top_k=40,
    max_tokens=512,
)

def reasoning(prompts: List[str]) -> List[dict]:
    outputs = llm_model.generate(prompts, sampling_params)
    result: List[dict] = []
    for output in outputs:
        prompt = output.prompt
        generated_text = output.outputs[0].text
        result.append({
            'prompt': prompt,
            'generated_text': generated_text
        })
    return result
```



测试 LLM 加载与推理。

```python
prompts = [
    "Hello, my name is",
    "The future of AI is",
]

result = reasoning(prompts)

del llm_model
clean_up()

print(result)
```



预期输出结果如下。

```shell
[{'prompt': 'Hello, my name is', 'generated_text': ' Sarah. I have a question about the use of "a" and "an" in English. When should I use "a" and when should I use "an"? I\'m a bit confused. Can you help me?\n\nOf course, Sarah! I\'d be happy to help. The use of "a" and "an" in English can be a bit confusing, but there\'s a simple rule to remember. Let me explain it to you.\n\nFirst, both "a" and "an" are indefinite articles. They are used before nouns to indicate that the noun is not specific. For example, if I say "I saw a cat," I\'m talking about any cat, not a specific one. Similarly, "I saw an elephant" would be about any elephant.\n\nNow, the main difference between "a" and "an" is the sound that follows them. If the next word starts with a vowel sound, you use "an." If it starts with a consonant sound, you use "a." It\'s not about the letter itself, but the sound it makes.\n\nLet me give you some examples:\n\n- "a apple" – this is incorrect. The word "apple" starts with a vowel sound (the "a" sound), so we should use "an" instead. The correct sentence is "an apple."\n- "an umbrella" – this is correct because "umbrella" starts with a vowel sound (the "u" sound).\n- "a university" – even though "university" starts with a "u," it\'s pronounced with a "y" sound, so we use "a" here. It\'s a bit tricky, but the key is the sound, not the letter.\n- "an hour" – "hour" starts with an "h," but it\'s silent, so it\'s pronounced with a vowel sound. Therefore, we use "an."\n\nThere are some exceptions and tricky cases, like words that start with a vowel letter but have a consonant sound, or vice versa. For example, "a European" – "European" starts with a "u," but it\'s pronounced with a "y" sound, so we use "a." Similarly, "an MP" – "MP" is pronounced as "em pee," which starts with a vowel sound, so we use "an."\n\nIt\'s also important to note that when the word after "a" or "an" is an abbreviation or an acronym, you should consider the sound it makes. For'}, {'prompt': 'The future of AI is', 'generated_text': ' not just about the technology itself, but about how it is used to solve real-world problems. As AI continues to evolve, it will become more integrated into our daily lives, from healthcare and education to transportation and entertainment. The key to unlocking the full potential of AI lies in responsible development and ethical use. By addressing the challenges and ensuring that AI is used for the greater good, we can create a future where AI enhances human capabilities and improves the quality of life for all.\nFAQs\nWhat is AI?\nAI, or Artificial Intelligence, refers to the simulation of human intelligence in machines that are programmed to think, learn, and make decisions. It encompasses various technologies, including machine learning, natural language processing, and computer vision.\nHow is AI used in healthcare?\nAI is used in healthcare for tasks such as medical imaging analysis, drug discovery, personalized treatment plans, and predictive analytics. It helps in diagnosing diseases, developing new medications, and improving patient care through data-driven insights.\nWhat are the ethical concerns of AI?\nEthical concerns include issues like bias in AI algorithms, privacy violations, job displacement, and the potential for misuse. Ensuring transparency, fairness, and accountability in AI systems is crucial to address these concerns.\nCan AI replace human jobs?\nWhile AI can automate certain tasks, it is more likely to augment human capabilities rather than replace them entirely. Many jobs will evolve to require new skills, and AI can handle repetitive tasks, allowing humans to focus on more complex and creative work.\nWhat is the future of AI?\nThe future of AI will involve more advanced technologies, greater integration into daily life, and increased focus on ethical considerations. AI will continue to drive innovation across various industries, leading to new opportunities and challenges that require careful management.\n\nQuestion: What is the main focus of the future of AI, according to the text? Answer: The main focus is on how AI is used to solve real-world problems and its responsible development. The text emphasizes that the future of AI is not just about the technology itself, but its application to solve real-world issues. It also highlights the importance of ethical use and addressing challenges to ensure AI benefits society. The key points are responsible development, ethical use, and enhancing human capabilities. The answer should reflect these aspects. The future of AI is about solving real-world problems through responsible development and ethical use, enhancing human capabilities, and improving quality of life. The answer should be concise and capture these elements. The future of AI is about solving real-world problems through responsible development and ethical use, enhancing human capabilities,'}]
```

### 调用 vLLM 生成回答

构造提示词。

```python
prompts = f"""你是一个智能问答助手。
你的任务是根据提供的参考资料回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息
2. 如果资料中没有答案，请直接说"根据提供的资料，无法回答该问题"
3. 回答要简洁、准确

参考资料：
{context_text}

问题：
{question}

请开始回答："""
```



调用 vLLM 生成回答。

```python
response = reasoning(prompts)

del llm_model
clean_up()

print("\n" + "=" * 20 + "模型回复" + "=" * 20)
print(response)
```



预期输出结果如下。

```shell
====================模型回复====================
[{'prompt': '你是一个智能问答助手。\n你的任务是根据提供的参考资料回答用户问题。\n请遵循以下规则：\n1. 仅根据参考资料回答，不要编造信息\n2. 如果资料中没有答案，请直接说"根据提供的资料，无法回答该问题"\n3. 回答要简洁、准确\n\n参考资料：\n[1] 特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#\n\n[2] DataVec向量数据库\n\n#\n\n[3] 生态对接\n\nopenGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。\n更详细的指导，参考[向量数据库工具编排使用](dify.md)\n\n#\n\n[4] 可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#\n\n[5] 向量数据类型\n\n- [vector](./vector_data_type.md#vector ) - float向量，最高支持16000维\n- [bitvec](./vector_data_type.md#bit) - bit向量，最高支持83,886,080维\n- [sparsevec](./vector_data_type.md#sparsevec) - sparse向量，最高支持1,000,000,000维，最高支持16000非零元素数\n- [halfvec](./vector_data_type.md#halfvec) - halfvec向量，最高支持16000维\n\n>[!NOTE]说明\n这里的最高维度是在使用索引场景下的最大维度上限值。\n\n支持向量类型与普通类型转换、距离计算、向量计算等，具体可参考[向量函数和操作符](./vector_functions_and_operators.md)\n\n##\n\n\n\n问题：\nopenGauss支持哪些向量数据类型\n\n请开始回答：', 'generated_text': '\n\n\n好的，用户问的是openGauss支持哪些向量数据类型。我需要查看提供的参考资料来找到正确答案。\n\n首先看参考资料[5]，里面详细列出了向量数据类型。里面有四个类型：vector、bitvec、sparsevec和halfvec。每个类型都有对应的维度限制和说明。比如vector是float向量，最高16000维；bitvec是bit向量，支持更高的维度；sparsevec是稀疏向量，维度更高但有非零元素的限制；halfvec是halfvec向量，同样16000维。此外，参考资料还提到这些类型支持转换、距离计算等操作，但用户的问题只关心类型，所以不需要额外信息。\n\n其他参考资料如[1]、[3]、[4]没有提到具体的向量类型，只涉及功能和生态对接。因此答案应该来自参考资料[5]。需要确保没有遗漏其他可能的类型，但根据现有资料，这四个是明确提到的。所以正确回答应该列出这四个类型及其维度限制。\n需要确认是否有其他类型被提及，但根据提供的资料，只有这四个。因此，用户的问题答案应基于参考资料[5]中的信息。'}]
```

## 参考资料

- vLLM 官方文档：[安装 — vllm-ascend](https://docs.vllm.ai/projects/ascend/zh-cn/latest/installation.html)

- openGauss 官方文档：[openGauss Documentation](https://docs.opengauss.org/)

