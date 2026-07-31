# 使用 Ragas 评估基于 openGauss 的 RAG 管道

RAGAS（Retrieval-Augmented Generation Assessment）是一个专为评估 RAG（检索增强生成）系统而设计的开源框架。它的核心作用是利用大模型作为“评估模型”，自动化地对 RAG 系统的性能进行量化评估。RAGAS 通过分析“问题-上下文-答案”这三者之间的关系，从检索质量和生成质量两个维度，帮助开发者精准定位系统问题（如检索不准或模型幻觉）。

其四大核心指标为：

- **忠实度 (Faithfulness)**：检测答案是否基于上下文，防止幻觉。
- **答案相关性 (Answer Relevancy)**：评估答案是否切题。
- **上下文精确率 (Context Precision)**：衡量检索内容的相关性（去噪）。
- **上下文召回率 (Context Recall)**：衡量关键信息是否被完整检索。

在本教程中，我们将向您展示如何使用 使用 Ragas 评估基于 openGauss 的 RAG 管道。

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
| 评测模型   | qwen3.6-plus                                      |
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
CONTAINER ID   IMAGE                           COMMAND                  CREATED         STATUS         PORTS                                         NAMES
a10287f00892   opengauss/opengauss:7.0.0-RC1   "entrypoint.sh gauss…"   3 seconds ago   Up 3 seconds   0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp   opengauss
```

> [!NOTE]说明
> openGauss  容器默认已创建 gaussdb 用户和 postgres 库，启动时使用环境变量 `GS_PASSWORD` 指定用户密码。

### 配置项目环境

```shell
# 创建并进入项目目录
$ mkdir ragas && cd ragas

# 创建虚拟环境
$ uv venv

# 安装依赖包
$ uv pip install openai pandas psycopg2-binary ragas tqdm
```

之后的操作均在该项目环境中进行。

## 构建 RAG 管道

定义使用 openGauss 作为向量存储、OpenAI 作为 LLM 的 RAG 类：该类包含 `load` 方法（将文本数据加载到 Milvus）、`retrieve` 方法（检索与给定问题最相似的文本数据）和 `answer` 方法（使用检索到的知识回答给定问题）。

```python
class OpenGaussClient:
    """A simple openGauss client using psycopg2."""

    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host or os.getenv("OPENGAUSS_HOST", "localhost")
        self.port = port or int(os.getenv("OPENGAUSS_PORT", 5432))
        self.user = user or os.getenv("OPENGAUSS_USER", "gaussdb")
        self.password = password or os.getenv("OPENGAUSS_PASSWORD", "openGauss@123")
        self.database = database or os.getenv("OPENGAUSS_DB", "postgres")
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database
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
        return self.cursor

    def create_table(self, table_name: str, dim: int = 1024):
        """Create table."""
        self.cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS public.{table_name}
                (
                    id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    embedding vector({dim}) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """
            ).format(table_name=sql.Identifier(table_name), dim=sql.Literal(dim)),
        )
        self.conn.commit()
        return self.cursor

    def insert_into_table(self, table_name: str, rows: List[List[Any]]) -> None:
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
        return self.cursor

    def query(self, table_name: str, question_vec: List[float], top_k: int = 3) -> List[Any]:
        """Execute a query and return the results."""
        self.cursor.execute(
            sql.SQL(
                """
                SELECT content
                FROM public.{table_name}
                ORDER BY embedding <-> %s::vector
                LIMIT %s:: int;
                """
            )
            .format(table_name=sql.Identifier(table_name)),
            (question_vec, top_k),
        )

        return [row[0] for row in self.cursor.fetchall()]

    def close(self):
        """Close the connection."""
        self.cursor.close()
        self.conn.close()


def chunk_source(urls: List[str]) -> List[str]:
    """Chunk text into chunks."""
    all_chunks: List[str] = []

    for url in tqdm(urls, desc="Chunking text"):
        random_name = "".join(random.choices(string.ascii_letters + string.digits, k=8))
        file_path = f"./{random_name}.md"
        try:
            urllib.request.urlretrieve(url, file_path)
            with open(file_path, "r") as file:
                file_text = file.read()
            text_lines = file_text.split("# ")
            text_lines = [line for line in text_lines if line.strip()]
            all_chunks.extend(text_lines)
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    return all_chunks


class RAG:
    """RAG (Retrieval-Augmented Generation) class built upon OpenAI and openGauss."""

    def __init__(self, openai_client: OpenAI = None, opengauss_client: OpenGaussClient = None):
        self._prepare_openai(openai_client)
        self._prepare_opengauss(opengauss_client)

    def _embedder(self, text: str) -> List[float]:
        """embed text into embedding vector."""
        response = self.openai_client.embeddings.create(
            input=text,
            model=self.embedding_model
        )
        return response.data[0].embedding

    def _prepare_openai(self, openai_client: OpenAI, llm_model: str = "qwen3.5-plus", embedding_model: str = "text-embedding-v4") -> None:
        """Prepare OpenAI."""
        self.openai_client = openai_client
        self.llm_model = llm_model
        self.embedding_model = embedding_model
        self.SYSTEM_PROMPT = """你是一个智能问答助手。
你的任务是根据提供的参考资料（Context）回答用户问题。
请遵循以下规则：
1. 仅根据参考资料回答，不要编造信息。
2. 如果资料中没有答案，请直接说“根据提供的资料，无法回答该问题”。
3. 回答要简洁、准确。"""
        self.USER_PROMPT = """请根据以下参考资料回答问题。

<参考资料>
{contexts_text}
</参考资料>

<问题>
{question}
</问题>

请开始回答："""

    def _prepare_opengauss(self, opengauss_client: OpenGaussClient, table_name: str = "ragas_test"):
        """Prepare OpenGauss."""
        self.opengauss_client = opengauss_client
        self.table_name = table_name

        self.opengauss_client.drop_table(self.table_name)
        dim = len(self._embedder("get_dim"))
        self.opengauss_client.create_table(self.table_name, dim)

    def load(self, urls: List[str]):
        """Load the urls."""
        rows: list[Any] = []
        texts = chunk_source(urls)
        for text in tqdm(texts, desc="Loading data"):
            rows.append(
                (
                    str(uuid.uuid4()),
                    text,
                    self._embedder(text)
                )
            )

        self.opengauss_client.insert_into_table(table_name=self.table_name, rows=rows)

    def retrieve(self, question: str, top_k: int=3):
        """Retrieve the top k results."""
        question_vec = self._embedder(question)
        retrieve_texts = self.opengauss_client.query(self.table_name, question_vec, top_k)
        return retrieve_texts

    def answer(self, question: str, top_k: int = 3, return_retrieved_text: bool = False):
        """Answer the question."""
        retrieve_texts = self.retrieve(question, top_k)

        retrieve_texts_str : str = ""
        for i, c in enumerate(retrieve_texts, start=1):
            retrieve_texts_str += f"[{i}] {c}\n\n"

        completion = self.openai_client.chat.completions.create(
            model=self.llm_model,
            messages = [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": self.USER_PROMPT.format(contexts_text=retrieve_texts_str, question=question),
                }
            ]
        )
        if return_retrieved_text:
            return completion.choices[0].message.content, retrieve_texts
        else:
            return completion.choices[0].message.content
```



使用 OpenAI 和 openGauss 客户端初始化 RAG 类。

```python
openai_client = OpenAI(
    api_key=os.getenv(
        "DASHSCOPE_API_KEY",
        "sk-5e589e8d7788451fae664ec63be51685"
    ),
    base_url="https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
)

opengauss_client = OpenGaussClient()

rag = RAG(openai_client=openai_client, opengauss_client=opengauss_client)
```

## 测试 RAG 管道

本文以 [Markdown](https://gitcode.com/openGauss/docs/blob/master/docs/zh/datavec/datavec_overview.md) 文件作为源文件进行操作演示。

将 Markdown 文件加载到 RAG 管道中。

```python
urls = ["https://raw.gitcode.com/opengauss/docs/raw/master/docs/zh/datavec/datavec_overview.md"]

rag.load(urls)
```



定义关于 openGauss 向量数据库概述的查询问题，然后使用`answer` 方法获取答案和检索到的上下文文本。

```python
question = "Datavec从openGauss的哪个版本开始引入？"
response = rag.answer(question=question, top_k=3, return_retrieved_text=True)
print(response)
```



预期输出结果如下。

```shell
('自 openGauss 6.0.3 版本开始引入。', ['生态对接\n\nopenGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。\n更详细的指导，参考[向量数据库工具编排使用](dify.md)\n\n#', '特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。作为openGauss的内核特性，DataVec使用熟悉的SQL语法操作向量，简化了用户使用向量数据库的过程。\n\n#', '可获得性\n本特性自openGauss 6.0.3版本开始引入。\n\n#'])
```



准备由问题及参考答案组成的评测集，并查看实际获得的结果。

```python
user_input_list = [
    "Datavec从openGauss的哪个版本开始引入？",
    "DataVec目前支持的向量功能有哪些？",
    "openGauss DataVec支持的向量数据类型有哪些？"
]

reference_list = [
    "从openGauss 6.0.3版本开始引入。",
    "精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。",
    "vector、bitvec、sparsevec、halfvec四种向量数据类型。"
]

retrieved_contexts_list: List[List[str]] = []
response_list: List[str] = []

for user_input in tqdm(user_input_list, desc="Answering questions"):
    response, retrieved_context = rag.answer(question=user_input, top_k=3, return_retrieved_text=True)
    retrieved_contexts_list.append(retrieved_context)
    response_list.append(response)

df = pd.DataFrame(
    {
        "user_input": user_input_list,
        "retrieved_contexts": retrieved_contexts_list,
        "response": response_list,
        "reference": reference_list,
    }
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 200)
pd.set_option("display.width", 2000)
print(df)
```



预期输出结果如下。

|      | user_input                                  | retrieved_contexts                                           | response                                                     | reference                                                    |
| ---- | ------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0    | Datavec从openGauss的哪个版本开始引入？      | [生态对接\n\nopenGauss DataVec 提供Python、Java、Node.js、Go等多语言生态对接，让你能够通过API调用，快速使能向量数据库能力。同时， DataVec拥抱开源第三方组件，在RAG场景下做到快速兼容，多样选择。\n更详细的指导，参考[向量数据库工具编排使用](dify.md)\n\n#, 特性简介\n\nopenGauss DataVec 向量数据库是... | openGauss DataVec 自 openGauss 6.0.3 版本开始引入。          | 从openGauss 6.0.3版本开始引入。                              |
| 1    | DataVec目前支持的向量功能有哪些？           | [DataVec向量数据库\n\n#, 特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec... | 根据参考资料，DataVec 目前支持的向量功能有：精确和近似的最近邻搜索、L2 距离&余弦距离&内积、向量索引、向量操作函数和操作符。 | 精确和近似的最近邻搜索、L2距离&余弦距离&内积、向量索引、向量操作函数和操作符。 |
| 2    | openGauss DataVec支持的向量数据类型有哪些？ | [特性简介\n\nopenGauss DataVec 向量数据库是一个基于openGauss的向量引擎， 提供向量数据类型的存储、检索。在处理大规模高维向量数据时，能够提供快速、准确的检索结果。适用于智能知识检索、 检索增强生成 RAG(Retrieval-Augmented Generation) 等各种复杂应用场景的智能应用。\n\nDataVec目前支持的向量功能有：精确和近似的最近... | 根据提供的资料，openGauss DataVec 支持的向量数据类型包括：\n\n- **vector**（float 向量）\n- **bitvec**（bit 向量）\n- **sparsevec**（sparse 向量）\n- **halfvec**（halfvec 向量） | vector、bitvec、sparsevec、halfvec四种向量数据类型。         |

## 使用 Ragas 进行评估

```python
import asyncio
from openai import AsyncOpenAI
from pydantic import BaseModel

from ragas import experiment
from ragas.dataset import Dataset
from ragas.metrics.collections import AnswerRelevancy, Faithfulness, ContextRecall, ContextPrecision

from ragas.llms import llm_factory
from ragas.embeddings.base import embedding_factory


class EvalResult(BaseModel):
    user_input: str
    answer_relevancy: float
    faithfulness: float
    context_recall: float
    context_precision: float


evaluator_client = AsyncOpenAI(
    api_key=os.getenv(
        "DASHSCOPE_API_KEY",
        "sk-5e589e8d7788451fae664ec63be51685"
    ),
    base_url="https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
)

evaluator_llm = llm_factory(
    model="qwen3.6-plus",
    client=evaluator_client,
)

evaluator_embeddings = embedding_factory(
    provider="openai",
    model="text-embedding-v4",
    client=evaluator_client,
)

answer_relevancy = AnswerRelevancy(llm=evaluator_llm, embeddings=evaluator_embeddings)
faithfulness = Faithfulness(llm=evaluator_llm)
context_recall = ContextRecall(llm=evaluator_llm)
context_precision = ContextPrecision(llm=evaluator_llm)


@experiment(EvalResult)
async def run_evaluation(row):
    ar, f, cr, cp = await asyncio.gather(
        answer_relevancy.ascore(
            user_input=row["user_input"],
            response=row["response"]
        ),
        faithfulness.ascore(
            user_input=row["user_input"],
            response=row["response"],
            retrieved_contexts=row["retrieved_contexts"]
        ),
        context_recall.ascore(
            user_input=row["user_input"],
            retrieved_contexts=row["retrieved_contexts"],
            reference=row["reference"]
        ),
        context_precision.ascore(
            user_input=row["user_input"],
            reference=row["reference"],
            retrieved_contexts=row["retrieved_contexts"]
        ),
    )

    return EvalResult(
        user_input=row["user_input"],
        answer_relevancy=ar.value,
        faithfulness=f.value,
        context_recall=cr.value,
        context_precision=cp.value,
    )


dataset = Dataset.from_pandas(df, name="rag_eval", backend="inmemory")
results = asyncio.run(run_evaluation.arun(dataset=dataset))
df_results = results.to_pandas()

print(df_results[["answer_relevancy", "faithfulness", "context_recall", "context_precision"]].mean())
```



预期输出结果如下。

```shell
Running experiment: 100%|██████████████████████| 3/3 [01:47<00:00, 35.98s/it]
answer_relevancy     0.950449
faithfulness         1.000000
context_recall       1.000000
context_precision    0.388889
dtype: float64
```



> [!NOTE]说明
> `context_precision` 低是因为本文只简单使用 `#`（Markdown 标题）来分块，导致切出来的文本块过大或者包含大量无关信息。

## 参考资料

- Ragas 官网：[Ragas](https://docs.ragas.io/en/stable/)

- Milvus 官网文档：[使用 Ragas 进行评估 | Milvus 文档](https://milvus.io/docs/zh/integrate_with_ragas.md)

- 阿里云百炼模型 API：[大模型服务平台百炼控制台](https://bailian.console.aliyun.com/cn-beijing?spm=5176.12818093_47.resourceCenter.1.1b8416d0xa1cot&tab=model#/model-market/detail/qwen3.5-plus?serviceSite=asia-pacific-china)


