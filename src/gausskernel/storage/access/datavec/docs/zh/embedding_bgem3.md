# 使用BGE M3和openGauss DataVec进行向量生成与存储

[BGE M3](https://huggingface.co/BAAI/bge-m3)是一款由BAAI开发的多语言高性能文本嵌入模型，能够将文本转化为语义丰富的高维向量表示。本文将围绕BGE M3和向量数据库openGauss DataVec，介绍如何实现文本向量生成与高效存储。通过这两个工具的结合，能够构建更加智能化的数据检索和处理系统。

注：openGauss DataVec容器化部署详见[链接](https://docs.opengauss.org/zh/docs/latest/installation_guide/installing_the_container_image.html)。

## 案例一：FlagEmbedding + openGauss DataVec

### 环境准备

- 安装依赖包

`FlagEmbedding`是一个专注于检索增强型大语言模型的工具包，提供多种文本嵌入模型和重排序模型，使用bge-m3模型前需要先安装该包，详细教程可以参考[huggingface官网](https://huggingface.co/BAAI/bge-m3)。

```bash
pip3 install -U FlagEmbedding
pip3 install psycopg2
```

- 加载bge-m3模型

实际使用中，`FlagEmbedding`框架支持通过指定模型名称从Hugging Face模型库自动加载模型。以下将补充说明离线模型的下载步骤，若选择自动加载模型，可直接跳过此部分内容。

```bash
git lfs install ; git clone https://www.modelscope.cn/BAAI/bge-m3.git
```

注意这里需要安装好`Git LFS`工具后才能下载完整的模型数据，附上git-lfs官网[下载地址](https://packagecloud.io/github/git-lfs)。

### 实践

在下面的示例中，我们使用`FlagEmbedding`中的bge-m3嵌入模型生成向量数据，并将其存储在openGauss DataVec向量数据库中。

```python
import psycopg2
from psycopg2 import sql
from typing import List
import numpy as np
from FlagEmbedding import BGEM3FlagModel

def embedding(text):
    model = BGEM3FlagModel(model_name_or_path = "BAAI/bge-m3")
    sentence_vector_dict = model.encode(
        text,
        return_dense = True,  # 设置返回dense embedding，默认打开
        return_sparse = False, # 设置返回sparse embedding，默认关闭
        return_colbert_vecs = False # 设置返回multi-vector(ColBERT)，默认关闭
    )
    return sentence_vector_dict.get("dense_vecs")

def create_connection(dbname:str, user:str, password:str, host:str, port:int):
    conn = psycopg2.connect(
        dbname = dbname,
        user = user,
        password = password,
        host = host,
        port = port
    )
    cursor = conn.cursor()
    return conn, cursor

def create_table(conn, cursor, table_name:str, dim:int):
    cursor.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS public.{table_name} (id BIGINT PRIMARY KEY, embedding vector({dim}));"
        ).format(table_name = sql.Identifier(table_name), dim = sql.Literal(dim))
    )
    conn.commit()

def insert(conn, cursor, table_name:str, embeddings:List[List[float]], ids:List[int]):
    data = list(zip(ids, embeddings))
    cursor.executemany(
        sql.SQL("INSERT INTO public.{table_name} (id, embedding) VALUES(%s, %s);")
        .format(table_name = sql.Identifier(table_name)), data
    )
    conn.commit()
    print("数据插入成功！")

if __name__ == '__main__':
    text = "openGauss 是一款开源数据库"
    emb = embedding(text)
    dimensions = len(emb)
    print("text : {}, embedding dim : {}, embedding : {} ...".format(text, dimensions, emb[:10]))

    conn, cursor = create_connection("testdb", "test_user", YourPassword, "localhost", 5432)
    create_table(conn, cursor, "test_table1", dimensions)
    insert(conn, cursor, "test_table1", [emb.tolist()], [0])
```

输出结果如下：

```python
text : openGauss 是一款开源数据库, embedding dim : 768, enbedding : [-0.05427849 -0.02701874 -0.05441538 0.0294214 -0.01936925 -0.00815862 0.01310737 -0.0480913 0.01261776 0.2954952] ...
数据插入成功！
```

openGauss DataVec的具体使用可以参考[Python SDK对接向量数据库](integration_python.md)

## 案例二：ollama + openGauss DataVec

### 环境准备

- 加载模型

ollama安装可以参考[openGauss-RAG实践](opengauss_ragpratice.md)

```bash
ollama pull bge-m3
```

- 检验

```bash
ollama list

NAME              ID             SIZE    MODIFIED
bge-m3:latest     790764642607   1.2GB   18 minutes ago
```

### 实践

在下面的示例中，我们使用`ollama`中的bge-m3嵌入模型生成向量数据，并将其存储在openGauss DataVec向量数据库中。

```python
import ollama
import psycopg2
from psycopg2 import sql
from typing import List

def embedding(text):
    vector = ollama.embeddings(model="bge-m3", prompt=text)
    return vector["embedding"]

def create_connection(dbname:str, user:str, password:str, host:str, port:int):
    conn = psycopg2.connect(
        dbname = dbname,
        user = user,
        password = password,
        host = host,
        port = port
    )
    cursor = conn.cursor()
    return conn, cursor

def create_table(conn, cursor, table_name:str, dim:int):
    cursor.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS public.{table_name} (id BIGINT PRIMARY KEY, embedding vector({dim}));"
        ).format(table_name = sql.Identifier(table_name), dim = sql.Literal(dim))
    )
    conn.commit()

def insert(conn, cursor, table_name:str, embeddings:List[List[float]], ids:List[int]):
    data = list(zip(ids, embeddings))
    cursor.executemany(
        sql.SQL("INSERT INTO public.{table_name} (id, embedding) VALUES(%s, %s);")
        .format(table_name = sql.Identifier(table_name)), data
    )
    conn.commit()
    print("数据插入成功！")

if __name__ == '__main__':
    text = "openGauss 是一款开源数据库"
    emb = embedding(text)
    dimensions = len(emb)
    print("text : {}, embedding dim : {}, embedding : {} ...".format(text, dimensions, emb[:10]))

    conn, cursor = create_connection("testdb", "test_user", YourPassword, "localhost", 5432)
    create_table(conn, cursor, "test_table1", dimensions)
    insert(conn, cursor, "test_table1", [emb], [0])
```

输出结果如下：

```python

text : openGauss 是一款开源数据库, embedding dim : 768, enbedding : [-0.5359194278717041, 1.3424185514450073, -3.524909734725952, -1.0017194747924805, -0.1950572431087494, 0.28160029649734497, -0.473337858915329, 0.08056074380874634, -0.22012852132320404, -0.9982725977897644] ...
数据插入成功！
```

openGauss DataVec的具体使用可以参考[Python SDK对接向量数据库](integration_python.md)
