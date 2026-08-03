# 使用nomic-embed-text和openGauss DataVec进行向量化搜索

nomic-embed-text是一个专门用于文本转化为高维向量表示的高性能嵌入模型，本文将介绍如何通过nomic-embed-text和openGauss DataVec轻松实现从文本到向量的转化，并基于语义的相似性快速进行搜索操作。

注：openGauss DataVec容器化部署详见[链接](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)。

## 环境准备

- 加载模型

ollama安装可以参考[openGauss-RAG实践](opengauss_ragpratice.md)

```bash
ollama pull nomic-embed-text
```

- 检验

```bash
ollama list

NAME                        ID             SIZE    MODIFIED
nomic-embed-text:latest     0a109f422b47   274MB   18 minutes ago
```

## 实践

在下面的示例中，我们使用ollama中的nomic-embed-text嵌入模型生成向量数据，并将其存储在openGauss DataVec向量数据库中。

```python
import ollama
import psycopg2
from psycopg2 import sql
from typing import List

def embedding(text):
    vector = ollama.embeddings(model="nomic-embed-text", prompt=text)
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
            "CREATE TABLE IF NOT EXISTS public.{table_name} (id BIGINT PRIMARY KEY, content text, embedding vector({dim}));"
        ).format(table_name = sql.Identifier(table_name), dim = sql.Literal(dim))
    )
    conn.commit()

def insert(conn, cursor, table_name:str, embeddings:List[List[float]], ids:List[int], contents:List[str]):
    data = list(zip(ids, embeddings, contents))
    cursor.executemany(
        sql.SQL("INSERT INTO public.{table_name} (id, embedding, content) VALUES(%s, %s, %s);")
        .format(table_name = sql.Identifier(table_name)), data
    )
    conn.commit()
    print("数据插入成功！")


texts = ["openGauss 是一款开源数据库", "DataVec是一个基于openGauss的向量数据库"]
embs = [embedding(t) for t in texts] # 生成底库向量数据
dimensions = len(embs[0])
ids = [i for i in range(len(embs))]
print("text : {}, embedding dim : {}, embedding : {} ...".format(text[0], dimensions, embs[:10]))

# 插入数据
conn, cursor = create_connection("testdb", "test_user", xxxxxx, "localhost", 5432)
create_table(conn, cursor, "test_table1", dimensions)
insert(conn, cursor, "test_table1", embs, ids, texts)
```

输出结果如下：

```python
text : openGauss 是一款开源数据库, embedding dim : 768, embedding : [-0.5359194278717041, 1.3424185514450073, -3.524909734725952, -1.0017194747924805, -0.1950572431087494, 0.28160029649734497, -0.473337858915329, 0.08056074380874634, -0.22012852132320404, -0.9982725977897644] ...
数据插入成功！
```

<br>
准备好数据后，我们就可以输入查询文本在向量数据库中进行近似查询。

```python
def select(conn, cursor, table_name:str, queries:List[List[float]], topk:int):
    ids = []
    contents = []
    for emb in queries:
        cursor.execute(
            sql.SQL(
                "SELECT * FROM public.{table_name} ORDER BY embedding <-> %s::vector LIMIT %s::int;"
            ).format(table_name = sql.Identifier(table_name)), (emb, topk)
        )
        conn.commit()
        result = cursor.fetchall()
        ids.append([int(i[0]) for i in result])
        contents.append([i[1] for i in result])
    return ids, contents

# 生成请求向量数据
query = "openGauss数据库是什么"
q_emb = embedding(query)

# 近似查询
ids, contents = select(conn, cursor, "test_table1", [q_emb], 1)
print(f"id : {ids[0]}, contents: {contents[0]}")
```

输出结果如下：

```python
id: [0], contents: ['openGauss 是一款开源数据库']
```

openGauss DataVec的具体使用可以参考[Python SDK对接向量数据库](integrationpython.md)
