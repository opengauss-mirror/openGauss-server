# Vector Generation and Storage with BGE M3 and openGauss DataVec

[BGE M3](https://huggingface.co/BAAI/bge-m3) is a multilingual, high-performance text embedding model developed by BAAI that converts text into semantically rich high-dimensional vector representations. This document focuses on BGE M3 and the vector database openGauss DataVec, and describes how to implement text vector generation and efficient storage. By combining these two tools, you can build more intelligent data retrieval and processing systems.

Note: For containerized deployment of openGauss DataVec, see [the link](../installation_guide/installing_the_container_image.md).

## Case 1: FlagEmbedding + openGauss DataVec

### Environment Preparation

- Install dependencies

`FlagEmbedding` is a toolkit focused on retrieval-augmented large language models, providing various text embedding models and reranking models. Before using the bge-m3 model, you need to install this package first. For a detailed tutorial, refer to the [Hugging Face official website](https://huggingface.co/BAAI/bge-m3).

```bash
pip3 install -U FlagEmbedding
pip3 install psycopg2
```

- Load the bge-m3 model

In actual use, the `FlagEmbedding` framework supports automatically loading models from the Hugging Face model library by specifying the model name. The following supplements the steps for downloading the offline model. If you choose to load the model automatically, you can skip this section directly.

```bash
git lfs install ; git clone https://www.modelscope.cn/BAAI/bge-m3.git
```

Note that you need to install the `Git LFS` tool before you can download the complete model data. The download address is available on the [git-lfs official website](https://packagecloud.io/github/git-lfs).

### Practice

In the following example, we use the bge-m3 embedding model from `FlagEmbedding` to generate vector data and store it in the openGauss DataVec vector database.

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
        return_dense = True,  # Set to return dense embedding, enabled by default
        return_sparse = False, # Set to return sparse embedding, disabled by default
        return_colbert_vecs = False # Set to return multi-vector (ColBERT), disabled by default
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
    print("Data inserted successfully!")

if __name__ == '__main__':
    text = "openGauss is an open-source database"
    emb = embedding(text)
    dimensions = len(emb)
    print("text : {}, embedding dim : {}, embedding : {} ...".format(text, dimensions, emb[:10]))

    conn, cursor = create_connection("testdb", "test_user", YourPassword, "localhost", 5432)
    create_table(conn, cursor, "test_table1", dimensions)
    insert(conn, cursor, "test_table1", [emb.tolist()], [0])
```

The output is as follows:

```python
text : openGauss is an open-source database, embedding dim : 768, enbedding : [-0.05427849 -0.02701874 -0.05441538 0.0294214 -0.01936925 -0.00815862 0.01310737 -0.0480913 0.01261776 0.2954952] ...
Data inserted successfully.
```

For details about using openGauss DataVec, see [Python SDK Integration with Vector Database](integration_python.md)

## Case 2: ollama + openGauss DataVec

### Environment Preparation

- Load the model

For ollama installation, see [openGauss-RAG Practice](opengauss_ragpratice.md)

```bash
ollama pull bge-m3
```

- Verification

```bash
ollama list

NAME              ID             SIZE    MODIFIED
bge-m3:latest     790764642607   1.2GB   18 minutes ago
```

### Practice

In the following example, we use the bge-m3 embedding model in `ollama` to generate vector data and store it in the openGauss DataVec vector database.

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
    print("Data inserted successfully!")

if __name__ == '__main__':
    text = "openGauss is an open-source database"
    emb = embedding(text)
    dimensions = len(emb)
    print("text : {}, embedding dim : {}, embedding : {} ...".format(text, dimensions, emb[:10]))

    conn, cursor = create_connection("testdb", "test_user", YourPassword, "localhost", 5432)
    create_table(conn, cursor, "test_table1", dimensions)
    insert(conn, cursor, "test_table1", [emb], [0])
```

The output is as follows:

```python

text : openGauss is an open-source database, embedding dim : 768, enbedding : [-0.5359194278717041, 1.3424185514450073, -3.524909734725952, -1.0017194747924805, -0.1950572431087494, 0.28160029649734497, -0.473337858915329, 0.08056074380874634, -0.22012852132320404, -0.9982725977897644] ...
Data inserted successfully.
```

For details about how to use openGauss DataVec, see [Python SDK Integration with Vector Database](integration_python.md)
