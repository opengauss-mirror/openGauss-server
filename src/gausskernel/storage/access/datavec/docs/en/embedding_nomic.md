# Vectorized Search with nomic-embed-text and openGauss DataVec

nomic-embed-text is a high-performance embedding model specifically designed for converting text into high-dimensional vector representations. This document describes how to easily convert text into vectors using nomic-embed-text and openGauss DataVec, and quickly perform search operations based on semantic similarity.

Note: For details, refer to [containerized deployment of openGauss DataVec](../installation_guide/installing_the_container_image.md).

## Environment Preparation

- Load the model

For ollama installation, see [openGauss-RAG Practice](opengauss_ragpratice.md)

```bash
ollama pull nomic-embed-text
```

- Verification

```bash
ollama list

NAME                        ID             SIZE    MODIFIED
nomic-embed-text:latest     0a109f422b47   274MB   18 minutes ago
```

## Practice

In the following example, we use the nomic-embed-text embedding model in ollama to generate vector data and store it in the openGauss DataVec vector database.

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
    print("Data inserted successfully!")


texts = ["openGauss is an open-source database", "DataVec is a vector database based on openGauss"]
embs = [embedding(t) for t in texts] # Generate the base vector data
dimensions = len(embs[0])
ids = [i for i in range(len(embs))]
print("text : {}, embedding dim : {}, embedding : {} ...".format(text[0], dimensions, embs[:10]))

# Insert data
conn, cursor = create_connection("testdb", "test_user", xxxxxx, "localhost", 5432)
create_table(conn, cursor, "test_table1", dimensions)
insert(conn, cursor, "test_table1", embs, ids, texts)
```

The output is as follows:

```python
text : openGauss is an open-source database, embedding dim : 768, embedding : [-0.5359194278717041, 1.3424185514450073, -3.524909734725952, -1.0017194747924805, -0.1950572431087494, 0.28160029649734497, -0.473337858915329, 0.08056074380874634, -0.22012852132320404, -0.9982725977897644] ...
Data inserted successfully.
```

<br>
After the data is ready, we can enter a query text to perform an approximate query in the vector database.

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

# Generate the query vector data
query = "What is openGauss database"
q_emb = embedding(query)

# Approximate query
ids, contents = select(conn, cursor, "test_table1", [q_emb], 1)
print(f"id : {ids[0]}, contents: {contents[0]}")
```

The output is as follows:

```python
id: [0], contents: ['openGauss is an open-source database']
```

For details about how to use openGauss DataVec, see [Python SDK for Vector Database](integration_python.md)
