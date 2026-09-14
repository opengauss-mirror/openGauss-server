# Connecting Python SDK to a Vector Database

This document describes how to use the Python language to invoke the openGauss vector database.

## Environment Preparation

Restriction:<br>
If the database is not installed using the OM tool, Python 3.11 or later is recommended.<br>
If the database is installed using the OM tool, Python 3.6 to 3.10 is recommended.

- Online Installation

  ```bash
  pip3 install psycopg2
  ```

  Note that the psycopg2 package installed here is from PyPI and does not include the multi-vector query feature. To use the multi-vector query feature, refer to the offline installation.

- Offline Installation<br>
  1) Download the psycopg2 package adapted for openGauss. Download link: [gitcode official website](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2).<br>
  2) Go to the root directory of openGauss-connector-python-psycopg2 and run

  ```bash
  sh build.sh -bd /data/compile/openGauss-server/dest/ -v 5.0.0
  ```

  -bd: specifies the directory of the openGauss database build result.<br>
  -v: specifies the version number of the build package. If not specified, the default is 5.0.0.<br>
  After compilation, the driver is in the output directory. After decompressing the installation package, you will get two directories: lib and psycopg2.<br>
  3) Copy the psycopg2 directory to the site-packages directory of the Python interpreter (you need to run pip3 uninstall psycopg2 first). For the lib folder, you need to set the environment variable.

  ```bash
  echo "export LD_LIBRARY_PATH=[/path/to/lib]:$LD_LIBRARY_PATH" >> ~/.bashrc
  source ~/.bashrc
  ```

## Basic Operations

### 1. Connect to the Database

```python
import psycopg2
from psycopg2 import sql
import numpy as np
from typing import List

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
```

### 2. Create a Table

```python
def create_table(conn, cursor, table_name:str, dim:int):
    cursor.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS public.{table_name} (id BIGINT PRIMARY KEY, embedding vector({dim}));"
        ).format(table_name = sql.Identifier(table_name), dim = sql.Literal(dim))
    )
    conn.commit()
```

### Create an Index

```python
def create_index(conn, cursor, table_name:str, index_name:str):
    cursor.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS {index_name} ON public.{table_name}
            USING hnsw (embedding vector_l2_ops);
            """
        ).format(index_name = sql.Identifier(index_name), table_name = sql.Identifier(table_name))
    )
    conn.commit()
```

### 4. Insert/Delete/Update Data

- Batch insert

```python
def insert(conn, cursor, table_name:str, embeddings:List[List[float]], ids:List[int]):
    data = list(zip(ids, embeddings))
    cursor.executemany(
        sql.SQL("INSERT INTO public.{table_name} (id, embedding) VALUES(%s, %s);")
        .format(table_name = sql.Identifier(table_name)), data
    )
    conn.commit()
```

- Delete

```python
def delete(conn, cursor, table_name:str, ids:List[int]):
    cursor.execute(
        sql.SQL(
            "DELETE FROM public.{table_name} WHERE id IN ({ids});"
        ).format(table_name = sql.Identifier(table_name), ids = sql.SQL(',').join(map(sql.Literal, ids)))
    )
    delete_count = cursor.rowcount
    conn.commit()
    return delete_count
```

- Update

```python
def update(conn, cursor, table_name:str, id:int, vector:List[List[float]]):
    cursor.execute(
        sql.SQL(
            "UPDATE public.{table_name} SET embedding = %s WHERE id = %s;"
        ).format(table_name = sql.Identifier(table_name)), (vector ,id)
    )
    conn.commit()
```

### 5. Query

```python
# Perform serial batch query.
def select(conn, cursor, table_name:str, queries:List[List[float]], topk:int):
    ids = []
    for emb in queries:
        cursor.execute(
            sql.SQL(
                "SELECT * FROM public.{table_name} ORDER BY embedding <-> %s::vector LIMIT %s::int;"
            ).format(table_name = sql.Identifier(table_name)), (emb, topk)
        )
        conn.commit()
        result = cursor.fetchall()
        ids.append([int(i[0]) for i in result])
    return ids
```

### 6. Drop Table

```python
def drop_table(conn, cursor, table_name:str):
    cursor.execute(
        sql.SQL(
            "DROP TABLE IF EXISTS public.{table_name};"
        ).format(table_name = sql.Identifier(table_name))
    )
    conn.commit()
```

### 7. Closing the Connection

```python
def close_connection(conn, cursor):
    conn.close()
    cursor.close()
```

### 8. Multi-Vector Concurrent Query

Multi-vector recall supports submitting multiple query vectors in a single search request. openGauss searches the query vectors in parallel and returns multiple sets of results.

#### Function Name

```python
execute_multi_search(dbconfig, conn_pool_mgr, sql_template, argslist, scan_params, max_workers)
```

#### Input Parameters

- dbconfig: database connection configuration, including user, password, dbname, host, and port
- conn_pool_mgr: connection pool management object, which can be customized. When it is None, the function creates one internally.
- sql_template: query statement, which must be a single query statement (starting with select) and contain a vector operator (<->/<=>/<#>/<+>/<~>/<%>)
- argslist: query parameters, which must be in the format of a list of tuples and must not be empty
- scan_params: parameters that need to be set via set (such as hnsw_ef_search and nprobes)
- max_workers: maximum number of connections in the connection pool, which is related to the maximum number of database connections (set by the max_connections parameter). Generally, the maximum number of connections in the connection pool should be smaller than the maximum number of database connections, but the database's connection limit for administrator users slightly exceeds the max_connections setting.

#### Output Parameters

- Query results, in the form of `[[(1, '[1,2,3]'),(2, '[2,2,2]')], [],...]`, representing the limit results corresponding to n query vectors.

#### Usage Example

```python
from psycopg2.extras import execute_multi_search, init_conn_pool, close_conn_pool
sql_template = "SELECT * FROM test_table1 ORDER BY embedding <-> %s LIMIT %s;"
scan_params = {"enable_seqscan": "off", "hnsw_ef_search" : 40}
dbconfig = {'user': 'yourusername', 'password': 'xxxxxx', 'host': 'yourhost', 'dbname': 'yourdbname', 'port' : 5432}
argslist = [('[1,1,1]', 1), ('[2,2,3]', 2)]

conn_pool_mgr = init_conn_pool(dbconfig, 2, scan_params)
res = execute_multi_search(dbconfig, conn_pool_mgr, sql_template, argslist, scan_params, 2)
close_conn_pool(conn_pool_mgr)
```

>Note:<br>
>If the database configuration password in the concurrency is entered incorrectly, the error "ERROR:  The account has been locked" may be reported. In this case, you need to log in as a superuser to unlock the current user. The specific command is: `ALTER ROLE username ACCOUNT UNLOCK;`

## Use Cases

```python
conn, cursor = create_connection("testdb", "test_user", YourPassword, "localhost", 5432)
create_table(conn, cursor, "test_table1", 3)
create_index(conn, cursor, "test_table1", "idx_test1")
insert(conn, cursor, "test_table1", [[1.2, 3, 5], [4.3, 5.2, 1]], [0, 1])
delete(conn, cursor, "test_table1", [0])
update(conn, cursor, "test_table1", 1, [1, 3, 3])
select(conn, cursor, "test_table1", [[1, 2, 2], [3, 5, 1]], 2)
drop_table(conn, cursor, "test_table1")
close_connection(conn, cursor)
```

## Multimodal Retrieval Capability

The openGauss Python SDK also provides advanced multimodal retrieval capabilities, including vector retrieval, BM25 full-text retrieval, hybrid retrieval, and AI model integration (Embedding, Rerank, Chat). For a detailed usage guide, see [Python SDK Multimodal Retrieval Guide](./multimodal_retrieval_python_sdk.md).

[More operation examples](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2)
