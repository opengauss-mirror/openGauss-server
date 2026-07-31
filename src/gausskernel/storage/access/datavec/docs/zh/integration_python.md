# Python SDK对接向量数据库

本文介绍如何使用Python语言调用openGauss向量数据库

## 环境准备

限制：<br>
如果数据库非OM工具安装，建议python版本为3.11及以上<br>
如果数据库是OM工具安装，建议python版本为3.6-3.10

- 在线安装

  ```bash
  pip3 install psycopg2
  ```

  注意这里安装的是pypi上的psycopg2的包，不包含多向量查询的特性。如果要使用多向量查询的特性，参考离线安装。

- 离线安装<br>
  1）下载适配好openGauss的psycopg2包，下载链接：[gitcode官网](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2)。<br>
  2）进入openGauss-connector-python-psycopg2根目录，执行

  ```bash
  sh build.sh -bd /data/compile/openGauss-server/dest/ -v 5.0.0
  ```

  -bd: 指定openGauss数据库构建结果目录<br>
  -v: 指定构建包的版本号。不指定则默认为5.0.0<br>
  编译完成后的驱动，在 output 目录下，解压安装包后，会得到两个目录 lib 和 psycopg2。<br>
  3) 将 psycopg2 目录拷贝到 Python 解释器的 site-packages 下（需要先pip3 uninstall psycopg2），对于lib文件夹需要设置环境变量。

  ```bash
  echo "export LD_LIBRARY_PATH=[/path/to/lib]:$LD_LIBRARY_PATH" >> ~/.bashrc
  source ~/.bashrc
  ```

## 基本操作

### 1.连接数据库

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

### 2.创建表

```python
def create_table(conn, cursor, table_name:str, dim:int):
    cursor.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS public.{table_name} (id BIGINT PRIMARY KEY, embedding vector({dim}));"
        ).format(table_name = sql.Identifier(table_name), dim = sql.Literal(dim))
    )
    conn.commit()
```

### 3.创建索引

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

### 4.插入/删除/更新数据

- 批量插入

```python
def insert(conn, cursor, table_name:str, embeddings:List[List[float]], ids:List[int]):
    data = list(zip(ids, embeddings))
    cursor.executemany(
        sql.SQL("INSERT INTO public.{table_name} (id, embedding) VALUES(%s, %s);")
        .format(table_name = sql.Identifier(table_name)), data
    )
    conn.commit()
```

- 删除

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

- 更新

```python
def update(conn, cursor, table_name:str, id:int, vector:List[List[float]]):
    cursor.execute(
        sql.SQL(
            "UPDATE public.{table_name} SET embedding = %s WHERE id = %s;"
        ).format(table_name = sql.Identifier(table_name)), (vector ,id)
    )
    conn.commit()
```

### 5.查询

```python
# 串行批量查询
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

### 6.删除表

```python
def drop_table(conn, cursor, table_name:str):
    cursor.execute(
        sql.SQL(
            "DROP TABLE IF EXISTS public.{table_name};"
        ).format(table_name = sql.Identifier(table_name))
    )
    conn.commit()
```

### 7.关闭连接

```python
def close_connection(conn, cursor):
    conn.close()
    cursor.close()
```

### 8.多向量并发查询

多向量召回支持在单次搜索请求中同时提交多个查询向量，openGauss将并行对查询向量进行搜索，并返回多组结果。

#### 函数名

```python
execute_multi_search(dbconfig, conn_pool_mgr, sql_template, argslist, scan_params, max_workers)
```

#### 输入参数

- dbconfig:数据库连接配置，包含user、password、dbname、host、port
- conn_pool_mgr：连接池管理对象，可以自定义设置，当其为None时，函数内部会自行创建
- sql_template:查询语句，要求是单条查询语句(select为首单词)、包含向量操作符（<->/<=>/<#>/<+>/<~>/<%>）
- argslist：查询参数，需要元组列表的格式，要求不为空
- scan_params：需要通过set设置的参数（如hnsw_ef_search、nprobes）
- max_workers:连接池最大连接数，和数据库最大连接数（由参数max_connections设置）有关，一般来说，连接池最大连接数要小于数据库最大连接数，但是数据库对于管理员用户的连接限制会略超过max_connections设置。

#### 输出参数

- 查询结果，形式为`[[(1, '[1,2,3]'),(2, '[2,2,2]')], [],...]`，表示n个查询向量对应的limit个结果。

#### 使用案例

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

>注意：<br>
>如果并发中的数据库配置密码输入错误，可能会报ERROR:  The account has been locked，这时候需要用超级用户登录为当前用户解锁，具体命令为：`ALTER ROLE username ACCOUNT UNLOCK;`

## 用例

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

## 多模检索能力

openGauss Python SDK 还提供了高级多模检索能力，包括向量检索、BM25 全文检索、混合检索（Hybrid Search）以及 AI 模型集成（Embedding、Rerank、Chat）。详细使用指南请参考 [Python SDK 多模检索使用指南](./multimodal_retrieval_python_sdk.md)。

[更多操作示例参考](https://gitcode.com/opengauss/openGauss-connector-python-psycopg2)
