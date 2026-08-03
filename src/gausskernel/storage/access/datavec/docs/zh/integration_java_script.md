# Node.js SDK对接向量数据库

本文介绍如何使用JavaScript语言调用openGauss向量数据库

## 环境准备

 - 下载源码[openGauss-connector-nodejs](https://gitcode.com/opengauss/openGauss-connector-nodejs)

```bash
npm install & npm run build
```

## 基本操作

### 1.连接数据库

```javascript
const connect = async (host, port, username, database, password) => {
    const config = {
        host: host,
        port: port,
        username: username,
        database: database,
        password: password
    };

    const client = new Client(config);
    await client.connect();
    return client;
}
```

### 2.创建表

```javascript
const create_table = async (client, table_name, dim) => {
    const querystr = `
        CREATE TABLE IF NOT EXISTS public.${table_name} (id BIGINT PRIMARY KEY, embedding vector(${dim}));
    `;
    const result = await client.query(querystr);
}
```

### 3.创建索引

```javascript
const create_index = async (client, table_name, index_name) => {
    const querystr = `
        CREATE INDEX IF NOT EXISTS ${index_name} ON public.${table_name} USING hnsw (embedding vector_l2_ops);
    `;
    const result = await client.query(querystr);
}
```

### 4.插入/删除/更新数据

- 插入

```javascript
const insert_vector = async (client, table_name, vector, id) => {
    const querystr = `
        INSERT INTO public.${table_name} (id, embedding) VALUES(${id}, '${vector}');
    `;
    const result = await client.query(querystr);
}
```

- 删除

```javascript
const delete_vector = async (client, table_name, id) => {
    const querystr = `
        DELETE FROM public.${table_name} WHERE id = ${id};
    `;
    const result = await client.query(querystr);
}
```

- 更新

```javascript
const update_vector = async (client, table_name, vector, id) => {
    const querystr = `
        UPDATE public.${table_name} SET embedding = '${vector}' WHERE id = ${id};
    `;
    const result = await client.query(querystr);
}
```

### 5.查询

```javascript
const query = async (client, table_name, vector, topk) => {
    const querystr = `
        SELECT * FROM public.${table_name} ORDER BY embedding <-> '${vector}'::vector LIMIT ${topk}::int;
    `;
    const result = await client.query(querystr);
    return result;
}
```

### 6.删除表

```javascript
const delete_table = async (client, table_name) => {
    const querystr = `
        DROP TABLE IF EXISTS public.${table_name};
    `;
    const result = await client.query(querystr);
}
```

### 7.关闭连接

```javascript
const close = async (client) => {
    await client.end();
}
```

### 8.多向量并发查询

多向量召回支持在单次搜索请求中同时提交多个查询向量，openGauss将并行对查询向量进行搜索，并返回多组结果。

#### 函数名

```javascript
async executeMultiSearch(dbConfig, sqlTemplate, paramsList, searchParams, maxThreads)
```

#### 输入参数

- dbConfig：数据库连接配置，包含user、password、host、database、port
- sqlTemplate：查询语句，要求是单条查询语句(select为首单词)、包含向量操作符（<->/<=>/<#>/<+>/<~>/<%>）
- paramsList：查询参数，要求不为空
- searchParams：需要通过set设置的参数（如hnsw_ef_search、nprobes）
- maxThreads：连接池最大连接数，和数据库最大连接数（由参数max_connections设置）有关，一般来说，连接池最大连接数要小于数据库最大连接数，但是数据库对于管理员用户的连接限制会略超过max_connections设置。

#### 输出参数

- 查询结果，形式为`[[{id:1, embedding:'[1,2,3]'},{id:2, embedding:'[2,2,2]'}], [],...]`，表示n个查询向量对应的limit个结果。

#### 使用案例

```javascript
const { ParallelSearch } = require('pg')
async function run() {
  const dbConfig = {
    user: 'username',
    host: 'localhost',
    database: "dbname",
    password: "yourpassword",
    port: yourport
  };

  const sqlTemplate = 'SELECT id, embedding FROM test_table1 ORDER BY embedding <-> $1 LIMIT $2;';

  const searchParams = {
    hnsw_ef_search: 40,
    enable_seqscan: 'off'
  };
  const paramsList = [
    [JSON.stringify([5,5,5]), 3],
    [JSON.stringify([2,2,2]), 5]
  ];

  const queryManager = new ParallelSearch();
try {
    const results = await queryManager.executeMultiSearch(dbConfig, sqlTemplate, paramsList, searchParams,2);
    console.log(results)
    results.forEach((res, idx) => {
      console.log(`\n查询 ${idx + 1} 结果：`);
      if (res.success) {
        console.log('数据列表：', res.data);
      } else {
        console.log('错误：', res.error);
      }
    });
  } catch (err) {
    console.error('执行失败：', err.message);
  }
}

run().catch(console.error);
```

## 用例

```javascript
const client = connect('host', 5432, 'username', 'postgres', 'password');
create_table(client, 'test_table1', 3);
create_index(client, 'test_table1', 'idx_test1');
insert_vector(client, 'test_table1', '[1.2, 3, 5]', 0);
insert_vector(client, 'test_table1', '[4.3, 5.2, 1]', 1);
update_vector(client, 'test_table1', '[1, 3, 3]', 1);
query(client, 'test_table1', '[1, 2, 2]', 1);
delete_vector(client, 'test_table1', 0);
delete_table(client, 'test_table1');
close(client);
```

注：实例SDK代码中，函数均为async函数，实际使用中需要视情况增加await。

```javascript
const client = await connect('host', 5432, 'username', 'postgres', 'password');
await create_table(client, 'test_table1', 3);
...
await close(client);
```
