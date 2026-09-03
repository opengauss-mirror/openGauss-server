# Connecting to a Vector Database Using the Node.js SDK

This document describes how to use JavaScript to connect to the openGauss vector database.

## Environment Preparation

 - Download the source code [openGauss-connector-nodejs](https://gitcode.com/opengauss/openGauss-connector-nodejs)

```bash
npm install & npm run build
```

## Basic Operations

### 1. Connect to the Database

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

### 2. Create a Table

```javascript
const create_table = async (client, table_name, dim) => {
    const querystr = `
        CREATE TABLE IF NOT EXISTS public.${table_name} (id BIGINT PRIMARY KEY, embedding vector(${dim}));
    `;
    const result = await client.query(querystr);
}
```

### 3. Create an Index

```javascript
const create_index = async (client, table_name, index_name) => {
    const querystr = `
        CREATE INDEX IF NOT EXISTS ${index_name} ON public.${table_name} USING hnsw (embedding vector_l2_ops);
    `;
    const result = await client.query(querystr);
}
```

### 4. Insert/Delete/Update Data

- Insert

```javascript
const insert_vector = async (client, table_name, vector, id) => {
    const querystr = `
        INSERT INTO public.${table_name} (id, embedding) VALUES(${id}, '${vector}');
    `;
    const result = await client.query(querystr);
}
```

- Delete

```javascript
const delete_vector = async (client, table_name, id) => {
    const querystr = `
        DELETE FROM public.${table_name} WHERE id = ${id};
    `;
    const result = await client.query(querystr);
}
```

- Update

```javascript
const update_vector = async (client, table_name, vector, id) => {
    const querystr = `
        UPDATE public.${table_name} SET embedding = '${vector}' WHERE id = ${id};
    `;
    const result = await client.query(querystr);
}
```

### 5. Query

```javascript
const query = async (client, table_name, vector, topk) => {
    const querystr = `
        SELECT * FROM public.${table_name} ORDER BY embedding <-> '${vector}'::vector LIMIT ${topk}::int;
    `;
    const result = await client.query(querystr);
    return result;
}
```

### 6. Delete a Table

```javascript
const delete_table = async (client, table_name) => {
    const querystr = `
        DROP TABLE IF EXISTS public.${table_name};
    `;
    const result = await client.query(querystr);
}
```

### 7. Close Connection

```javascript
const close = async (client) => {
    await client.end();
}
```

### 8. Multi-vector Concurrent Query

Multi-vector recall supports submitting multiple query vectors in a single search request. openGauss searches the query vectors in parallel and returns multiple sets of results.

#### Function Name

```javascript
async executeMultiSearch(dbConfig, sqlTemplate, paramsList, searchParams, maxThreads)
```

#### Input Parameters

- dbConfig: Database connection configuration, including user, password, host, database, and port.
- sqlTemplate: Query statement. It must be a single query statement (starting with select) and contain a vector operator (<->/<=>/<#>/<+>/<~>/<%>).
- paramsList: Query parameters. It must not be empty.
- searchParams: Parameters that need to be set via set (such as hnsw_ef_search and nprobes).
- maxThreads: Maximum number of connections in the connection pool. It is related to the maximum number of database connections (set by the max_connections parameter). Generally, the maximum number of connections in the connection pool should be smaller than the maximum number of database connections. However, the connection limit for administrator users in the database slightly exceeds the max_connections setting.

#### Output Parameters

- Query results, in the form of `[[{id:1, embedding:'[1,2,3]'},{id:2, embedding:'[2,2,2]'}], [],...]`, representing the `limit` results corresponding to n query vectors.

#### Usage Example

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
      console.log(`\nQuery ${idx + 1} result:`);
      if (res.success) {
        console.log('Data list:', res.data);
      } else {
        console.log('Error:', res.error);
      }
    });
  } catch (err) {
    console.error('Execution failed:', err.message);
  }
}

run().catch(console.error);
```

## Use Case

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

Note: In the sample SDK code, all functions are async functions. In actual use, add await as needed.

```javascript
const client = await connect('host', 5432, 'username', 'postgres', 'password');
await create_table(client, 'test_table1', 3);
...
await close(client);
```
