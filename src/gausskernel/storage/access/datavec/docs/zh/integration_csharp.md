# C# SDK对接向量数据库

本文介绍如何使用C#语言调用openGauss向量数据库。

## 环境要求

- 使用`dotnet --version`查看是否安装.NET开发工具，若无，则需安装dotnet-sdk
- 安装相关库

   ```
    dotnet add package Pgvector
    dotnet add package Npgsql
   ```

## 基本操作

### 1.连接数据库

```C#
public async Task<NpgsqlConnection> Connect(string connStr)
{
    var dataSourceBuilder = new NpgsqlDataSourceBuilder(connStr);
    dataSourceBuilder.UseVector();
    await using var dataSource = dataSourceBuilder.Build();
    var conn = dataSource.OpenConnection();
    conn.ReloadTypes();
    return conn;
}
```

### 2.创建表

```C#
public async Task CreateTableAsync(NpgsqlConnection conn)
{
    const string create = "CREATE TABLE items (id serial PRIMARY KEY, embedding vector(3))";
    await using var cmd = new NpgsqlCommand(create, conn);
    await cmd.ExecuteNonQueryAsync();
}
```

### 3.创建索引

```C#
public async Task CreateIndexAsync(NpgsqlConnection conn)
{
    const string createIndex = "CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)";
    await using var cmd = new NpgsqlCommand(createIndex, conn);
    await cmd.ExecuteNonQueryAsync();
}
```

### 4.插入/删除/更新数据

- 批量插入

```C#
public async Task InsertDataAsync(NpgsqlConnection conn, Vector vector)
{
    const string insert = "INSERT INTO items (embedding) VALUES ($1)";
    await using var cmd = new NpgsqlCommand(insert, conn);
    cmd.Parameters.AddWithValue(vector);
    await cmd.ExecuteNonQueryAsync();
}
```

- 删除

```C#
public async Task DeleteDataAsync(NpgsqlConnection conn, int id)
{
    const string delete = "DELETE FROM items WHERE id = $1";
    await using var cmd = new NpgsqlCommand(delete, conn);
    cmd.Parameters.AddWithValue(id);
    await cmd.ExecuteNonQueryAsync();
}
```

- 更新

```C#
public async Task UpdateDataAsync(NpgsqlConnection conn, Vector vector, int id)
{
    const string update = "UPDATE items SET embedding = $1 WHERE id = $2";
    await using var cmd = new NpgsqlCommand(update, conn);
    cmd.Parameters.AddWithValue(vector).DataTypeName = "vector";
    cmd.Parameters.AddWithValue(id);
    await cmd.ExecuteNonQueryAsync();
}
```

### 5.查询

```C#
public async Task<System.Collections.Generic.List<(int, Vector)>> QueryAsync(NpgsqlConnection conn, Vector vector, int limit)
{
    const string query = "SELECT * FROM items ORDER BY embedding <-> $1 LIMIT $2"
    await using var cmd = new NpgsqlCommand(query, conn);
    cmd.Parameters.AddWithValue(vector).DataTypeName = "vector";
    cmd.Parameters.AddWithValue(limit);

    var results = new System.Collections.Generic.List<(int, Vector)>();
    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        var id = reader.GetInt32(0);
        var embedding = (Vector)reader.GetValue(1);
        results.Add((id, embedding));
    }
    return results;
}
```

### 6.删除表

```C#
public async Task DropTableAsync(NpgsqlConnection conn)
{
    const string drop = "DROP TABLE IF EXISTS items";
    await using var cmd = new NpgsqlCommand(drop, conn);
    await cmd.ExecuteNonQueryAsync();
}
```

### 7.关闭连接

```C#
public async Task CloseConnectionAsync(NpgsqlConnection conn)
{
    if(conn != null)
    {
        await conn.CloseAsync();
        await conn.DisposeAsync();
    }
}
```

## 用例

```C#
using System;
using Pgvector;
using Npgsql;
namespace Demo
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var connstr = "Host=localhost;Database=Yourdb;Port=Yourport;Username=Yourname;Password=YourPassword";
            var dbHandler = new Program();
            var conn = await dbHandler.Connect(connstr);

            await dbHandler.CreateTableAsync(conn);

            await dbHandler.CreateIndexAsync(conn);

            var vectorq = new Vector(new float[] {1, 1, 1});
            await dbHandler.InsertDataAsync(conn, vectorq);

            var results = await dbHandler.QueryAsync(conn, vectorq, 10);

            await dbHandler.CloseConnectionAsync(conn);
        }
    }
}

```
