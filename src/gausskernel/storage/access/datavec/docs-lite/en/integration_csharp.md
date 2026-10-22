# Connecting to a Vector Database Using the C# SDK

This document describes how to use the C# language to access the openGauss vector database.

## Environment Requirements

- Run `dotnet --version` to check whether the .NET development tools are installed. If not, install dotnet-sdk.
- Install the required libraries.

   ```
    dotnet add package Pgvector
    dotnet add package Npgsql
   ```

## Basic Operations

### 1. Connect to the Database

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

### 2. Create a Table

```C#
public async Task CreateTableAsync(NpgsqlConnection conn)
{
    const string create = "CREATE TABLE items (id serial PRIMARY KEY, embedding vector(3))";
    await using var cmd = new NpgsqlCommand(create, conn);
    await cmd.ExecuteNonQueryAsync();
}
```

### 3. Create an Index

```C#
public async Task CreateIndexAsync(NpgsqlConnection conn)
{
    const string createIndex = "CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)";
    await using var cmd = new NpgsqlCommand(createIndex, conn);
    await cmd.ExecuteNonQueryAsync();
}
```

### 4. Insert/Delete/Update Data

- Batch insert

```C#
public async Task InsertDataAsync(NpgsqlConnection conn, Vector vector)
{
    const string insert = "INSERT INTO items (embedding) VALUES ($1)";
    await using var cmd = new NpgsqlCommand(insert, conn);
    cmd.Parameters.AddWithValue(vector);
    await cmd.ExecuteNonQueryAsync();
}
```

- Delete

```C#
public async Task DeleteDataAsync(NpgsqlConnection conn, int id)
{
    const string delete = "DELETE FROM items WHERE id = $1";
    await using var cmd = new NpgsqlCommand(delete, conn);
    cmd.Parameters.AddWithValue(id);
    await cmd.ExecuteNonQueryAsync();
}
```

- Update

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

### 5. Query

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

### 6. Drop Table

```C#
public async Task DropTableAsync(NpgsqlConnection conn)
{
    const string drop = "DROP TABLE IF EXISTS items";
    await using var cmd = new NpgsqlCommand(drop, conn);
    await cmd.ExecuteNonQueryAsync();
}
```

### 7. Close Connection

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

## Use Case

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
