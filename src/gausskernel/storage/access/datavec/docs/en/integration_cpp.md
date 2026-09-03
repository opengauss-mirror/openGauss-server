# Connecting to the Vector Database Using the C++ SDK

This document describes how to use the C++ language to call the openGauss vector database.

## Environment Preparation

- g++
- libpq library
For details, see [Development Process Based on libpq](../developer_guide/development_process_libpq.md).

## Basic Operations

### 1. Connect to the Database

```cpp
#include <iostream>
#include <vector>
#include <string>
#include <libpq-fe.h>
#include <sstream>
#include <stdexcept>

class OpenGaussManager {
private:
    PGconn* conn;

    // Escape identifiers (table names, column names, etc.)
    std::string escape_identifier(const std::string& identifier) {
        char* escaped = PQescapeIdentifier(conn, identifier.c_str(), identifier.size());
        if (!escaped) throw std::runtime_error(PQerrorMessage(conn));
        std::string result(escaped);
        PQfreemem(escaped);
        return result;
    }

    // Execute the SQL and check the result.
    void execute_sql(const std::string& sql) {
        PGresult* res = PQexec(conn, sql.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::string err = PQerrorMessage(conn);
            PQclear(res);
            throw std::runtime_error("SQL error: " + err);
        }
        PQclear(res);
    }

    // Convert the vector to PostgreSQL array format.
    std::string vector_to_string(const std::vector<float>& vec) {
        std::ostringstream oss;
        oss << "'[";
        for (size_t i = 0; i < vec.size(); ++i) {
            if (i > 0) oss << ",";
            oss << vec[i];
        }
        oss << "]'";
        return oss.str();
    }

public:
    // Constructor (only initializes the connection pointer)
    OpenGaussManager(const std::string& conninfo){
        conn = PQconnectdb(conninfo.c_str());

        if (PQstatus(conn) != CONNECTION_OK) {
            std::cerr << "Connection failed: " << PQerrorMessage(conn) << std::endl;
            PQfinish(conn);
            conn = nullptr;
        }
    }

    // Destructor (ensures the connection is released)
    ~OpenGaussManager() {
        disconnectDB();
    }
    //Other methods
};
```

### 2. Create a Table

```cpp
void create_table(const std::string& table_name, int vector_dim) {
    std::string sql = 
        "CREATE TABLE IF NOT EXISTS public." + escape_identifier(table_name) + 
        " (id BIGINT PRIMARY KEY, " +
        "embedding vector(" + std::to_string(vector_dim) + "))";
    
    execute_sql(sql);
}
```

### 3. Create an Index

```cpp
void create_index(const std::string& table_name) {
    std::string sql = 
        "CREATE INDEX ON " + escape_identifier(table_name) + 
        "USING hnsw(embedding vector_l2_ops);";
    execute_sql(sql);
}
```

### 4. Insert/Delete/Update Data

- Insert

```cpp
void insert(const std::string& table_name, int id, const std::vector<float>& embedding) {
    std::string sql = 
        "INSERT INTO public." + escape_identifier(table_name) + 
        " (id, embedding) VALUES (" +
        std::to_string(id) + ", " +
        vector_to_string(embedding) + ")";
    
    execute_sql(sql);
}
```

- Delete

```cpp
int delete_by_id(const std::string& table_name, int id) {
    std::string sql = 
        "DELETE FROM public." + escape_identifier(table_name) + 
        " WHERE id = " + std::to_string(id) +
        " RETURNING id";
    
    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        throw std::runtime_error("Delete failed: " + std::string(PQerrorMessage(conn)));
    }
    int deleted_count = PQntuples(res);
    PQclear(res);
    return deleted_count;
}
```

- Update

```cpp
void update(const std::string& table_name, 
                    int id, 
                    const std::vector<float>& embedding) {
    std::string sql = 
        "UPDATE public." + escape_identifier(table_name) + 
        " SET embedding = " + vector_to_string(embedding) + 
        " WHERE id = " + std::to_string(id);
    execute_sql(sql);
}
```

### 5. Query

```cpp
std::vector<std::pair<int, std::vector<float>>> select(
        const std::string& table_name,
        const std::vector<float>& query_vec,
        int topk = 10
    ) {
    std::vector<std::pair<int, std::vector<float>>> results;
    
    std::string sql = 
        "SELECT id, embedding FROM public." + escape_identifier(table_name) +
        " ORDER BY embedding <-> " + vector_to_string(query_vec) +
        " LIMIT " + std::to_string(topk);
    
    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        throw std::runtime_error("Query failed: " + std::string(PQerrorMessage(conn)));
    }
    // Parse the result.
    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        // Parse the ID.
        int id = std::stoi(PQgetvalue(res, i, 0));
        
        // Parse the vector.
        std::vector<float> vec;
        std::string vec_str = PQgetvalue(res, i, 1);
        std::istringstream iss(vec_str.substr(1, vec_str.size() -2)); 
        std::string val;
        while (std::getline(iss, val, ',')) {
            vec.push_back(std::stof(val));
        }
        
        results.emplace_back(id, vec);
    }
    
    PQclear(res);
    return results;
}
```

### 6. Drop Table

```cpp
void drop_table(const std::string& table_name, bool cascade = false) {
    std::string sql = 
        "DROP TABLE IF EXISTS public." + escape_identifier(table_name) +
        (cascade ? " CASCADE" : "");
    
    execute_sql(sql);
    execute_sql("COMMIT");
}
```

### 7. Close the Connection

```cpp
void disconnectDB() {
    if (conn != nullptr) {
        PQfinish(conn);
        conn = nullptr;
    }
}
```

### 8. Multi-Vector Concurrent Query

Multi-vector recall supports submitting multiple query vectors in a single search request. openGauss searches the query vectors in parallel and returns multiple sets of results.

#### Function Name

```cpp
PGresult **PQexecMultiSearchParams(const char *connParams, const char *queryTemplate, const QueryParams *queryParams, const int queryCount, const char *preExecForConn, int threadCount)
```

#### Input Parameters

- connParamsi: Database connection configuration, including host, dbname, user, password, and port
- queryTemplate: Query statement, which must be a single query statement (starting with select) and contain a vector operator (<->/<=>/<#>/<+>/<~>/<%>)
- queryParams: Query parameters, which must not be empty
- queryCount: Number of query requests
- preExecForConn: SQL statement for setting connection parameters, for example: "set hnsw_ef_search=200;"
- threadCount: Maximum number of connections in the connection pool, which is related to the maximum number of database connections (set by the max_connections parameter). Generally, the maximum number of connections in the connection pool should be smaller than the maximum number of database connections, but the database connection limit for administrator users may slightly exceed the max_connections setting.

#### Output Parameters

- Query results, an array of PGresult* type, in the form of [[[id:1, vector:[1,2,3]], [id:2 vector:[4,5,6]],...], [[id:3, vector:[1,2,2]], [id:2 vector:[4,5,6]],...], ...], representing the limit results corresponding to n query vectors. Refer to the example for parsing.

#### Usage Example

```cpp
#include <iostream>
#include <vector>
#include <string>
#include <libpq-fe.h>
#include <sstream>
#include <stdexcept>

const char *get_example_vector(int index) {
    static const char *vectors[] = {
        "[0.12, 0.34, 0.56]",
        "[4, 5, 6]"
    };
    return vectors[index % 2];
}

int main()
{
     const char *conn_params = "host=127.0.0.1 dbname=postgres user=test password=yourpassword port=5432";
     const int num_connections = 2;
     int success_count = 0;
     const char *query_template = "select id, embedding from vectors order by embedding <-> $1 limit 2;";
     const char *preExec = "set enable_seqscan=true;";

     int num_vectors = 2;
     QueryParams query_params[num_vectors];
     const char *param_values[num_vectors][1];
     for (int i = 0; i < num_vectors; i++) {
         param_values[i][0] = get_example_vector(i);

         query_params[i].paramCount = 1;
         query_params[i].paramValues = param_values[i];
         query_params[i].paramLengths = NULL;
         query_params[i].paramFormats = NULL;
         query_params[i].resultFormat = 0;
     }

     PGresult **results = PQexecMultiSearchParams(conn_params, query_template, query_params, num_vectors, preExec, num_connections);
     if (!results) {
         printf("search error!\n");
     }
     for (int i = 0; i < num_vectors; i++) {
         PGresult *res = results[i];
         if (!res) {
            std::cout << "result is invalid, query id:" << i << std::endl;
            continue;
         }
         ExecStatusType status = PQresultStatus(res);
         int rows = PQntuples(res);
         int cols = PQnfields(res);
         std::cout << "search query id:" << i << ", rows:" << rows << ", cols:" << cols << ", status:" << status <<
            ", errMsg:" << PQresultErrorMessage(res) << std::endl;
         for (int j = 0; j < rows; ++j) {
             int id = std::stoi(PQgetvalue(res, j, 0));
             std::cout << "id:" << id << std::endl;

             std::vector<float> vec;
             std::string vec_str = PQgetvalue(res, j, 1);
             std::istringstream iss(vec_str.substr(1, vec_str.size() - 2));
             std::string val;
             std::cout << "vector: [";
             while (std::getline(iss, val, ',')) {
                 vec.push_back(std::stof(val));
                 std::cout << val << ",";
             }
             std::cout << "]" << std::endl;
         }
     }
     PQclearMultiResults(results, num_vectors);
     return 0;
}
```

## Use Case

```cpp
int main() {
    try {
        // 1. Connect to the database.
        OpenGaussManager db("host=127.0.0.1 dbname=vector_db user=admin password=xxxxxx port=5432");

        // 2. Create a table (dimension 3).
        db.create_table("image_vectors", 3);
        std::cout << "Table created successfully." << std::endl;

        // 3. Insert test data.
        std::vector<int> ids = {1};
        std::vector<std::vector<float>> embeddings = {
            {0.1f, 0.2f, 0.128f},
        };
        db.insert("image_vectors", ids[0], embeddings[0]);
        std::cout << "Inserted " << ids.size() << " vectors." << std::endl;

        // 4. Query similar vectors.
        std::vector<float> query_vec = {0.12f, 0.22f, 0.128f};
        auto results = db.select("image_vectors", query_vec, 3);
        std::cout << "Top " << results.size() << " similar vectors:" << std::endl;
        for (const auto& [id, vec] : results) {
            std::cout << "ID: " << id << " Vector: [";
            for (size_t i = 0; i < std::min(5UL, vec.size()); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << vec[i];
            }
            std::cout << ", ...]" << std::endl;
        }

        // 5. Update the vector.
        std::vector<float> new_vec = {0.15f, 0.25f, 0.128f};
        db.update("image_vectors", 1, new_vec);
        std::cout << "Updated vector with ID=1" << std::endl;

        // 6. Delete data.
        int deleted = db.delete_by_id("image_vectors", 1);
        std::cout << "Deleted " << deleted << " records" << std::endl;

        // 7. Drop the table.
        db.drop_table("image_vectors");
        std::cout << "Table dropped successfully." << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
```

Compile and run:

```cpp
g++ -o test test.cpp -I /<YourPath>/include/ -L /<YourPath>/lib/ -lpq
./test
```
