# Connecting Go SDK to a Vector Database

This document describes how to use the Go language to call the openGauss vector database.

## Environment Requirements

- Install Go 1.19 or later.

## Installing the SDK

Developers can run the following command to install the Go SDK from the [official repository](http://gitcode.com/opengauss/openGauss-connector-go-pq), and import the package into the project.

```
Install SDK
go get gitcode.com/opengauss/openGauss-connector-go-pq@master

Import package to the project
import (
 "database/sql"

 _ "gitcode.com/opengauss/openGauss-connector-go-pq"
)

```

>[!NOTE]Note <br>
>Currently, gitcode does not support `go get`. Please refer to the usage guide below for manual installation.
>
## Basic Operations
>
>[!NOTE]Note <br>
>The passwords and sslmode=disable used in all materials and test files (copy_test.go, encode_test_go, etc.) in this repository are for demonstration only. When using them, configure the correct password based on your actual situation and use a secure sslmode (the default value prefer).
>
### 1. Connect to the Database

```go
// connectInfo format:
// "host=127.0.0.1 port=5432 user=username password=userpassword dbname=userdbname sslmode=disable"
func CreateDBClient(connectInfo string) (*sql.DB, error) {
    return sql.Open("opengauss", connectInfo)
}
```

### 2. Create a Table

```go
func CreateTable(client *sql.DB, dim int) error {
    execSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS demotable(id INTEGER, content TEXT, embedding vector(%d))", dim)
    _, err := client.Exec(execSql)
    return err
}
```

### 3. Create an Index

```go
// Create an HNSW vector index using L2 distance.
func CreateIndex(client *sql.DB) error {
    execSql := fmt.Sprint("CREATE INDEX ON demotable USING hnsw (embedding vector_l2_ops)")
    _, err := client.Exec(execSql)
    return err
}
```

### 4. Insert/Delete/Update

- Insert

 ```go
// Insert a single record
type TableData struct {
    Id      int
    Content string
    Vector  string
}
func InsertDataSingle(client *sql.DB, data TableData) error {
    execSql := fmt.Sprintf("INSERT INTO demotable VALUES(%d, '%s', '%s')", data.Id, data.Content, data.Vector)
    _, err := client.Exec(execSql)
    return err
}
```

- Delete

```go
func DeleteData(client *sql.DB) error {
    execSql := fmt.Sprint("DELETE FROM demotable where id > 10")
    _, err := client.Exec(execSql)
    return err
}
```

- Update

```go
func UpdateData(client *sql.DB, vector string) error {
    execSql := fmt.Sprintf("UPDATE demotable set embedding = '%s' where id = 10", vector)
    _, err := client.Exec(execSql)
    return err
}
```

### 5. Query

```go
func SearchVectors(client *sql.DB, efsearch int, vector string, topK int) []string {
    var res []string
    // Set the query parameters.
    paramsql := fmt.Sprintf("set hnsw_ef_search = %d", efsearch);
    querysql := fmt.Sprintf("SELECT * FROM demotable ORDER BY embedding <-> '%s' LIMIT %d;", vector, topK);
    _, _ = client.Exec(paramsql)
    rows, _ := client.Query(querysql)
    for rows.Next() {
        var id int
        var content string
        var vector []byte
        _ = rows.Scan(&id, &content, &vector)
        embedding := string(vector)
        row := fmt.Sprintf("id: %d, content: %s, embedding: %s", id, content, embedding);
        res = append(res, row)
    }
    return res
}
```

### 6. Drop a Table

```go
func DropTable(client *sql.DB, tableName string) error {
    execSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
    _, err := client.Exec(execSql)
    return err
}
```

### 7. Multi-vector Concurrent Query

Multi-vector recall supports submitting multiple query vectors in a single search request. openGauss searches the query vectors in parallel and returns multiple sets of results.

#### Function Name

```java
func ExecuteMultiSearch(conninfo string, query string, args [][]interface{}, scanParams map[string]interface{}, threadCount int)
```

#### Input Parameters

- conninfo: Database connection configuration, including host, port, user, password, and dbname
- query: Query statement, which must be a single query statement (starting with select) and contain a vector operator (<->/<=>/<#>/<+>/<~>/<%>)
- args: Query parameters, which must not be empty
- scanParams: Parameters that need to be set via set (such as hnsw_ef_search and nprobes)
- threadCount: Maximum number of connections in the connection pool. It is related to the maximum number of database connections (set by the max_connections parameter). Generally, the maximum number of connections in the connection pool should be smaller than the maximum number of database connections, but the connection limit for administrator users in the database will slightly exceed the max_connections setting.

#### Output Parameters

- Query results, in the form of `[[map[id:1, embedding:'[1,2,3]'],map[id:2, embedding:'[2,2,2]']], [],...]`, representing the limit results corresponding to n query vectors.

#### Usage Example

```go
import (
    "gitcode.com/opengauss/openGauss-connector-go-pq"
)
conninfo := "host=localhost port=5432 user=test password=yourpassword dbname=testdb"
scanParams := map[string]interface{}{
    "hnsw_ef_search":"40",
    "enable_seqscan":"off"
}
query := "select id from demotable order by embedding <-> $1 limit $2;"
threadCount := 2
args := [][]interface{}{
    {"[1,2,3]", 2},
    {"[2,2,2]", 3},
}
res := pq.ExecuteMultiSearch(conninfo, query, args, scanParams, threadCount)
```

## Use Case Guide

- **Install openGauss-connector-go-pq**

```
# Create the deployment script.
cat << 'EOL' > setup_opengauss_go.sh
#!/bin/bash

# Set the project name and driver information.
PROJECT_NAME="opengauss-go"
MODULE_NAME="opengauss"
DRIVER_MODULE_PATH="gitcode.com/opengauss/openGauss-connector-go-pq"
DRIVER_REPO_URL="https://$DRIVER_MODULE_PATH.git"
DRIVER_VERSION="v1.0.7"

# Obtain GOPATH.
GOPATH=$(go env GOPATH)
DRIVER_LOCAL_PATH="$GOPATH/src/$DRIVER_MODULE_PATH"

# Create the project directory and initialize go mod.
echo "🚀 Initialize the Go project..."
mkdir -p "$PROJECT_NAME"
cd "$PROJECT_NAME" || exit 1
go mod init "$MODULE_NAME"

# Clone the openGauss driver to a local path.
echo "📦 Cloning the openGauss Go driver..."
mkdir -p "$DRIVER_LOCAL_PATH"
git clone "$DRIVER_REPO_URL" "$DRIVER_LOCAL_PATH"

# Modify go.mod to add require and replace.
echo "⚙️ Update the go.mod file..."
cat <<EOL2 >> go.mod

require $DRIVER_MODULE_PATH $DRIVER_VERSION

replace $DRIVER_MODULE_PATH => $DRIVER_LOCAL_PATH
EOL2

# Clean the module cache to ensure it takes effect
echo "🧹 Clean the module cache..."
go clean -modcache

# Download dependencies
echo "📥 Install dependency packages..."
go get "$DRIVER_MODULE_PATH"

echo "✅ Initialization and dependency installation complete!"
EOL

# Grant executable permission
chmod +x setup_opengauss_go.sh

# Execute the script to start the installation
./setup_opengauss_go.sh
```

- **Use the Go SDK to connect to openGauss and perform vector operations**

```
# Create main.go and fill in the following content.
vim main.go
package main
import (
    "fmt"
    "log"
    "database/sql"

    _ "gitcode.com/opengauss/openGauss-connector-go-pq"
)

/*
 * Copy the creation, deletion, and other functions above to this location
 */

func main(){
    connStr := "host=YourIP port=YourPort user=YourUserName password=YourPassWord dbname=YourDBName sslmode=disable"
    dbClient, err := CreateDBClient(connStr)
    if err != nil {
        log.Fatal(err)
    }
    
    err = DropTable(dbClient, "demotable")
    err = CreateTable(dbClient, 3)
    err = CreateIndex(dbClient)

    data := TableData{
        Id:      1,
        Content: "test",
        Vector:  "[1,2,3]",
    }
    err = InsertDataSingle(dbClient, data)

    data = TableData{
        Id:      11,
        Content: "test1",
        Vector:  "[3,4,5]",
    }
    err = InsertDataSingle(dbClient, data)

    data = TableData{
        Id:      10,
        Content: "test3",
        Vector:  "[2,2,2]",
    }
    err = InsertDataSingle(dbClient, data)

    err = UpdateData(dbClient, "[3,3,3]")

    err = DeleteData(dbClient)

    vectors := SearchVectors(dbClient, 1, "[3,2,4]", 5)
    fmt.Println(vectors)
    err = DropTable(dbClient, "demotable")
}
```

[More operation examples](https://gitcode.com/opengauss/openGauss-connector-go-pq)
