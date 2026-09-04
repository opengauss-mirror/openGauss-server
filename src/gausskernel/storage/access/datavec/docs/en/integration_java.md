# Connecting to a Vector Database Using the Java SDK

This document describes how to use the Java language to access the openGauss vector database.

## Requirements

- Install Java 1.8 or later
- Apache Maven

## Installing the SDK

- Online installation

    Developers can directly obtain the jar package from the Maven Central Repository [Maven Central Repository download](https://central.sonatype.com/artifact/org.opengauss/opengauss-jdbc), or download it from the openGauss official website [Community website download](https://opengauss.org/en/download/), and run the following command to install the Java SDK

    ```xml
    <dependency>
        <groupId>org.opengauss</groupId>
        <artifactId>opengauss-jdbc</artifactId>
        <version>your version</version>
    </dependency>
    ```

- Offline installation

    1) You can download the openGauss-connector-jdbc source code from the open source community

    ```bash
    git clone https://gitcode.com/opengauss/openGauss-connector-jdbc.git
    ```

    Switch to the code directory and run `sh build.sh`. After successful compilation, two jar packages will be generated, namely opengauss-jdbc-${version}.jar and postgresql.jar (in the output directory)

    2) You can directly obtain the relevant JDBC compressed package
    Download link: `https://download-opengauss.osinfra.cn/archive_test/7.0.0-RC2/openGauss7.0.0-RC2.B019/openEuler20.03/arm/openGauss-JDBC-7.0.0-RC2.tar.gz`. You can select the corresponding compressed package based on the operating system version and server architecture. After decompressing the package, you will get opengauss-jdbc-${version}.jar and postgresql.jar.

## Basic Operations

### 1. Connect to the Database

```java
public Connection getConnection(String username, String passwd)
{
    String driver = "org.opengauss.Driver";
    String sourceURL = "jdbc:opengauss://localhost:port/database_name";
    Connection conn = null;
    
    try {
        Class.forName(driver).getDeclaredConstructor().newInstance();
    } catch(Exception e) {
        e.printStackTrace();
        return null;
    }
    try {
        conn = DriverManager.getConnection(sourceURL, username, passwd);
        System.out.println("Connection succeed!");
    } catch(Exception e) {
        e.printStackTrace();
        return null;
    }
    return conn;
}
```

### 2. Create a Table

```java
// Execute a common SQL statement.
public void ExecuteSQL(Connection conn, String sql)
{
    Statement stmt = null;
    try {
        stmt = conn.createStatement();
        int rc = stmt.executeUpdate(sql);
        stmt.close();
    } catch (SQLException e) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
        e.printStackTrace();
    }
}

public void CreateTable(Connection conn, int dim)
{
    String sql = String.format("CREATE TABLE IF NOT EXISTS demotable(id INTEGER, content TEXT, embedding vector(%d));", dim);
    ExecuteSQL(conn, sql);
}
```

### 3. Create an Index

```java
// Create an HNSW vector index using L2 distance.
public void CreateIndex(Connection conn)
{
    String sql = String.format("CREATE INDEX ON demotable USING hnsw (embedding vector_l2_ops);");
    ExecuteSQL(conn, sql);
}
```

### 4. Insert/Delete/Update

- Insert

 ```java
public void InsertDataSingle(Connection conn, int id, String content, String vector)
{
    String sql = String.format("INSERT INTO demotable VALUES(%d, '%s', '%s');", id, content, vector);
    ExecuteSQL(conn, sql);
}
```

- Delete

```java
public void DeleteData(Connection conn)
{
    String sql = String.format("DELETE FROM demotable where id > 10;");
    ExecuteSQL(conn, sql);
}
```

- Update

```java
public void UpdateData(Connection conn, String vector)
{
    String sql = String.format("UPDATE demotable set embedding = '%s' where id = 10;", vector);
    ExecuteSQL(conn, sql);
}
```

### 5. Query

```java
public String findNearestVectors(Connection conn, int efsearch, String vector, int topK)
{
    Statement statement = null;
    ResultSet resultSet = null;
    String res = "";
    // Set the query parameters.
    String paramsql = String.format("set hnsw_ef_search = %d;", efsearch);
    ExecuteSQL(conn, paramsql);
    String querysql = String.format("SELECT * FROM demotable ORDER BY embedding <-> '%s' LIMIT %d;", vector, topK);
    try {
        statement = conn.createStatement();
        resultSet = statement.executeQuery(querysql);
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String content = resultSet.getString("content");
            Object embed = resultSet.getObject("embedding");
            // Replace with the result you want.
            res += "id: " + id + ", content: " + content + ",embedding: " + embed + "\n";
        }
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        try { if (resultSet != null) resultSet.close(); } catch(Exception e) {}
        try { if (statement != null) statement.close(); } catch(Exception e) {}
    }
    return res;
}
```

### 6. Multi-vector Concurrent Query

Multi-vector recall supports submitting multiple query vectors in a single search request. openGauss searches the query vectors in parallel and returns multiple sets of results.

#### Function name

```java
public List<List<Map<String, Object>>> executeMultiSearch(Map<String, String> dbConfig,
        String sqlTemplate, List<List<Object>> parameters, Map<String, Object> scanParams, int threadCount)
```

#### Input Parameters

- dbConfig: Database connection configuration, including jdbcUrl, user, and password.
- sqlTemplate: Query statement, which must be a single query statement (starting with select) and contain a vector operator (<->/<=>/<#>/<+>/<~>/<%>).
- parameters: Query parameters, which must not be empty.
- scanParams: Parameters that need to be set via set (such as hnsw_ef_search and nprobes).
- threadCount: Maximum number of connections in the connection pool, which is related to the maximum number of database connections (set by the max_connections parameter). Since the HikariCP connection pool used by Java does not pre-establish database connections during the creation phase, generally speaking, the maximum number of connections executing requests at the same time should be less than the maximum number of database connections. However, the database's connection limit for administrator users may slightly exceed the max_connections setting.

#### Output Parameters

- Query results, in the form of `[[{id=1, embedding='[1,2,3]'},{id=2, embedding='[2,2,2]'}], [],...]`, representing the limit results corresponding to n query vectors.

#### Usage Example

```java
import java.util.*;
import org.opengauss.util.ParallelSearch

String jdbcUrl = "jdbc:opengauss://localhost:port/dbname?allowMultiQueries=true";
Map<String, String> dbConfig = new HashMap<>();
dbConfig.put("jdbcUrl", jdbcUrl);
dbConfig.put("username", "YourName");
dbConfig.put("auth", "YourPassword");

String sqlTemplate = "select id from demotable order by embedding <-> '?'::vector limit ?;";
int threadCount = 2;
Map<String, Object> scanParams = new HashMap<>();
scanParams.put("enable_seqscan", "off");
scanParams.put("hnsw_ef_search", 40);

List<List<Object>> parameters = new ArrayList<>();
parameters.add(new ArrayList<>(Arrays.asList(Arrays.toString(new int[]{1, 2, 3})), 1));
parameters.add(new ArrayList<>(Arrays.asList(Arrays.toString(new int[]{2, 2, 3})), 2));

ParallelSearch ps = new ParallelSearch();
List<List<Map<String, Object>>> res = ps.executeMultiSearch(dbConfig, sqlTemplate, parameters,  scanParams, threadCount);
```

## Use Case

Compilation and run commands:

```bash
javac -cp ./openGauss-connector-jdbc/output/opengauss-jdbc-7.0.0-RC2.jar <yourfilename>.java
java -cp ./openGauss-connector-jdbc/output/opengauss-jdbc-7.0.0-RC2.jar:. <yourfilename>
```

```java
public static void main(String[] args) {
        String username = "test2";     // Replace with your username
        String password = "YourPassword"; // Replace with your password
        int embeddingDim = 3;

        Connection conn = getConnection(username, password);
        if (conn != null) {
            CreateTable(conn, embeddingDim);
            CreateIndex(conn);
            InsertDataSingle(conn, 0, "test", "[1,2,3]");
            DeleteData(conn);
            UpdateData(conn, "[1,1,1]");
            String res=findNearestVectors(conn, 20, "[2,2,2]", 2);
            System.out.println("Connection successful!"+res);
            try {
                conn.close();
                System.out.println("Connection closed.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
```

[More operation examples](https://gitcode.com/opengauss/openGauss-connector-jdbc)
[Common Java examples](https://docs.opengauss.org/en/docs/latest/getting_started/java.html)
